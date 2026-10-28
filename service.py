import asyncio, json, logging, math, os, random, statistics, time
import aiohttp
from aiohttp import web
import rustworkx as rx

# ===================================================================== 1. TOPOLOGIES
# nodes: name -> (workers, mean processing seconds)      edges: (caller, callee, call probability)
TOPOS = {
    # a chain with a deep slow service F, three hops below the root, plus a healthy side branch
    "deep": dict(
        nodes={"A": (8, .005), "B": (8, .010), "C": (8, .010), "D": (8, .010), "E": (8, .010), "F": (2, .040)},
        edges=[("A", "B", .6), ("A", "C", .4), ("B", "D", 1.), ("D", "F", 1.), ("C", "E", 1.)],
        root="A", base=20, spike=120),
    # wide fan-in: four front services all call one slow shared service H
    "hub": dict(
        nodes={"A": (8, .005), "B1": (8, .008), "B2": (8, .008), "B3": (8, .008), "B4": (8, .008), "H": (2, .040)},
        edges=[("A", "B1", .25), ("A", "B2", .25), ("A", "B3", .25), ("A", "B4", .25),
               ("B1", "H", 1.), ("B2", "H", 1.), ("B3", "H", 1.), ("B4", "H", 1.)],
        root="A", base=20, spike=100),
}

# ===================================================================== 2. TUNABLES
SLO = 0.5               # a request slower than this counts as failed (seconds)
QMAX = 10               # local safety net: reject if this many requests already wait
PERIOD = 0.25           # control loop period (seconds)
TARGET_WAIT = 0.02      # local controller target queueing delay (seconds)
SIG_EXPIRY = 1.0        # a signal not refreshed for this long is forgotten
REQ_MEM = 256 * 1024    # bytes held per in-flight request (makes memory pressure real)
MEM_BUDGET = 6 * 1024 * 1024   # "memory limit" per service
# --- depth policy knobs
W = dict(rate=0.35, drain=0.35, cpu=0.15, mem=0.15)   # weights of the four pressures (sum = 1)
RATE_SAT = 0.5          # arrival/discharge ratio 1.5 counts as full rate-pressure
DRAIN_REF = SLO / 2     # queue that needs this long to drain counts as full drain-pressure
MSG_COST = 0.15         # price of notifying ONE service; raise it => shorter signals
X_MIN = 1               # an overloaded service always tells at least its direct callers


def clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


# ===================================================================== 3. TOPOLOGY ANALYSIS (rustworkx)
def analyze_topology(topo):
    """For every service t return what the depth policy needs to know about the graph ABOVE it:
         fan_in / fan_out : direct callers / direct callees (branching factors)
         hops[h-1] = (n_h, reach_h)
             n_h     = number of services exactly h hops upstream of t     (upstream branching)
             reach_h = average probability that a request entering such a service ends up at t
                       (uses call probabilities, i.e. downstream branching of the ancestors)
    """
    g = rx.PyDiGraph()
    idx = {n: g.add_node(n) for n in topo["nodes"]}
    for u, v, q in topo["edges"]:
        g.add_edge(idx[u], idx[v], q)                 # edge payload = call probability
    rev = g.copy(); rev.reverse()                     # reversed graph => "upstream" distances
    order = list(rx.topological_sort(g))
    info = {}
    for t, ti in idx.items():
        # reach[a] = P(request at a eventually calls t); children come later in topological order
        reach = {ti: 1.0}
        for a in reversed(order):
            if a == ti: continue
            miss = 1.0
            for _, c, q in g.out_edges(a):
                miss *= 1.0 - q * reach.get(c, 0.0)
            reach[a] = 1.0 - miss
        dist = rx.digraph_dijkstra_shortest_path_lengths(rev, ti, lambda e: 1.0)   # hops upstream
        hops = []
        for h in range(1, int(max(dist.values(), default=0)) + 1):
            ring = [a for a, d in dist.items() if int(d) == h]
            hops.append((len(ring), statistics.fmean(reach[a] for a in ring)))
        info[t] = dict(fan_in=g.in_degree(ti), fan_out=g.out_degree(ti), hops=hops)
    return info


def choose_x(need, hops):
    """Adaptive depth. Walk upstream hop by hop; stay on while the benefit of telling that ring of
    services (need * reach) is at least its price (number of services * MSG_COST)."""
    x = 0
    for h, (n_h, reach_h) in enumerate(hops, start=1):
        if need * reach_h >= MSG_COST * n_h:
            x = h
        else:
            break
    return min(max(x, X_MIN), max(len(hops), X_MIN))


# ===================================================================== 4. ONE MICROSERVICE
class Service:
    def __init__(self, name, topo, mode, info, session):
        workers, self.mean_s = topo["nodes"][name]
        self.name, self.mode, self.info, self.session = name, mode, info[name], session
        self.workers = workers
        self.children = [(c, f"http://service-{c.lower()}:8000/", q) for u, c, q in topo["edges"] if u == name]
        self.q_to = {c: q for c, _, q in self.children}       # my call probability per child
        self.signalling = mode not in ("none", "local")
        self.fixed_x = int(mode.split(":")[1]) if mode.startswith("fixed:") else None
        self.sem = asyncio.Semaphore(workers)
        # live state
        self.p, self.x, self.need = 1.0, 0, 0.0               # admit prob., signal depth, pressure
        self.waiting = self.in_use = self.inflight = 0
        self.waits = []                                        # queueing delays in this window
        self.arrivals = self.completions = 0                   # counters of this window
        self.mu = self.busy = None                             # smoothed discharge rate / utilisation
        self.sigs = {}        # (via_child, origin) -> (p, reach, x, path, time_received)
        # statistics for the report
        self.s = dict(sig_bytes=0, sig_msgs=0, x_sum=0, x_n=0, need_sum=0.0, rejected=0, total=0,
                      peak_inflight=0, paths={})

    # ---------------- signals: receive, store, forward -----------------
    def fresh(self):
        now = time.monotonic()
        for k in [k for k, v in self.sigs.items() if now - v[4] > SIG_EXPIRY]:
            del self.sigs[k]
        return self.sigs

    def ingest(self, header, via):
        """Signals that came back on the response of child `via`."""
        if not header or not self.signalling: return
        for item in header.split(";"):
            try:
                origin, p, x, reach, path = item.split("|")
            except ValueError:
                continue
            path = path.split(">")
            if self.name in path: continue                     # loop guard
            self.sigs[(via, origin)] = (float(p), float(reach), int(x), path, time.monotonic())
            key = f"{origin} via {'>'.join(path)}"             # what path details taught us
            self.s["paths"][key] = self.s["paths"].get(key, 0) + 1

    def outgoing(self):
        """Header to put on responses to MY callers: my own signal + signals I must forward."""
        if not self.signalling or self.info["fan_in"] == 0:    # no callers (the root) => nobody to tell
            return ""
        items = []
        if self.x > 0:                                         # I am a bottleneck: start a signal
            items.append(f"{self.name}|{self.p:.2f}|{self.x}|1.00|{self.name}")
        for (via, origin), (p, reach, x, path, _) in self.fresh().items():
            if len(path) < x:                                  # still has hops left
                r = reach * self.q_to[via]                     # one hop further up: reach shrinks
                items.append(f"{origin}|{p:.2f}|{x}|{r:.2f}|{'>'.join(path + [self.name])}")
        text = ";".join(items)
        self.s["sig_bytes"] += len(text); self.s["sig_msgs"] += len(items)
        return text

    # ---------------- admission (path-aware early rejection) -----------------
    def admit(self):
        """Decide which children this request will call, then admit/reject it BEFORE using a worker."""
        plan = [(c, url) for c, url, q in self.children if random.random() < q]
        if self.mode == "none":
            return True, plan
        f = self.p                                             # my own AIMD admission probability
        if self.signalling:
            sigs = self.fresh()
            for c, _ in plan:                                  # only signals from children I WILL call
                for (via, _), (p, reach, _, _, _) in sigs.items():
                    if via == c:
                        f *= 1.0 - reach * (1.0 - p)           # shed share = reach * (1 - p)
        return (random.random() <= f and self.waiting < QMAX), plan

    def reply(self, status, work, dropped_at=""):
        return web.Response(status=status, text="ok", headers={
            "X-Sig": self.outgoing(), "X-Work": f"{work:.5f}", "X-Drop": dropped_at})

    # ---------------- the HTTP handler: queue -> process (sleep) -> call children -----------------
    async def handle(self, request):
        self.arrivals += 1; self.s["total"] += 1
        ok, plan = self.admit()
        if not ok:
            self.s["rejected"] += 1
            return self.reply(503, 0.0, self.name)
        self.inflight += 1
        self.s["peak_inflight"] = max(self.s["peak_inflight"], self.inflight)
        buf = bytearray(b"\x01") * REQ_MEM                     # memory held while the request is here
        t = time.monotonic(); self.waiting += 1
        await self.sem.acquire(); self.waiting -= 1; self.in_use += 1
        try:
            self.waits.append(time.monotonic() - t)
            st = random.expovariate(1 / self.mean_s)
            await asyncio.sleep(st)                            # <- simulated processing time
            work = st
            for c, url in plan:                                # synchronous calls; worker stays held
                try:
                    async with self.session.get(url) as r:
                        await r.read()
                        self.ingest(r.headers.get("X-Sig"), via=c)
                        work += float(r.headers.get("X-Work", 0))
                        if r.status != 200:
                            return self.reply(503, work, r.headers.get("X-Drop", ""))
                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
                    return self.reply(503, work, self.name)
            return self.reply(200, work)
        finally:
            self.in_use -= 1; self.sem.release(); self.inflight -= 1; self.completions += 1
            del buf

    # ---------------- control loop: local AIMD + pressures + choice of x -----------------
    async def controller(self):
        last = time.monotonic()
        while True:
            await asyncio.sleep(PERIOD)
            now = time.monotonic(); dt = now - last; last = now
            lam, done = self.arrivals / dt, self.completions / dt   # request rate, discharge rate
            self.arrivals = self.completions = 0
            self.mu = done if self.mu is None else 0.7 * self.mu + 0.3 * done
            util = self.in_use / self.workers
            self.busy = util if self.busy is None else 0.7 * self.busy + 0.3 * util

            # (a) local admission control: AIMD on queueing delay
            w = statistics.fmean(self.waits) if self.waits else 0.0
            self.waits.clear()
            self.p = max(0.05, self.p * 0.85) if w > TARGET_WAIT else min(1.0, self.p + 0.03)

            # (b) the four live pressures, each in [0,1]
            rho = lam / max(self.mu, 1.0) if lam > 5 else 0.0     # offered load / discharge rate
            pressure = dict(
                rate=clamp((rho - 1.0) / RATE_SAT),
                drain=clamp((self.waiting / max(self.mu, 1.0)) / DRAIN_REF),   # queue / discharge = drain time
                cpu=clamp((self.busy - 0.5) / 0.5),
                mem=clamp(self.inflight * REQ_MEM / MEM_BUDGET))
            self.need = sum(W[k] * pressure[k] for k in W)

            # (c) signal depth: only a shedding service signals
            overloaded = self.p < 0.95 and self.signalling
            if not overloaded:
                self.x = 0
            elif self.fixed_x is not None:
                self.x = self.fixed_x
            else:
                self.x = choose_x(self.need, self.info["hops"])
            if self.x > 0:
                self.s["x_sum"] += self.x; self.s["x_n"] += 1; self.s["need_sum"] += self.need

    async def stats(self, request):
        return web.json_response(dict(self.s, cpu_s=time.process_time(), name=self.name,
            p=self.p, x=self.x, need=self.need, waiting=self.waiting,
            in_use=self.in_use, inflight=self.inflight, mode=self.mode))



async def main():
    name = os.environ.get("SERVICE_NAME", "A")
    topo_name = os.environ.get("TOPO", "deep")
    mode = os.environ.get("MODE", "variable")
    seed = int(os.environ.get("SEED", "0"))
    topo = TOPOS[topo_name]
    if name not in topo["nodes"]:
        raise ValueError(f"{name} is not a service in {topo_name}")
    random.seed(seed * 1000 + sum(ord(c) for c in name))
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0), timeout=timeout) as session:
        svc = Service(name, topo, mode, analyze_topology(topo), session)
        app = web.Application()
        app.router.add_get("/", svc.handle)
        app.router.add_get("/stats", svc.stats)
        app.router.add_get("/health", lambda request: web.json_response({"ready": True, "service": name}))
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", 8000).start()
        print(f"Ready: {name} ({topo_name}, {mode})", flush=True)
        task = asyncio.create_task(svc.controller()) if mode != "none" else None
        try:
            await asyncio.Event().wait()
        finally:
            if task: task.cancel()
            await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
