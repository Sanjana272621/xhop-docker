"""One experiment = one fixed schedule (calm -> spike -> recovery) against the root service, with everything
that matters recorded.  Runs INSIDE the compose network.

Writes   results/summary.csv            one row per experiment (append)   <- compare configurations with this
         results/<LABEL>_timeline.csv   one row per second                <- look at this to understand WHY

Env: TOPO, LABEL, SEED, CALM_S (15), SPIKE_S (30), RECOVER_S (20), BASE_RPS / SPIKE_RPS (default: from TOPOS),
     SLO (0.5), OUT (/results).  Tunables (MSG_COST, W_*, ...) are read through service.py, exactly as the services do.
"""
import asyncio, csv, json, os, random, statistics, time
import aiohttp
import service as S

TOPO = os.environ.get("TOPO", "deep")
topo = S.TOPOS[TOPO]
NAMES, ROOT = list(topo["nodes"]), topo["root"]
MODE = os.environ.get("MODE", "variable")
LABEL = os.environ.get("LABEL") or f"{TOPO}_{MODE}_{int(time.time())}"
SEED = int(os.environ.get("SEED", "0"))
CALM_S = float(os.environ.get("CALM_S", 15))
SPIKE_S = float(os.environ.get("SPIKE_S", 30))
RECOVER_S = float(os.environ.get("RECOVER_S", 20))
BASE = float(os.environ.get("BASE_RPS") or topo["base"])
SPIKE = float(os.environ.get("SPIKE_RPS") or topo["spike"])
SLO = float(os.environ.get("SLO", 0.5))
OUT = os.environ.get("OUT", "/results")
TOTAL = CALM_S + SPIKE_S + RECOVER_S
SPIKE_END = CALM_S + SPIKE_S
HOST = "http://service-{}:8000/"


def phase_of(t):
    return "calm" if t < CALM_S else "spike" if t < SPIKE_END else "recover"


def pct(vals, q):
    if not vals: return None
    v = sorted(vals)
    return v[min(len(v) - 1, int(q * (len(v) - 1) + 0.5))]


async def wait_ready(session):
    for n in NAMES:
        for _ in range(120):
            try:
                async with session.get(HOST.format(n.lower()) + "health", timeout=aiohttp.ClientTimeout(total=2)) as r:
                    if r.status == 200: break
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
            await asyncio.sleep(0.5)
        else:
            raise RuntimeError(f"service {n} never became healthy")


async def get_stats(session, n):
    try:
        async with session.get(HOST.format(n.lower()) + "stats", timeout=aiohttp.ClientTimeout(total=1)) as r:
            return await r.json()
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        return None


async def main():
    random.seed(SEED)
    reqs, samples = [], []                      # reqs: one dict per client request; samples: one dict per second
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
        await wait_ready(session)
        first = {n: await get_stats(session, n) for n in NAMES}          # counters are cumulative: remember start
        print(f"[{LABEL}] topo={TOPO} mode={MODE} calm {CALM_S}s@{BASE}/s, spike {SPIKE_S}s@{SPIKE}/s, recover {RECOVER_S}s", flush=True)
        t0 = time.monotonic()

        async def one(ts):
            st = time.monotonic(); rec = dict(t=ts, ok=False, lat=None, status=0, work=0.0, drop="")
            try:
                async with session.get(HOST.format(ROOT.lower()), timeout=aiohttp.ClientTimeout(total=5)) as r:
                    await r.read()
                    rec["lat"] = time.monotonic() - st; rec["status"] = r.status
                    rec["work"] = float(r.headers.get("X-Work", 0) or 0)
                    rec["drop"] = r.headers.get("X-Drop", "")
                    rec["ok"] = r.status == 200 and rec["lat"] <= SLO
            except (aiohttp.ClientError, asyncio.TimeoutError):
                rec["status"] = -1                                       # client-side timeout / connection error
            reqs.append(rec)

        async def generate():
            tasks, next_at = set(), t0
            while True:
                now = time.monotonic(); el = now - t0
                if el >= TOTAL: break
                rate = SPIKE if CALM_S <= el < SPIKE_END else BASE
                next_at = max(next_at, now) + random.expovariate(rate)
                await asyncio.sleep(max(0, next_at - time.monotonic()))
                ts = time.monotonic() - t0
                if ts >= TOTAL: break
                t = asyncio.create_task(one(ts)); tasks.add(t); t.add_done_callback(tasks.discard)
            if tasks: await asyncio.wait(tasks, timeout=6)

        async def sample():
            k = 0
            while True:
                await asyncio.sleep(max(0, t0 + k + 1 - time.monotonic())); k += 1
                if k > TOTAL: break
                st = await asyncio.gather(*(get_stats(session, n) for n in NAMES))
                samples.append(dict(t=k, svc={n: s for n, s in zip(NAMES, st) if s}))

        await asyncio.gather(generate(), sample())
        last = {n: await get_stats(session, n) for n in NAMES}

    # ------------------------------------------------------------------ summarise
    win = lambda a, b: [r for r in reqs if a <= r["t"] < b]
    calm, spike, rec_ = win(0, CALM_S), win(CALM_S, SPIKE_END), win(SPIKE_END, TOTAL)
    ok = lambda L: [r for r in L if r["ok"]]
    bad = lambda L: [r for r in L if not r["ok"]]

    wasted = sum(r["work"] for r in bad(spike)); useful = sum(r["work"] for r in ok(spike))
    drops = {}
    for r in bad(spike):
        if r["status"] == 503 and r["drop"]: drops[r["drop"]] = drops.get(r["drop"], 0) + 1
    n503 = sum(drops.values())
    lats = [1000 * r["lat"] for r in ok(spike)]

    # signalling cost: bytes / messages that rode on responses, per client request, whole run
    dsum = lambda key: sum((last[n] or {}).get(key, 0) - (first[n] or {}).get(key, 0) for n in NAMES if last[n] and first[n])
    root_reqs = max(1, ((last[ROOT] or {}).get("total", 0) - (first[ROOT] or {}).get("total", 0)))
    x_sum, x_n = dsum("x_sum"), dsum("x_n")

    # timing metrics from the 1-second samples
    def first_t(pred, start):
        for s in samples:
            if s["t"] > start and pred(s): return s["t"] - start
        return None
    react_any = first_t(lambda s: any(v["x"] > 0 for v in s["svc"].values()), CALM_S)
    react_root = first_t(lambda s: ROOT in s["svc"] and len(s["svc"][ROOT]["signals_seen"]) > 0, CALM_S)
    false_alarm = sum(1 for s in samples if 3 < s["t"] <= CALM_S and any(v["x"] > 0 for v in s["svc"].values()))
    flaps = 0
    for n in NAMES:
        xs = [s["svc"][n]["x"] for s in samples if n in s["svc"]]
        flaps += sum(1 for a, b in zip(xs, xs[1:]) if a != b)
    hurt = sorted({n for s in samples if CALM_S < s["t"] <= SPIKE_END for n, v in s["svc"].items() if v["p"] < 0.9})
    max_x = max([v["x"] for s in samples for v in s["svc"].values()] or [0])

    per_sec = {}
    for r in reqs:
        a = per_sec.setdefault(int(r["t"]), [0, 0]); a[0] += r["ok"]; a[1] += 1
    recovery = None
    for sec in range(int(SPIKE_END), int(TOTAL) - 1):
        good = lambda k: k in per_sec and per_sec[k][1] and per_sec[k][0] / per_sec[k][1] >= 0.95
        if good(sec) and good(sec + 1): recovery = sec - SPIKE_END; break

    row = dict(
        label=LABEL, topo=TOPO, mode=MODE, seed=SEED,
        # --- knobs actually in effect
        msg_cost=S.MSG_COST, w_rate=round(S.W["rate"], 3), w_drain=round(S.W["drain"], 3), w_cpu=round(S.W["cpu"], 3),
        w_mem=round(S.W["mem"], 3), drain_ref=S.DRAIN_REF, rate_sat=S.RATE_SAT, period=S.PERIOD,
        sig_expiry=S.SIG_EXPIRY, target_wait=S.TARGET_WAIT, mem_budget=S.MEM_BUDGET, qmax=S.QMAX, x_min=S.X_MIN,
        base_rps=BASE, spike_rps=SPIKE,
        # --- 1. throughput (did we lose goodput?)
        calm_success_ratio=round(len(ok(calm)) / max(1, len(calm)), 3),
        spike_goodput_rps=round(len(ok(spike)) / SPIKE_S, 1),
        spike_success_ratio=round(len(ok(spike)) / max(1, len(spike)), 3),
        recover_success_ratio=round(len(ok(rec_)) / max(1, len(rec_)), 3),
        # --- 2. latency of the requests that succeeded
        p50_ms=round(pct(lats, .5), 1) if lats else "", p95_ms=round(pct(lats, .95), 1) if lats else "",
        p99_ms=round(pct(lats, .99), 1) if lats else "",
        # --- 3. waste: work spent on requests that still failed, and WHERE they were turned away
        wasted_work_share=round(wasted / (wasted + useful), 3) if wasted + useful else "",
        wasted_work_s=round(wasted, 2),
        dropped_at_root_share=round(drops.get(ROOT, 0) / n503, 3) if n503 else "",
        drops_by_service=json.dumps(drops, sort_keys=True),
        timeouts=sum(1 for r in spike if r["status"] == -1),
        # --- 4. signalling overhead
        sig_bytes_per_req=round(dsum("sig_bytes") / root_reqs, 2), sig_msgs_per_req=round(dsum("sig_msgs") / root_reqs, 3),
        mean_x=round(x_sum / x_n, 2) if x_n else "", max_x=max_x,
        # --- 5. speed and stability of the reaction
        first_signal_s=react_any if react_any is not None else "", root_warned_s=react_root if react_root is not None else "",
        recovery_s=recovery if recovery is not None else "", false_alarm_s=false_alarm, x_flaps=flaps,
        # --- 6. how far the damage spread
        services_hurt=len(hurt), hurt_list="|".join(hurt),
    )
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, "summary.csv")
    new = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row)); 
        if new: w.writeheader()
        w.writerow(row)

    with open(os.path.join(OUT, f"{LABEL}_timeline.csv"), "w", newline="") as f:
        cols = ["t", "phase", "sent", "ok", "ratio", "p95_ms"] + [f"{n}_{k}" for n in NAMES for k in ("x", "p", "need", "arr")]
        w = csv.writer(f); w.writerow(cols)
        smap = {s["t"]: s for s in samples}
        for sec in range(int(TOTAL)):
            rs = [r for r in reqs if int(r["t"]) == sec]; l = [1000 * r["lat"] for r in rs if r["ok"]]
            line = [sec, phase_of(sec), len(rs), sum(r["ok"] for r in rs), round(sum(r["ok"] for r in rs) / max(1, len(rs)), 3),
                    round(pct(l, .95), 1) if l else ""]
            sv = smap.get(sec + 1, {}).get("svc", {})
            for n in NAMES:
                v = sv.get(n)
                line += [v["x"], round(v["p"], 2), round(v["need"], 3), round(v["arrival_rate_rps"], 1)] if v else ["", "", "", ""]
            w.writerow(line)

    print(f"[{LABEL}] spike goodput {row['spike_goodput_rps']}/s of {SPIKE}  success {row['spike_success_ratio']}  "
          f"p95 {row['p95_ms']}ms  wasted {row['wasted_work_share']}  root-drop {row['dropped_at_root_share']}  "
          f"sig {row['sig_bytes_per_req']}B/req  mean_x {row['mean_x']}  react {row['first_signal_s']}s  "
          f"hurt {row['hurt_list']}  false_alarm {false_alarm}s  flaps {flaps}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
