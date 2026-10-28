"""Infinite open-loop Poisson arrivals, cyclic load phases, live adjustable rate."""
import asyncio, json, os, random, time
import aiohttp
from aiohttp import web
from service import TOPOS

TOPO=os.environ.get('TOPO','deep')
BASE=float(os.environ.get('BASE_RPS',str(TOPOS[TOPO]['base'])))
SPIKE=float(os.environ.get('SPIKE_RPS',str(TOPOS[TOPO]['spike'])))
PHASE=float(os.environ.get('PHASE_SECONDS','15'))
ROOT='http://service-'+TOPOS[TOPO]['root'].lower()+':8000/'

class Generator:
    def __init__(self):
        self.started=time.monotonic(); self.override=None; self.sent=0; self.success=0
        self.errors=0; self.active=0; self.latency_ms=0; self.phase='base'
        self.rate=BASE; self.tasks=set(); self.window_sent=0; self.window_success=0
        self.window_start=time.monotonic(); self.actual_rps=0; self.goodput_rps=0
        self.session=None
    def snapshot(self):
        return dict(topo=TOPO,phase=self.phase,target_rps=self.rate,override=self.override,
                    sent=self.sent,success=self.success,errors=self.errors,inflight=self.active,
                    observed_rps=self.actual_rps,goodput_rps=self.goodput_rps,
                    elapsed_seconds=round(time.monotonic()-self.started,1),
                    phase_seconds=PHASE,base_rps=BASE,spike_rps=SPIKE)
    async def one(self):
        self.active+=1
        try:
            async with self.session.get(ROOT,timeout=aiohttp.ClientTimeout(total=5)) as r:
                await r.read()
                if r.status==200:
                    self.success+=1; self.window_success+=1
                else: self.errors+=1
        except (aiohttp.ClientError,asyncio.TimeoutError): self.errors+=1
        finally: self.active-=1
    async def run(self):
        random.seed(int(os.environ.get('SEED','0')))
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
            self.session=session
            for name in TOPOS[TOPO]['nodes']:
                for _ in range(120):
                    try:
                        async with session.get(f'http://service-{name.lower()}:8000/health',timeout=aiohttp.ClientTimeout(total=2)) as r:
                            if r.status==200: break
                    except (aiohttp.ClientError,asyncio.TimeoutError): pass
                    await asyncio.sleep(.5)
                else: raise RuntimeError(f'Service {name} unavailable')
            print('Continuous load generator started',flush=True)
            next_at=time.monotonic()
            while True:
                now=time.monotonic()
                elapsed=now-self.started
                phase=int(elapsed//PHASE)%3
                self.phase=['base','spike','recovery'][phase]
                self.rate=self.override if self.override is not None else (SPIKE if phase==1 else BASE)
                if now-self.window_start>=1:
                    dt=now-self.window_start
                    self.actual_rps=self.window_sent/dt; self.goodput_rps=self.window_success/dt
                    self.window_sent=0; self.window_success=0; self.window_start=now
                if self.rate<=0:
                    await asyncio.sleep(.1); next_at=time.monotonic(); continue
                next_at=max(next_at,now)+random.expovariate(self.rate)
                await asyncio.sleep(max(0,next_at-time.monotonic()))
                self.sent+=1; self.window_sent+=1
                task=asyncio.create_task(self.one())
                self.tasks.add(task); task.add_done_callback(self.tasks.discard)

async def main():
    g=Generator(); app=web.Application()
    async def state(req): return web.json_response(g.snapshot())
    async def setrate(req):
        try:
            data=await req.json(); rate=data.get('rate')
            if rate is None: g.override=None
            else:
                rate=float(rate)
                if not 0<=rate<=2000: raise ValueError()
                g.override=rate
        except (ValueError,TypeError,KeyError):
            return web.json_response({'error':'rate must be null or 0..2000'},status=400)
        return web.json_response(g.snapshot())
    app.router.add_get('/state',state)
    app.router.add_post('/rate',setrate)
    runner=web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner,'0.0.0.0',8001).start()
    task=asyncio.create_task(g.run())
    try: await task
    finally: await runner.cleanup()

if __name__=='__main__': asyncio.run(main())
