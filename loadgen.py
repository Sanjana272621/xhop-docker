"""Open-loop Poisson request generator; one container, no coordinated user pacing."""
import asyncio, aiohttp, csv, json, math, os, random, statistics, time
from pathlib import Path
from service import TOPOS, analyze_topology

async def main():
    topo_name=os.environ.get('TOPO','deep')
    topo=TOPOS[topo_name]
    T=float(os.environ.get('T','45'))
    seed=int(os.environ.get('SEED','0'))
    slo=float(os.environ.get('SLO','0.5'))
    spike=(T/3, 2*T/3)
    random.seed(seed)
    base=topo['base']; high=topo['spike']
    root=f"http://service-{topo['root'].lower()}:8000/"
    names=list(topo['nodes'])
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
        for name in names:
            url=f'http://service-{name.lower()}:8000/health'
            for attempt in range(120):
                try:
                    async with session.get(url,timeout=aiohttp.ClientTimeout(total=2)) as r:
                        if r.status==200: break
                except (aiohttp.ClientError,asyncio.TimeoutError): pass
                await asyncio.sleep(.5)
            else: raise RuntimeError(f'Service {name} did not become healthy')
        print(f'All services ready. topo={topo_name} T={T} seed={seed} rates={base}/{high}/{base}',flush=True)
        print('Upstream topology:',json.dumps(analyze_topology(topo)),flush=True)
        rows=[]; tasks=[]; start=time.monotonic()
        async def one(scheduled):
            t0=time.monotonic()
            try:
                async with session.get(root,timeout=aiohttp.ClientTimeout(total=5)) as r:
                    await r.read()
                    lat=time.monotonic()-t0
                    rows.append(dict(scheduled=scheduled,latency=lat,success=int(r.status==200 and lat<=slo),status=r.status,work=float(r.headers.get('X-Work',0)),drop=r.headers.get('X-Drop',''),sig=r.headers.get('X-Sig',''),actual_start=t0-start))
            except (aiohttp.ClientError,asyncio.TimeoutError) as exc:
                rows.append(dict(scheduled=scheduled,latency=time.monotonic()-t0,success=0,status=0,work=0,drop=type(exc).__name__,sig='',actual_start=t0-start))
        # Generate exponential interarrival times within each phase, resetting at phase boundaries.
        for phase_start,phase_end,rate in [(0,T/3,base),(T/3,2*T/3,high),(2*T/3,T,base)]:
            nxt=phase_start
            while True:
                nxt+=random.expovariate(rate)
                if nxt>=phase_end: break
                await asyncio.sleep(max(0,start+nxt-time.monotonic()))
                tasks.append(asyncio.create_task(one(nxt)))
        await asyncio.gather(*tasks)
        stats={}
        for name in names:
            async with session.get(f'http://service-{name.lower()}:8000/stats') as r:
                stats[name]=await r.json()
    spike_rows=[r for r in rows if spike[0]<=r['scheduled']<spike[1]]
    good=[r for r in spike_rows if r['success']]
    bad=[r for r in spike_rows if not r['success']]
    lats=sorted(r['latency'] for r in good)
    xn=sum(s['x_n'] for s in stats.values())
    metrics=dict(topology=topo_name,mode=stats[topo['root']]['mode'],T=T,seed=seed,
        offered=len(spike_rows)/(T/3),goodput=len(good)/(T/3),success=len(good)/max(1,len(spike_rows)),
        p99_ms=lats[max(0,math.ceil(.99*len(lats))-1)]*1000 if lats else None,
        wasted=sum(r['work'] for r in bad)/max(1e-9,sum(r['work'] for r in spike_rows)),
        shed_root=sum(r['drop']==topo['root'] for r in bad)/max(1,len(bad)),
        sig_B_per_req=sum(s['sig_bytes'] for s in stats.values())/max(1,len(rows)),
        mean_x=sum(s['x_sum'] for s in stats.values())/xn if xn else 0,
        root_paths=stats[topo['root']]['paths'])
    folder=Path(os.environ.get('OUT','/results'));folder.mkdir(parents=True,exist_ok=True)
    stem=f"{topo_name}_{metrics['mode'].replace(':','-')}_seed{seed}"
    with (folder/f'{stem}_requests.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]) if rows else ['scheduled','latency','success','status','work','drop','sig','actual_start']);w.writeheader();w.writerows(rows)
    (folder/f'{stem}_stats.json').write_text(json.dumps(stats,indent=2))
    (folder/f'{stem}_metrics.json').write_text(json.dumps(metrics,indent=2))
    print(json.dumps(metrics,indent=2),flush=True)

if __name__=='__main__': asyncio.run(main())
