"""Read-only service metrics dashboard with a load-rate control."""
import asyncio, os
import aiohttp
from aiohttp import web

TOPO=os.environ.get('TOPO','deep')
NAMES=['A','B','C','D','E','F', 'G', 'H'] if TOPO=='deep' else ['A','B1','B2','B3','B4','H']

async def main():
    timeout=aiohttp.ClientTimeout(total=1.5)
    async with aiohttp.ClientSession(timeout=timeout) as client:
        async def index(request): return web.FileResponse('/app/dashboard.html')
        async def state(request):
            async def fetch(name):
                try:
                    async with client.get(f'http://service-{name.lower()}:8000/stats') as r:
                        return await r.json()
                except (aiohttp.ClientError,asyncio.TimeoutError): return {'name':name,'offline':True}
            data=await asyncio.gather(*(fetch(n) for n in NAMES))
            try:
                async with client.get('http://loadgen:8001/state') as r: gen=await r.json()
            except (aiohttp.ClientError,asyncio.TimeoutError): gen={'offline':True}
            return web.json_response({'topology':TOPO,'services':data,'generator':gen})
        async def rate(request):
            try:
                body=await request.json()
                async with client.post('http://loadgen:8001/rate',json=body) as r:
                    return web.json_response(await r.json(),status=r.status)
            except (aiohttp.ClientError,asyncio.TimeoutError):
                return web.json_response({'error':'load generator unavailable'},status=503)
        app=web.Application()
        app.router.add_get('/',index)
        app.router.add_get('/api/state',state)
        app.router.add_post('/api/rate',rate)
        runner=web.AppRunner(app); await runner.setup()
        await web.TCPSite(runner,'0.0.0.0',8080).start()
        print('Dashboard listening on :8080',flush=True)
        try: await asyncio.Event().wait()
        finally: await runner.cleanup()

if __name__=='__main__': asyncio.run(main())
