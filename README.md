# Live adaptive X-hop Docker testbed

This is a **live demonstration** version of the previous finite experiment. Every service runs in a separate container, requests and downstream calls are real HTTP, and processing durations are sampled from an exponential distribution. The overload controller and variable-depth signal algorithm remain in `service.py`.

## Run on Windows PowerShell (Docker Desktop)

Open a terminal in this folder and run:

```powershell
docker compose --profile deep --profile live up -d --build
```

Open **http://localhost:8088** in your browser. Allow a few seconds for the services and load generator to start. The load generator runs forever. It cycles through 15 seconds at 20 req/s, 15 seconds at 120 req/s, and 15 seconds at 20 req/s. The browser UI updates every second.

To inspect logs:

```powershell
docker compose logs -f loadgen
docker compose ps
```

To stop everything:

```powershell
docker compose --profile deep --profile live down
```

**Important:** Don't run `docker compose --profile load run --rm loadgen` for this version; the load generator is a long-running service and starts with the `live` profile. Do not mix these files with the previous Compose file.

## Dashboard controls

Use **Set constant load** to send an arbitrary fixed rate from 0 to 2000 req/s, or **Resume 20 → 120 → 20 cycle** to return to the automatic repeating workload. A service turns red when its own controller chooses `x > 0` (meaning it is originating an overload warning). `Signals received` lists the warning origins learned via downstream responses. A warning may remain in flight briefly after its origin's status changes.

Each card shows configured theoretical local capacity (`workers / mean_processing_time`), in-flight requests, recent incoming requests/s, actual completed requests/s, configured theoretical processing rate, waiting requests, busy workers, queue delay, admission probability `p`, overload need and chosen hop count `x`.

### Change the per-service capacity

Edit `TOPOS['deep']['nodes']` in `service.py`. Example:

```python
"F": (2, .040)  # two concurrent worker slots; 40 ms mean local processing
"F": (1, .050)  # one worker; 50 ms mean -> 20 local completions/s max
```

Then rebuild/recreate:

```powershell
docker compose --profile deep --profile live up -d --build --force-recreate
```

To change the cycle rate or duration, edit `.env` and recreate the loadgen:

```ini
BASE_RPS=20
SPIKE_RPS=120
PHASE_SECONDS=15
```

The local controller updates every 250 ms; the dashboard polls every second. `arrival_rate_rps` and `completion_rate_rps` are measured over the latest controller interval and may fluctuate substantially. They are not the same as the configured capacity.

## What Docker Desktop shows

Docker Desktop's Containers view displays individual containers, logs and aggregate CPU/memory/network metrics. It does **not** automatically know application-specific HTTP arrival rates, admission probability, x-hop propagation or request counts. Use this dashboard for those values.

## Research caveats

- `asyncio.sleep(random.expovariate(...))` simulates latency, not CPU consumption. Worker slots are held during the sleep and downstream calls.
- Local capacity is an upper bound for the service's own work, not end-to-end throughput. A parent retains its worker while waiting on a child.
- Red indicates `x>0`, which in the signaling modes means local AIMD is shedding; it is **not** a direct hardware CPU overload flag. Services that are only receiving a downstream signal are not red.
- The service pressure formula still uses manually chosen weights. The dashboard makes it observable, not validated.
- Continuous load is useful for live demos but **not** a controlled benchmark: run separate isolated trials, multiple seeds and measure warmup/cooldown to compare policies scientifically.
- The previous finite-run CSV summary workflow has been replaced by live counters in this version. This dashboard does not persist time-series data.

## Modes

Set `MODE=variable` (default), `local`, `none`, `fixed:1`, `fixed:2`, or `fixed:3` in `.env` and recreate all services. `none` and `local` do not emit signals and therefore won't turn red under the current red=`x>0` display rule.

## Hub topology

Change `TOPO=hub` in `.env` and run `docker compose --profile hub --profile live up -d --build`. Stop and remove the deep topology first to avoid mixing incompatible containers.
