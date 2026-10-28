# X-hop overload signaling — Docker Compose testbed

A containerized port of the supplied `xhop_real_v2.py`: **one aiohttp service per container**, actual Docker-network HTTP calls, exponential `asyncio.sleep(random.expovariate(1 / mean_s))`, independent AIMD controllers, adaptive/fixed x-hop response-header signaling, and an **open-loop Poisson** load generator. Requires WSL2 + Docker Desktop with WSL integration.

## Run (Ubuntu WSL shell)

```bash
unzip xhop-docker.zip -d xhop-docker
cd xhop-docker
mkdir -p results
docker compose --profile deep build
# Choose topology, mode, duration, seed by editing .env
# .env defaults: TOPO=deep MODE=variable T=45 SEED=0
docker compose --profile deep up -d service-a service-b service-c service-d service-e service-f
# Wait for services to start, then run one experiment:
docker compose --profile load run --rm loadgen
# Results appear in ./results/
docker compose --profile deep down
```

**Repeat for each mode:** edit `MODE` in `.env`, then **recreate all service containers** (`docker compose --profile deep up -d --force-recreate ...`) before rerunning `loadgen`. Do not reuse running services: controllers and counters persist. For `MODE=none`, controller disabled; `local` uses local AIMD only; `fixed:1`, `fixed:2`, `fixed:3` have fixed signaling depth; `variable` uses choose_x().

**Hub topology:** set `TOPO=hub` in `.env` and use:

```bash
docker compose --profile hub up -d service-a service-b1 service-b2 service-b3 service-b4 service-h
docker compose --profile load run --rm loadgen
docker compose --profile hub down
```

**Quick diagnostics** (with deep running):

```bash
docker compose logs -f service-f
docker compose exec service-f python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8000/stats').read().decode())"
docker compose ps
```

**Interpretation:** `T=45` yields 15 seconds at 20 req/s, 15 seconds at 120 req/s (hub 100 req/s), then 15 seconds at 20 req/s. These are Poisson *expected* rates, not exact counts. `goodput` and `success` refer to requests *scheduled* during the spike; success requires HTTP 200 and latency <= 0.5 s. `p99_ms` is calculated on successful requests only. `wasted` is a response-header-derived work proxy and undercounts timed-out/cancelled work. `sig_B_per_req` is header string length, not actual on-wire bytes. `mean_x` averages positive x values over controller intervals, not individual requests. `cpu` pressure is worker occupancy, not actual CPU; `mem` pressure uses request-buffer allocation, not container RSS. The code's weights and signaling cost are heuristics, not calibrated physical units.

**Important experimental limitations:** Docker networking adds real communication overhead, but all containers still run on one host and share its CPU. `asyncio.sleep` models elapsed service time, not CPU work. The synthetic load generator is open-loop and can lag at high rates; compare `actual_start` with `scheduled` in CSV. The deep topology's independent Bernoulli branches mean A may call both B and C. HTTP response piggybacking means signals only propagate when responses return. Multiple runs/seeds and confidence intervals are needed before claiming improvement. `stats` counters are cumulative; restart containers for each run. The fixed and variable modes can over-shed. The implementation is a research testbed, not a production service mesh.

**Next research improvements:** add per-controller-tick JSONL telemetry (N, x, p, per-hop benefit/cost), measure signal age at the root, vary branch probabilities/processing times/worker limits, add a Prometheus exporter, and evaluate cost sensitivity and statistical confidence intervals. A later Locust frontend is possible, but Locust's default user model is closed-loop and will not automatically reproduce the exact open-loop 20/120/20 offered arrival process.
