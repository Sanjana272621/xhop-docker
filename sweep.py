#!/usr/bin/env python3
"""Run recorded experiments one after another.  Needs only Python + docker compose on the host.

    python sweep.py baseline                    # STAGE 1: which graphs does variable x-hop help on?
    python sweep.py tune                        # STAGE 2: one knob at a time, variable mode only
    python sweep.py one --label t1 --topo chain --mode variable --set MSG_COST=0.4 W_RATE=0.5
    add  --dry-run   to print what would run;  --repeats 3  for 3 seeds;  --topos chain deep  to pick graphs

Each experiment: write the knobs into exp.env -> recreate the services for that topology -> run `record`
(15 s calm, 30 s spike, 20 s recovery) -> a row appears in results/summary.csv.
"""
import argparse, subprocess, sys, time

ALL_TOPOS = ["deep", "hub", "chain", "shallow", "fanout", "diamond", "long"]
PROFILES = [a for t in ALL_TOPOS for a in ("--profile", t)] + ["--profile", "exp"]

# knobs applied to EVERY experiment.  3145728 = 3 MB; service.py's default is 3*1024 (3 KB) which makes memory
# pressure saturate at the first in-flight request. Delete this line to run with the repo default instead.
BASE_KNOBS = {"MEM_BUDGET": "3145728"}

MODES = ["none", "local", "fixed:1", "fixed:3", "fixed:9", "variable"]   # fixed:9 = tell everybody, always

WEIGHTS = {                       # rate / drain / cpu / mem  (normalised to 1 by service.py)
    "equal":  dict(W_RATE=.25, W_DRAIN=.25, W_CPU=.25, W_MEM=.25),
    "repo":   dict(W_RATE=.25, W_DRAIN=.35, W_CPU=.25, W_MEM=.15),
    "rate":   dict(W_RATE=.55, W_DRAIN=.15, W_CPU=.15, W_MEM=.15),
    "drain":  dict(W_RATE=.15, W_DRAIN=.55, W_CPU=.15, W_MEM=.15),
    "cpu":    dict(W_RATE=.15, W_DRAIN=.15, W_CPU=.55, W_MEM=.15),
    "nomem":  dict(W_RATE=.30, W_DRAIN=.40, W_CPU=.30, W_MEM=0),
}


def plan_tune(topos):
    for t in topos:
        for c in (.05, .15, .25, .4, .6):
            yield f"{t}_msgcost{c}", t, "variable", dict(MSG_COST=c)
        for name, w in WEIGHTS.items():
            yield f"{t}_w-{name}", t, "variable", w
        for d in (.05, .15, .3):
            yield f"{t}_drainref{d}", t, "variable", dict(DRAIN_REF=d)
        for per, exp in ((.25, 1), (.5, 1), (1, 1), (1, 2), (.5, 2)):
            yield f"{t}_period{per}_expiry{exp}", t, "variable", dict(PERIOD=per, SIG_EXPIRY=exp)


def plan_baseline(topos):
    for t in topos:
        for m in MODES:
            yield f"{t}_{m.replace(':', '')}", t, m, {}


def sh(cmd, env_extra, dry):
    print("  $", " ".join(cmd), flush=True)
    if dry: return 0
    import os
    return subprocess.call(cmd, env={**os.environ, **{k: str(v) for k, v in env_extra.items()}})


def run(label, topo, mode, knobs, seed, dry):
    allk = {**BASE_KNOBS, **{k: str(v) for k, v in knobs.items()}}
    label = f"{label}_s{seed}"
    print(f"\n=== {label}  topo={topo} mode={mode} knobs={allk}", flush=True)
    if not dry:
        with open("exp.env", "w") as f:
            f.write("".join(f"{k}={v}\n" for k, v in allk.items()))
    env = dict(TOPO=topo, MODE=mode, SEED=seed)
    sh(["docker", "compose", *PROFILES, "down", "-t", "1"], env, dry)
    if sh(["docker", "compose", "--profile", topo, "up", "-d", "--build", "--wait"], env, dry) != 0:
        print(f"!! services for {label} did not start - skipping"); return
    sh(["docker", "compose", "--profile", "exp", "run", "--rm", "-e", f"LABEL={label}", "record"], env, dry)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["baseline", "tune", "one"])
    ap.add_argument("--topos", nargs="+", default=None)
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--label", default="manual"); ap.add_argument("--topo", default="deep")
    ap.add_argument("--mode", default="variable"); ap.add_argument("--set", nargs="*", default=[])
    a = ap.parse_args()

    if a.stage == "one":
        plan = [(a.label, a.topo, a.mode, dict(kv.split("=", 1) for kv in a.set))]
    elif a.stage == "baseline":
        plan = list(plan_baseline(a.topos or ALL_TOPOS))
    else:
        plan = list(plan_tune(a.topos or ["chain", "deep", "hub"]))
    jobs = [(l, t, m, k, s) for (l, t, m, k) in plan for s in range(a.repeats)]
    print(f"{len(jobs)} experiments, ~{len(jobs) * 95 // 60} min")
    for l, t, m, k, s in jobs:
        run(l, t, m, k, s, a.dry_run)
    if not a.dry_run:
        open("exp.env", "w").close()                       # leave the stack on defaults
        sh(["docker", "compose", *PROFILES, "down", "-t", "1"], {}, False)
    print("\nDone -> results/summary.csv  (+ one *_timeline.csv per experiment)")


if __name__ == "__main__":
    main()
