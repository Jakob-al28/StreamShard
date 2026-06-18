# Benchmark results

Each result file is named `{N}node_{instance}_{rf}{suffix}.json`, produced by
`bench/run_benchmark.sh`. The JSON holds the per-step load sweep (offered vs committed
rps, per-status reject rates, latency) plus the peak committed throughput. A matching
`{...}_cpu.csv` (from `bench/poll_cpu.sh`) holds per-tier CPU sampled during the run.

The files here are the clean result set: every run used `--no-idempotent` + unique ids
(so committed = real durable writes, not dedup no-ops) and `enable_swim=false`, with 12
k6 workers. The older dedup-era results were removed.

## Instance labels

| label   | machine        |
|---------|----------------|
| `e2`    | e2-standard-2  |
| `e2big` | e2-standard-4 (vertical-scaling comparison) |

## `rf`

`rf1` = replication factor 1, `rf3` = replication factor 3. At 1 node, RF=3 falls back
to RF=1 (nothing to replicate), so the 1-node point is identical across RF=3 modes and
equals the RF=1 point.

## Suffix (replication path)

The suffix records how replication was driven — the variable the study compares:

| suffix  | replication path                                               |
|---------|----------------------------------------------------------------|
| (none)  | gateway fans out replica writes (default)                      |
| `_pr`   | primary node fans out replica writes (`--primary-replication`) |

Headline finding: gateway fan-out is ~2× faster than primary-replication, which is
CPU-bound on the primary node (see top-level README).

## Plots

`scaling_*.png` (peak vs node count, with USL fit) and `*_sweep.png` (offered vs
committed curves) come from `bench/analysis/usl.py`. Use `--xmax` to clip the
load-generator tail at high offered loads (where `dropped_iterations` ≫ `committed`,
i.e. k6 can no longer sustain the rate) so the plot focuses on the real knee.
