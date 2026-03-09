# Lance Streaming Zero-Copy Investigation (SF=5, store_sales)

Date: 2026-03-09  
Scope: `tpcds_benchmark --format lance --table store_sales --scale-factor 5 --zero-copy`

## Goal

Investigate extra memory usage and copy overhead in Lance streaming path, with focus on Rust/Tokio overhead and true zero-copy delivery of Arrow batches.

## Hypotheses

1. Per-row C++ builder path causes avoidable copies before Rust.
2. Rust stream handoff may add extra copies/queue overhead.
3. Tokio runtime configuration may contribute to memory overhead.
4. Optional schema rewrite in stream path may add avoidable work/copies.

## Implemented Experiments

1. `store_sales` direct Arrow column-buffer batching (C++ side) instead of builder append path.
2. Rust-side scatter/gather stream handoff with chunked queue:
   - `--lance-sg-batches`
   - `--lance-sg-queue-chunks`
3. Rust-side memory stage logging (`--lance-mem-profile`, `--lance-mem-every`).
4. Perf profiling from `~/CLAUDE.md` workflow:
   - `perf record --no-buildid -e cpu-clock:u -g -F 99 ...`

## Key Findings

1. C++ direct-buffer batching reduced main-thread copy share, but did not remove Tokio-side copy hotspot.
2. Across runs, top copy hotspot remained:
   - `tokio-runtime-w libc.so.6 __memmove_avx_unaligned_erms`
3. Scatter/gather changed throughput/stall behavior, but did not consistently reduce Tokio memmove share.
4. Disabling stream schema rewrite (`--lance-no-schema-rewrite`) was not useful for the target copy hotspot.

## Scatter/Gather Matrix (3 runs each, queue=8)

Median results from `/tmp/sg_matrix_q8_runs.tsv`:

| sg-batches | median elapsed | median rate | median stalls | median stall ms | median Tokio memmove |
|---:|---:|---:|---:|---:|---:|
| 1 | 48.54s | 296,639 rows/s | 13 | 44,014.0 | 12.47% |
| 2 | 38.52s | 373,833 rows/s | 13 | 34,079.4 | 12.16% |
| 4 | 48.96s | 294,146 rows/s | 14 | 40,837.7 | 12.53% |
| 8 | 62.08s | 231,948 rows/s | 13 | 51,909.3 | 10.24% |

Notes:
- Best median throughput in this sample was `sg=2`.
- `sg=8` lowered memmove percentage but had worst median runtime.
- Run-to-run variance is high, so medians are required for decisions.

## Cleanup Decisions From This Investigation

Removed as non-useful in recent experiments:

1. `--lance-no-schema-rewrite` path and related FFI/config plumbing.
2. `--lance-tokio-current-thread` path and related FFI/config plumbing.

Kept:

1. Scatter/gather experiment controls (`--lance-sg-batches`, `--lance-sg-queue-chunks`).
2. Rust memory profile controls (`--lance-mem-profile`, `--lance-mem-every`).
3. `store_sales` direct Arrow column-buffer generation path.

## Current Conclusion

The dominant copy hotspot is still inside Rust/Lance processing path (Tokio worker), not in the C++ row-builder layer.  
Scatter/gather is useful as a throughput/stall tuning lever, but not a direct fix for Tokio memmove overhead.
