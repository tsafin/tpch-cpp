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

## SF=5 Re-evaluation Across 3 Largest TPC-DS Tables

Tables:

1. `store_sales` (`14,400,052` rows)
2. `catalog_sales` (`7,199,490` rows)
3. `web_sales` (`3,599,503` rows)

Command shape:

- `./tpcds_benchmark --format lance --scale-factor 5 --max-rows 0 --zero-copy --zero-copy-mode <sync|async|auto>`

Initial sweep (`/tmp/tpcds_lance_sf5_modes.txt`):

| table | sync (time, RSS) | async (time, RSS) | auto (time, RSS) |
|---|---|---|---|
| store_sales | 18.92s, 101,476 KB | 21.09s, 876,036 KB | 18.61s, 101,732 KB |
| catalog_sales | 41.80s, 110,636 KB | 10.03s, 1,099,008 KB | 8.08s, 111,252 KB |
| web_sales | 50.91s, 110,244 KB | 3.78s, 1,068,776 KB | 3.95s, 110,052 KB |

Run-order check showed strong outliers in sync mode for `catalog_sales` and `web_sales`.
When rerun with flipped order (`auto` then `sync`) on `web_sales`, results were close:

1. `auto`: 4.25s, 109,240 KB
2. `sync`: 4.37s, 109,428 KB

Conclusion from stable runs:

1. `sync` and `auto` are similar for single-table generation.
2. `async` consistently increases peak RSS by about `8x-10x`.
3. Throughput differences are workload/noise-sensitive; memory delta is robust.

## Agreed Next Plan

1. Add `--zero-copy-mode sync|async|auto`.
2. Make `auto` choose sync for single-table generation.
3. Implement synchronous bounded streaming path for Lance:
   - preserve memory capping goal of `--zero-copy`
   - avoid Tokio background task/queue overhead for single table
4. Keep async path for cases where overlap can help (for example, multi-table parallel generation).
5. Generalize and clean current `store_sales`-specific column-buffer path into a table-agnostic columnar batching framework.

## Implementation Status

Implemented in code:

1. `--zero-copy-mode auto|sync|async` option.
2. Lance single-table default behavior:
   - `auto` selects synchronous bounded mode
   - `async` keeps Tokio background streaming mode
3. New bounded buffered flush configuration in Rust FFI (for sync mode memory capping).
4. Removed `store_sales`-specific column-buffer hack path and switched `store_sales` back to generic generation flow for consistency.

Still pending:

1. Table-agnostic generalized columnar batching framework (clean replacement for specialized experiments).

## Perf Profiling (SF=5, Lance, zero-copy)

Using `~/CLAUDE.md` workflow:

- `sudo perf record --no-buildid -e cpu-clock:u -g -F 99 -o /tmp/perf_*.data -- ./tpcds_benchmark ...`
- `sudo perf report --stdio --no-children ...`

Top-40 `tokio-runtime-w` share in report:

| table | sync | async |
|---|---:|---:|
| store_sales | 0.00% | 11.87% |
| catalog_sales | 0.00% | 12.50% |
| web_sales | 0.40% | 12.88% |

Recurring async-specific hotspots:

1. `tokio-runtime-w libc.so.6 __memmove_avx_unaligned_erms`
2. `tokio-runtime-w ...run_count::count_runs`
3. `tokio-runtime-w ...Iterator::fold`

This confirms meaningful CPU work migration into Tokio worker threads in async mode, together with much higher RSS.

## Post-Implementation Sanity Check (SF=5 store_sales)

`--format lance --table store_sales --scale-factor 5 --max-rows 0 --zero-copy`

1. `--zero-copy-mode sync`
   - elapsed: `22.07s`
   - rate: `652,552 rows/s`
   - max RSS: `102,524 KB`
2. `--zero-copy-mode async`
   - elapsed: `33.87s`
   - rate: `425,172 rows/s`
   - max RSS: `851,160 KB`

Result in this run:

Synchronous bounded zero-copy mode is both faster and significantly lower memory than async mode for single-table `store_sales` SF=5.
