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
5. `tpcds_benchmark` default `--zero-copy-mode` changed to `sync` for single-table Lance generation.
6. Added explicit copy telemetry at close:
   - C++ side: `Lance Copy Profile: mode=<sync|async> cxx_to_rust_bytes=...` and async queue peak MB
   - Rust side: `Lance FFI copy: reader_batches/rows/input_bytes/rewrap_bytes + SG queue bytes/chunks/peak`

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

## Async Memory-Tuning Experiment (Requested Follow-up)

Target:

- `store_sales`, SF=5, Lance, `--zero-copy --zero-copy-mode async`

Matrix (`/tmp/tpcds_async_tuning_store_sales_sf5.log`):

| config | key params | elapsed | rate | max RSS |
|---|---|---:|---:|---:|
| baseline | `queue=4, sg=1, sgq=4, blocking=8` | 32.21s | 447,177 r/s | 864,288 KB |
| q1_sg1 | `queue=1, sg=1, sgq=1, blocking=8` | 28.00s | 517,007 r/s | 895,780 KB |
| q1_sg2 | `queue=1, sg=2, sgq=1, blocking=8` | 28.52s | 505,492 r/s | 896,136 KB |
| q2_sg2 | `queue=2, sg=2, sgq=2, blocking=8` | 33.15s | 434,802 r/s | 895,072 KB |
| q1_sg4 | `queue=1, sg=4, sgq=1, blocking=8` | 27.91s | 516,731 r/s | 890,796 KB |
| q1_sg1_b2 | `queue=1, sg=1, sgq=1, blocking=2` | 66.09s | 217,918 r/s | 864,548 KB |

Reference sync run:

- `--zero-copy --zero-copy-mode sync`: `20.30s`, `709,727 r/s`, `102,308 KB` RSS

Outcome:

1. Async queue/chunk tuning changed throughput and queue behavior, but did **not** bring async RSS close to sync.
2. Async RSS stayed in a narrow high band (`~864–896 MB`) despite aggressive queue reduction.
3. Lowering Tokio blocking threads to 2 did not reduce RSS materially, but severely hurt performance.
4. Best async throughput in this sample (`q1_sg4`) is still significantly slower and much higher memory than sync reference.

Updated recommendation:

For single-table TPC-DS generation, keep synchronous zero-copy as the default path; treat async as experimental/optional.

## Async RSS Floor Isolation (Next Step)

Goal:

Identify whether the async RSS floor is caused by C++ queue buffering or Rust/Lance-side processing.

Method:

`store_sales`, SF=5, `--format lance --zero-copy` with Rust memory profiling:

- `--lance-mem-profile --lance-mem-every 100`

Runs (`/tmp/tpcds_mem_isolation_store_sales_sf5.log`):

1. `sync_profile`: `--zero-copy-mode sync`
2. `async_profile_default`: `--zero-copy-mode async --lance-stream-queue 4 --lance-sg-batches 1 --lance-sg-queue-chunks 4`
3. `async_profile_lowq`: `--zero-copy-mode async --lance-stream-queue 1 --lance-sg-batches 1 --lance-sg-queue-chunks 1`

Results:

| case | elapsed | rate | max RSS | C++ queue peak | Rust reader max RSS | Rust RSS after execute |
|---|---:|---:|---:|---:|---:|---:|
| sync_profile | 22.95s | 628,064 r/s | 103,152 KB | n/a | n/a | n/a |
| async_profile_default | 21.14s | 681,483 r/s | 869,800 KB | 5.625 MB | 855,976 KB | 817,292 KB |
| async_profile_lowq | 21.09s | 683,142 r/s | 850,112 KB | 1.406 MB | 822,592 KB | 791,656 KB |

Interpretation:

1. Shrinking C++ queue memory by `~4.2 MB` changed total RSS only by `~19.7 MB`.
2. Async run RSS is dominated by Rust/Lance-side memory during stream execution (`reader_next` stage reaching `~823–856 MB`).
3. `reader_input_bytes == reader_rewrap_bytes` in both async runs, confirming schema rewrap itself is not duplicating payload size.
4. SG queue bytes were zero in this test (`sg=1`), so scatter/gather queue buffering is not the source here.

Conclusion:

The async memory floor is primarily inside Lance async stream execution (Tokio worker + Lance encode/accumulation), not in the C++ producer queue. Queue-depth tuning alone cannot close the gap to sync memory.

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
