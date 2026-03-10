# Store Sales Lance Zero-Copy Scaling Profile (SF=5/10/20)

Date: 2026-03-09
Mode: `tpcds_benchmark --format lance --table store_sales --zero-copy`
Observed default mode: `sync`

## Measured runs

### perf record (user CPU hotspots)

| SF | rows | elapsed (run log) | rate rows/s | max rss kb |
|---:|---:|---:|---:|---:|
| 5 | 14,400,052 | 18.17s | 792,469 | 101,260 |
| 10 | 28,800,991 | 66.36s | 434,016 | 104,732 |
| 20 | 57,598,932 | 250.53s | 229,912 | 108,100 |

Top user-space symbols stayed broadly stable:
- `__memmove_avx_unaligned_erms`: ~9.4% to 10.5%
- `tpcds::append_store_sales_to_builders`: ~7.4% to 10.0%
- `genrand_decimal`, `decimal_t_op`, `arrow::NumericBuilder<...>::Append`: similar ordering at all scales
- No new dominant user-space hotspot appears at SF=10 or SF=20

### perf stat (task-clock vs elapsed)

| SF | elapsed (run log) | task-clock:u | CPUs utilized | page faults |
|---:|---:|---:|---:|---:|
| 5 | 21.87s | 10.83s | 0.495 | 950,875 |
| 10 | 201.33s | 25.56s | 0.127 | 1,943,553 |
| 20 | 316.92s | 53.24s | 0.168 | 4,113,395 |

Interpretation:
- SF=5 already spends about half of wall time outside user CPU.
- At SF=10 and SF=20 the benchmark spends most wall time stalled or sleeping, not executing user-space compute.
- This is why `perf record -e cpu-clock:u` does not show a new hot function: the slowdown is dominated by non-user-CPU time.

## Output shape at SF=20

Latest SF=20 output under `/tmp/store_sales.lance`:
- Total size: `9.8G`
- Total files: `2637`
- `_versions` manifests: `879`
- `_transactions`: `879`
- `data` files: `879`
- Average data file size: `11.25 MB`
- Estimated rows per data file: `57,598,932 / 879 = 65,528 rows`

This is the key scaling signal:
- The writer is producing about one data file / one transaction / one manifest per ~65K rows.
- As scale increases, the benchmark performs hundreds more write/commit cycles.
- The CPU hotspot mix does not change, but wall time does, which is consistent with append/commit/writeback overhead rather than row generation cost.

## Likely root cause

The throughput collapse is most likely caused by the Lance write path committing too frequently with small fragments:
- many small data files
- many manifest updates
- many transaction files
- increasing filesystem writeback / metadata overhead

This matches all observed evidence:
- stable user-space hotspots
- flat RSS
- low CPU utilization at larger SF
- high file / manifest / transaction counts at SF=20

## What this means

The main problem is not Arrow batch construction or zero-copy import itself.
The main problem is fragment / commit granularity on the Lance side.

## Recommended next experiments

1. Force much larger flush / fragment sizes for sync zero-copy and re-measure SF=20.
2. Measure file count and throughput together to verify the slope improves when data files drop from ~879 to something much smaller.
3. If needed, bypass append-per-chunk behavior and write a single longer stream / transaction for sync mode too.
