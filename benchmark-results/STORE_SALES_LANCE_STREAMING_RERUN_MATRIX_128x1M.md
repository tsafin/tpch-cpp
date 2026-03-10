# Store Sales Lance Zero-Copy Rerun Matrix with 128-batch / 1,048,576-row Flush

Date: 2026-03-10
Command: `./tpcds_benchmark --format lance --table store_sales --scale-factor <sf> --max-rows 0 --zero-copy --output-dir /tmp`
Mode: sync zero-copy
Flush setting: `128` batches / `1,048,576` rows

## Rerun results

| SF | elapsed | TIME_SEC | throughput rows/s | MAX_RSS_KB | data files | manifests | txns |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 5 | 20.58s | 23.48 | 699,707 | 541,632 | 14 | 14 | 14 |
| 10 | 50.30s | 50.32 | 572,530 | 571,844 | 28 | 28 | 28 |
| 20 | 180.06s | 180.08 | 319,894 | 603,268 | 55 | 55 | 55 |

## Previous tiny-fragment baseline

| SF | elapsed | throughput rows/s | MAX_RSS_KB | data/manifests/txns |
|---:|---:|---:|---:|---:|
| 5 | 21.87s | 658,385 | 101,260 | about 220 each expected by shape |
| 10 | 201.33s | 143,053 | 104,732 | about 440 each expected by shape |
| 20 | 316.92s | 181,745 | 108,100 | 879 each |

## Comparison

Improvements with larger fragments:
- SF=5: `699,707 / 658,385 = 1.06x`
- SF=10: `572,530 / 143,053 = 4.00x`
- SF=20: `319,894 / 181,745 = 1.76x`

Memory tradeoff:
- RSS rose from about `100 MB` to about `540-603 MB`
- still well within the machine memory limit

Scaling slope with new setting:
- SF=5 -> SF=10 throughput drop: `699,707 -> 572,530` (`0.82x`)
- SF=10 -> SF=20 throughput drop: `572,530 -> 319,894` (`0.56x`)

Conclusion:
- The catastrophic collapse at SF=10 was largely caused by tiny fragments / transactions.
- The new setting removes most of that pathologically bad behavior.
- There is still a noticeable decline by SF=20, so fragment sizing was not the only factor.
- But it is now plausible to continue profiling at this setting; the previous one was clearly misleadingly bad.

## SF=20 Reprofile at 128-batch / 1,048,576-row Flush

### perf record (`cpu-clock:u`)
Run result:
- `elapsed=153.23s`
- `375,896 rows/s`

Top user-space symbols:
- `11.12%` `__memmove_avx_unaligned_erms`
- `9.37%` `tpcds::append_store_sales_to_builders`
- `9.34%` `decimal_t_op`
- `8.93%` `genrand_decimal`
- `7.21%` `arrow::NumericBuilder<Int64>::Append`
- `6.14%` `genrand_integer`
- `6.11%` `arrow::NumericBuilder<Double>::Append`
- `6.01%` `getTableFromColumn`
- `4.91%` `genrand_key`
- `3.36%` `set_pricing`
- `2.61%` `lance_encoding::...run_count::count_runs`

Interpretation:
- After fixing fragment size, the user-space profile is still dominated by row generation, Arrow append, and memcpy.
- Lance encoding work is visible but not dominant in user CPU samples.

### perf stat
Run result:
- `elapsed=146.03s`
- `394,427 rows/s`
- `task-clock=40.82s`
- `CPUs utilized=0.276`
- `context-switches=25,138`
- `cpu-migrations=771`
- `page-faults=2,661,633`

Interpretation:
- The fragment fix improved CPU utilization versus the old tiny-fragment path (`~0.276` vs `~0.168`).
- But the run is still mostly outside user CPU.
- Remaining slowdown is still dominated by stall / wait / writeback effects, not a new hot loop in row generation.
