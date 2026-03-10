# Store Sales Lance Zero-Copy Large Fragment Experiment

Date: 2026-03-10
Command: `./tpcds_benchmark --format lance --table store_sales --scale-factor 20 --max-rows 0 --zero-copy --output-dir /tmp`
Mode: sync zero-copy (`--zero-copy-mode sync` default)

## Baseline before changes
Hardcoded sync flush config in `src/tpcds_main.cpp`:
- `8` batches
- `65,536` rows

Observed result:
- elapsed: `316.92s`
- throughput: `181,745 rows/s`
- max RSS: `~108 MB`
- output files: `879 data + 879 manifests + 879 transactions`

## Experiment A: larger fragment / transaction
Changed sync flush config to:
- `128` batches
- `1,048,576` rows

Observed result:
- elapsed: `210.77s`
- `TIME_SEC=212.26`
- throughput: `273,274 rows/s`
- `MAX_RSS_KB=602084`
- output files: `55 data + 55 manifests + 55 transactions`

Delta vs baseline:
- throughput: about `+50%`
- file / manifest / transaction count: about `-16x`
- RSS: about `+494 MB`

Interpretation:
- The original sync path was over-fragmenting badly.
- Larger transactions help a lot.
- The cost is higher bounded memory, but still well below machine capacity.

## Experiment B: too large
Changed sync flush config to:
- `256` batches
- `2,097,152` rows

Observed result:
- elapsed: `617.79s`
- `TIME_SEC=623.82`
- throughput: `93,234 rows/s`
- `MAX_RSS_KB=866080`
- output files: `28 data + 28 manifests + 28 transactions`

Interpretation:
- Reducing transaction count further did not help.
- This setting likely pushes too much buffered data into a worse writeback / stall regime.
- Bigger transactions are not monotonic wins.

## Conclusion
For this machine and workload, a moderate increase in transaction / fragment size is the winning direction:
- `128` / `1,048,576` looks much better than `8` / `65,536`
- `256` / `2,097,152` is too large

This strongly supports the earlier diagnosis:
- the main scaling problem is Lance fragment / commit granularity in sync zero-copy mode
- not a new row-generation CPU hotspot

## Recommended default
Keep sync zero-copy bounded, but use:
- `128` batches
- `1,048,576` rows

Then re-evaluate SF=10/SF=20/SF=100 projections with that setting.
