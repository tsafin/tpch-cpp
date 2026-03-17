# TPC-H / TPC-DS C++ Data Generator

High-performance TPC-H and TPC-DS data generators with multiple output format support (Parquet, ORC, CSV, Paimon, Iceberg, Lance) and optional asynchronous I/O via Linux io_uring.

## Features

- **Multiple Output Formats**: Parquet, ORC, CSV, Apache Paimon, Apache Iceberg, Lance
- **Apache Arrow Integration**: central in-memory columnar representation, unified API across all formats
- **Zero-Copy Streaming Writes**: O(batch) peak RAM regardless of scale factor — essential at SF≥5
- **Parallel Generation**: fork-after-init with rolling N-slot window — all tables concurrently, one init cost
- **io_uring Support**: kernel async I/O for Parquet (IoUringOutputStream) and Lance (Rust runtime)
- **Both TPC-H and TPC-DS**: all 8 TPC-H tables and 24 TPC-DS tables implemented

## Quick Start

### Docker (recommended — no build required)

Pre-built images are available for every platform:

```bash
docker pull ghcr.io/tsafin/tpch-cpp-all:latest

# Generate all TPC-H tables at SF=10, Parquet, parallel, zero-copy
docker run --rm -v /data:/data ghcr.io/tsafin/tpch-cpp-all:latest \
    tpch_benchmark --scale-factor 10 --format parquet --output-dir /data \
                   --parallel --zero-copy --max-rows 0

# Generate all TPC-DS tables at SF=5, Parquet, parallel, zero-copy
docker run --rm -v /data:/data ghcr.io/tsafin/tpch-cpp-all:latest \
    tpcds_benchmark --scale-factor 5 --format parquet --output-dir /data \
                    --parallel --zero-copy --max-rows 0
```

Image registry: https://github.com/tsafin/tpch-cpp/pkgs/container/tpch-cpp-all

### Build from Source

#### Prerequisites

- Linux (WSL2 supported) — io_uring requires kernel ≥ 5.11
- GCC 11+ or Clang 13+, CMake 3.22+
- `libarrow-dev`, `libparquet-dev`
- Rust ≥ 1.85 (only for Lance format)

Install system dependencies:

```bash
./scripts/install_deps.sh
```

#### Build

```bash
mkdir build && cd build

# Minimal build (Parquet + CSV only, includes tpch_benchmark)
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
cmake --build . -j$(nproc)

# With TPC-DS:
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTPCDS_ENABLE=ON ..
cmake --build . --target tpcds_benchmark -j$(nproc)

# With ORC:
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTPCH_ENABLE_ORC=ON ..

# With Lance (requires Rust):
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTPCH_ENABLE_LANCE=ON ..

# Everything:
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DTPCDS_ENABLE=ON \
      -DTPCH_ENABLE_ORC=ON \
      -DTPCH_ENABLE_LANCE=ON \
      ..
cmake --build . -j$(nproc)
```

## Usage

### tpch_benchmark

```
Usage: tpch_benchmark [options]

  --scale-factor <SF>   TPC-H scale factor (default: 1)
  --format <fmt>        Output format: parquet, csv, orc, paimon, iceberg, lance
                        (default: parquet)
  --output-dir <dir>    Output directory (default: /tmp)
  --max-rows <N>        Max rows to generate (default: 1000; use 0 for all rows)
  --table <name>        Single table: lineitem, orders, customer, part, partsupp,
                        supplier, nation, region (default: lineitem)
  --parallel            Generate all 8 tables in parallel (fork-after-init)
  --parallel-tables <N> Max concurrent child processes (default: all 8)
  --zero-copy           Streaming writes — O(batch) RAM; required at SF≥5 with --parallel
  --zero-copy-mode <m>  Lance streaming variant: sync (default), auto, async
  --compression <c>     Parquet compression: zstd (default), snappy, none
  --io-uring            Kernel async I/O: IoUringOutputStream for Parquet,
                        delegated to Rust runtime for Lance
  --verbose             Verbose output
```

**Common invocations:**

```bash
# Single table, all rows, Parquet
./tpch_benchmark --scale-factor 5 --format parquet --output-dir /data \
                 --table lineitem --max-rows 0

# All 8 tables in parallel, Parquet, zero-copy (recommended for SF≥5)
./tpch_benchmark --scale-factor 10 --format parquet --output-dir /data \
                 --parallel --zero-copy --max-rows 0

# Limit to 4 concurrent children (reduces peak RAM further)
./tpch_benchmark --scale-factor 10 --format parquet --output-dir /data \
                 --parallel --parallel-tables 4 --zero-copy --max-rows 0

# With io_uring for kernel-async disk writes
./tpch_benchmark --scale-factor 10 --format parquet --output-dir /data \
                 --parallel --zero-copy --io-uring --max-rows 0

# Lance format, streaming mode
./tpch_benchmark --scale-factor 5 --format lance --output-dir /data \
                 --parallel --zero-copy --max-rows 0
```

### tpcds_benchmark

```
Usage: tpcds_benchmark [OPTIONS]

  --format <fmt>         Output format: parquet, csv, paimon, lance (default: parquet)
  --table <name>         Single TPC-DS table (default: store_sales)
  --scale-factor <sf>    Scale factor (default: 1)
  --output-dir <dir>     Output directory (default: /tmp)
  --max-rows <n>         Max rows to generate (0=all, default: 1000)
  --compression <c>      Parquet compression: zstd (default), snappy, none
  --zero-copy            Streaming mode — O(batch) RAM; required at SF≥5 with --parallel
  --zero-copy-mode <m>   Lance streaming variant: sync, auto, async (default: sync)
  --parallel             Generate all 24 tables in parallel (fork-after-init)
  --parallel-tables <N>  Max concurrent child processes (default: all)
  --verbose              Verbose output
```

**Common invocations:**

```bash
# All TPC-DS tables in parallel, Parquet, zero-copy
./tpcds_benchmark --scale-factor 5 --format parquet --output-dir /data \
                  --parallel --zero-copy --max-rows 0

# Single table smoke test
./tpcds_benchmark --format parquet --table store_sales --scale-factor 1

# Limit parallelism
./tpcds_benchmark --scale-factor 10 --format parquet --output-dir /data \
                  --parallel --parallel-tables 6 --zero-copy --max-rows 0
```

**TPC-DS tables:**

| Category | Tables |
|----------|--------|
| Fact | store_sales, inventory, catalog_sales, web_sales, store_returns, catalog_returns, web_returns |
| Dimension | customer, item, date_dim, call_center, catalog_page, web_page, web_site, warehouse, ship_mode, household_demographics, customer_demographics, customer_address, income_band, reason, time_dim, promotion, store |

## Build Options

| CMake Option | Default | Description |
|---|---|---|
| `TPCDS_ENABLE` | OFF | Build `tpcds_benchmark` (TPC-DS) |
| `TPCH_ENABLE_ORC` | OFF | ORC format support |
| `TPCH_ENABLE_PAIMON` | OFF | Apache Paimon format support |
| `TPCH_ENABLE_ICEBERG` | OFF | Apache Iceberg format support |
| `TPCH_ENABLE_LANCE` | OFF | Lance format support (requires Rust ≥ 1.85) |
| `TPCH_ENABLE_ASYNC_IO` | OFF | Build io_uring pool (auto-detected at runtime; needed for `--io-uring`) |
| `TPCH_ENABLE_ASAN` | OFF | AddressSanitizer (for development only — do not benchmark with ASAN) |
| `TPCH_BUILD_EXAMPLES` | ON | Build example applications |
| `TPCH_BUILD_TESTS` | OFF | Build unit tests |

## Dependencies

| Library | Required | Ubuntu Package | Notes |
|---------|----------|----------------|-------|
| Apache Arrow + Parquet | Yes | `libarrow-dev libparquet-dev` | ≥ 10.0 |
| CMake | Yes | `cmake` | ≥ 3.22 |
| GCC/Clang | Yes | `build-essential` | GCC ≥ 11 |
| Apache ORC | No | `liborc-dev` | Needed for ORC format |
| liburing | No | `liburing-dev` | Needed for `--io-uring` (`TPCH_ENABLE_ASYNC_IO=ON`) |
| Rust | No | via rustup | ≥ 1.85, needed for Lance format |

## Performance Notes

- Always use `--zero-copy` at SF≥5 — without it each child accumulates all batches in RAM before writing, which OOMs at scale.
- `--parallel` forks children after one shared dbgen initialization (COW), giving full CPU utilization with a single init cost.
- `--io-uring` offloads write syscalls to the kernel async worker pool. Useful when disk I/O is the bottleneck; has no effect on CPU-bound workloads (e.g. heavy ZSTD compression).
- Do not use `TPCH_ENABLE_ASAN` for performance measurement — ASAN adds 30–50% overhead and distorts comparisons.

## License

See LICENSE file.
