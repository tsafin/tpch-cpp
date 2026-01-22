# GitHub Actions CI Setup for tpch-cpp

This document describes the GitHub Actions CI workflow implementation for the tpch-cpp project.

## Overview

The CI workflow validates code buildability and runs comprehensive benchmarks across all supported formats (CSV, Parquet, ORC) and optimization modes at Scale Factor 1.

### Workflow Structure

**Two-Stage Pipeline:**

1. **Build Matrix** (`.github/workflows/ci.yml` - `build-matrix` job)
   - Compiles project in 4 configurations in parallel
   - Tests all feature flag combinations (ORC on/off, Async-IO on/off)
   - Uploads build artifacts for benchmark suite

2. **Benchmark Suite** (parallel execution)
   - **Format Coverage** (24 tests): All formats × All 8 tables
   - **Optimization Modes** (9 tests): Baseline, zero-copy, true-zero-copy on Parquet
   - **Results Aggregation**: Collects and summarizes all metrics

**Estimated Runtime:** ~15 minutes (varies by runner performance)

## Files

### Workflow Configuration

- **`.github/workflows/ci.yml`** - Main GitHub Actions workflow
  - Build matrix with 4 configurations
  - Format coverage benchmark matrix
  - Optimization mode benchmark matrix
  - Results aggregation and status checks
  - All artifacts retained for 30 days

### Helper Scripts

- **`scripts/ci_install_deps.sh`** - Dependency installation
  - Installs system packages from Ubuntu 22.04 repositories
  - Arrow, Parquet, ORC, liburing, compression libraries
  - Initializes git submodules
  - Fast (~30 seconds) compared to building from source

- **`scripts/ci_run_benchmarks.sh`** - Benchmark execution (reference only)
  - Local testing script showing benchmark patterns
  - Not directly used in CI (workflow runs benchmarks inline)
  - Useful for replicating CI benchmarks locally

- **`scripts/parse_benchmark_logs.py`** - Log aggregation
  - Parses benchmark output logs
  - Extracts metrics: throughput, timing, file size
  - Generates JSON summary for artifact storage
  - Calculates per-format, per-table, per-mode statistics

## Workflow Triggers

CI runs automatically on:
- **Push** to `main` or `develop` branches
- **Pull Requests** targeting `main` or `develop`
- **Manual dispatch** via GitHub Actions UI

No scheduled runs configured (can be added later if needed).

## Build Configurations

The workflow builds 4 configurations to ensure all feature combinations work:

| Configuration | ORC | Async-IO | Purpose |
|---------------|-----|----------|---------|
| `build-orc-async` | ON | ON | Full features |
| `build-orc-only` | ON | OFF | ORC without async |
| `build-async-only` | OFF | ON | Async without ORC |
| `build-baseline` | OFF | OFF | Minimal features |

All build with `CMAKE_BUILD_TYPE=RelWithDebInfo` (optimized code with debug symbols).

## Benchmark Matrix

### Format Coverage Tests (24 total)

Tests all 3 formats across all 8 TPC-H tables at baseline optimization:

| Format | Tables |
|--------|--------|
| **CSV** | lineitem, orders, customer, part, partsupp, supplier, nation, region |
| **Parquet** | lineitem, orders, customer, part, partsupp, supplier, nation, region |
| **ORC** | lineitem (only; others excluded to reduce matrix size) |

**Scale Factor:** 1 (lineitem = 6M rows, orders = 1.5M rows)
**Optimization Mode:** baseline (no --zero-copy or --true-zero-copy flags)

### Optimization Mode Tests (9 total)

Tests optimization modes on Parquet with representative table sizes:

| Mode | Tables |
|------|--------|
| **baseline** | lineitem, orders, part |
| **--zero-copy** | lineitem, orders, part |
| **--true-zero-copy** | lineitem, orders, part |

**Scale Factor:** 1
**Format:** Parquet (standard column format)

### Sanity Checks

Workflow includes:
- Successful build compilation (fail if build errors)
- Benchmark execution without crashes (timeout 10 minutes per benchmark)
- Log file generation (verify outputs created)
- Throughput sanity check: lineitem baseline > 100K rows/sec

## Success Criteria

✅ **Build Phase:**
- All 4 build configurations compile successfully
- `tpch_benchmark` executable created
- No CMake errors or C++ warnings-as-errors

✅ **Benchmark Phase:**
- All format coverage benchmarks complete
- All optimization mode benchmarks complete
- Throughput > 100K rows/sec for lineitem baseline
- No segfaults, crashes, or hangs (10-minute timeout)

✅ **Reporting:**
- Benchmark logs uploaded as artifacts
- JSON summary generated
- Results accessible for 30 days

**Note:** Per design, CI does NOT fail on performance regression. Benchmarks are reported but never cause workflow failure (unless build fails).

## Artifact Retention

| Artifact | Retention | Purpose |
|----------|-----------|---------|
| Build artifacts (4) | 1 day | Temporary, used for benchmarking |
| Benchmark logs | 30 days | Historical record, performance analysis |
| ci_summary.json | 30 days | Structured metrics for analysis |

## Local Testing

### Test Build Locally

```bash
# Install dependencies
bash scripts/ci_install_deps.sh

# Build single configuration
cmake -B build \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DTPCH_ENABLE_ORC=ON \
  -DTPCH_ENABLE_ASYNC_IO=ON

cmake --build build -j$(nproc)

# Verify executable
./build/tpch_benchmark --help
```

### Run Benchmarks Locally

```bash
# Format coverage (one format, one table)
./build/tpch_benchmark \
  --use-dbgen \
  --scale-factor 1 \
  --format parquet \
  --table lineitem \
  --output-dir ./test-results \
  --verbose

# With optimization mode
./build/tpch_benchmark \
  --use-dbgen \
  --scale-factor 1 \
  --format parquet \
  --table lineitem \
  --zero-copy \
  --output-dir ./test-results \
  --verbose
```

### Parse Results Locally

```bash
# Generate JSON summary from logs
python3 scripts/parse_benchmark_logs.py ./test-results > results.json

# View summary
python3 -m json.tool results.json
```

## Debugging CI Failures

### Build Failure

1. Check workflow logs in GitHub Actions UI
2. Inspect which configuration failed (ORC, Async-IO, etc.)
3. Local reproduction:
   ```bash
   # Reproduce exact CI build
   bash scripts/ci_install_deps.sh
   cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo \
     -DTPCH_ENABLE_ORC=<ON|OFF> \
     -DTPCH_ENABLE_ASYNC_IO=<ON|OFF>
   cmake --build build -j$(nproc)
   ```

### Benchmark Failure

1. Check individual benchmark logs in artifacts
2. Common issues:
   - **Timeout**: Benchmark took > 600s (check for hang/deadlock)
   - **Crash**: Zero output or "Segmentation fault" in logs
   - **Format Error**: "Invalid format" message (build issue)

3. Local reproduction:
   ```bash
   timeout 600 ./build/tpch_benchmark \
     --use-dbgen \
     --scale-factor 1 \
     --format <format> \
     --table <table> \
     --verbose
   ```

### Dependency Installation Issue

The `ci_install_deps.sh` script uses system packages from Ubuntu 22.04:

- **Arrow/Parquet:** `libarrow-dev`, `libparquet-dev` (apt packages)
- **ORC:** `liborc-dev` (apt package)
- **Async-IO:** `liburing-dev` (apt package)

If specific versions are needed:
1. Edit `ci_install_deps.sh` to pin versions
2. Or use `scripts/build_arrow_from_source.sh` as fallback (slower, ~10 min)

## Performance Considerations

### Why Scale Factor 1?

- Lineitem table: 6M rows (compresses to ~200-300MB)
- Benchmark completes in ~30 seconds per format
- Entire suite runs in ~15 minutes
- Fast feedback loop for PRs

### Why No Regression Failures?

Per CLAUDE.md guidance on ASAN overhead, we don't fail CI on performance changes because:
- Benchmarks on CI runners may vary (shared resources, cloud variability)
- Small changes require high statistical significance to avoid false positives
- Historical comparison is more meaningful than absolute numbers

**Approach:** Report metrics, let developers review trends in artifact history.

## Future Enhancements

Not in scope but worth considering:

- **Nightly benchmarks** at SF=10 for comprehensive testing
- **Performance regression detection** with historical comparison
- **Multi-architecture testing** (ARM64 via QEMU, RISC-V)
- **Docker image builds** for reproducible environments
- **Benchmark result visualization** (charts, trend analysis)
- **Integration with external services** (Conbench, etc.)

## Related Documentation

- **CLAUDE.md:** Project-wide guidelines and architecture notes
- **PHASE14_RESULTS.md** (if applicable): Detailed benchmark analysis
- **CMakeLists.txt:** Build configuration and feature flags

## Questions?

Check workflow status:
- GitHub Actions tab in repository
- Click on any workflow run to see detailed logs
- Download artifacts for offline analysis

For issues:
- Review `.github/workflows/ci.yml` for trigger conditions
- Check `scripts/ci_install_deps.sh` for dependency problems
- Consult `src/main.cpp` for available CLI options
