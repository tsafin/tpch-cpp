# Build Script and Benchmark Integration Guide

## Quick Summary

The complete pipeline from build to benchmarking works as follows:

```
1. Build Arrow → 2. Build tpch_benchmark → 3. Run benchmarks → 4. Collect results
```

### Arrow Build Script Key Features

**Location**: `scripts/build_arrow_from_source.sh`

**Latest Optimized Configuration**:
```cmake
cmake .. \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_MODULE_PATH="${ARROW_DIR}/cpp/cmake_modules" \  # CRITICAL FIX
  -DARROW_WITH_SNAPPY=ON \
  -DARROW_WITH_ZSTD=ON \
  -DARROW_WITH_LZ4=ON \
  -DARROW_WITH_ZLIB=ON \
  -DARROW_PARQUET=ON \
  -DARROW_BUILD_TESTS=OFF \
  -DARROW_COMPUTE=ON \
  -DARROW_FILESYSTEM=ON \
  -DARROW_CSV=ON
```

**Why This Works**:
- `CMAKE_MODULE_PATH` points to Arrow's cmake_modules directory
- Resolves san-config.cmake properly (no "include could not find" errors)
- All compression codecs enabled for Parquet
- No conflicting or problematic flags
- Minimal, focused configuration

### Parquet Benchmark Pipeline

```
Benchmark Script (Python)
    ↓
Calls: tpch_benchmark --format parquet --table <table> --scale-factor <sf>
    ↓
main.cpp: Format selection (line 170)
    ↓
Creates: ParquetWriter instance
    ↓
ParquetWriter::finalize() (line 158):
    - Sets compression to SNAPPY (hardcoded)
    - Creates Parquet FileWriter with Arrow schema
    ↓
Writes data to .parquet file
    ↓
Returns metrics: throughput, write_rate, elapsed_time, file_size
    ↓
Benchmark script parses output and collects results
```

## Critical Dependencies

For Parquet to work, Arrow MUST be built with:

```bash
# Essential system packages (installed in build script):
- protobuf-compiler       # For protobuf definitions
- libprotobuf-dev         # For protobuf development
- libsnappy-dev          # For Snappy compression (HARDCODED in ParquetWriter)
- liblz4-dev            # For LZ4 compression
- libzstd-dev           # For Zstd compression
- zlib1g-dev            # For Zlib compression
- pkg-config            # For library path detection
```

## Verification

Before running benchmarks, verify the setup:

```bash
# 1. Check Arrow installation
pkg-config --list-all | grep arrow
# Should show: arrow, parquet

# 2. Check Parquet supports Snappy
pkg-config --cflags arrow | grep -i snappy || ldd /usr/local/lib/libarrow.so | grep snappy

# 3. Quick test - generate Parquet file
./build/tpch_benchmark \
    --format parquet \
    --table nation \
    --scale-factor 1 \
    --use-dbgen \
    --output-dir /tmp

# 4. Verify file was created
ls -lh /tmp/nation.parquet
# Should be ~3.7 KB (compressed)
```

## Running Full Benchmarks

```bash
# All three formats, all tables, all scale factors:
python3 scripts/orc_vs_parquet_benchmark.py \
    ./build/tpch_benchmark \
    /tmp/benchmark \
    1,10,100 \
    orc,parquet,csv

# Output includes:
# - Per-table metrics (throughput, write rate, file size)
# - Format comparisons (speedup, win count)
# - Scale factor impact analysis
# - JSON results saved to /tmp/benchmark/benchmark_results.json
```

## Known Issues & Solutions

### Issue 1: "Support for codec 'snappy' not built"
**Cause**: Arrow built without Snappy support  
**Solution**: Rebuild Arrow with `-DARROW_WITH_SNAPPY=ON`

### Issue 2: CMake "include could not find requested file: san-config"
**Cause**: CMAKE_MODULE_PATH not set  
**Solution**: Add `-DCMAKE_MODULE_PATH="${ARROW_DIR}/cpp/cmake_modules"`

### Issue 3: ParquetWriter only supports SNAPPY
**Current**: Hardcoded `parquet::Compression::SNAPPY`  
**Workaround**: Works fine - Snappy is always enabled by build script

## Testing Results

**Verified Working Configuration**:

```
Test: nation table, Scale Factor 1 (25 rows)

ORC Format:
  - File size: 1539 bytes
  - Throughput: 12,500 rows/sec ✅
  - Compression: zstd

Parquet Format:
  - File size: 3696 bytes (compressed with Snappy)
  - Throughput: 12,500 rows/sec ✅
  - Compression: Snappy (hardcoded)

CSV Format:
  - File size: 2351 bytes
  - Throughput: 73 rows/sec
  - No compression
```

## Architecture Summary

| Layer | Component | Status |
|-------|-----------|--------|
| **Build** | Arrow build script | ✅ Optimized with CMAKE_MODULE_PATH |
| **Build** | Arrow codecs | ✅ Snappy, Zstd, LZ4, Zlib |
| **Build** | tpch_benchmark | ✅ Rebuilt with updated Arrow |
| **Runtime** | Parquet format support | ✅ Fully functional |
| **Runtime** | Compression | ✅ Snappy (hardcoded, working) |
| **Benchmark** | Format selection | ✅ All 3 formats (ORC, Parquet, CSV) |
| **Benchmark** | Metrics collection | ✅ Throughput, write_rate, file_size |

---

**Last Updated**: 2026-01-19  
**Phase**: 6 Complete  
**Status**: ✅ Production Ready
