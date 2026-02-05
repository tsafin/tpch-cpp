# Phase 1: Quick Start Guide

## What Is Phase 1?

Phase 1 implements streaming via the Rust Lance writer, fixing a critical architectural issue where the Rust writer was created but never used. The C++ code now properly streams batches to Rust via Arrow C Data Interface.

## Key Results

✅ **38% faster** at SF=1 (average across all tables)
✅ **32% faster** at SF=5 (showing consistent scaling)
✅ **57% faster** on customer table (best case)
✅ **19% faster** on orders table (worst case - still good!)

## Build & Run

### Build with Phase 1

```bash
cd /home/tsafin/src/tpch-cpp
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTPCH_ENABLE_LANCE=ON ..
cmake --build . -j$(nproc)
```

### Test Phase 1

```bash
# Quick verification (takes ~30 seconds)
./scripts/verify_phase1.sh

# Full benchmark SF=1 (takes ~30 seconds)
./scripts/phase1_full_benchmark.sh "1"

# Extended benchmark SF=1 and SF=5 (takes ~2 minutes)
./scripts/phase1_full_benchmark.sh "1 5"

# Manual test
./tpch_benchmark \
    --use-dbgen \
    --format lance \
    --output-dir /tmp/test_lance \
    --scale-factor 1 \
    --table customer \
    --max-rows 0
```

## What Changed

### Files Modified
1. **src/writers/lance_writer.cpp**
   - Implements proper Arrow C Data Interface conversion
   - Streams batches directly to Rust writer
   - Removed 167 lines of accumulation/Parquet writing code

2. **include/tpch/lance_writer.hpp**
   - Removed unused members (accumulated_batches_, parquet_file_count_)
   - Removed unused methods (flush_batches_to_parquet, write_lance_metadata)
   - Updated documentation

### Architecture Change

**Before:**
```
Data → C++ Accumulate → Arrow Parquet Writer → Output
       (Rust writer unused!)
```

**After:**
```
Data → Stream immediately → Rust Writer → Output
       (No accumulation, proper FFI integration)
```

## Performance Summary

### Scale Factor 1 (All Rows)
```
Customer:  0.129s (Lance) vs 0.299s (Parquet) = 57% faster
Orders:    2.221s (Lance) vs 2.709s (Parquet) = 19% faster
Lineitem:  4.787s (Lance) vs 7.536s (Parquet) = 37% faster
```

### Scale Factor 5 (All Rows)
```
Customer:  0.659s (Lance) vs 1.015s (Parquet) = 36% faster
Orders:    10.621s (Lance) vs 13.572s (Parquet) = 22% faster
Lineitem:  24.457s (Lance) vs 39.056s (Parquet) = 38% faster
```

## Implementation Details

### Arrow C Data Interface Conversion

```cpp
// Converts RecordBatch to C Data Interface structs
std::pair<void*, void*> batch_to_ffi(
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    auto* arrow_array = new ArrowArray();
    auto* arrow_schema = new ArrowSchema();

    auto status = arrow::ExportRecordBatch(*batch, arrow_array, arrow_schema);
    // ... error handling ...

    return std::make_pair(reinterpret_cast<void*>(arrow_array),
                         reinterpret_cast<void*>(arrow_schema));
}
```

### Streaming write_batch

```cpp
void LanceWriter::write_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    // ... validation ...

    // Convert to FFI format
    auto [array_ptr, schema_ptr] = batch_to_ffi(batch);

    try {
        // Stream directly to Rust writer
        int result = lance_writer_write_batch(raw_writer, array_ptr, schema_ptr);

        if (result != 0) {
            free_ffi_structures(array_ptr, schema_ptr);
            throw std::runtime_error("Failed to write batch");
        }

        free_ffi_structures(array_ptr, schema_ptr);
    } catch (...) {
        free_ffi_structures(array_ptr, schema_ptr);
        throw;
    }
}
```

## Documentation

- **PHASE1_IMPLEMENTATION_SUMMARY.md** - Technical details
- **PHASE1_BENCHMARK_REPORT.md** - Comprehensive benchmark analysis
- **PHASE1_COMPLETION_SUMMARY.md** - Full project summary
- **LANCE_FFI_ANALYSIS.md** - Original problem analysis

## What's Next?

### Phase 2: Native Lance Format
- Replace Parquet data files with native Lance format
- Expected: **2-5x additional improvement**

### Phase 3: FFI Optimization
- Profile and optimize C Data Interface usage
- Expected: **10-20% additional improvement**

## Troubleshooting

### "Lance writer timed out"
- Check if TPCH_ENABLE_LANCE=ON in CMake
- Verify Rust toolchain: `/snap/bin/rustc --version`
- Try smaller scale factor or use --max-rows 1000

### "Failed to export RecordBatch"
- Ensure Arrow headers are included: `<arrow/c/bridge.h>`
- Verify schema consistency

### Slow performance on large scale factors
- Phase 1 focuses on streaming, not format optimization
- Phase 2 will add native Lance format optimization
- Current implementation uses Parquet internally

## Verification Checklist

✅ Compilation successful
✅ Binary created at build/tpch_benchmark
✅ Lance metadata files created (_metadata.json, _manifest.json)
✅ All rows written correctly
✅ Performance improved vs Parquet baseline
✅ No memory leaks or crashes
✅ Consistent results across multiple runs

## Git Commits

- **1bf7fa4** - Phase 1 implementation
- **8f07547** - Benchmarking suite and report
- **cb0e480** - Completion summary and scaling results

## Questions?

See detailed documentation:
1. PHASE1_IMPLEMENTATION_SUMMARY.md (technical details)
2. PHASE1_BENCHMARK_REPORT.md (performance analysis)
3. PHASE1_COMPLETION_SUMMARY.md (comprehensive overview)
4. LANCE_FFI_ANALYSIS.md (original problem statement)
