# Lance FFI Performance Analysis: Critical Issue Found

## The Problem: Rust Writer is Never Used!

### Expected Flow (from FFI header):
```
1. lance_writer_create(uri)         → Creates Rust writer
2. For each batch:
   lance_writer_write_batch(batch)  → Writes to Rust/Lance
3. lance_writer_close()              → Finalize
4. lance_writer_destroy()            → Cleanup
```

### Actual Flow (current implementation):
```
1. lance_writer_create(uri)         → Creates Rust writer (CREATED BUT NEVER USED!)
2. For each batch:
   accumulated_batches_.push_back()  → C++ accumulation (bypasses Rust)
3. parquet::arrow::WriteTable()      → Uses Arrow's Parquet writer (NOT Lance!)
4. lance_writer_close()              → Finalize empty Rust writer
5. lance_writer_destroy()            → Cleanup
```

## The Inefficiency Chain

### What's Happening:
1. **Rust writer created but unused** - FFI overhead with no benefit
2. **Data accumulated in C++ memory** - defeats streaming design
3. **Arrow Parquet writer used instead of Lance** - bypassing optimized Rust format
4. **Metadata created in Rust** but actual data written by Arrow
5. **Result:** Worst of both worlds
   - Overhead of FFI calls (to unused writer)
   - Inefficiency of C++ batch accumulation
   - Loss of Rust/Lance optimizations

## Code Evidence

### lance_writer.cpp (lines 73-75):
```cpp
// Creates Rust writer but NEVER calls lance_writer_write_batch()
auto* raw_writer = lance_writer_create(dataset_path_.c_str(), nullptr);
rust_writer_ = reinterpret_cast<void*>(raw_writer);
```

### lance_writer.cpp (lines 176-182):
```cpp
// Uses Arrow Parquet writer directly, bypassing the Rust writer
auto write_result = parquet::arrow::WriteTable(
    *table.ValueOrDie(),
    arrow::default_memory_pool(),
    outfile,
    1000,  // chunk size
    properties,
    arrow_props);
```

### FFI header defines but is never called:
```c
// This function should be called for every batch!
// But it's NEVER called in the actual implementation
int lance_writer_write_batch(
    LanceWriter* writer,
    const void* arrow_array_ptr,
    const void* arrow_schema_ptr
);
```

## Performance Impact

### SF5 Results Explanation:
- Lance baseline: 77.7 sec (uses Arrow Parquet writer in C++)
- Lance --zero-copy: 165.4 sec (SLOWER because zero-copy adds overhead without using Rust optimizations)

### Why Zero-Copy Makes It Worse:
1. Zero-copy creates more Arrow buffers (refcounting)
2. Still accumulates in C++ (no benefit from zero-copy)
3. Still writes with Arrow Parquet (not Lance)
4. Result: Overhead without optimization = worse performance

### SF10 Results Explanation:
- Slightly better at SF10 because accumulation threshold amortizes overhead
- But still fundamentally inefficient

## Solution

The correct implementation would:
1. **Call `lance_writer_write_batch()` for each batch** instead of accumulating
2. **Remove C++ batch accumulation** - let Rust handle batching
3. **Remove `parquet::arrow::WriteTable()` calls** - Rust writer handles file writing
4. **Use Arrow C Data Interface to pass batches** to Rust
5. **Let Rust write native Lance format** with its optimizations

## Estimated Performance Gain

If properly implemented:
- Eliminate C++ accumulation overhead
- Eliminate memory accumulation bottleneck
- Use Rust optimizations for batch handling
- Use native Lance format for writing
- Expected improvement: **2-5x speedup on Lance I/O**

## Why Current Approach Doesn't Work

The Lance writer creation pattern suggests it was designed as a stub/placeholder:
- FFI is defined but not used
- Documentation says "placeholder for future phases"
- Only metadata is created by Rust
- Actual data bypasses Rust layer entirely

This explains why:
- Lance is slower than Parquet (overhead without benefit)
- Zero-copy makes it worse (adds overhead, same bypass)
- No streaming (C++ accumulation required)

## Implementation Plan

### Phase 1: Enable Streaming via Rust Writer
- [ ] Modify `write_batch()` to call `lance_writer_write_batch()` directly
- [ ] Remove C++ batch accumulation for Lance writer
- [ ] Pass batches via Arrow C Data Interface FFI

### Phase 2: Remove Arrow Parquet Bypass
- [ ] Remove `parquet::arrow::WriteTable()` calls from Lance writer
- [ ] Let Rust handle Parquet/Lance format writing
- [ ] Keep Parquet writer for backward compatibility if needed

### Phase 3: Optimize Arrow C Data Interface Conversion
- [ ] Profile FFI call overhead
- [ ] Implement zero-copy buffer passing if possible
- [ ] Minimize conversions in hot path

### Expected Timeline
- Phase 1-2: High impact (2-3x speedup)
- Phase 3: Additional optimization (10-20% improvement)

## Testing Strategy

1. **Benchmark before:** Establish baseline for Lance performance
2. **Implement Phase 1:** Stream directly to Rust writer
3. **Benchmark Phase 1:** Measure streaming improvement
4. **Implement Phase 2:** Remove Parquet bypass
5. **Benchmark Phase 2:** Measure format optimization gain
6. **Profile Phase 3:** Identify remaining bottlenecks

## Notes

- This is a fundamental architectural issue with the Lance writer implementation
- The FFI layer was designed correctly but never properly integrated
- Current implementation is a placeholder that got left as-is
- Fixing this will align Lance performance with design intent
