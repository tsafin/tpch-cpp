# Phase 2.0c-2: Statistics Caching Implementation Plan

**Date**: February 7, 2026
**Objective**: Optimize XXH3 hashing and HyperLogLog statistics computation
**Target**: +2-5% speedup (574K → 603K-622K rows/sec)
**Effort**: ~2 hours

---

## Current Bottlenecks (from Phase 2.0b Profiling)

| Operation | CPU % | Time per 6M rows |
|-----------|-------|------------------|
| XXH3 hashing | 8.79% | ~0.92 sec |
| HyperLogLog stats | 7.23% | ~0.75 sec |
| Encoding strategy | 6.07% | ~0.63 sec |
| **Total overhead** | **22.09%** | **~2.3 sec** |

**Key insight**: For lineitem (16 columns), these operations happen per-column, creating multiplicative overhead.

---

## Implementation Strategy

### Phase 2.0c-2.1: Additional Lance WriteParams Configuration

**Goal**: Reduce encoding/statistics overhead through configuration

**Options to explore**:
1. Disable statistics computation (if available)
2. Reduce statistics granularity
3. Enable batch mode statistics (compute once per group)
4. Add encoding hints to schema metadata

**Implementation**:
- Extend WriteParams configuration in FFI
- Add boolean flags for optimization:
  - `stats_enabled: bool` (default: true)
  - `fast_stats: bool` (approximate stats, default: false)
  - `batch_statistics: bool` (compute per-group, not per-batch)

### Phase 2.0c-2.2: Schema Metadata Encoding Hints

**Goal**: Reduce encoding strategy selection overhead

**Approach**:
- Add encoding hints to Arrow schema metadata
- Pre-computed encoding strategies based on data characteristics
- Skip redundant strategy evaluation for homogeneous columns

**Example**:
```rust
// For integer columns, hint fixed-width encoding
schema.metadata["lance-encoding:int64"] = "fixed-width"

// For string columns, hint dictionary encoding
schema.metadata["lance-encoding:string"] = "dictionary"
```

### Phase 2.0c-2.3: Batch-Level Statistics Merging

**Goal**: Reduce per-column statistics computation

**Approach**:
- Compute statistics once per group (4096 rows), not per batch (5000 rows)
- Merge statistics from batches in-memory before Lance write
- Reuse computed statistics across similar columns

---

## Implementation Details

### Step 1: Investigate Lance 2.0 API for Statistics Control

**Check available options in WriteParams**:
```rust
let params = WriteParams {
    // Existing options:
    max_rows_per_group: 4096,

    // Investigate if these exist:
    // - enable_statistics: bool
    // - fast_mode: bool
    // - stats_sample_size: Option<usize>

    ..Default::default()
};
```

**Alternative: Check for DatasetWriterOptions or EncodingOptions**

### Step 2: Add Encoding Hints to Schema

**Modify Arrow schema before writing**:
```rust
// Create schema with encoding hints
let mut schema = batches[0].schema();
let mut metadata = schema.metadata().unwrap_or_default().clone();

// Add encoding hints for each column
for field in schema.fields() {
    match field.data_type() {
        DataType::Int64 | DataType::Int32 => {
            metadata.insert(
                format!("lance-encoding:{}", field.name()),
                "fixed-width".to_string()
            );
        }
        DataType::Utf8 => {
            metadata.insert(
                format!("lance-encoding:{}", field.name()),
                "dictionary".to_string()
            );
        }
        _ => {}
    }
}

// Create new schema with hints
let schema_with_hints = Schema::new_with_metadata(
    schema.fields().clone(),
    metadata
);
```

### Step 3: Benchmark Individual Optimizations

**Test each optimization separately**:
1. Baseline (current): 574K r/s
2. With encoding hints: Measure impact
3. With configuration options: Measure impact
4. Combined: Measure total improvement

---

## Expected Outcomes

### Conservative Estimate (+2%)
- Optimize XXH3 through caching/batching: -50% = +4.4%
- But only achievable for 50% of columns: +2.2%

### Moderate Estimate (+3-4%)
- Optimize XXH3 & HyperLogLog by 40%: +8%
- Achievable on 40% of overhead: +3.2%

### Optimistic Estimate (+5%)
- Significant optimization of both: achieve +5% through:
  - Batch-level statistics merging
  - Skip redundant computation
  - Encoding hints

**Target range**: +2-5% → 603K-622K rows/sec

---

## Risk Assessment

### Risk 1: Lance API Limitations
**Issue**: Lance might not expose configuration for statistics
**Mitigation**:
- Fall back to encoding hints only
- Accept smaller improvement if options unavailable
- Document findings for future Lance versions

### Risk 2: Encoding Hints Not Supported
**Issue**: Lance metadata hints might not be implemented
**Mitigation**:
- Test with simple hints first (fixed-width for integers)
- Verify impact with benchmarking
- Fall back to default if no improvement

### Risk 3: Performance Regression
**Issue**: Hints might conflict with Lance's automatic optimization
**Mitigation**:
- Benchmark carefully before/after
- Use feature flags to enable/disable hints
- Keep default behavior as fallback

---

## Implementation Phases

### Phase A: Research & Configuration (30 min)
1. Check Lance 2.0 documentation for statistics control options
2. Identify available WriteParams fields
3. Test if configuration options affect performance

### Phase B: Encoding Hints (45 min)
1. Add schema metadata construction
2. Generate encoding hints for each column type
3. Benchmark with hints enabled

### Phase C: Testing & Validation (30 min)
1. Run comprehensive benchmarks
2. Test on all 8 TPC-H tables
3. Measure improvement vs Phase 2.0c-2a baseline
4. Document findings

---

## Success Criteria

✅ **Implementation successful if**:
- Achieves 2-5% improvement (603K-622K r/s)
- No performance regression
- All 8 tables benchmark without errors
- Code changes minimal and focused

⚠️ **Partial success if**:
- Achieves 1-2% improvement
- Some tables show improvement, others neutral
- Limited by Lance API constraints

---

## Fallback Options

If statistics caching not feasible:
1. **Compression codec optimization**: Test LZ4 (5-10% potential)
2. **Per-column encoding**: More granular control
3. **Async tuning**: Better Tokio runtime configuration
4. **Accept limitations**: Acknowledge column-count effect as fundamental

---

## Next Steps After 2.0c-2

Regardless of 2.0c-2 results:
- **Phase 2.0c-3**: Encoding strategy simplification (+3-8%)
- **Phase 2.0c-4**: Async runtime tuning (+2-4%)
- Combined target: Reach 670K+ rows/sec (80% of Parquet)

---

## Files to Modify

1. `third_party/lance-ffi/src/lib.rs`
   - Add WriteParams configuration
   - Generate encoding hints
   - Implement statistics optimization

2. `scripts/phase2_0c2_stats_caching_benchmark.py`
   - New benchmark framework
   - Test statistics optimization
   - Measure per-column improvement

---

**Status**: Ready for implementation
**Next Action**: Begin Phase A (Research & Configuration)
