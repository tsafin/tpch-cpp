# Phase 2.0c: Zero-Copy & Streaming Architecture Opportunity

**Analysis Date**: February 7, 2026
**Topic**: Alternative write strategy - streaming vs batch accumulation
**Impact**: Potential memory reduction + encoding overlap

---

## Current Architecture Problem

### Batch Accumulation Model (Current)
```
Main thread:                Tokio async thread:
[1] Generate rows ────→ [6] Encode/Hash ───────→ [7] Write to disk
[2] Build batch   ────→      (all batches at
[3] Export FFI    ────→       once in close())
[4] Call FFI      ────→
[5] Accumulate
    (601 batches
     in memory)
```

**Issues with current approach:**
- All 601 batches held in memory simultaneously
- Memory usage: ~6M rows × 200 bytes/row = ~1.2GB in memory during write
- Encoding doesn't start until all batches accumulated
- FFI-to-Rust import happens per batch, but results aren't written until later
- Tokio runtime does all encoding work in burst at end

### Proposed Streaming Model
```
Main thread:                Tokio async thread:
[1] Generate rows ────→ [4] Encode batch ─────→ [5] Write fragment
[2] Build batch   ────→     (start encoding    (incremental
[3] Export FFI    ────→      immediately)       fragments)
[4] Call FFI with
    batch_write()
    + flush hint ──→ Overlapped:
(Next batch)          Main thread: generating batch N+2
                      Tokio: encoding batch N+1
                      Disk: writing batch N
```

**Benefits of streaming:**
- Constant memory usage (hold only current batch + encoding buffer)
- Parallel execution: generation overlaps with encoding overlaps with writing
- Faster time-to-first-write
- Better cache locality (single batch in encoding pipeline)
- Reduced memory pressure (6M rows spread across time, not held at once)

---

## Lance API Analysis

### Current Implementation
```rust
// Phase 2 current approach: Batch accumulation
let result = writer.runtime.block_on(async {
    let schema = batches[0].schema();
    let batch_iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    lance::Dataset::write(batch_iter, &uri, Default::default()).await
});
```

**Problem**: `Dataset::write()` consumes all batches at once.

### Lance 2.0 API Options

**Option A: DatasetWriter (if available)**
```rust
// Potential approach (needs verification)
let mut writer = lance::DatasetWriter::new(&uri)?;
for batch in batches {
    writer.write_batch(batch)?;
}
writer.close()?;
```

**Option B: Incremental writes with flush**
```rust
// Write first fragment, then append
let dataset = lance::Dataset::create(&uri, schema)?;
dataset.add_batches(batches1)?;
dataset.add_batches(batches2)?;
// Final flush
dataset.commit()?;
```

**Option C: Lance append mode**
```rust
// Write initial fragment, then append more
let mut dataset = lance::Dataset::write(batches1, &uri)?;
let mut appender = dataset.appender()?;
appender.add_batches(batches2)?;
appender.finish()?;
```

---

## Impact Analysis

### Memory Usage Reduction
**Current approach:**
- Holds all 601 batches in memory
- Each batch: ~10,000 rows × 200 bytes/row ≈ 2MB per batch
- Total: 601 × 2MB = ~1.2GB peak memory

**Streaming approach:**
- Holds current batch + encoding buffer
- Peak memory: 2MB (current batch) + 10MB (encoding working set) ≈ 12MB
- **Reduction: 1.2GB → 12MB = 100× reduction**

### Performance Impact

**Potential speedup mechanisms:**

1. **Encoding parallelism**
   - Current: Encode batches 1-601 sequentially (after all generated)
   - Streaming: Encode batch N while generating batch N+1
   - Potential overlap: 10-20% time savings if encoding is IO bound

2. **Memory bandwidth**
   - Current: Peak memory bandwidth = 1.2GB + working set
   - Streaming: Constant memory bandwidth = 12MB + working set
   - Better cache utilization, less memory contention

3. **L1/L2 cache efficiency**
   - Current: Large batch stays in cache, then next batch flushes it
   - Streaming: Single batch fits in L3, hot data stays hot
   - Potential: 2-5% speedup from better cache behavior

4. **System memory pressure**
   - Current: 1.2GB allocation causes page faults, swap risk
   - Streaming: 12MB allocation, minimal paging
   - Potential: 5-10% speedup if current approach causes swapping

**Potential bottleneck:**
- If Lance's `add_batches()` is less efficient than `Dataset::write()`
- Streaming might add per-batch overhead that outweighs benefits
- Needs empirical testing

---

## Implementation Strategy

### Phase 2.0c-Alternative: Zero-Copy Streaming

**Step 1: Investigate Lance API** (30 min)
- Check Lance 2.0 documentation for streaming write capability
- Test `DatasetWriter` or `append` mode if available
- Create proof-of-concept with simple 1000-row test

**Step 2: Implement Streaming Writer** (1-2 hours)
- Modify `LanceWriterHandle` to use streaming API
- Remove batch accumulation vector
- Implement per-batch flush instead of final flush
- Add flush hint to FFI API if needed

**Step 3: Benchmark Streaming vs Accumulation** (1 hour)
- Test small batches (100 rows) - flush overhead visible
- Test medium batches (10K rows) - current baseline
- Test large batches (100K rows) - memory/cache impact
- Measure: throughput + memory usage + encoding time breakdown

**Step 4: Optimize Flush Strategy** (1 hour)
- Test different flush frequencies: every batch, every 10, every 100
- Find optimal balance between:
  - Encoding parallelism benefit
  - Flush overhead cost
  - Memory bandwidth pressure

---

## Comparison with C++ Approach

### Why C++ is Already Efficient
**C++ data generation uses:**
```cpp
const size_t batch_size = 10000;
size_t rows_in_batch = 0;

auto append_callback = [&](const void* row) {
    append_row_to_builders(...);
    rows_in_batch++;

    if (rows_in_batch >= batch_size) {
        auto batch = finish_batch(schema, builders, rows_in_batch);
        writer->write_batch(batch);  // ← Write immediately, don't accumulate
        reset_builders(builders);
        rows_in_batch = 0;
    }
};
```

**Key insight**: C++ writes each batch immediately without accumulating.

**But Rust currently:**
1. Accepts batch via FFI
2. Stores in memory vector
3. Only writes when close() called

**Mismatch**: C++ is streaming-ready, Rust is batch-accumulating!

---

## Risk Assessment

### Risks of Streaming Approach

**Risk 1: Lance API compatibility**
- Lance 2.0 might not have efficient streaming API
- `DatasetWriter` might not exist
- `append()` mode might be inefficient
- **Mitigation**: Prototype first before committing

**Risk 2: Fragment creation overhead**
- Each batch = new Lance fragment?
- Fragments have metadata overhead
- Final metadata merge could be expensive
- **Mitigation**: Test fragment count impact

**Risk 3: Performance regression**
- Streaming overhead > memory saving benefits
- Cache effects negative on small batches
- Per-batch flush overhead dominates
- **Mitigation**: Benchmark before/after carefully

**Risk 4: Complexity increase**
- Streaming requires more state management
- Error handling in middle of write more complex
- **Mitigation**: Start with simple version

---

## Decision Framework

### Should We Do Streaming?

**YES if:**
- Lance 2.0 has efficient streaming API
- Memory usage is actually problematic (it's not at SF=1, might be at SF=100+)
- Profiling shows memory bandwidth as bottleneck
- Streaming speedup > 2-3% to justify complexity

**NO if:**
- Lance requires all-at-once write (no good append API)
- Streaming adds per-batch overhead that negates benefits
- Memory usage isn't a problem for reasonable scale factors
- Complexity > benefit

**MAYBE if:**
- Lance has streaming but with caveats
- Benefit is 2-3% speedup but risks are moderate
- Can defer to Phase 2.1 after current optimizations

---

## Recommendation

### Near-term (Phase 2.0c): Stay with Current Architecture
- Current batch accumulation is working correctly
- Profiling identified real bottleneck: encoding overhead (22% CPU)
- Optimization focus should be on encoding efficiency, not memory/streaming
- Memory usage is not the bottleneck

**Why**: The profiling data shows:
- Encoding operations dominate (XXH3, HyperLogLog, strategy selection)
- Memory operations are 17.57% (not high)
- Main thread generation is only 7.14% (not a bottleneck)

### Medium-term (Phase 2.1): Investigate Streaming IF Needed
- After Phase 2.0c optimizations, re-profile
- If memory bandwidth becomes bottleneck, revisit streaming
- If Lance 2.1 adds better streaming API, reconsider
- Large scale factors (SF=100+) might benefit from streaming

---

## What Should Be Done Now

### For Phase 2.0c (Encoding Optimization)
Focus on the identified bottlenecks:
1. **Batch size tuning** - affects encoding frequency
2. **Statistics optimization** - reduce HyperLogLog/XXH3 overhead
3. **Encoding strategy** - simplify column encoding decisions
4. **Async tuning** - better parallelism between main + Tokio

These will likely provide 12-27% speedup without architectural changes.

### For Phase 2.0c-Alternative (If Pursuing Streaming)
Only do this IF Lance 2.0 has clean streaming API:
1. Prototype DatasetWriter or append mode
2. Benchmark memory usage reduction
3. Measure throughput impact
4. Document findings

---

## Conclusion

**Zero-copy streaming is architecturally sound** but:
1. Current bottleneck is encoding overhead (22%), not memory/streaming
2. Encoding optimization (Phase 2.0c) should be priority
3. Streaming should be Phase 2.1 decision after Phase 2.0c complete

**Memory usage analysis:**
- Current: ~1.2GB peak (acceptable for SF=1)
- Streaming: ~12MB peak (great for large scale factors)
- Worth revisiting for SF=100+ scenarios

**Bottom line**: Fix the encoding overhead first (Phase 2.0c), then reconsider streaming for Phase 2.1 if needed.

---

**Analysis Complete**: February 7, 2026
