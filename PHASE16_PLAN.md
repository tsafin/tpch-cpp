# Phase 16: Zero-Copy Hang Investigation & Full Optimization Benchmark

## Status: PLANNED

**Date**: January 17, 2026  
**Focus**: Debug and fix zero-copy hang issue, complete full optimizations benchmark

## Summary

Phase 15 successfully benchmarked **all 8 TPC-H tables at SF=10 with complete row counts** (158.6M total rows). However, the zero-copy optimization paths (`--zero-copy`, `--true-zero-copy`) hang when generating full datasets, preventing comparison of optimization strategies.

**Phase 16 goal**: Debug the hang, fix it, and re-run comprehensive benchmark with all optimizations.

## Discovered Issues

### Critical: Zero-Copy Hang with Full Datasets

**Symptoms:**
- Works fine: Baseline (--no-flags) generates 6M SF=1 lineitem in 7.6s ✅
- Works fine: Zero-copy with `--max-rows 1000000` (1M rows) completes in 0.85s ✅
- **HANGS**: Zero-copy with `--max-rows 5000000` (5M rows) - no output, no file created
- **HANGS**: Zero-copy with `--max-rows 0` (full 6M SF=1) - process stuck indefinitely
- **HANGS**: Zero-copy with `--max-rows 6001215` (exact full count) - no file created

**Affected code paths:**
- `generate_lineitem_zero_copy()` in src/main.cpp:294
- `generate_orders_zero_copy()` in src/main.cpp:334
- `generate_customer_zero_copy()` in src/main.cpp:370
- `generate_part_zero_copy()` in src/main.cpp:406
- `generate_partsupp_zero_copy()` in src/main.cpp:442
- Similar for supplier, nation, region

**Root causes to investigate:**
1. **Infinite loop in batch iterator**: `has_next()` always returns true?
2. **Memory allocation failure**: Large batch accumulation causes OOM?
3. **Lock/deadlock in dbgen**: Global state corruption with large row counts?
4. **Buffer overflow in ZeroCopyConverter**: Span creation fails silently?
5. **Arrow builder issue**: Builder not flushed properly for large batches?

## Investigation Plan

### Step 1: Create Debug Build with Instrumentation
- Add debug logging to `LineitemBatchIterator::next()`
- Track `remaining_`, `current_order_`, `batch.size()`
- Log in `ZeroCopyConverter::lineitem_to_recordbatch()`
- Add memory profiling to detect allocations

### Step 2: Reproduce Issue Minimally
- Create minimal test program that calls batch iterator directly
- Test with increasing row limits: 1K → 10K → 100K → 1M → 5M → 6M
- Identify exact threshold where hang occurs

### Step 3: Binary Search for Root Cause
- Check if issue is in iterator or converter
- Test converter directly with pre-generated batches
- Test iterator with no conversion (just counting)

### Step 4: Fix and Validate
- Implement fix (likely memory management or loop condition)
- Run Phase 15 benchmark again with ALL optimizations
- Compare baseline vs zero-copy vs true-zero-copy

## Phase 15 Results (Partial)

Completed with baseline + async-io (zero-copy disabled):

### Full Dataset Performance (SF=10, 158.6M rows)

| Configuration | Total Time | Speedup |
|---------------|-----------|---------|
| Baseline (seq) | 170s | 1.00x |
| Async I/O (seq) | Various (8-11% slower for CPU-bound) | 0.89-1.06x |
| **Parallel baseline** | 219.5s | - |
| **Parallel + Async I/O** | 127.0s | **1.73x** ✅ |

### Async I/O Findings
- **CPU-bound Parquet generation**: Async-io adds overhead (0.89-1.06x)
- **Parallel mode**: Massive improvement with async-io (1.73x) due to concurrent I/O
- Per-table results show async-io benefit varies by table characteristics

## Phase 16 Deliverables

### Code Changes
- [ ] Fix zero-copy hang in dbgen_wrapper.cpp or zero_copy_converter.cpp
- [ ] Add bounds checking or loop guards to prevent infinite iterations
- [ ] Add memory safety checks

### Benchmarking
- [ ] Re-run Phase 15 with all optimizations enabled
- [ ] Benchmark all 12 modes:
  - Baseline
  - Zero-copy (Phase 14.1)
  - True zero-copy (Phase 14.2.3)
  - Async-io
  - All combinations above
  - Parallel variants

### Documentation
- [ ] PHASE16_RESULTS.md with full benchmark comparison
- [ ] Root cause analysis of zero-copy hang
- [ ] Recommendations for further optimization

## Expected Outcomes

**If successful:**
- Zero-copy should show 4-19% speedup over baseline (from Phase 14 research)
- True zero-copy should show up to 60% speedup for numeric-heavy tables (lineitem)
- Async-io benefits for parallel mode
- Complete performance matrix for all 8 TPC-H tables

**If unsuccessful:**
- Document hang as known limitation
- Consider alternative zero-copy implementation for large datasets
- Focus on async-io benefits which are proven

## Estimated Effort
- Investigation: 30-45 minutes
- Fix implementation: 15-30 minutes
- Re-benchmark: 30-60 minutes (depending on binary search speed)
- Documentation: 15-30 minutes

**Total: 1.5-3 hours**

## Success Criteria
1. Zero-copy modes complete without hanging for all 8 TPC-H tables at SF=10
2. No segmentation faults or memory errors
3. Zero-copy speedup validated >= 4% over baseline
4. All results reproducible with 2+ runs
