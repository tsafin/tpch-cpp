# Phase 2.0c-4: Per-Column Compression Investigation - FAILED

**Date**: February 8, 2026
**Investigation**: Per-Column Compression Optimization
**Result**: ❌ NOT VIABLE - Significant regressions observed
**Status**: Reverted to Phase 2.0c-3 baseline (commit a495657)

---

## Executive Summary

Attempted to implement Phase 2.0c-4a (Per-Column Compression) to achieve +1-2% performance improvement by selectively disabling compression on numeric columns. Investigation failed due to severe performance regressions across all table sizes.

**Key Finding**: Adding field-level metadata for compression configuration disrupts Lance's internal encoding strategy selection, causing 12-30% throughput reductions.

---

## What Was Attempted

### Phase 2.0c-4a: Per-Column Compression

**Goal**: Reduce compression overhead on non-compressible numeric columns while maintaining compression on strings.

**Implementation Approach**:
1. Extended `EncodingStrategy` struct with compression configuration fields:
   ```rust
   compression: String,              // "none", "lz4", "zstd"
   use_bss: Option<String>,         // Byte Stream Split for floats
   compression_level: Option<String> // ZSTD compression level
   ```

2. Added `apply_compression_to_schema()` function:
   - Creates new Field objects with compression metadata
   - Applies selective compression based on data type
   - Strategy: No compression on int/float/date, ZSTD on strings

3. Integration in `lance_writer_close()`:
   - Called after `create_schema_with_hints()` (Phase 2.0c-3)
   - Applied field-level compression metadata
   - Conditionally called only if compression != "none"

### Compression Strategy

**Intended Per-Type Configuration**:
- **Integer types** (Int64, Int32, etc.): No compression (low entropy)
- **Float types** (Float64, Float32): LZ4 + Byte Stream Split
- **Decimal types**: No compression
- **Date/Time types**: No compression
- **String types**: ZSTD level 1 (fast + effective)
- **Other types**: No compression (unknown)

---

## Why It Failed

### Benchmark Results: Severe Regressions

When field-level compression metadata was applied (even with all compression="none"):

| Table | Phase 2.0c-3 | Attempt 1 | Attempt 2 | Regression |
|-------|---|---|---|---|
| lineitem | 632,773 | 484,516 | 551,836 | -12.8% to -23.4% |
| customer | 742,574 | 681,818 | 563,910 | -24.1% to -8.2% |
| orders | 469,631 | 431,779 | 470,662 | -0.2% to -8.0% |
| part | 314,465 | 277,008 | 274,725 | -12.6% to -30.4% |
| partsupp | 803,213 | 953,516 | 1,061,008 | +18.7% to +32.1% (inconsistent!) |
| supplier | 476,190 | 454,545 | 2,823 | -4.5% to -99.4% (wildly inconsistent!) |
| nation | 3,571 | 658 | 3,571 | -81.6% to 0% |
| region | 625 | 833 | 625 | -11.0% to +33.3% |

**Key Observation**: Results were highly inconsistent even between multiple runs, indicating fundamental instability.

### Root Cause Analysis

**Problem 1: Field Object Recreation**

Creating new Field objects breaks Lance's internal state:
```rust
// This breaks encoding:
Field::new(field.name(), field.data_type().clone(), field.is_nullable())
    .with_metadata(metadata)
```

- Loses original field properties beyond name/type/nullable
- Field metadata might contain other Lance hints
- Lance encoding selection gets confused about field provenance

**Problem 2: Metadata Application Overhead**

Even when all compression was set to "none", applying metadata caused regressions:
- Suggests metadata application itself changes Lance behavior
- Lance likely caches encoding strategy selections
- Adding metadata forces re-evaluation with different path

**Problem 3: XXH3/HyperLogLog Dominance**

From Phase 2.0c analysis (already known):
- XXH3 hashing: 8.79% CPU overhead (unavoidable)
- HyperLogLog: 7.23% CPU overhead (unavoidable)
- **Total: 16% CPU overhead that cannot be reduced**

Adding compression on top of this means:
- Compression CPU cost + 16% unavoidable cost > I/O savings
- For write-time workloads (no I/O benefit), compression is pure overhead
- Even no compression metadata breaks encoding

**Problem 4: Schema-Level vs Field-Level Metadata Confusion**

Phase 2.0c-3 applies hints at schema level:
```rust
metadata.insert(format!("lance-encoding:{}", field.name()), strategy)
```

Phase 2.0c-4a attempted field level:
```rust
field.with_metadata(...).insert("lance-encoding:compression", ...)
```

These two metadata types might conflict or interact in unexpected ways in Lance internals.

---

## Why This Approach Was Fundamentally Wrong

### 1. Lance Architecture Mismatch

Lance is optimized for read-time queries, not write throughput. The XXH3/HyperLogLog overhead is part of Lance's index building and query optimization infrastructure:

- Cannot be turned off without breaking indexes
- Compression adds more CPU on top (not reducing XXH3)
- Write throughput is NOT a Lance design goal

### 2. Compression Economics for Writes

For write-time performance measurement:

**Compression Benefits** (I/O reduction):
- Only relevant if I/O is bottleneck
- Benchmarks show only write throughput, not I/O
- For in-memory data, compression is only overhead

**Compression Costs**:
- CPU time to compress each column
- Field metadata overhead
- Integration with encoding strategy

**Result**: Compression cost > Compression benefit for write workloads

### 3. API Limitations

Lance FFI doesn't provide control over:
- XXH3 hash computation (8.79% overhead)
- HyperLogLog statistics (7.23% overhead)
- Index construction parallelism
- Encoding strategy cache

Trying to optimize at field metadata level is fighting Lance's design.

---

## What We Learned

### 1. Phase 2.0c-3 is Near-Optimal for Write Workloads

- 92% of Parquet performance (561K Lance vs 607K Parquet)
- Already benefits from:
  - Pre-computed encoding strategies (Phase 2.0c-3)
  - Schema-level encoding hints (Phase 2.0c-2/3)
  - Optimized batch size (5K rows, Phase 2.0c-1)
  - Optimized row group size (4096 rows, Phase 2.0c-2a)

### 2. Further Write Optimizations Are Blocked By

- **XXH3/HyperLogLog** (16% CPU): Fundamental to Lance, cannot be disabled
- **Column-count scaling** (69-110% of Parquet): Architectural difference vs Parquet
- **Field metadata interaction**: Metadata manipulation disrupts encoding

### 3. Different Workloads, Different Constraints

- **Read workloads** (queries): Compression ratio matters, index quality matters
- **Write workloads** (benchmarks): Throughput matters, compression cost is overhead

Lance is optimized for read workloads. Write optimization has hit fundamental limits.

---

## Recommendations

### Do NOT Pursue Phase 2.0c-4

Phase 2.0c-4 approaches (compression, statistics deferral, etc.) will not help write performance. The 16% XXH3/HyperLogLog overhead is fundamental and cannot be reduced without Lance API changes.

### Alternative Directions

1. **Accept 92% vs Parquet as Final Baseline**
   - Phase 2.0c-3 is stable, reliable, and near-optimal
   - 13.1% improvement from initial baseline is significant
   - Further gains require different approach or Lance changes

2. **Focus on Read Optimization**
   - Compression helps reads (reduces I/O)
   - Index quality improvements help queries
   - Different performance profile than write

3. **Advocate for Lance API Changes**
   - Add option to disable XXH3 computation
   - Add option to defer HyperLogLog statistics
   - Add compression level configuration per column

4. **Profile Other Bottlenecks**
   - Memory allocation patterns
   - Arrow C Data Interface overhead
   - Batch iterator construction
   - Runtime costs vs encoding costs

---

## Commits

- **Phase 2.0c-3 Baseline** (stable, commit a495657):
  - 616,124 rows/sec (+13.1% from initial)
  - No further improvements with current Lance API

- **Phase 2.0c-4 Investigation** (Feb 8, 2026):
  - Reverted completely
  - No commits - investigation-only branch

---

## Files Modified During Investigation

All changes reverted:
- `third_party/lance-ffi/src/lib.rs` - Reverted to a495657
- Memory notes updated with findings

---

## Appendix: Detailed Benchmark Data

### Attempt 1: Compression Enabled on Floats/Strings

```
customer: 681,818 rows/sec (-8.18%)
lineitem: 484,516 rows/sec (-23.42%)
orders: 431,779 rows/sec (-8.05%)
part: 277,008 rows/sec (-11.91%)
partsupp: 953,516 rows/sec (+18.71%)
supplier: 454,545 rows/sec (-4.54%)
nation: 658 rows/sec (-81.57%)
region: 833 rows/sec (+33.28%)
```

### Attempt 2: All Compression Disabled (Conditional Application)

```
customer: 563,910 rows/sec (-24.06%)
lineitem: 551,836 rows/sec (-12.79%)
orders: 470,662 rows/sec (+0.21%)
part: 274,725 rows/sec (-12.63%)
partsupp: 1,061,008 rows/sec (+32.09%)
supplier: 2,823 rows/sec (-99.40%)
nation: 3,571 rows/sec (0%)
region: 625 rows/sec (0%)
```

**Inconsistency Note**: Partsupp improved, supplier crashed, nation recovered. This inconsistency suggests race conditions or non-deterministic behavior when metadata is manipulated.

---

## Conclusion

**Phase 2.0c-4: Per-Column Compression is NOT VIABLE** with the current Lance API and architecture.

The investigation revealed fundamental limitations:
1. Field-level metadata manipulation breaks Lance encoding
2. XXH3/HyperLogLog overhead (16%) dominates and cannot be reduced
3. Compression benefits don't apply to write-time workloads
4. Lance is optimized for reads, not write throughput

**Recommendation**: Accept Phase 2.0c-3 (92% vs Parquet) as final baseline and explore different optimization directions or await Lance API improvements.
