# Consolidated Performance Benchmarks - All Formats

**Last Updated**: February 20, 2026
**Test System**: WSL2, GCC 11.4, RelWithDebInfo build

## Format Performance Matrix (Lineitem Table)

### Scale Factor Comparison

| Format | SF | Optimization | Throughput (rows/sec) | Notes |
|--------|----|--------------|-----------------------|-------|
| **Parquet** | 1 | Baseline (row-by-row) | 820,398 | Reference baseline |
| **Parquet** | 1 | Zero-copy (batch) | 1,345,895 | +164% improvement |
| **Parquet** | 5 | Baseline (row-by-row) | 460,317 | -44% vs SF=1 |
| **Parquet** | 5 | Zero-copy (batch) | 1,250,982 | +172% improvement |
| **Parquet** | 10 | Baseline | 542,000 | Phase 16 benchmark |
| **Parquet** | 10 | Zero-copy | 824,000 | +52% improvement |
| **ORC** | 10 | Baseline | 727,000 | Phase 16 benchmark |
| **ORC** | 10 | Zero-copy | 993,000 | +36% improvement |
| **Lance** | 1 | Zero-copy (batch) | 642,330 | Phase 2.0c-3 optimized |
| **Lance** | 5 | Zero-copy (batch) | 300,834 | 🔴 2.1× slower than SF=1 |
| **CSV** | - | - | *Not benchmarked* | I/O-bound format |
| **Paimon** | 1 | - | 84,000 (customer) | Limited data available |
| **Iceberg** | - | - | *Not benchmarked* | Uses Parquet backing |

### Key Observations

1. **Parquet Scaling**: Best scaling behavior (-7% from SF=1 to SF=5 with zero-copy)
2. **ORC Raw Performance**: Highest absolute throughput at SF=10 (993K rows/sec)
3. **Lance Scaling Issue**: 2.1× performance degradation from SF=1 to SF=5 (needs investigation)
4. **Paimon**: Very limited data, appears significantly slower than native Parquet

---

## All Tables Performance (SF=10, Phase 16)

### ORC Performance

| Table | Rows | Baseline | Zero-Copy | Speedup | Stability |
|-------|------|----------|-----------|---------|-----------|
| lineitem | 60M | 727K r/s | 993K r/s | **1.36×** | Excellent (2.4% σ) |
| partsupp | 80M | 1.13M r/s | 1.35M r/s | **1.19×** | Excellent (1.4% σ) |
| orders | 15M | ❌ FAIL | 635K r/s | ✅ Fixed | Excellent (2.0% σ) |
| customer | 1.5M | ❌ FAIL | 1.02M r/s | ✅ Fixed | Good (2.8% σ) |
| part | 2M | ❌ FAIL | 412K r/s | ✅ Fixed | Excellent (0.06% σ) |
| supplier | 100K | ❌ FAIL | 1.09M r/s | ✅ Fixed | - |
| nation | 250 | 18.75K r/s | 25K r/s | 1.33× | - |
| region | 50 | 3.75K r/s | 2.08K r/s | 0.56× | - |

**Average Speedup**: 1.11× (excluding tiny tables)
**Stability**: Excellent (1.7% average variance)

### Parquet Performance

| Table | Rows | Baseline | Zero-Copy | Speedup | Stability |
|-------|------|----------|-----------|---------|-----------|
| lineitem | 60M | 542K r/s | 824K r/s | **1.52×** | ⚠️ High (20.9% σ) |
| partsupp | 80M | 940K r/s | 1.11M r/s | **1.18×** | Moderate (9.7% σ) |
| orders | 15M | ❌ FAIL | 631K r/s | ✅ Fixed | Good (7.7% σ) |
| customer | 1.5M | ❌ FAIL | 1.05M r/s | ✅ Fixed | Good (2.5% σ) |
| part | 2M | ❌ FAIL | 322K r/s | ✅ Fixed | ⚠️ Very high (52.2% σ) |
| supplier | 100K | ❌ FAIL | 905K r/s | ✅ Fixed | - |
| nation | 250 | 10.4K r/s | 25K r/s | **2.40×** | - |
| region | 50 | 2.5K r/s | 3.75K r/s | 1.50× | - |

**Average Speedup**: 1.65× (48% higher than ORC)
**Stability**: Poor (18.6% average variance, much less stable than ORC)

---

## Lance Performance (Phase 2.0c-3 Optimizations)

### All 8 Tables (SF=1, optimized with encoding hints)

| Table | Columns | Lance (rows/sec) | Parquet (rows/sec) | Lance % |
|-------|---------|------------------|--------------------|---------|
| customer | 7 | 742,574 | 700,935 | **106%** ✨ |
| supplier | 4 | 476,190 | 434,783 | **110%** ✨ |
| part | 9 | 314,465 | 383,142 | 82% |
| orders | 9 | 469,631 | 602,894 | 78% |
| partsupp | 5 | 803,213 | 1,001,252 | 80% |
| lineitem | 16 | 632,773 | 919,163 | 69% |
| nation | 4 | 3,571 | 12,500 | 29% |
| region | 3 | 625 | 5,000 | 12% |

**Aggregate Performance**: 561K rows/sec vs 607K Parquet = **92%**

**Key Insight**: Lance outperforms Parquet on simple schemas (≤7 columns), but scales poorly with column count.

---

## Format Comparison Summary

### Raw Throughput (Lineitem, SF=10)

| Metric | ORC | Parquet | Lance (SF=1) | Winner |
|--------|-----|---------|--------------|--------|
| **Baseline** | 727K r/s | 542K r/s | N/A | ORC |
| **Zero-Copy** | 993K r/s | 824K r/s | 642K r/s | ORC |
| **Optimization Gain** | 1.36× | 1.52× | N/A | Parquet |
| **Stability** | 1.7% σ | 18.6% σ | Unknown | ORC |

### Format Selection Guide

#### Use ORC When:
- ✅ Maximum raw performance needed (993K rows/sec)
- ✅ Stability and predictability critical (1.7% variance)
- ✅ Large tables (50M+ rows)
- ✅ Benchmarking consistency required

#### Use Parquet When:
- ✅ Broader ecosystem compatibility (Spark, DuckDB, Polars)
- ✅ Higher optimization gains desired (1.65× speedup)
- ✅ Industry standard format required
- ⚠️ Can tolerate higher variance (18.6%)

#### Use Lance When:
- ✅ Simple schemas (≤7 columns) - beats Parquet by 6-10%
- ✅ Native indexing and versioning needed
- ✅ Write-once, read-many workloads
- ❌ Not recommended for wide schemas (16+ columns, only 69% of Parquet)
- ❌ Scaling issues at larger data sizes (SF=5 shows 2.1× degradation)

#### Use Paimon When:
- ⚠️ Very limited performance data available (84K rows/sec on customer)
- ⚠️ Significantly slower than native Parquet
- ℹ️ Use for lakehouse features (metadata, time travel), not raw speed

#### Use Iceberg When:
- ℹ️ Uses Parquet backing format (expect similar performance)
- ✅ Need lakehouse features (schema evolution, partitioning)
- ✅ Spark/Trino/DuckDB compatibility critical

---

## Data Gaps & Future Benchmarking

### Missing Benchmarks:
- ❌ **CSV**: No comprehensive throughput data (known I/O-bound, 32% benefit from async I/O)
- ❌ **Iceberg**: No direct benchmarks (expected: ~Parquet performance)
- ⚠️ **Paimon**: Only one table tested (customer, 84K rows/sec)
- ⚠️ **Lance**: No SF=10 benchmarks, scaling issues at SF=5

### Recommended Additions:
1. CSV benchmarking at multiple scale factors
2. Iceberg end-to-end performance (with metadata overhead)
3. Paimon all-tables benchmark suite
4. Lance SF=10 testing + scaling investigation
5. Async I/O impact across all formats

---

## Performance Targets vs Actual

| Target | Format | Actual | Status |
|--------|--------|--------|--------|
| Lineitem 1M+ rows/sec | Parquet | 1.35M (SF=1) | ✅ Exceeded |
| Lineitem 1M+ rows/sec | ORC | 993K (SF=10) | ⚠️ Close |
| Lineitem 1M+ rows/sec | Lance | 642K (SF=1) | ❌ Below |
| All tables 500K+ avg | Parquet | 607K avg | ✅ Exceeded |
| All tables 500K+ avg | Lance | 561K avg | ✅ Met |

---

## Technical Notes

- **Zero-Copy Optimization**: Critical for all formats (1.36-1.65× speedup)
- **Parallel Mode**: Not working for any format (all failed in Phase 16)
- **Async I/O**: Beneficial for I/O-bound workloads (CSV: +32%, Parquet: +7.8%)
- **ASAN**: Never use for performance benchmarking (30-50% overhead distorts results)
- **Build Type**: Always use `RelWithDebInfo` for accurate measurements

---

**Source Data**:
- Phase 16: `/home/tsafin/src/tpch-cpp/benchmark-results/PHASE16_COMPREHENSIVE_ANALYSIS.md`
- Phase 2.0c-3: `~/.claude/projects/-home-tsafin-src-tpch-cpp/memory/MEMORY.md`
- Zero-copy analysis: Memory notes (Feb 9, 2026)
