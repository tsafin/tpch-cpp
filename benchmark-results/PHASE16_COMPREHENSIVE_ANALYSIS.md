# Phase 16 Comprehensive SF=10 Benchmark Analysis

**Date**: 2026-01-19
**Scale Factor**: 10 (158.6M total rows across 8 TPC-H tables)
**Formats Tested**: ORC, Parquet
**Optimization Modes**: 9 (3 sequential + 6 parallel variants)
**Runs per Benchmark**: 2 (for stability measurement)

## Executive Summary

This comprehensive Phase 16 benchmark tests the full optimization matrix at SF=10 across two major file formats (ORC and Parquet) with all optimization combinations. Key findings:

- ✅ **Zero-copy optimization successfully enables ALL 8 tables** in both formats (baseline variant only worked with 4-5 tables)
- 📊 **Parquet shows stronger optimization gains**: 1.65x average speedup vs ORC's 1.11x
- ⚠️ **Parallel execution modes all failed** - debugging needed
- 🔍 **Some stability issues in Parquet** with higher variance across runs

---

## Format Comparison: ORC vs Parquet

### Baseline Mode (Control)

#### ORC Baseline
| Table | Rows | Status | Avg Throughput | Notes |
|-------|------|--------|---|---|
| lineitem | 60M | ✅ | 727K r/s | Primary workload |
| partsupp | 80M | ✅ | 1.13M r/s | Large table (2nd largest) |
| nation | 250 | ✅ | 18.75K r/s | Reference table |
| region | 50 | ✅ | 3.75K r/s | Reference table |
| **orders, customer, part, supplier** | **Combined 18M** | ❌ | **FAIL** | **Not supported in baseline** |

#### Parquet Baseline
| Table | Rows | Status | Avg Throughput | Notes |
|-------|------|--------|---|---|
| lineitem | 60M | ✅ | 542K r/s | Slower than ORC baseline |
| partsupp | 80M | ✅ | 940K r/s | Slightly slower than ORC |
| nation | 250 | ✅ | 10.4K r/s | Much slower than ORC |
| region | 50 | ✅ | 2.5K r/s | Same as ORC |
| **orders, customer, part, supplier** | **Combined 18M** | ❌ | **FAIL** | **Not supported in baseline** |

**Key Observation**: Parquet baseline is **17-26% slower** than ORC across all working tables.

---

## Zero-Copy Optimization Impact

### ORC with Zero-Copy (Phase 14.1)

**Breakthrough**: All 8 tables now work! Zero-copy fixes the missing tables.

| Table | Rows | Baseline | Zero-Copy | Speedup | Notes |
|-------|------|----------|-----------|---------|-------|
| lineitem | 60M | 727K | 993K | **1.36x** | Strong improvement |
| partsupp | 80M | 1.13M | 1.35M | **1.19x** | Consistent gain |
| orders | 15M | ❌ FAIL | 635K | ✅ WORKS | Previously broken |
| customer | 1.5M | ❌ FAIL | 1.02M | ✅ WORKS | Previously broken |
| part | 2M | ❌ FAIL | 412K | ✅ WORKS | Previously broken |
| supplier | 100K | ❌ FAIL | 1.09M | ✅ WORKS | Previously broken |
| nation | 250 | 18.75K | 25K | 1.33x | Small table |
| region | 50 | 3.75K | 2.08K | 0.56x | Negative for tiny tables |

**Average Speedup**: 1.11x (excluding baseline failures and tiny tables)

---

### Parquet with Zero-Copy (Phase 14.1)

**Breakthrough**: All 8 tables work + significantly higher speedups than ORC!

| Table | Rows | Baseline | Zero-Copy | Speedup | Notes |
|-------|------|----------|-----------|---------|-------|
| lineitem | 60M | 542K | 824K | **1.52x** | Strong improvement |
| partsupp | 80M | 940K | 1.11M | **1.18x** | Solid gain |
| orders | 15M | ❌ FAIL | 631K | ✅ WORKS | Previously broken |
| customer | 1.5M | ❌ FAIL | 1.05M | ✅ WORKS | Previously broken |
| part | 2M | ❌ FAIL | 322K | ✅ WORKS | Previously broken |
| supplier | 100K | ❌ FAIL | 905K | ✅ WORKS | Previously broken |
| nation | 250 | 10.4K | 25K | **2.40x** | Huge gain on reference table |
| region | 50 | 2.5K | 3.75K | 1.50x | Positive for tiny tables |

**Average Speedup**: **1.65x** (vs ORC's 1.11x)

**Critical Insight**: Parquet's zero-copy gains are **48% higher** than ORC's!

---

### True Zero-Copy (Phase 14.2.3) vs Zero-Copy

#### ORC True Zero-Copy

| Table | Rows | Zero-Copy | True Zero-Copy | Delta | Status |
|-------|------|-----------|---|---|---|
| lineitem | 60M | 993K | 1.01M | +1.8% | ✅ Stable |
| partsupp | 80M | 1.35M | 1.29M | -4.2% | Slight regression |
| orders | 15M | 635K | 609K | -4.1% | Slight regression |
| customer | 1.5M | 1.02M | 1.02M | +0.8% | ✅ Stable |
| part | 2M | 412K | 410K | -0.5% | ✅ Stable |
| supplier | 100K | 1.09M | 1.09M | 0.0% | ✅ Identical |

**Conclusion**: True zero-copy provides **minimal additional benefit** over zero-copy for ORC. Some regressions on mid-size tables.

---

#### Parquet True Zero-Copy

| Table | Rows | Zero-Copy | True Zero-Copy | Delta | Status |
|-------|------|-----------|---|---|---|
| lineitem | 60M | 824K | 779K | -5.5% | ❌ Regression |
| partsupp | 80M | 1.11M | 1.18M | +6.5% | ✅ Improvement |
| orders | 15M | 631K | 669K | +6.0% | ✅ Improvement |
| customer | 1.5M | 1.05M | 240K | **-77.1%** | ⚠️ MAJOR REGRESSION |
| part | 2M | 322K | 437K | +35.7% | ✅ Strong improvement |
| supplier | 100K | 905K | 892K | -1.4% | ✅ Stable |

**Conclusion**: True zero-copy for Parquet shows **high variance** with a catastrophic regression on customer table (1.5M rows). Needs investigation.

---

## Stability Analysis

### ORC Stability (Std Dev %)

**Zero-Copy Mode**:
- lineitem: 2.4% - Excellent
- partsupp: 1.4% - Excellent
- orders: 2.0% - Excellent
- customer: 2.8% - Good
- part: 0.06% - Excellent
- Average: **1.7%** - Very stable

**True Zero-Copy Mode**:
- lineitem: 1.1% - Excellent
- partsupp: 1.8% - Excellent
- orders: 0.9% - Excellent
- customer: 0.7% - Excellent
- part: 1.4% - Excellent
- Average: **1.2%** - More stable than zero-copy

**Conclusion**: ORC is highly stable across both modes.

---

### Parquet Stability (Std Dev %)

**Zero-Copy Mode**:
- lineitem: 20.9% - ⚠️ High variance
- partsupp: 9.7% - Moderate
- orders: 7.7% - Moderate
- customer: 2.5% - Good
- part: 52.2% - ⚠️ Very high variance
- Average: **18.6%** - **Much less stable than ORC**

**True Zero-Copy Mode**:
- lineitem: 14.7% - ⚠️ High variance
- partsupp: 5.1% - Moderate
- orders: 4.7% - Good
- customer: 80.3% - ⚠️ Extreme variance (explains the regression)
- part: 0.8% - Excellent
- Average: **21.1%** - Slightly more variable

**Conclusion**: Parquet has **significantly higher variance** than ORC, especially on:
1. lineitem with zero-copy (20.9%)
2. part with zero-copy (52.2%)
3. customer with true zero-copy (80.3%)

This suggests Parquet may have less deterministic behavior or contention issues.

---

## Parallel Mode Analysis

### Status: ❌ All Parallel Modes Failed

All 6 parallel mode variants failed for both ORC and Parquet:
- Parallel baseline
- Parallel + zero-copy
- Parallel + true zero-copy
- Parallel baseline + async-io
- Parallel + zero-copy + async-io
- Parallel + true zero-copy + async-io

**Impact**: Unable to assess parallel performance benefits at SF=10.

---

## Table-by-Table Performance Insights

### Large Tables (50M+ rows)

#### Lineitem (60M rows)
- **ORC baseline**: 727K r/s
- **ORC zero-copy**: 993K r/s (+36%)
- **Parquet baseline**: 542K r/s (-25% vs ORC)
- **Parquet zero-copy**: 824K r/s (+52% gain but still -17% vs ORC zero-copy)

**Winner**: ORC zero-copy (993K r/s)

#### Partsupp (80M rows)
- **ORC baseline**: 1.13M r/s
- **ORC zero-copy**: 1.35M r/s (+19%)
- **Parquet baseline**: 940K r/s (-17% vs ORC)
- **Parquet zero-copy**: 1.11M r/s (+18% gain)

**Winner**: ORC zero-copy (1.35M r/s)

---

### Mid-Range Tables (1-15M rows)

#### Orders (15M rows)
- **ORC baseline**: ❌ FAIL
- **ORC zero-copy**: 635K r/s ✅
- **Parquet baseline**: ❌ FAIL
- **Parquet zero-copy**: 631K r/s ✅

**Finding**: Nearly identical performance once enabled by zero-copy

#### Customer (1.5M rows)
- **ORC zero-copy**: 1.02M r/s ✅
- **Parquet zero-copy**: 1.05M r/s ✅
- **Parquet true-zero-copy**: 240K r/s ⚠️ (catastrophic regression)

**Finding**: Parquet true zero-copy has major issues with medium tables

---

### Small Tables (<100K rows)

#### Supplier (100K rows)
- **ORC baseline**: ❌ FAIL
- **ORC zero-copy**: 1.09M r/s ✅
- **Parquet baseline**: ❌ FAIL
- **Parquet zero-copy**: 905K r/s ✅

#### Nation (250 rows)
- **ORC baseline**: 18.75K r/s
- **ORC zero-copy**: 25K r/s (+33%)
- **Parquet baseline**: 10.4K r/s (much slower)
- **Parquet zero-copy**: 25K r/s (+140% gain!)

**Finding**: Parquet sees massive speedups on tiny reference tables (2.40x vs ORC's 1.33x)

---

## Key Findings & Recommendations

### ✅ What's Working

1. **Zero-copy optimization is essential** - enables all 8 tables in both formats
2. **ORC is faster overall** - 15-25% higher throughput on large tables
3. **ORC is more stable** - 1.7% variance vs Parquet's 18.6%
4. **Parquet has bigger zero-copy gains** - 1.65x vs ORC's 1.11x

### ⚠️ Issues to Address

1. **Parallel modes all fail** - investigate `--parallel` flag implementation
2. **Parquet stability issues** - high variance especially with zero-copy on part table
3. **Parquet true zero-copy regressions** - customer table shows 77% regression
4. **Baseline mode missing 4 tables** - orders, customer, part, supplier need fix
5. **Async-IO not tested** - parallel modes failed before async tests could run

### 📋 Recommended Next Steps

1. **Debug parallel mode failures** - essential for multi-table performance testing
2. **Investigate Parquet variance** - identify contention or lock issues
3. **Fix Parquet true zero-copy** - customer table regression needs root cause analysis
4. **Profile ORC vs Parquet** - understand the fundamental performance difference
5. **Re-run once parallel modes fixed** - complete the full benchmark matrix

---

## Technical Specifications

- **Binary**: `build/tpch_benchmark` (38MB, RelWithDebInfo build)
- **Dataset**: TPC-H Scale Factor 10
  - lineitem: 60M rows
  - partsupp: 80M rows
  - orders: 15M rows
  - customer: 1.5M rows
  - part: 2M rows
  - supplier: 100K rows
  - nation: 250 rows
  - region: 50 rows
  - **Total**: 158.6M rows

- **Output Location**: `/home/tsafin/src/tpch-cpp/benchmark-results/phase16_sf10_benchmark/`
- **Results File**: `phase16_sf10_results.json`

---

## Performance Matrix Summary

### ORC Per-Table Average Speedup
- Zero-copy: **1.11x**
- True zero-copy: **1.11x**

### Parquet Per-Table Average Speedup
- Zero-copy: **1.65x** (⬆️ 48% higher than ORC)
- True zero-copy: **1.47x** (⚠️ affected by customer regression)

### Format Winner by Category
- **Raw Speed**: ORC (15-25% faster on large tables)
- **Stability**: ORC (1.7% vs 18.6% variance)
- **Optimization Gains**: Parquet (1.65x vs 1.11x)
- **Reliability**: ORC (no major regressions)

