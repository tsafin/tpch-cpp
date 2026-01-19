# Phase 16 Excel Visualization Guide

**File**: `PHASE16_Results.xlsx` (14 KB)
**Format**: Microsoft Excel 2007+ (.xlsx)
**Created**: 2026-01-20

## 📋 Sheet Overview

### 1. Summary Sheet
**Purpose**: High-level overview of benchmark results

**Contains**:
- Benchmark metadata (date, scale factor, total rows)
- Format comparison table with:
  - Baseline throughput (rows/sec)
  - Zero-copy optimization gain (speedup multiplier)
  - Stability metric (std dev %)
  - Working table count

**Key Metrics**:
| Format | Baseline | Gain | Stability | Tables |
|--------|----------|------|-----------|--------|
| ORC | ~900K r/s | 1.11x | 1.7% | 8 |
| Parquet | ~540K r/s | 1.65x | 18.6% | 8 |

**Color Coding**: Green highlights = better performance

---

### 2. ORC Sheet
**Purpose**: Detailed per-table performance for ORC format

**Columns**:
- **Table**: TPC-H table name (lineitem, orders, customer, etc.)
- **Rows**: Row count at SF=10
- **Baseline (r/s)**: Throughput without optimization
- **Zero-Copy (r/s)**: Throughput with zero-copy optimization
- **True Zero-Copy (r/s)**: Throughput with true zero-copy
- **Zero-Copy Gain**: Speedup multiplier (zero-copy / baseline)
- **Stability (%)**: Coefficient of variation across runs

**Color Coding**:
- **Green**: Speedup > 1.2x
- **Yellow**: Speedup between 1.0x and 1.2x
- **Red**: Speedup < 1.0x or stability > 10%

**Performance Analysis**:
- Large tables (60M+): 1.1x to 1.4x speedup
- Medium tables (1-15M): 1.0x to 1.6x speedup
- Small tables (<100K): Highly variable

---

### 3. Parquet Sheet
**Purpose**: Detailed per-table performance for Parquet format

**Columns**: Same as ORC sheet

**Key Differences from ORC**:
- Higher optimization gains (1.65x average vs 1.11x)
- Higher variance/instability (18.6% vs 1.7%)
- Some catastrophic regressions with true zero-copy
- Baseline 15-25% slower than ORC

**Notable Issues**:
- Customer table with true zero-copy: 77% regression ⚠️
- Part table high variance with zero-copy
- Generally less stable than ORC

---

### 4. ORC vs Parquet Sheet
**Purpose**: Side-by-side format comparison

**Columns**:
- **Table**: TPC-H table name
- **Rows**: Row count
- **ORC Baseline**: ORC throughput without optimization
- **ORC Zero-Copy**: ORC throughput with optimization
- **Parquet Baseline**: Parquet throughput without optimization
- **Parquet Zero-Copy**: Parquet throughput with optimization
- **Winner**: Which format is faster (zero-copy comparison)
- **Difference**: Percentage difference (ORC vs Parquet)

**Color Coding**:
- **Green winner cell**: ORC faster by > 10%
- **Yellow winner cell**: Similar performance (±10%)
- **Percentage positive**: ORC faster
- **Percentage negative**: Parquet faster

**Summary**:
```
ORC wins: Larger speedup multiplier + more stable
Parquet wins: Some specific optimizations show better gains
Overall: ORC recommended for production
```

---

### 5. Stability Analysis Sheet
**Purpose**: Performance variance comparison across all modes

**Rows**:
1. ORC Baseline (std dev)
2. ORC Zero-Copy (std dev)
3. ORC True Zero-Copy (std dev)
4. Parquet Baseline (std dev)
5. Parquet Zero-Copy (std dev)
6. Parquet True Zero-Copy (std dev)

**Columns**:
- **Format**: ORC or Parquet
- **Mode**: Optimization variant
- **Avg Std Dev (%)**: Coefficient of variation
- **Status**: Excellent/Good/Poor rating

**Interpretation**:
- **< 5%**: Excellent (consistent results)
- **5-15%**: Good (acceptable variance)
- **> 15%**: Poor (high variance, unreliable)

**Key Finding**:
```
ORC: 1.2-1.7% variance across all modes (Excellent)
Parquet: 18.6-21.1% variance (Poor - unreliable)
```

---

## 📊 Using the Data

### For Presentations
1. Copy the Summary sheet data for executive overview
2. Use ORC vs Parquet sheet for format decision support
3. Reference Stability Analysis for reliability concerns

### For Technical Analysis
1. ORC sheet shows consistent, predictable performance
2. Parquet sheet highlights optimization potential but stability risks
3. Review per-table gains to understand workload characteristics

### For Deployment Decisions

**Choose ORC when**:
- Raw performance critical (15-25% faster)
- Consistent results needed (benchmarking, SLAs)
- Large tables primary workload (60M+ rows)
- Production deployment

**Choose Parquet when**:
- Optimization gains prioritized (1.65x vs 1.11x)
- Smaller datasets acceptable
- Only use zero-copy mode (avoid true zero-copy)
- Testing environment or non-critical workloads

---

## 🔍 Interpreting Charts

### Performance Charts (Embedded in ORC/Parquet sheets)
- **X-axis**: Table names
- **Y-axis**: Throughput (rows/sec)
- **Bars**: Grouped by optimization mode (baseline, zero-copy, true zero-copy)

**Reading Tips**:
- Taller bars = better performance
- Compare bar heights across modes to see optimization impact
- Outliers (very short bars) = broken/unsupported tables

---

## 🎯 Key Takeaways

| Metric | Winner | Advantage |
|--------|--------|-----------|
| **Raw Speed** | ORC | 17-25% faster |
| **Optimization Gain** | Parquet | 1.65x vs 1.11x (+48%) |
| **Stability** | ORC | 1.7% vs 18.6% variance |
| **Reliability** | ORC | Zero regressions |
| **All Tables** | Tie | Both 8/8 with zero-copy |

---

## 📈 Data Quality Notes

**Complete Data**:
- ✅ ORC baseline: 4 tables (lineitem, partsupp, nation, region)
- ✅ ORC zero-copy: 8 tables (all)
- ✅ ORC true zero-copy: 8 tables (all)
- ✅ Parquet baseline: 4 tables
- ✅ Parquet zero-copy: 8 tables (all)
- ✅ Parquet true zero-copy: 8 tables (all)

**Missing Data**:
- ❌ Parallel execution modes (all 6 variants)
- ❌ Async-IO variants
- ⚠️ Baseline missing 4 tables (orders, customer, part, supplier)

**Success Rate**: 67% (96/144 test cases)

---

## 🔧 How This Was Generated

**Script**: `scripts/create_phase16_excel.py`

**Data Source**: `benchmark-results/phase16_sf10_benchmark/phase16_sf10_results.json`

**Metrics Calculated**:
- Average throughput across 2 runs
- Speedup multipliers (optimized / baseline)
- Coefficient of variation (stability)
- Performance comparisons

**Excel Features**:
- Formatted headers and sections
- Color-coded cells for quick interpretation
- Professional styling
- Embedded performance charts
- Proper number formatting

---

## 🐛 Known Issues with Data

1. **Parquet Customer Table Regression**: 77% performance drop with true zero-copy
   - Run 1: 376K r/s
   - Run 2: 104K r/s
   - Extreme variance indicates possible bug

2. **Parquet High Variance**: 18.6% average std dev
   - Affects reliability of Parquet comparisons
   - May indicate lock contention or scheduling issues

3. **Baseline Table Failures**: orders, customer, part, supplier
   - Workaround: use zero-copy optimization
   - Affects baseline comparison validity

---

## 📎 Related Files

- `PHASE16_README.md` - Comprehensive benchmark overview
- `PHASE16_COMPREHENSIVE_ANALYSIS.md` - Detailed technical analysis
- `PHASE16_SUMMARY.json` - Structured data summary
- `phase16_sf10_benchmark/` - Raw benchmark results directory

