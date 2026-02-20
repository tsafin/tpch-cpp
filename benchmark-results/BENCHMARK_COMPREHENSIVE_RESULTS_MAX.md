# Comprehensive Multi-Format Benchmark Results

**Date**: 2026-02-20 02:42:30

**Scale Factor**: 5

**Tables Tested**: lineitem, customer, orders, partsupp, part, supplier

**Formats Tested**: PARQUET, ORC, CSV, LANCE, PAIMON, ICEBERG

**Runs per Benchmark**: 3

**Optimization**: Zero-copy enabled (recommended mode)


## Consolidated Performance Table (SF=5, Zero-Copy Enabled, Maximum Values)

| Format | lineitem | customer | orders | partsupp | part | supplier | Avg Overall |
|--------|--------|--------|--------|--------|--------|--------|-------------|
| PARQUET | 944,132  | 966,495 ⚠️ | 567,494  | 1,110,186 ⚠️ | 396,983 ⚠️ | 641,026  | **771,053** |
| ORC | 973,893  | 997,340  | 622,252  | 1,371,272 ⚠️ | 422,476  | 1,041,667  | **904,817** |
| CSV | 329,824 ⚠️ | 191,034 ⚠️ | 297,030 ⚠️ | 350,939 ⚠️ | 213,538 ⚠️ | 28,588 ⚠️ | **235,159** |
| LANCE | 130,647 ⚠️ | 1,308,901 ⚠️ | 73,204 ⚠️ | 154,613 ⚠️ | 422,297 ⚠️ | 92,251 ⚠️ | **363,652** |
| PAIMON | 124,716  | 1,082,251 ⚠️ | 277,778 ⚠️ | 101,618 ⚠️ | 373,692 ⚠️ | 531,915 ⚠️ | **415,328** |
| ICEBERG | 235,531 ⚠️ | 1,001,335 ⚠️ | 204,968 ⚠️ | 135,217 ⚠️ | 286,287  | 245,098  | **351,406** |

*All values in rows/second. ⚠️ indicates >15% variance between runs.*


## Format Rankings (by Overall Average)

1. 🥇 **ORC**: 904,817 rows/sec

2. 🥈 **PARQUET**: 771,053 rows/sec

3. 🥉 **PAIMON**: 415,328 rows/sec

4.    **LANCE**: 363,652 rows/sec

5.    **ICEBERG**: 351,406 rows/sec

6.    **CSV**: 235,159 rows/sec


## Detailed Performance Metrics


### Lineitem Table

| Format | Maximum | Min | Avg | StdDev % | Stability |
|--------|---------|-----|-----|----------|-----------|
| PARQUET | 944,132 | 936,382 | 940,571 | 0.8% | Excellent ✅ |
| ORC | 973,893 | 917,846 | 941,262 | 6.0% | Good |
| CSV | 329,824 | 242,381 | 299,849 | 29.2% | Moderate ⚠️ |
| LANCE | 130,647 | 85,657 | 108,152 | 41.6% | Poor ❌ |
| PAIMON | 124,716 | 113,624 | 117,510 | 9.4% | Good |
| ICEBERG | 235,531 | 167,858 | 202,368 | 33.4% | Poor ❌ |

### Customer Table

| Format | Maximum | Min | Avg | StdDev % | Stability |
|--------|---------|-----|-----|----------|-----------|
| PARQUET | 966,495 | 789,474 | 871,692 | 20.3% | Moderate ⚠️ |
| ORC | 997,340 | 877,193 | 950,790 | 12.6% | Good |
| CSV | 191,034 | 160,462 | 175,373 | 17.4% | Moderate ⚠️ |
| LANCE | 1,308,901 | 502,344 | 1,017,656 | 79.3% | Poor ❌ |
| PAIMON | 1,082,251 | 247,525 | 603,824 | 138.2% | Poor ❌ |
| ICEBERG | 1,001,335 | 151,730 | 478,909 | 177.4% | Poor ❌ |

### Orders Table

| Format | Maximum | Min | Avg | StdDev % | Stability |
|--------|---------|-----|-----|----------|-----------|
| PARQUET | 567,494 | 507,614 | 542,885 | 11.0% | Good |
| ORC | 622,252 | 586,258 | 599,054 | 6.0% | Good |
| CSV | 297,030 | 231,782 | 274,080 | 23.8% | Moderate ⚠️ |
| LANCE | 73,204 | 37,607 | 59,310 | 60.0% | Poor ❌ |
| PAIMON | 277,778 | 86,271 | 176,058 | 108.8% | Poor ❌ |
| ICEBERG | 204,968 | 62,589 | 149,702 | 95.1% | Poor ❌ |

### Partsupp Table

| Format | Maximum | Min | Avg | StdDev % | Stability |
|--------|---------|-----|-----|----------|-----------|
| PARQUET | 1,110,186 | 406,669 | 870,915 | 80.8% | Poor ❌ |
| ORC | 1,371,272 | 988,142 | 1,238,143 | 30.9% | Poor ❌ |
| CSV | 350,939 | 265,816 | 302,470 | 28.1% | Moderate ⚠️ |
| LANCE | 154,613 | 69,566 | 98,703 | 86.2% | Poor ❌ |
| PAIMON | 101,618 | 70,869 | 88,949 | 34.6% | Poor ❌ |
| ICEBERG | 135,217 | 103,985 | 116,252 | 26.9% | Moderate ⚠️ |

### Part Table

| Format | Maximum | Min | Avg | StdDev % | Stability |
|--------|---------|-----|-----|----------|-----------|
| PARQUET | 396,983 | 174,703 | 274,993 | 80.8% | Poor ❌ |
| ORC | 422,476 | 418,060 | 420,117 | 1.1% | Excellent ✅ |
| CSV | 213,538 | 164,690 | 192,240 | 25.4% | Moderate ⚠️ |
| LANCE | 422,297 | 156,128 | 279,977 | 95.1% | Poor ❌ |
| PAIMON | 373,692 | 263,713 | 301,381 | 36.5% | Poor ❌ |
| ICEBERG | 286,287 | 249,128 | 272,740 | 13.6% | Good |

### Supplier Table

| Format | Maximum | Min | Avg | StdDev % | Stability |
|--------|---------|-----|-----|----------|-----------|
| PARQUET | 641,026 | 568,182 | 614,040 | 11.9% | Good |
| ORC | 1,041,667 | 1,020,408 | 1,027,494 | 2.1% | Excellent ✅ |
| CSV | 28,588 | 16,972 | 23,634 | 49.1% | Poor ❌ |
| LANCE | 92,251 | 61,881 | 80,113 | 37.9% | Poor ❌ |
| PAIMON | 531,915 | 312,500 | 425,150 | 51.6% | Poor ❌ |
| ICEBERG | 245,098 | 217,391 | 229,921 | 12.1% | Good |

## Key Findings

- **Fastest Format**: ORC (904,817 rows/sec average)

- **Most Stable**: ORC (9.8% average variance)


**Note**: All performance values show MAXIMUM throughput achieved across 3 runs.
