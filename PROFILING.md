# TPC-H Performance Profiling Guide

This document explains how to use the performance profiling tools added in Phase 13.1.

## Table of Contents

1. [Performance Counters](#performance-counters)
2. [System Profiling with perf](#system-profiling-with-perf)
3. [Flamegraph Visualization](#flamegraph-visualization)
4. [Performance Analysis](#performance-analysis)

---

## Performance Counters

### Overview

Lightweight, inline performance instrumentation built into the codebase. Tracks:
- **Timers**: Measure execution time of code sections
- **Counters**: Track event counts (rows processed, allocations, etc.)

### Enabling Performance Counters

Build with `-DTPCH_ENABLE_PERF_COUNTERS=ON`:

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DTPCH_ENABLE_PERF_COUNTERS=ON \
      -DTPCH_ENABLE_ASYNC_IO=ON \
      ..
make -j4
```

### Usage

Run the benchmark normally - the performance report is automatically printed at the end:

```bash
./build/tpch_benchmark --use-dbgen --table lineitem --scale-factor 1 \
    --max-rows 100000 --format parquet --output-dir /tmp
```

**Example Output**:

```
===============================================================================
Performance Counters Report
===============================================================================

## Timers

Name                                      Total (ms)     Calls    Avg (us)
------------------------------------------------------------------------------
parquet_encode_sync                          139.190         1      139190
arrow_append_lineitem                         57.430    100000           0
parquet_create_table                           0.830         1         830

===============================================================================
```

### Interpreting Results

- **Total (ms)**: Total accumulated time for this operation
- **Calls**: Number of times the operation was executed
- **Avg (us)**: Average time per call in microseconds

**Key Metrics to Watch**:
- `parquet_encode*`: Parquet encoding/compression time (should be <30% of total)
- `arrow_append_*`: Arrow RecordBatch building time (should be <20% of total)
- `parquet_create_table`: Arrow Table creation (usually negligible)

### Overhead

Performance counters have minimal overhead:
- **With counters disabled**: Zero overhead (macros expand to no-ops)
- **With counters enabled**: ~5-10 CPU cycles per timer (negligible for operations >1µs)

**Recommendation**: Enable for development and profiling, disable for production builds.

---

## System Profiling with perf

### Prerequisites

Install Linux perf tools:

```bash
sudo apt-get install linux-tools-generic linux-tools-$(uname -r)
```

### Running the Profiling Script

The `scripts/profile_benchmark.sh` script automates profiling with perf:

```bash
./scripts/profile_benchmark.sh [table] [scale_factor] [max_rows] [output_dir]
```

**Examples**:

```bash
# Profile lineitem generation with SF=1, 100k rows
./scripts/profile_benchmark.sh lineitem 1 100000 /tmp/tpch-profiling

# Profile orders generation with SF=10, 500k rows
./scripts/profile_benchmark.sh orders 10 500000 /tmp/profile-orders
```

### Output Files

The script generates:

1. **perf_report.txt**: Text-based hotspot report
2. **perf_annotate.txt**: Annotated source code with cycle counts
3. **flamegraph.svg**: Interactive visualization (if FlameGraph is installed)

### Example perf_report.txt

```
  23.45%  tpch_benchmark  libarrow.so.2200.0.0   [.] arrow::RecordBatchBuilder::Append
  18.32%  tpch_benchmark  libparquet.so.2200.0.0 [.] parquet::ColumnWriter::WriteBatch
  12.10%  tpch_benchmark  libc-2.31.so           [.] __memcpy_avx_unaligned_erms
   8.67%  tpch_benchmark  tpch_benchmark         [.] append_lineitem_to_builders
   5.43%  tpch_benchmark  libc-2.31.so           [.] malloc
```

**Interpretation**:
- Top function: `arrow::RecordBatchBuilder::Append` (23.45% of CPU time)
- Second: Parquet encoding (18.32%)
- Memory operations (memcpy, malloc) taking 18.1% combined

---

## Flamegraph Visualization

### Setup

Clone Brendan Gregg's FlameGraph repository:

```bash
git clone https://github.com/brendangregg/FlameGraph.git ~/FlameGraph
```

The profiling script will automatically detect and use it.

### Viewing Flamegraphs

Open `flamegraph.svg` in a web browser:

```bash
firefox /tmp/tpch-profiling/flamegraph.svg
```

### Interpreting Flamegraphs

- **X-axis (width)**: Percentage of CPU time
- **Y-axis (height)**: Call stack depth
- **Color**: Different code modules (red=user code, yellow=libs, green=kernel)

**Reading Tips**:
- Wide boxes = hotspots (consume lots of CPU time)
- Click on boxes to zoom into specific call chains
- Search for function names with Ctrl+F

**What to Look For**:
- Wide boxes in `arrow::` namespace → Arrow conversion overhead
- Wide boxes in `parquet::` namespace → Parquet encoding overhead
- Wide boxes in `malloc`/`free` → Memory allocation pressure

---

## Performance Analysis

### Automated Analysis Tool

The `scripts/analyze_performance.py` script categorizes hotspots and generates recommendations:

```bash
./scripts/analyze_performance.py /tmp/tpch-profiling/perf_report.txt
```

### Example Output

```
================================================================================
Performance Analysis Report
================================================================================

## CPU Time by Category

Category                       CPU Time     Impact
--------------------------------------------------------------------------------
arrow_conversion                  35.20% 🟠 HIGH
parquet_encoding                  28.40% 🟠 HIGH
memory_allocation                 18.30% 🟠 HIGH
string_operations                  8.10% 🟡 MEDIUM
dbgen_generation                   5.20% 🟢 LOW
other                              4.80% 🟢 LOW
--------------------------------------------------------------------------------
Total Accounted                  100.00%

## Optimization Recommendations

🟠 Arrow conversion is a HIGH priority optimization target
   ➜ Consider batch append operations
   ➜ Profile individual builder operations

🟠 Memory allocation overhead is CRITICAL (>15% CPU)
   ➜ Implement Phase 13.3: Memory pool optimizations
   ➜ Use arena allocator for temporary buffers
   ➜ Pre-allocate Arrow builders with capacity hints
```

### Understanding Impact Levels

- 🔴 **CRITICAL** (>20%): Immediate optimization target
- 🟠 **HIGH** (10-20%): High priority for optimization
- 🟡 **MEDIUM** (5-10%): Moderate optimization opportunity
- 🟢 **LOW** (<5%): Not a bottleneck

---

## Profiling Best Practices

### 1. Build with Debug Symbols

Always use `RelWithDebInfo` for profiling:

```bash
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
```

This ensures:
- Full optimization (same speed as Release)
- Debug symbols for accurate profiling
- Readable function names in reports

### 2. Use Representative Workloads

Profile with realistic data sizes:

```bash
# Too small - overhead dominates
./scripts/profile_benchmark.sh lineitem 1 1000     # ❌ BAD

# Good - representative of production
./scripts/profile_benchmark.sh lineitem 1 100000   # ✅ GOOD
```

### 3. Run Multiple Samples

Profiling has variance - run 3-5 samples and compare:

```bash
for i in {1..5}; do
    ./scripts/profile_benchmark.sh lineitem 1 100000 /tmp/profile-$i
done
```

### 4. Compare Before/After

Always profile before and after optimizations:

```bash
# Before optimization
./scripts/profile_benchmark.sh lineitem 1 100000 /tmp/baseline
cp /tmp/baseline/flamegraph.svg /tmp/flamegraph-baseline.svg

# After optimization
./scripts/profile_benchmark.sh lineitem 1 100000 /tmp/optimized
cp /tmp/optimized/flamegraph.svg /tmp/flamegraph-optimized.svg

# Compare side-by-side
firefox /tmp/flamegraph-baseline.svg /tmp/flamegraph-optimized.svg
```

---

## Common Profiling Scenarios

### Scenario 1: Identifying CPU Bottlenecks

**Goal**: Find which operations consume the most CPU time

**Steps**:
1. Enable performance counters: `-DTPCH_ENABLE_PERF_COUNTERS=ON`
2. Run benchmark with representative workload
3. Check performance counters report
4. Confirm with system profiling: `./scripts/profile_benchmark.sh`

**Expected Findings** (Phase 12.6 baseline):
- Arrow conversion: 30-40% CPU
- Parquet encoding: 20-30% CPU
- Memory allocation: 10-20% CPU

### Scenario 2: Validating Optimizations

**Goal**: Verify optimization reduced overhead as expected

**Steps**:
1. Profile baseline: `./scripts/profile_benchmark.sh lineitem 1 100000 /tmp/baseline`
2. Apply optimization
3. Profile optimized: `./scripts/profile_benchmark.sh lineitem 1 100000 /tmp/optimized`
4. Compare reports:
   ```bash
   diff /tmp/baseline/perf_report.txt /tmp/optimized/perf_report.txt
   ```

**Success Criteria**:
- Target category reduced by expected percentage
- No regressions in other categories
- Total runtime improved

### Scenario 3: Finding Memory Allocation Hotspots

**Goal**: Identify excessive malloc/free calls

**Steps**:
1. Run: `./scripts/profile_benchmark.sh lineitem 1 100000 /tmp/profile`
2. Check `perf_report.txt` for malloc/free percentages
3. Look for allocation call chains in flamegraph
4. Use analysis tool: `./scripts/analyze_performance.py`

**Optimization Targets**:
- `malloc` + `free` >15% → Implement memory pools (Phase 13.3)
- Repeated small allocations → Use arena allocator
- Temporary buffers → Pre-allocate or use stack memory

---

## Troubleshooting

### Error: "perf not found"

```bash
sudo apt-get install linux-tools-generic linux-tools-$(uname -r)
```

### Error: "Failed to open perf data"

Run with sudo (perf record requires root):

```bash
sudo ./scripts/profile_benchmark.sh lineitem 1 100000
```

### Error: "FlameGraph not found"

Install FlameGraph:

```bash
git clone https://github.com/brendangregg/FlameGraph.git ~/FlameGraph
```

Or run without flamegraph generation (perf report still works).

### Performance Counters Not Showing

Verify build configuration:

```bash
grep "TPCH_ENABLE_PERF_COUNTERS" build/CMakeCache.txt
# Should show: TPCH_ENABLE_PERF_COUNTERS:BOOL=ON
```

Rebuild if needed:

```bash
cd build
cmake -DTPCH_ENABLE_PERF_COUNTERS=ON ..
make -j4
```

---

## References

- **Performance Counters Implementation**: `include/tpch/performance_counters.hpp`
- **Profiling Script**: `scripts/profile_benchmark.sh`
- **Analysis Tool**: `scripts/analyze_performance.py`
- **Phase 13 Plan**: `~/.claude/plans/phase13-performance-optimization.md`

---

**Last Updated**: 2026-01-11 (Phase 13.1 completion)
