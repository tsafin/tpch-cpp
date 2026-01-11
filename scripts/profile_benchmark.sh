#!/bin/bash
# Profile tpch_benchmark with perf and generate flamegraph
# Usage: ./profile_benchmark.sh [table] [scale_factor] [max_rows]

set -e

BENCHMARK="./build/tpch_benchmark"
PERF_DATA="benchmark.perf.data"
FLAMEGRAPH_DIR="$HOME/FlameGraph"  # Clone from https://github.com/brendangregg/FlameGraph

# Parse args
TABLE="${1:-lineitem}"
SF="${2:-1}"
ROWS="${3:-100000}"
OUTPUT_DIR="${4:-/tmp/tpch-profiling}"

echo "=== Profiling TPC-H Benchmark ==="
echo "Table: $TABLE, SF: $SF, Rows: $ROWS"
echo "Output: $OUTPUT_DIR"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Check if benchmark binary exists
if [ ! -f "$BENCHMARK" ]; then
    echo "❌ Error: Benchmark binary not found at $BENCHMARK"
    echo "   Please build with: mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j4"
    exit 1
fi

# Check if perf is available
if ! command -v perf &> /dev/null; then
    echo "❌ Error: perf not found. Install with: sudo apt-get install linux-tools-generic"
    exit 1
fi

# Check if FlameGraph is available (optional)
if [ ! -d "$FLAMEGRAPH_DIR" ]; then
    echo "⚠️  Warning: FlameGraph not found at $FLAMEGRAPH_DIR"
    echo "   Clone with: git clone https://github.com/brendangregg/FlameGraph.git $FLAMEGRAPH_DIR"
    echo "   Continuing without flamegraph generation..."
    GENERATE_FLAMEGRAPH=0
else
    GENERATE_FLAMEGRAPH=1
fi

# Run perf record
echo ""
echo "Running perf record..."
sudo perf record -F 99 -g -o "$PERF_DATA" \
    "$BENCHMARK" --use-dbgen --table "$TABLE" \
    --scale-factor "$SF" --max-rows "$ROWS" \
    --format parquet --output-dir "$OUTPUT_DIR"

echo "✅ Perf recording completed"

# Generate text report
echo ""
echo "Generating perf report..."
sudo perf report -i "$PERF_DATA" --stdio > "$OUTPUT_DIR/perf_report.txt"
echo "✅ Perf report saved: $OUTPUT_DIR/perf_report.txt"

# Generate annotated source (top 10 functions)
echo ""
echo "Generating annotated report..."
sudo perf annotate -i "$PERF_DATA" --stdio > "$OUTPUT_DIR/perf_annotate.txt" 2>/dev/null || true
echo "✅ Annotated report saved: $OUTPUT_DIR/perf_annotate.txt"

# Generate flamegraph if available
if [ "$GENERATE_FLAMEGRAPH" -eq 1 ]; then
    echo ""
    echo "Generating flamegraph..."
    sudo perf script -i "$PERF_DATA" | \
        "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" | \
        "$FLAMEGRAPH_DIR/flamegraph.pl" > "$OUTPUT_DIR/flamegraph.svg"
    echo "✅ Flamegraph generated: $OUTPUT_DIR/flamegraph.svg"
fi

# Cleanup ownership
sudo chown -R $USER:$USER "$PERF_DATA" "$OUTPUT_DIR"

# Print summary
echo ""
echo "=== Profiling Complete ==="
echo "Results directory: $OUTPUT_DIR"
echo ""
echo "Top hotspots (by CPU %):"
sudo perf report -i "$PERF_DATA" --stdio -n --sort=dso,symbol | head -30

# Cleanup perf data
rm -f "$PERF_DATA"

echo ""
echo "✅ Profiling completed successfully!"
