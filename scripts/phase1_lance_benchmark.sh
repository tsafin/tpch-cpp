#!/bin/bash

#=============================================================================
# Lance FFI Phase 1 Benchmarking Script
#
# Verifies that Phase 1 streaming implementation improves performance
# by comparing Lance writer with Parquet writer across different scale factors.
#
# Usage: ./phase1_lance_benchmark.sh [scale_factors]
# Example: ./phase1_lance_benchmark.sh "0.1 1 5"
#=============================================================================

set -e

# Default scale factors if not provided
SCALE_FACTORS="${1:-0.1 1 5}"

# Build configuration
BUILD_DIR="$(cd "$(dirname "$0")/../build" && pwd)"
TPCH_BIN="$BUILD_DIR/tpch_benchmark"
OUTPUT_DIR="$BUILD_DIR/phase1_benchmark_results"

# Ensure binary exists
if [ ! -f "$TPCH_BIN" ]; then
    echo "ERROR: tpch_benchmark binary not found at $TPCH_BIN"
    echo "Please build with: mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTPCH_ENABLE_LANCE=ON .. && cmake --build . -j\$(nproc)"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "=========================================================================="
echo "Lance FFI Phase 1 Benchmarking"
echo "=========================================================================="
echo "Binary: $TPCH_BIN"
echo "Output: $OUTPUT_DIR"
echo "Scale factors: $SCALE_FACTORS"
echo "=========================================================================="
echo ""

# Test each scale factor
for SF in $SCALE_FACTORS; do
    echo "=========================================================================="
    echo "Testing Scale Factor: $SF"
    echo "=========================================================================="

    # Prepare output directories
    LANCE_OUTPUT="$OUTPUT_DIR/lance_sf${SF}.lance"
    PARQUET_OUTPUT="$OUTPUT_DIR/parquet_sf${SF}.parquet"

    rm -rf "$LANCE_OUTPUT" "$PARQUET_OUTPUT"
    mkdir -p "$LANCE_OUTPUT" "$PARQUET_OUTPUT"

    # Benchmark: Lance writer (Phase 1 streaming implementation)
    echo ""
    echo "--- Lance Writer (Phase 1) ---"
    LANCE_LOG="$OUTPUT_DIR/lance_sf${SF}.log"

    /usr/bin/time -v "$TPCH_BIN" \
        --use-dbgen \
        --format lance \
        --output "$LANCE_OUTPUT" \
        --scale-factor "$SF" \
        --table customer \
        2>&1 | tee "$LANCE_LOG"

    # Extract timing
    LANCE_TIME=$(grep "Elapsed (wall clock) time" "$LANCE_LOG" | awk '{print $NF}' | tr -d 's' || echo "N/A")
    LANCE_MAX_RSS=$(grep "Maximum resident set size" "$LANCE_LOG" | awk '{print $NF}' || echo "N/A")

    echo "Lance writer (SF=$SF): Time=$LANCE_TIME, Memory=${LANCE_MAX_RSS}KB"

    # Benchmark: Parquet writer (baseline for comparison)
    echo ""
    echo "--- Parquet Writer (Baseline) ---"
    PARQUET_LOG="$OUTPUT_DIR/parquet_sf${SF}.log"

    /usr/bin/time -v "$TPCH_BIN" \
        --use-dbgen \
        --format parquet \
        --output "$PARQUET_OUTPUT" \
        --scale-factor "$SF" \
        --table customer \
        2>&1 | tee "$PARQUET_LOG"

    # Extract timing
    PARQUET_TIME=$(grep "Elapsed (wall clock) time" "$PARQUET_LOG" | awk '{print $NF}' | tr -d 's' || echo "N/A")
    PARQUET_MAX_RSS=$(grep "Maximum resident set size" "$PARQUET_LOG" | awk '{print $NF}' || echo "N/A")

    echo "Parquet writer (SF=$SF): Time=$PARQUET_TIME, Memory=${PARQUET_MAX_RSS}KB"

    # Verify Lance dataset was created
    if [ -f "$LANCE_OUTPUT/_metadata.json" ]; then
        echo "✓ Lance metadata created successfully"
        LANCE_SIZE=$(du -sh "$LANCE_OUTPUT" | cut -f1)
        echo "  Dataset size: $LANCE_SIZE"
    else
        echo "✗ Lance metadata NOT created - check for errors above"
    fi

    # Verify Parquet output
    if [ -f "$PARQUET_OUTPUT/part-0.parquet" ]; then
        echo "✓ Parquet files created successfully"
        PARQUET_SIZE=$(du -sh "$PARQUET_OUTPUT" | cut -f1)
        echo "  Dataset size: $PARQUET_SIZE"
    else
        echo "✗ Parquet files NOT created - check for errors above"
    fi

    echo ""
done

# Generate comparison report
echo "=========================================================================="
echo "Benchmark Summary"
echo "=========================================================================="
echo ""
echo "All results saved to: $OUTPUT_DIR"
echo ""
echo "Key files:"
find "$OUTPUT_DIR" -maxdepth 1 -name "*.log" | sort | sed 's/^/  /'
echo ""

# Create summary CSV
SUMMARY_CSV="$OUTPUT_DIR/summary.csv"
cat > "$SUMMARY_CSV" << 'EOF'
scale_factor,format,time_seconds,memory_kb,dataset_size
EOF

for SF in $SCALE_FACTORS; do
    LANCE_LOG="$OUTPUT_DIR/lance_sf${SF}.log"
    PARQUET_LOG="$OUTPUT_DIR/parquet_sf${SF}.log"

    if [ -f "$LANCE_LOG" ]; then
        LANCE_TIME=$(grep "Elapsed (wall clock) time" "$LANCE_LOG" | awk '{print $NF}' | tr -d 's' || echo "N/A")
        LANCE_RSS=$(grep "Maximum resident set size" "$LANCE_LOG" | awk '{print $NF}' || echo "N/A")
        LANCE_SIZE=$(du -sh "$OUTPUT_DIR/lance_sf${SF}.lance" 2>/dev/null | cut -f1 || echo "N/A")
        echo "$SF,lance,$LANCE_TIME,$LANCE_RSS,$LANCE_SIZE" >> "$SUMMARY_CSV"
    fi

    if [ -f "$PARQUET_LOG" ]; then
        PARQUET_TIME=$(grep "Elapsed (wall clock) time" "$PARQUET_LOG" | awk '{print $NF}' | tr -d 's' || echo "N/A")
        PARQUET_RSS=$(grep "Maximum resident set size" "$PARQUET_LOG" | awk '{print $NF}' || echo "N/A")
        PARQUET_SIZE=$(du -sh "$OUTPUT_DIR/parquet_sf${SF}.parquet" 2>/dev/null | cut -f1 || echo "N/A")
        echo "$SF,parquet,$PARQUET_TIME,$PARQUET_RSS,$PARQUET_SIZE" >> "$SUMMARY_CSV"
    fi
done

echo "Summary CSV: $SUMMARY_CSV"
cat "$SUMMARY_CSV"
echo ""
echo "=========================================================================="
echo "Benchmarking Complete!"
echo "=========================================================================="
