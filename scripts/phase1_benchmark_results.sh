#!/bin/bash

#=============================================================================
# Lance FFI Phase 1 Benchmark - Performance Verification
#
# Compares Lance writer (Phase 1 streaming) with Parquet writer baseline
# using integer scale factors and multiple tables.
#=============================================================================

set -e

BUILD_DIR="$(cd "$(dirname "$0")/../build" && pwd)"
TPCH_BIN="$BUILD_DIR/tpch_benchmark"
OUTPUT_DIR="$BUILD_DIR/phase1_benchmarks"

SCALE_FACTORS="${1:-1 5}"  # Default: SF=1 and SF=5
TABLES=("customer" "orders" "lineitem")  # Key tables for testing

if [ ! -f "$TPCH_BIN" ]; then
    echo "ERROR: tpch_benchmark not found at $TPCH_BIN"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "=========================================================================="
echo "Phase 1 Lance Writer Benchmarking"
echo "=========================================================================="
echo "Binary: $TPCH_BIN"
echo "Output: $OUTPUT_DIR"
echo "Scale factors: $SCALE_FACTORS"
echo "=========================================================================="
echo ""

# Create summary CSV
SUMMARY_CSV="$OUTPUT_DIR/benchmark_summary.csv"
cat > "$SUMMARY_CSV" << 'EOF'
scale_factor,table,format,time_seconds,throughput_rows_per_sec,dataset_size_mb
EOF

for SF in $SCALE_FACTORS; do
    echo "=========================================================================="
    echo "Scale Factor: $SF"
    echo "=========================================================================="
    echo ""

    for TABLE in "${TABLES[@]}"; do
        echo "Table: $TABLE"

        # Lance writer
        LANCE_OUTPUT="$OUTPUT_DIR/sf${SF}_${TABLE}_lance"
        rm -rf "$LANCE_OUTPUT"

        LANCE_LOG="$OUTPUT_DIR/sf${SF}_${TABLE}_lance.log"
        echo "  Lance writer... " | tr -d '\n'

        START_TIME=$(date +%s%N)

        if "$TPCH_BIN" \
            --use-dbgen \
            --format lance \
            --output-dir "$LANCE_OUTPUT" \
            --scale-factor "$SF" \
            --table "$TABLE" \
            > "$LANCE_LOG" 2>&1; then

            END_TIME=$(date +%s%N)
            ELAPSED=$(echo "scale=3; ($END_TIME - $START_TIME) / 1000000000" | bc)

            # Extract metrics from log
            ROWS=$(grep "Rows written:" "$LANCE_LOG" | awk '{print $NF}')
            THROUGHPUT=$(grep "Throughput:" "$LANCE_LOG" | awk '{print $(NF-1)}')
            SIZE=$(grep "Total size" "$LANCE_LOG" | awk '{print $NF}' | sed 's/bytes//')
            SIZE_MB=$(echo "scale=2; $SIZE / 1048576" | bc)

            echo "✓ (${ELAPSED}s, $THROUGHPUT rows/sec)"
            echo "$SF,$TABLE,lance,$ELAPSED,$THROUGHPUT,$SIZE_MB" >> "$SUMMARY_CSV"

        else
            echo "✗ FAILED"
            tail -5 "$LANCE_LOG"
        fi

        # Parquet writer (baseline)
        PARQUET_OUTPUT="$OUTPUT_DIR/sf${SF}_${TABLE}_parquet"
        rm -rf "$PARQUET_OUTPUT"

        PARQUET_LOG="$OUTPUT_DIR/sf${SF}_${TABLE}_parquet.log"
        echo "  Parquet writer (baseline)... " | tr -d '\n'

        START_TIME=$(date +%s%N)

        if "$TPCH_BIN" \
            --use-dbgen \
            --format parquet \
            --output-dir "$PARQUET_OUTPUT" \
            --scale-factor "$SF" \
            --table "$TABLE" \
            > "$PARQUET_LOG" 2>&1; then

            END_TIME=$(date +%s%N)
            ELAPSED=$(echo "scale=3; ($END_TIME - $START_TIME) / 1000000000" | bc)

            # Extract metrics from log
            ROWS=$(grep "Rows written:" "$PARQUET_LOG" | awk '{print $NF}')
            THROUGHPUT=$(grep "Throughput:" "$PARQUET_LOG" | awk '{print $(NF-1)}')
            SIZE=$(grep "Total size" "$PARQUET_LOG" | awk '{print $NF}' | sed 's/bytes//')
            SIZE_MB=$(echo "scale=2; $SIZE / 1048576" | bc)

            echo "✓ (${ELAPSED}s, $THROUGHPUT rows/sec)"
            echo "$SF,$TABLE,parquet,$ELAPSED,$THROUGHPUT,$SIZE_MB" >> "$SUMMARY_CSV"

        else
            echo "✗ FAILED"
            tail -5 "$PARQUET_LOG"
        fi

        echo ""
    done
done

echo "=========================================================================="
echo "Benchmark Results Summary"
echo "=========================================================================="
echo ""
cat "$SUMMARY_CSV"
echo ""

# Calculate performance comparison
echo "Performance Analysis:"
echo ""

for SF in $SCALE_FACTORS; do
    for TABLE in "${TABLES[@]}"; do
        LANCE_TIME=$(grep "^$SF,$TABLE,lance," "$SUMMARY_CSV" | cut -d, -f4 || echo "N/A")
        PARQUET_TIME=$(grep "^$SF,$TABLE,parquet," "$SUMMARY_CSV" | cut -d, -f4 || echo "N/A")

        if [ "$LANCE_TIME" != "N/A" ] && [ "$PARQUET_TIME" != "N/A" ]; then
            # Calculate ratio (Lance/Parquet)
            RATIO=$(echo "scale=2; $LANCE_TIME / $PARQUET_TIME" | bc)
            echo "SF=$SF, $TABLE: Lance/Parquet ratio = $RATIO"
            if (( $(echo "$RATIO < 1" | bc -l) )); then
                IMPROVEMENT=$(echo "scale=1; (1 - $RATIO) * 100" | bc)
                echo "  ✓ Lance is ${IMPROVEMENT}% faster"
            else
                REGRESSION=$(echo "scale=1; ($RATIO - 1) * 100" | bc)
                echo "  ⚠ Lance is ${REGRESSION}% slower (expected for Phase 1)"
            fi
        fi
    done
done

echo ""
echo "Notes:"
echo "- Phase 1 focuses on streaming, not format optimization"
echo "- Lance/Parquet ratio < 1 indicates Lance is faster"
echo "- Phase 2 will optimize format writing to improve performance"
echo ""
echo "Results saved to: $OUTPUT_DIR"
