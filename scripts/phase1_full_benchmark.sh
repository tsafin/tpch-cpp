#!/bin/bash

#=============================================================================
# Lance FFI Phase 1 Full Benchmark
#
# Comprehensive benchmark comparing Lance (Phase 1) vs Parquet
# Tests multiple scale factors with full row generation.
#=============================================================================

set -e

BUILD_DIR="$(cd "$(dirname "$0")/../build" && pwd)"
TPCH_BIN="$BUILD_DIR/tpch_benchmark"
OUTPUT_DIR="$BUILD_DIR/phase1_full_benchmark"

SCALE_FACTORS="${1:-1}"  # Default: SF=1
TABLES=("customer" "orders" "lineitem")

if [ ! -f "$TPCH_BIN" ]; then
    echo "ERROR: tpch_benchmark not found"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "=========================================================================="
echo "Phase 1 Full Benchmark - Lance vs Parquet"
echo "=========================================================================="
echo "Scale factors: $SCALE_FACTORS"
echo "Max rows: All (0 = unlimited)"
echo "=========================================================================="
echo ""

# Create summary CSV
SUMMARY_CSV="$OUTPUT_DIR/results.csv"
cat > "$SUMMARY_CSV" << 'EOF'
scale_factor,table,format,rows_written,time_seconds,throughput_rows_per_sec,size_mb
EOF

for SF in $SCALE_FACTORS; do
    echo "Scale Factor: $SF"
    echo "=========================================================================="

    for TABLE in "${TABLES[@]}"; do
        echo ""
        echo "Table: $TABLE"

        # Lance writer
        echo "  Lance... " | tr -d '\n'
        LANCE_OUTPUT="$OUTPUT_DIR/sf${SF}_${TABLE}_lance"
        rm -rf "$LANCE_OUTPUT"
        LANCE_LOG="$OUTPUT_DIR/sf${SF}_${TABLE}_lance.log"

        START=$(date +%s%N)
        if timeout 180 "$TPCH_BIN" \
            --use-dbgen \
            --format lance \
            --output-dir "$LANCE_OUTPUT" \
            --scale-factor "$SF" \
            --table "$TABLE" \
            --max-rows 0 \
            > "$LANCE_LOG" 2>&1; then

            END=$(date +%s%N)
            ELAPSED=$(echo "scale=3; ($END - $START) / 1000000000" | bc)

            ROWS=$(grep "Rows written:" "$LANCE_LOG" | awk '{print $NF}')
            THROUGHPUT=$(grep "Throughput:" "$LANCE_LOG" | awk '{print $(NF-1)}' | sed 's/rows.*//')
            SIZE=$(grep "Total size" "$LANCE_LOG" | grep -oE '[0-9.]+' | head -1)
            SIZE_MB=$(echo "scale=2; $SIZE / 1048576" | bc)

            echo "✓ (${ELAPSED}s, $ROWS rows, $THROUGHPUT rows/sec)"
            echo "$SF,$TABLE,lance,$ROWS,$ELAPSED,$THROUGHPUT,$SIZE_MB" >> "$SUMMARY_CSV"
        else
            echo "✗ FAILED or TIMEOUT"
            tail -3 "$LANCE_LOG"
        fi

        # Parquet writer (baseline)
        echo "  Parquet... " | tr -d '\n'
        PARQUET_OUTPUT="$OUTPUT_DIR/sf${SF}_${TABLE}_parquet"
        rm -rf "$PARQUET_OUTPUT"
        PARQUET_LOG="$OUTPUT_DIR/sf${SF}_${TABLE}_parquet.log"

        START=$(date +%s%N)
        if timeout 180 "$TPCH_BIN" \
            --use-dbgen \
            --format parquet \
            --output-dir "$PARQUET_OUTPUT" \
            --scale-factor "$SF" \
            --table "$TABLE" \
            --max-rows 0 \
            > "$PARQUET_LOG" 2>&1; then

            END=$(date +%s%N)
            ELAPSED=$(echo "scale=3; ($END - $START) / 1000000000" | bc)

            ROWS=$(grep "Rows written:" "$PARQUET_LOG" | awk '{print $NF}')
            THROUGHPUT=$(grep "Throughput:" "$PARQUET_LOG" | awk '{print $(NF-1)}' | sed 's/rows.*//')
            SIZE=$(grep "Total size" "$PARQUET_LOG" | grep -oE '[0-9.]+' | head -1)
            SIZE_MB=$(echo "scale=2; $SIZE / 1048576" | bc)

            echo "✓ (${ELAPSED}s, $ROWS rows, $THROUGHPUT rows/sec)"
            echo "$SF,$TABLE,parquet,$ROWS,$ELAPSED,$THROUGHPUT,$SIZE_MB" >> "$SUMMARY_CSV"
        else
            echo "✗ FAILED or TIMEOUT"
            tail -3 "$PARQUET_LOG"
        fi
    done

    echo ""
done

# Print results
echo "=========================================================================="
echo "Results Summary"
echo "=========================================================================="
echo ""
cat "$SUMMARY_CSV" | column -t -s,
echo ""

# Calculate improvements
echo "Performance Comparison (Lance vs Parquet):"
echo ""

for SF in $SCALE_FACTORS; do
    echo "Scale Factor: $SF"
    for TABLE in "${TABLES[@]}"; do
        LANCE_LINE=$(grep "^$SF,$TABLE,lance," "$SUMMARY_CSV")
        PARQUET_LINE=$(grep "^$SF,$TABLE,parquet," "$SUMMARY_CSV")

        if [ -n "$LANCE_LINE" ] && [ -n "$PARQUET_LINE" ]; then
            LANCE_TIME=$(echo "$LANCE_LINE" | cut -d, -f5)
            PARQUET_TIME=$(echo "$PARQUET_LINE" | cut -d, -f5)

            RATIO=$(echo "scale=2; $LANCE_TIME / $PARQUET_TIME" | bc)
            IMPROVEMENT=$(echo "scale=1; (1 - $RATIO) * 100" | bc)

            printf "  %10s: " "$TABLE"
            if (( $(echo "$RATIO < 1" | bc -l) )); then
                echo "Lance is ${IMPROVEMENT}% faster"
            else
                REGRESSION=$(echo "scale=1; ($RATIO - 1) * 100" | bc)
                echo "Lance is ${REGRESSION}% slower"
            fi
        fi
    done
    echo ""
done

echo "Results saved to: $OUTPUT_DIR"
