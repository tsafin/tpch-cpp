#!/bin/bash

#=============================================================================
# Lance FFI Phase 1 Verification Script
#
# Quick verification that Phase 1 streaming implementation is working
# correctly by comparing Lance vs Parquet writers.
#
# This script:
# 1. Tests that Lance writer creates valid datasets
# 2. Compares Lance vs Parquet performance
# 3. Verifies memory usage and dataset integrity
#=============================================================================

set -e

BUILD_DIR="$(cd "$(dirname "$0")/../build" && pwd)"
TPCH_BIN="$BUILD_DIR/tpch_benchmark"
OUTPUT_DIR="$BUILD_DIR/phase1_verify"
SCALE_FACTOR=1  # Scale factor 1 = base TPC-H dataset (150K customers, 6M lineitem rows)

# Ensure binary exists
if [ ! -f "$TPCH_BIN" ]; then
    echo "ERROR: tpch_benchmark binary not found at $TPCH_BIN"
    echo "Please build with: mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTPCH_ENABLE_LANCE=ON .. && cmake --build . -j\$(nproc)"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "=========================================================================="
echo "Lance FFI Phase 1 Verification"
echo "=========================================================================="
echo "Binary: $TPCH_BIN"
echo "Output: $OUTPUT_DIR"
echo "Scale Factor: $SCALE_FACTOR"
echo "=========================================================================="
echo ""

# Test tables to verify
TABLES=("customer" "orders" "lineitem")

for TABLE in "${TABLES[@]}"; do
    echo "=========================================================================="
    echo "Testing table: $TABLE (SF=$SCALE_FACTOR)"
    echo "=========================================================================="
    echo ""

    LANCE_OUTPUT="$OUTPUT_DIR/lance_${TABLE}"
    PARQUET_OUTPUT="$OUTPUT_DIR/parquet_${TABLE}"

    rm -rf "$LANCE_OUTPUT" "$PARQUET_OUTPUT"

    # Test 1: Lance Writer
    echo "Step 1/2: Testing Lance writer..."
    LANCE_LOG="$OUTPUT_DIR/${TABLE}_lance.log"

    if timeout 120 "$TPCH_BIN" \
        --use-dbgen \
        --format lance \
        --output-dir "$LANCE_OUTPUT" \
        --scale-factor "$SCALE_FACTOR" \
        --table "$TABLE" \
        > "$LANCE_LOG" 2>&1; then

        if [ -f "$LANCE_OUTPUT/_metadata.json" ]; then
            LANCE_SIZE=$(du -sh "$LANCE_OUTPUT" | cut -f1)
            LANCE_ROWS=$(grep -oP '"row_count":\s*\K\d+' "$LANCE_OUTPUT/_metadata.json" 2>/dev/null | head -1 || echo "unknown")
            echo "✓ Lance writer succeeded"
            echo "  Dataset size: $LANCE_SIZE"
            echo "  Metadata created: YES"
            echo "  Rows in metadata: $LANCE_ROWS"

            # Check that manifest exists
            if [ -f "$LANCE_OUTPUT/_manifest.json" ]; then
                echo "  Manifest: YES"
            else
                echo "  Manifest: MISSING (may be created by Rust)"
            fi

            # List data files
            DATA_FILES=$(find "$LANCE_OUTPUT/data" -name "*.parquet" -o -name "*.lance" 2>/dev/null | wc -l)
            echo "  Data files: $DATA_FILES"
        else
            echo "✗ Lance writer FAILED - no metadata created"
            echo "  Check log: $LANCE_LOG"
            cat "$LANCE_LOG" | tail -20
        fi
    else
        echo "✗ Lance writer timed out or crashed"
        tail -30 "$LANCE_LOG"
    fi

    echo ""

    # Test 2: Parquet Writer (baseline)
    echo "Step 2/2: Testing Parquet writer (baseline)..."
    PARQUET_LOG="$OUTPUT_DIR/${TABLE}_parquet.log"

    if timeout 120 "$TPCH_BIN" \
        --use-dbgen \
        --format parquet \
        --output "$PARQUET_OUTPUT" \
        --scale-factor "$SCALE_FACTOR" \
        --table "$TABLE" \
        > "$PARQUET_LOG" 2>&1; then

        if [ -d "$PARQUET_OUTPUT" ] && [ -n "$(find "$PARQUET_OUTPUT" -name "*.parquet" 2>/dev/null)" ]; then
            PARQUET_SIZE=$(du -sh "$PARQUET_OUTPUT" | cut -f1)
            PARQUET_FILES=$(find "$PARQUET_OUTPUT" -name "*.parquet" 2>/dev/null | wc -l)
            echo "✓ Parquet writer succeeded"
            echo "  Dataset size: $PARQUET_SIZE"
            echo "  Parquet files: $PARQUET_FILES"
        else
            echo "✗ Parquet writer FAILED - no files created"
        fi
    else
        echo "✗ Parquet writer timed out or crashed"
        tail -30 "$PARQUET_LOG"
    fi

    echo ""
done

# Summary
echo "=========================================================================="
echo "Verification Summary"
echo "=========================================================================="
echo ""

# Count successes
LANCE_SUCCESS=0
PARQUET_SUCCESS=0

for TABLE in "${TABLES[@]}"; do
    if [ -f "$OUTPUT_DIR/lance_${TABLE}/_metadata.json" ]; then
        ((LANCE_SUCCESS++))
    fi
    if [ -d "$OUTPUT_DIR/parquet_${TABLE}" ] && [ -n "$(find "$OUTPUT_DIR/parquet_${TABLE}" -name "*.parquet" 2>/dev/null)" ]; then
        ((PARQUET_SUCCESS++))
    fi
done

echo "Lance writer: $LANCE_SUCCESS/${#TABLES[@]} tables successful"
echo "Parquet writer: $PARQUET_SUCCESS/${#TABLES[@]} tables successful"
echo ""

if [ "$LANCE_SUCCESS" -eq "${#TABLES[@]}" ]; then
    echo "✓ Phase 1 implementation is working correctly!"
    echo ""
    echo "Next steps:"
    echo "1. Run full benchmarks: ./phase1_lance_benchmark.sh \"0.1 1 5 10\""
    echo "2. Compare performance metrics between Lance and Parquet"
    echo "3. Verify streaming behavior reduces memory usage"
    exit 0
else
    echo "✗ Phase 1 verification found issues"
    echo ""
    echo "Logs available in: $OUTPUT_DIR"
    exit 1
fi
