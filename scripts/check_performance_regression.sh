#!/bin/bash
# Performance Regression Check for Phase 13 Optimizations
# Fails CI if performance regresses by more than 5%

set -e

# Configuration
BASELINE_THROUGHPUT=510000  # rows/sec (Phase 12.6 baseline)
THRESHOLD=0.95  # 95% of baseline (5% regression allowed)
TPCH_BINARY="${1:-./build/tpch_benchmark}"
RESULTS_DIR="${2:-./benchmark-results}"

echo "========================================"
echo "Phase 13 Performance Regression Check"
echo "========================================"
echo "Binary: $TPCH_BINARY"
echo "Results: $RESULTS_DIR"
echo "Baseline: $BASELINE_THROUGHPUT rows/sec"
echo "Threshold: $(echo "$BASELINE_THROUGHPUT * $THRESHOLD" | bc -l | cut -d. -f1) rows/sec (5% regression allowed)"
echo ""

# Verify binary exists
if [ ! -f "$TPCH_BINARY" ]; then
    echo "❌ Error: Binary not found: $TPCH_BINARY"
    exit 1
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "❌ Error: jq is required but not installed"
    echo "   Install: sudo apt-get install jq"
    exit 1
fi

# Run benchmark suite
echo "Running benchmark suite..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$SCRIPT_DIR/phase13_benchmark.py" "$TPCH_BINARY" "$RESULTS_DIR"

# Check if results file exists
RESULTS_FILE="$RESULTS_DIR/phase13_results.json"
if [ ! -f "$RESULTS_FILE" ]; then
    echo "❌ Error: Results file not found: $RESULTS_FILE"
    exit 1
fi

echo ""
echo "========================================"
echo "Regression Analysis"
echo "========================================"

# Get current best throughput (any optimization)
CURRENT_BEST=$(jq -r 'max_by(.throughput_rows_per_sec) | .throughput_rows_per_sec' "$RESULTS_FILE")
CURRENT_BEST_NAME=$(jq -r 'max_by(.throughput_rows_per_sec) | .name' "$RESULTS_FILE")

# Get baseline throughput (regular path)
CURRENT_BASELINE=$(jq -r '.[] | select(.name | contains("Baseline")) | .throughput_rows_per_sec' "$RESULTS_FILE" | head -1)

# Get zero-copy throughput
ZERO_COPY=$(jq -r '.[] | select(.name | contains("Zero-Copy") and (contains("Parallel") | not)) | .throughput_rows_per_sec' "$RESULTS_FILE" | head -1)

# Calculate minimum acceptable throughput
MIN_ACCEPTABLE=$(echo "$BASELINE_THROUGHPUT * $THRESHOLD" | bc -l | cut -d. -f1)

echo "Current best: $CURRENT_BEST rows/sec ($CURRENT_BEST_NAME)"
echo "Current baseline: $CURRENT_BASELINE rows/sec"
if [ ! -z "$ZERO_COPY" ]; then
    echo "Zero-copy: $ZERO_COPY rows/sec"
fi
echo "Minimum acceptable: $MIN_ACCEPTABLE rows/sec"
echo ""

# Check for regression
if [ $(echo "$CURRENT_BEST < $MIN_ACCEPTABLE" | bc -l) -eq 1 ]; then
    echo "❌ PERFORMANCE REGRESSION DETECTED!"
    echo "   Current best: $CURRENT_BEST rows/sec"
    echo "   Expected: >= $MIN_ACCEPTABLE rows/sec"
    echo "   Regression: $(echo "scale=1; ($MIN_ACCEPTABLE - $CURRENT_BEST) / $MIN_ACCEPTABLE * 100" | bc -l)%"
    exit 1
fi

# Check if we achieved improvement over baseline
if [ ! -z "$CURRENT_BASELINE" ] && [ ! -z "$ZERO_COPY" ]; then
    IMPROVEMENT=$(echo "scale=1; ($ZERO_COPY - $CURRENT_BASELINE) / $CURRENT_BASELINE * 100" | bc -l)
    echo "Zero-copy improvement: ${IMPROVEMENT}%"

    # Target: at least 30% improvement with zero-copy
    TARGET_IMPROVEMENT=30
    if [ $(echo "$IMPROVEMENT >= $TARGET_IMPROVEMENT" | bc -l) -eq 1 ]; then
        echo "✅ Achieved target improvement: ${IMPROVEMENT}% (target: >=${TARGET_IMPROVEMENT}%)"
    else
        echo "⚠️ Below target improvement: ${IMPROVEMENT}% (target: >=${TARGET_IMPROVEMENT}%)"
    fi
fi

echo ""
echo "✅ Performance check PASSED: $CURRENT_BEST rows/sec"
echo "   Speedup vs Phase 12.6 baseline: $(echo "scale=2; $CURRENT_BEST / $BASELINE_THROUGHPUT" | bc -l)x"

# Success
exit 0
