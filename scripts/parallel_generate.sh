#!/bin/bash
#
# Parallel TPC-H Data Generation
# Generates all 8 tables concurrently using separate processes
#
# Usage: ./parallel_generate.sh [scale_factor] [format] [output_dir]
#

set -e

# Configuration
SF=${1:-1}
FORMAT=${2:-parquet}
OUTPUT_DIR=${3:-./data}
BENCHMARK_BIN="./build/tpch_benchmark"

# All TPC-H tables in standard order
TABLES=(
    "region"      # Smallest - ~1-10 rows
    "nation"      # Small - ~25 rows
    "supplier"    # Medium - SF * 10,000 rows
    "part"        # Medium - SF * 200,000 rows
    "partsupp"    # Large - SF * 800,000 rows
    "customer"    # Large - SF * 150,000 rows
    "orders"      # Very Large - SF * 1,500,000 rows
    "lineitem"    # Huge - SF * 6,000,000 rows
)

# Validate inputs
if [ ! -f "$BENCHMARK_BIN" ]; then
    echo "Error: $BENCHMARK_BIN not found"
    echo "Please build the project first: mkdir build && cd build && cmake .. && make"
    exit 1
fi

if [ ! -d "$OUTPUT_DIR" ]; then
    mkdir -p "$OUTPUT_DIR"
fi

echo "======================================================================"
echo "Parallel TPC-H Data Generation"
echo "======================================================================"
echo "Scale Factor: $SF"
echo "Format:       $FORMAT"
echo "Output Dir:   $OUTPUT_DIR"
echo "Tables:       ${#TABLES[@]}"
echo "======================================================================"
echo ""

# Track process IDs and table names
declare -a pids
declare -a table_names

# Start time
start_time=$(date +%s.%N)

# Launch all tables in parallel
echo "Launching table generation processes..."
for table in "${TABLES[@]}"; do
    output_file="$OUTPUT_DIR/$table.$FORMAT"

    # For now, print what we would do (actual implementation would use tpch_benchmark)
    echo "  [$(printf '%10s' "$table")] -> $output_file"

    # Launch in background
    $BENCHMARK_BIN --use-dbgen --table "$table" \
        --format "$FORMAT" --scale-factor "$SF" \
        --output-dir "$OUTPUT_DIR" \
        --max-rows 0 > /dev/null 2>&1 &

    pids+=($!)
    table_names+=("$table")
done

echo ""
echo "Waiting for all processes to complete..."
echo ""

# Monitor progress
completed=0
for i in "${!pids[@]}"; do
    pid=${pids[$i]}
    table=${table_names[$i]}

    if wait $pid; then
        completed=$((completed + 1))
        file_size=$(stat -f%z "$OUTPUT_DIR/$table.$FORMAT" 2>/dev/null || echo "unknown")
        printf "[%d/%d] ✓ %s (size: %s)\n" $completed ${#pids[@]} "$table" "$file_size"
    else
        printf "[%d/%d] ✗ %s (failed)\n" $completed ${#pids[@]} "$table"
        exit 1
    fi
done

# Calculate duration
end_time=$(date +%s.%N)
duration=$(echo "$end_time - $start_time" | bc)

echo ""
echo "======================================================================"
echo "All tables generated successfully!"
echo "======================================================================"
echo "Total files: ${#TABLES[@]}"
echo "Duration:   ${duration}s"
echo "Output:     $OUTPUT_DIR"
echo "======================================================================"

# Summary statistics
echo ""
echo "Files generated:"
for table in "${TABLES[@]}"; do
    file="$OUTPUT_DIR/$table.$FORMAT"
    if [ -f "$file" ]; then
        size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null)
        size_mb=$((size / 1024 / 1024))
        printf "  %-12s %8d MB\n" "$table" "$size_mb"
    fi
done
