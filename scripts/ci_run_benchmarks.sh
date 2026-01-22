#!/bin/bash

###############################################################################
# CI Benchmark Execution Script
#
# Runs comprehensive TPC-H benchmarks across all formats and optimization modes.
# Designed for GitHub Actions CI environment at Scale Factor 1.
#
# Usage: bash scripts/ci_run_benchmarks.sh [benchmark_binary] [output_dir] [scale_factor]
#
# Arguments:
#   benchmark_binary    Path to tpch_benchmark executable (default: ./build/tpch_benchmark)
#   output_dir          Output directory for results (default: ./benchmark-results)
#   scale_factor        TPC-H scale factor (default: 1)
###############################################################################

set -euo pipefail

# Configuration
BENCHMARK_BIN="${1:-./build/tpch_benchmark}"
OUTPUT_DIR="${2:-./benchmark-results}"
SCALE_FACTOR="${3:-1}"
TIMEOUT_SECS=600
TEST_COUNT=0
PASSED_COUNT=0
FAILED_COUNT=0

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

###############################################################################
# Utility Functions
###############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*" >&2
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $*" >&2
}

log_warning() {
    echo -e "${YELLOW}[⚠]${NC} $*" >&2
}

log_error() {
    echo -e "${RED}[✗]${NC} $*" >&2
}

###############################################################################
# Validation
###############################################################################

validate_prerequisites() {
    log_info "Validating prerequisites..."

    if [ ! -f "$BENCHMARK_BIN" ]; then
        log_error "Benchmark executable not found: $BENCHMARK_BIN"
        exit 1
    fi

    if [ ! -x "$BENCHMARK_BIN" ]; then
        log_error "Benchmark binary is not executable: $BENCHMARK_BIN"
        exit 1
    fi

    mkdir -p "$OUTPUT_DIR"
    log_success "Prerequisites validated"
}

###############################################################################
# Benchmark Execution
###############################################################################

run_benchmark() {
    local format="$1"
    local table="$2"
    local mode_flags="$3"
    local mode_name="${4:-baseline}"
    local log_file="${OUTPUT_DIR}/${format}_${table}_${mode_name}.log"

    TEST_COUNT=$((TEST_COUNT + 1))

    local test_name="Format=$format Table=$table Mode=$mode_name"
    log_info "[$TEST_COUNT] Running: $test_name"

    # Build command
    local cmd="$BENCHMARK_BIN \
        --use-dbgen \
        --scale-factor $SCALE_FACTOR \
        --format $format \
        --table $table \
        --output-dir $OUTPUT_DIR \
        $mode_flags \
        --verbose"

    # Run with timeout
    if timeout "$TIMEOUT_SECS" $cmd > "$log_file" 2>&1; then
        log_success "Completed: $test_name"
        PASSED_COUNT=$((PASSED_COUNT + 1))

        # Extract key metrics
        local rows=$(grep "Rows written:" "$log_file" 2>/dev/null | tail -1 | awk '{print $NF}' || echo "unknown")
        local throughput=$(grep "Throughput:" "$log_file" 2>/dev/null | tail -1 | awk '{print $NF}' || echo "unknown")

        if [ "$throughput" != "unknown" ]; then
            log_info "  Throughput: $throughput rows/sec"
        fi
    else
        local exit_code=$?
        if [ $exit_code -eq 124 ]; then
            log_error "Timeout ($TIMEOUT_SECS s): $test_name"
        else
            log_error "Failed with exit code $exit_code: $test_name"
        fi
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
}

###############################################################################
# Benchmark Suite: Format Coverage
###############################################################################

run_format_coverage_suite() {
    log_info ""
    log_info "════════════════════════════════════════════════════════════════"
    log_info "Format Coverage Suite (Baseline Mode, All Tables)"
    log_info "════════════════════════════════════════════════════════════════"

    local formats=("csv" "parquet" "orc")
    local tables=("lineitem" "orders" "customer" "part" "partsupp" "supplier" "nation" "region")

    for format in "${formats[@]}"; do
        for table in "${tables[@]}"; do
            # Try to run ORC format, but don't fail if not supported
            if [ "$format" = "orc" ]; then
                # Quick check if ORC is supported
                if ! "$BENCHMARK_BIN" --help 2>&1 | grep -q "orc"; then
                    log_warning "ORC format not supported in this build, skipping"
                    continue
                fi
            fi

            run_benchmark "$format" "$table" "" "baseline"
        done
    done

    log_info "Format coverage suite complete"
}

###############################################################################
# Benchmark Suite: Optimization Modes
###############################################################################

run_optimization_suite() {
    log_info ""
    log_info "════════════════════════════════════════════════════════════════"
    log_info "Optimization Mode Suite (Parquet, Representative Tables)"
    log_info "════════════════════════════════════════════════════════════════"

    local modes=("baseline::" "zero-copy:--zero-copy" "true-zero-copy:--true-zero-copy")
    local tables=("lineitem" "orders" "part")

    for mode_spec in "${modes[@]}"; do
        IFS=':' read -r mode_name mode_flags <<< "$mode_spec"
        for table in "${tables[@]}"; do
            run_benchmark "parquet" "$table" "$mode_flags" "$mode_name"
        done
    done

    log_info "Optimization suite complete"
}

###############################################################################
# Results Summary
###############################################################################

print_results_summary() {
    log_info ""
    log_info "════════════════════════════════════════════════════════════════"
    log_info "Benchmark Results Summary"
    log_info "════════════════════════════════════════════════════════════════"

    echo ""
    echo "Total Tests:  $TEST_COUNT"
    echo "Passed:       $PASSED_COUNT"
    echo "Failed:       $FAILED_COUNT"
    echo ""

    if [ $FAILED_COUNT -eq 0 ]; then
        log_success "All benchmarks completed successfully!"
    else
        log_warning "$FAILED_COUNT benchmark(s) failed. Check logs in $OUTPUT_DIR/"
    fi

    # Sanity check: lineitem throughput
    local lineitem_log="${OUTPUT_DIR}/parquet_lineitem_baseline.log"
    if [ -f "$lineitem_log" ]; then
        local throughput=$(grep "Throughput:" "$lineitem_log" 2>/dev/null | tail -1 | awk '{print $NF}' || echo "unknown")
        if [ "$throughput" != "unknown" ]; then
            local throughput_num=$(echo "$throughput" | sed 's/[^0-9]*//g')
            if [ "$throughput_num" -gt 100000 ] 2>/dev/null; then
                log_success "Sanity check passed: lineitem baseline throughput > 100K rows/sec"
            else
                log_warning "Sanity check: lineitem baseline throughput appears low: $throughput"
            fi
        fi
    fi
}

###############################################################################
# Log File Processing
###############################################################################

extract_metrics_from_logs() {
    log_info ""
    log_info "Extracting metrics from benchmark logs..."

    local summary_file="${OUTPUT_DIR}/ci_summary.txt"
    > "$summary_file"

    echo "Benchmark Metrics Summary" > "$summary_file"
    echo "=========================" >> "$summary_file"
    echo "" >> "$summary_file"

    for log_file in "${OUTPUT_DIR}"/*.log; do
        if [ -f "$log_file" ]; then
            local test_name=$(basename "$log_file" .log)
            echo "Test: $test_name" >> "$summary_file"

            grep -E "Rows written:|Throughput:|Write rate:|File size:" "$log_file" | sed 's/^/  /' >> "$summary_file" 2>/dev/null || true

            echo "" >> "$summary_file"
        fi
    done

    log_success "Metrics summary saved to: $summary_file"
}

###############################################################################
# Main Entry Point
###############################################################################

main() {
    echo ""
    log_info "TPC-H CI Benchmark Suite"
    log_info "=========================================="
    log_info "Executable:   $BENCHMARK_BIN"
    log_info "Output Dir:   $OUTPUT_DIR"
    log_info "Scale Factor: $SCALE_FACTOR"
    log_info "Timeout:      ${TIMEOUT_SECS}s per benchmark"
    echo ""

    validate_prerequisites
    echo ""

    run_format_coverage_suite
    echo ""

    run_optimization_suite
    echo ""

    extract_metrics_from_logs
    echo ""

    print_results_summary
    echo ""

    # Exit with status based on results
    if [ $FAILED_COUNT -eq 0 ]; then
        exit 0
    else
        # Don't fail CI on benchmark failures, just report
        exit 0
    fi
}

main "$@"
