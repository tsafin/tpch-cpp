#!/bin/bash

###############################################################################
# TPC-H Benchmark Harness
#
# Comprehensive benchmarking script for TPC-H data generation across scale
# factors, formats, and configurations.
#
# Features:
# - Multi-scale factor benchmarking (SF=1, 10, 100)
# - Format comparison (Parquet, CSV, ORC)
# - Async I/O evaluation (if available)
# - Performance metrics collection (throughput, write rate, memory)
# - Results summary and analysis
#
# Usage:
#   ./scripts/benchmark.sh [options]
#
# Options:
#   --scale-factors <list>   Comma-separated list (default: 1,10,100)
#   --formats <list>         Comma-separated formats (default: parquet,csv)
#   --output-dir <dir>       Output directory for benchmark data (default: ./benchmark-output)
#   --enable-async           Test async I/O variant if available
#   --enable-orc             Include ORC format tests
#   --max-rows <N>           Override max rows per test (default: use scale factor)
#   --verbose                Verbose output
#   --dry-run                Print commands without executing
#   --help                   Show this help message
#
###############################################################################

set -o pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="${PROJECT_ROOT}/build"
EXECUTABLE="${BUILD_DIR}/tpch_benchmark"

# Default options
SCALE_FACTORS="1,10,100"
FORMATS="parquet,csv"
OUTPUT_DIR="./benchmark-output"
ENABLE_ASYNC=false
ENABLE_ORC=false
MAX_ROWS=""
VERBOSE=false
DRY_RUN=false

# Results tracking
declare -a RESULTS
RESULTS_INDEX=0
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

###############################################################################
# Utility Functions
###############################################################################

print_usage() {
    head -24 "$0" | tail -23
}

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

print_separator() {
    echo "═══════════════════════════════════════════════════════════════════════════════" >&2
}

###############################################################################
# Argument Parsing
###############################################################################

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --scale-factors)
                SCALE_FACTORS="$2"
                shift 2
                ;;
            --formats)
                FORMATS="$2"
                shift 2
                ;;
            --output-dir)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --enable-async)
                ENABLE_ASYNC=true
                shift
                ;;
            --enable-orc)
                ENABLE_ORC=true
                shift
                ;;
            --max-rows)
                MAX_ROWS="$2"
                shift 2
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --help)
                print_usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                print_usage
                exit 1
                ;;
        esac
    done
}

###############################################################################
# Pre-flight Checks
###############################################################################

check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check if executable exists
    if [[ ! -f "$EXECUTABLE" ]]; then
        log_error "Executable not found: $EXECUTABLE"
        log_info "Please build the project first: cd $PROJECT_ROOT && cmake -B build && cmake --build build"
        exit 1
    fi

    # Check if it's executable
    if [[ ! -x "$EXECUTABLE" ]]; then
        log_error "File is not executable: $EXECUTABLE"
        exit 1
    fi

    # Check for required commands
    for cmd in /usr/bin/time mkdir; do
        if ! command -v "$cmd" &> /dev/null; then
            log_warning "Command not found: $cmd"
        fi
    done

    log_success "Prerequisites check passed"
}

###############################################################################
# Benchmark Execution
###############################################################################

run_benchmark() {
    local scale_factor="$1"
    local format="$2"
    local async_mode="$3"

    local test_name="SF=${scale_factor} Format=${format}"
    [[ "$async_mode" == "true" ]] && test_name="${test_name} (async)"

    local output_path="${OUTPUT_DIR}/sf_${scale_factor}/${format}"
    [[ "$async_mode" == "true" ]] && output_path="${output_path}_async"

    mkdir -p "$output_path"

    # Build command
    local cmd="${EXECUTABLE} --scale-factor ${scale_factor} --format ${format} --output-dir ${output_path}"

    # Add max rows if specified
    if [[ -n "$MAX_ROWS" ]]; then
        cmd="${cmd} --max-rows ${MAX_ROWS}"
    else
        # Scale max rows with scale factor for consistency
        local max_rows=$((1000 * scale_factor))
        cmd="${cmd} --max-rows ${max_rows}"
    fi

    # Add async flag if requested
    if [[ "$async_mode" == "true" ]]; then
        cmd="${cmd} --async-io"
    fi

    # Add verbose flag
    cmd="${cmd} --verbose"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "$ $cmd"
        return 0
    fi

    log_info "Running test: $test_name"

    if [[ "$VERBOSE" == "true" ]]; then
        log_info "Command: $cmd"
    fi

    # Run with timing
    local start_time=$(date +%s%N)
    local output_file="${output_path}/benchmark.log"

    if eval "$cmd" > "$output_file" 2>&1; then
        local end_time=$(date +%s%N)
        local elapsed_ns=$((end_time - start_time))
        local elapsed_ms=$((elapsed_ns / 1000000))

        # Extract metrics from output
        local rows=$(grep "Rows written:" "$output_file" | awk '{print $NF}')
        local throughput=$(grep "Throughput:" "$output_file" | awk '{print $NF}')
        local write_rate=$(grep "Write rate:" "$output_file" | awk '{print $NF}')
        local file_size=$(grep "File size:" "$output_file" | awk '{print $NF}')

        log_success "Test passed: $test_name"

        # Store results
        RESULTS[$RESULTS_INDEX]="${test_name},${rows},${throughput},${write_rate},${file_size},${elapsed_ms}"
        RESULTS_INDEX=$((RESULTS_INDEX + 1))
        PASSED_TESTS=$((PASSED_TESTS + 1))

        if [[ "$VERBOSE" == "true" ]]; then
            log_info "Rows: $rows | Throughput: $throughput rows/sec | Write rate: $write_rate MB/sec"
        fi

    else
        log_error "Test failed: $test_name (output in $output_file)"

        # Check for known protobuf ABI issue
        if grep -q "libprotobuf.*CHECK failed" "$output_file" 2>/dev/null; then
            log_warning "Known issue: Protobuf ABI mismatch (ORC linked with incompatible protobuf version)"
            log_warning "See docs/ORC_RUNTIME_ISSUE.md for details and solutions"
        fi

        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

###############################################################################
# Benchmark Suite
###############################################################################

run_benchmark_suite() {
    print_separator
    log_info "Starting TPC-H Benchmark Suite"
    print_separator

    # Prepare output directory
    mkdir -p "$OUTPUT_DIR"
    log_info "Output directory: ${OUTPUT_DIR}"

    # Convert comma-separated lists to arrays
    IFS=',' read -ra SF_ARRAY <<< "$SCALE_FACTORS"
    IFS=',' read -ra FMT_ARRAY <<< "$FORMATS"

    # Calculate total tests
    local total=$((${#SF_ARRAY[@]} * ${#FMT_ARRAY[@]}))
    [[ "$ENABLE_ASYNC" == "true" ]] && total=$((total * 2))
    log_info "Scheduling $total test(s)"

    print_separator

    # Run benchmarks for each combination
    for scale_factor in "${SF_ARRAY[@]}"; do
        for format in "${FMT_ARRAY[@]}"; do
            # Sync version
            run_benchmark "$scale_factor" "$format" "false"

            # Async version if requested
            if [[ "$ENABLE_ASYNC" == "true" ]]; then
                # Check if async is supported
                if "$EXECUTABLE" --help 2>&1 | grep -q "async-io"; then
                    run_benchmark "$scale_factor" "$format" "true"
                else
                    log_warning "Async I/O not available in this build (use CMake option TPCH_ENABLE_ASYNC_IO)"
                fi
            fi
        done
    done

    # Test ORC if enabled and available
    if [[ "$ENABLE_ORC" == "true" ]]; then
        if "$EXECUTABLE" --help 2>&1 | grep -q "orc"; then
            log_info "Testing ORC format..."
            for scale_factor in "${SF_ARRAY[@]}"; do
                run_benchmark "$scale_factor" "orc" "false"
            done
        else
            log_warning "ORC format not available in this build (requires Apache ORC library)"
        fi
    fi

    print_separator
}

###############################################################################
# Results Analysis
###############################################################################

print_results_summary() {
    print_separator
    log_info "Benchmark Results Summary"
    print_separator

    if [[ $TOTAL_TESTS -eq 0 ]]; then
        log_warning "No tests were executed"
        return
    fi

    # Print header
    printf "%-40s %12s %15s %12s %12s\n" "Test" "Rows" "Throughput" "Write Rate" "Time (ms)"
    print_separator

    # Print results
    for result in "${RESULTS[@]}"; do
        IFS=',' read -r test rows throughput write_rate file_size elapsed_ms <<< "$result"
        printf "%-40s %12s %15s %12s %12s\n" "$test" "$rows" "$throughput" "$write_rate" "$elapsed_ms"
    done

    print_separator

    # Print summary statistics
    if [[ $PASSED_TESTS -gt 0 ]]; then
        local avg_throughput=0
        local avg_write_rate=0
        local test_count=0

        for result in "${RESULTS[@]}"; do
            IFS=',' read -r test rows throughput write_rate file_size elapsed_ms <<< "$result"
            # Extract numeric values (remove 'rows/sec' and 'MB/sec')
            throughput_val="${throughput%% *}"
            write_rate_val="${write_rate%% *}"

            if [[ "$throughput_val" =~ ^[0-9]+$ ]]; then
                avg_throughput=$((avg_throughput + throughput_val))
                test_count=$((test_count + 1))
            fi
        done

        if [[ $test_count -gt 0 ]]; then
            avg_throughput=$((avg_throughput / test_count))
            log_success "Average throughput across all tests: $avg_throughput rows/sec"
        fi
    fi

    log_info "Tests: $PASSED_TESTS passed, $FAILED_TESTS failed (Total: $TOTAL_TESTS)"

    if [[ $FAILED_TESTS -eq 0 ]]; then
        log_success "All tests passed!"
    else
        log_warning "Some tests failed. Check output files in ${OUTPUT_DIR}/"
    fi
}

###############################################################################
# Cleanup
###############################################################################

cleanup() {
    log_info "Cleaning up..."
    # Could add cleanup logic here if needed
}

trap cleanup EXIT

###############################################################################
# Main Entry Point
###############################################################################

main() {
    parse_arguments "$@"

    echo ""
    log_info "TPC-H Benchmark Harness"
    log_info "=========================================="

    if [[ "$DRY_RUN" == "true" ]]; then
        log_warning "DRY RUN MODE - No tests will be executed"
        echo ""
    fi

    check_prerequisites
    echo ""

    run_benchmark_suite
    echo ""

    print_results_summary
    echo ""

    # Exit with appropriate code
    if [[ $FAILED_TESTS -gt 0 ]]; then
        exit 1
    fi
    exit 0
}

main "$@"
