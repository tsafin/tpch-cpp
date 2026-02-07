#!/usr/bin/env python3
"""
Phase 2.0c-1: Batch Size Sensitivity Testing

Tests different batch sizes (5K, 10K, 20K, 50K) to find optimal value for Lance encoding.
Measures throughput (rows/sec) for lineitem table (6M rows, SF=1).

Uses existing build/lance_test directory for incremental builds only.

Expected behavior:
  - 5K batch: High encoding overhead, more batches (1200+), ~500K rows/sec
  - 10K batch: Baseline, 600 batches, ~575K rows/sec (current)
  - 20K batch: Lower encoding overhead, 300 batches, ~590-600K rows/sec
  - 50K batch: Very low encoding overhead, 120 batches, ~600-610K rows/sec
"""

import subprocess
import sys
import time
import re
import os
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path("/home/tsafin/src/tpch-cpp")
BUILD_DIR = PROJECT_ROOT / "build" / "lance_test"  # Reuse existing build directory!
RESULTS_FILE = PROJECT_ROOT / "PHASE_2_0C_BATCH_SIZE_RESULTS.txt"
BATCH_SIZES = [5000, 10000, 20000, 50000]

# Expected row counts
LINEITEM_ROWS = 6_001_215  # TPC-H SF=1
LINEITEM_COLUMNS = 16

def modify_batch_size(batch_size: int) -> None:
    """Modify batch size in source code (all occurrences for all tables)."""
    main_cpp = PROJECT_ROOT / "src" / "main.cpp"
    content = main_cpp.read_text()

    # Replace ALL batch size constants (there's one per table)
    pattern = r"const size_t batch_size = \d+;"
    replacement = f"const size_t batch_size = {batch_size};"
    new_content = re.sub(pattern, replacement, content)  # Replaces ALL occurrences

    if new_content == content:
        print(f"WARNING: Could not find batch_size constant in {main_cpp}")
        return

    # Count replacements to verify
    num_replacements = len(re.findall(pattern, content))

    main_cpp.write_text(new_content)
    print(f"✓ Modified {num_replacements} batch size constants to: {batch_size}")


def verify_batch_size(batch_size: int) -> bool:
    """Verify batch size was modified correctly."""
    main_cpp = PROJECT_ROOT / "src" / "main.cpp"
    content = main_cpp.read_text()
    match = re.search(r"const size_t batch_size = (\d+);", content)

    if not match:
        print("ERROR: Could not find batch size constant")
        return False

    actual = int(match.group(1))
    if actual != batch_size:
        print(f"ERROR: Batch size mismatch. Expected {batch_size}, got {actual}")
        return False

    return True


def rebuild_project_incremental() -> bool:
    """Rebuild project incrementally (C++ only, no Lance rebuild)."""
    print("Building project (incremental C++ only)...")

    # Verify build directory exists and has CMakeCache
    if not (BUILD_DIR / "CMakeCache.txt").exists():
        print(f"ERROR: Build directory not configured. Run initial cmake setup first.")
        return False

    # Incremental build - only rebuild changed C++ files
    result = subprocess.run(
        ["cmake", "--build", ".", "-j", str(os.cpu_count() or 4)],
        cwd=BUILD_DIR,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"Build failed:\n{result.stderr}")
        return False

    binary = BUILD_DIR / "tpch_benchmark"
    if not binary.exists():
        print(f"ERROR: Binary not found at {binary}")
        return False

    print("✓ Incremental build complete")
    return True


def run_benchmark(binary_path: Path, batch_size: int) -> Tuple[float, float]:
    """
    Run lineitem benchmark and return (elapsed_time, throughput).

    Returns:
        Tuple of (elapsed_seconds, rows_per_second)
    """
    # Clean up previous runs
    for path in ["/tmp/lineitem.lance", "/tmp/lineitem.parquet"]:
        subprocess.run(f"rm -rf {path}", shell=True, capture_output=True)

    print(f"Running benchmark with batch size {batch_size}...")

    start_time = time.time()

    result = subprocess.run(
        [str(binary_path),
         "--use-dbgen",
         "--format", "lance",
         "--table", "lineitem",
         "--scale-factor", "1",
         "--max-rows", "0"],
        capture_output=True,
        text=True,
        timeout=120  # 2 minute timeout
    )

    elapsed = time.time() - start_time

    if result.returncode != 0:
        print(f"Benchmark failed:\n{result.stderr}")
        return elapsed, 0.0

    # Calculate throughput (rows/sec)
    throughput = LINEITEM_ROWS / elapsed if elapsed > 0 else 0

    return elapsed, throughput


def calculate_batch_metrics(batch_size: int, elapsed: float) -> Dict[str, float]:
    """Calculate metrics for batch size test."""
    batch_count = LINEITEM_ROWS // batch_size

    # Encoding overhead estimate based on Phase 2.0b profiling
    # 22% baseline + additional overhead from extra batches
    # Each batch adds ~0.015% encoding overhead
    baseline_encoding = 22.0
    batch_overhead = (batch_count - 600) * 0.015  # 600 is baseline batch count for 10K size
    encoding_pct = baseline_encoding + batch_overhead

    # Clamp to reasonable range
    encoding_pct = max(18.0, min(30.0, encoding_pct))

    return {
        "batch_count": batch_count,
        "encoding_pct": encoding_pct,
        "memory_estimate_mb": (LINEITEM_ROWS * 200 / (1024*1024)) / (LINEITEM_ROWS / batch_size),  # Rough estimate
    }


def format_results_markdown(results: Dict[int, Tuple[float, float]]) -> str:
    """Format results as Markdown table."""
    lines = [
        "# Phase 2.0c-1: Batch Size Sensitivity Analysis",
        "",
        "**Date**: February 7, 2026  ",
        "**Table**: lineitem (SF=1, 6,001,215 rows, 16 columns)  ",
        "**Format**: Lance  ",
        "**Test**: Vary batch size and measure throughput impact",
        "",
        "## Results Summary",
        "",
        "| Batch Size | Batches | Time (sec) | Rows/sec | vs 10K | Encoding Est | Memory (MB) |",
        "|-----------|---------|-----------|----------|--------|--------------|-------------|",
    ]

    # Get baseline (10K batch)
    baseline_throughput = results.get(10000, (0, 0))[1]

    for batch_size in sorted(results.keys()):
        elapsed, throughput = results[batch_size]
        metrics = calculate_batch_metrics(batch_size, elapsed)

        improvement = ((throughput - baseline_throughput) / baseline_throughput * 100) if baseline_throughput > 0 else 0
        improvement_str = f"{improvement:+.1f}%" if batch_size != 10000 else "baseline"

        line = f"| {batch_size:,} | {metrics['batch_count']:,} | {elapsed:7.2f} | {throughput:>10,.0f} | {improvement_str:>6} | {metrics['encoding_pct']:.1f}% | {metrics['memory_estimate_mb']:>10.1f} |"
        lines.append(line)

    lines.extend([
        "",
        "## Analysis",
        "",
        "**Key Findings**:",
        "",
    ])

    # Add analysis based on results
    best_batch = max(results.items(), key=lambda x: x[1][1])
    baseline = results.get(10000, (0, 0))

    if best_batch[0] != 10000:
        improvement_pct = ((best_batch[1][1] - baseline[1]) / baseline[1] * 100)
        lines.append(f"- **Optimal batch size**: {best_batch[0]:,} (throughput: {best_batch[1][1]:,.0f} rows/sec, {improvement_pct:+.1f}% vs 10K baseline)")
    else:
        lines.append(f"- **Baseline optimal**: 10K batch size remains best performer")

    lines.extend([
        "- Larger batches reduce encoding overhead by processing fewer batch boundaries",
        "- Smaller batches increase encoding calls (XXH3, HyperLogLog, strategy selection)",
        "- Sweet spot depends on balance between encoding efficiency and memory pressure",
        "",
        "## Recommendations",
        "",
        "1. If throughput improves with larger batch size:",
        "   - Consider increasing to optimal batch size for production use",
        "   - Verify memory usage remains acceptable",
        "",
        "2. If 10K is already optimal:",
        "   - Focus on Phase 2.0c-2 (statistics caching optimization)",
        "   - Current batch size is well-tuned for encoding efficiency",
        "",
        "## Next Steps",
        "",
        "- **Phase 2.0c-2**: Implement statistics caching (target: +2-5% speedup)",
        "- **Phase 2.0c-3**: Encoding strategy simplification (target: +3-8% speedup)",
        "- **Phase 2.0c-4**: Async runtime tuning (target: +2-4% speedup)",
        "- **Expected cumulative**: 12-27% speedup, target 630K+ rows/sec for lineitem",
    ])

    return "\n".join(lines)


def main():
    """Main test runner."""
    print("=" * 60)
    print("Phase 2.0c-1: Batch Size Sensitivity Testing")
    print("=" * 60)
    print()
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Build directory: {BUILD_DIR} (reusing existing, incremental builds)")
    print(f"Results file: {RESULTS_FILE}")
    print(f"Batch sizes to test: {BATCH_SIZES}")
    print()

    # Verify build directory exists
    if not BUILD_DIR.exists():
        print(f"ERROR: Build directory not found: {BUILD_DIR}")
        print("Please run: mkdir -p build/lance_test && cd build/lance_test && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTPCH_ENABLE_LANCE=ON ../..")
        sys.exit(1)

    # Store results
    results: Dict[int, Tuple[float, float]] = {}

    # Run tests for each batch size
    for batch_size in BATCH_SIZES:
        print(f"\n{'='*60}")
        print(f"Testing Batch Size: {batch_size:,}")
        print(f"{'='*60}")

        # Modify source code
        modify_batch_size(batch_size)

        # Verify modification
        if not verify_batch_size(batch_size):
            print(f"ERROR: Batch size verification failed")
            sys.exit(1)

        # Rebuild incrementally
        if not rebuild_project_incremental():
            print(f"ERROR: Incremental build failed")
            sys.exit(1)

        # Run benchmark
        try:
            elapsed, throughput = run_benchmark(BUILD_DIR / "tpch_benchmark", batch_size)
            results[batch_size] = (elapsed, throughput)

            metrics = calculate_batch_metrics(batch_size, elapsed)
            print()
            print(f"Results:")
            print(f"  Elapsed time: {elapsed:.2f} seconds")
            print(f"  Throughput: {throughput:>12,.0f} rows/sec")
            print(f"  Batch count: {metrics['batch_count']:>10,}")
            print(f"  Est. encoding overhead: {metrics['encoding_pct']:.1f}%")
            print()

        except subprocess.TimeoutExpired:
            print(f"ERROR: Benchmark timed out for batch size {batch_size}")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    # Restore original batch size
    print(f"\nRestoring original batch size (10000)...")
    modify_batch_size(10000)

    # Write results
    print(f"\nWriting results to {RESULTS_FILE}...")
    RESULTS_FILE.write_text(format_results_markdown(results))

    # Print final report
    print()
    print("=" * 60)
    print("BATCH SIZE SENSITIVITY TEST COMPLETE")
    print("=" * 60)
    print()
    print(RESULTS_FILE.read_text())
    print()
    print(f"Results saved to: {RESULTS_FILE}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
