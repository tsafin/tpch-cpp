#!/usr/bin/env python3
"""
Comprehensive Multi-Format Benchmark Script

Benchmarks all supported formats (Parquet, ORC, CSV, Lance, Paimon, Iceberg)
at SF=5 with multiple tables and 3 repetitions.

Usage:
    python3 comprehensive_benchmark.py [--output-dir DIR]
"""

import subprocess
import json
import time
import argparse
import sys
from pathlib import Path
from collections import defaultdict
import re

# Configuration
SCALE_FACTOR = 5
TABLES = ["lineitem", "customer", "orders", "partsupp", "part", "supplier"]
FORMATS = ["parquet", "orc", "csv", "lance", "paimon", "iceberg"]
RUNS = 3
BENCHMARK_BINARY = "./build/tpch_benchmark"

def check_format_support(binary_path):
    """Check which formats are actually supported in the build"""
    try:
        result = subprocess.run(
            [binary_path, "--help"],
            capture_output=True,
            text=True,
            timeout=5
        )
        help_text = result.stdout

        # Extract supported formats from help text
        format_line = [line for line in help_text.split('\n') if '--format' in line]
        if not format_line:
            return []

        # Parse: "--format <format>     Output format: parquet, csv, orc, lance, ..."
        match = re.search(r'Output format:\s*([^\n]+)', format_line[0])
        if match:
            formats_str = match.group(1).strip()
            # Remove "(default: ...)" suffix if present
            formats_str = re.sub(r'\(default:.*\)', '', formats_str)
            supported = [f.strip() for f in formats_str.split(',')]
            return supported
        return []
    except Exception as e:
        print(f"Error checking format support: {e}", file=sys.stderr)
        return []

def run_benchmark(binary, table, format_name, scale_factor, output_dir):
    """Run a single benchmark and return throughput in rows/sec"""
    # Create output directory structure
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    cmd = [
        binary,
        "--use-dbgen",
        "--table", table,
        "--format", format_name,
        "--scale-factor", str(scale_factor),
        "--output-dir", output_dir,
        "--max-rows", "0",  # Generate all rows for the scale factor
        "--zero-copy"  # Always use zero-copy as recommended
    ]

    try:
        start_time = time.time()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        elapsed = time.time() - start_time

        if result.returncode != 0:
            print(f"  ❌ FAILED: {result.stderr[:200]}")
            return None

        # Parse throughput from output
        # Looking for: "Throughput: XXX rows/sec"
        output = result.stdout + result.stderr
        match = re.search(r'Throughput:\s*([\d,]+)\s*rows/sec', output)
        if match:
            throughput_str = match.group(1).replace(',', '')
            throughput = int(throughput_str)
            return throughput
        else:
            # Try alternative pattern: "Wrote X rows in Y seconds (Z rows/sec)"
            match = re.search(r'\((\d+)\s*rows/sec\)', output)
            if match:
                return int(match.group(1))

            print(f"  ⚠️  Could not parse throughput from output")
            return None

    except subprocess.TimeoutExpired:
        print(f"  ❌ TIMEOUT (>10 minutes)")
        return None
    except Exception as e:
        print(f"  ❌ ERROR: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Comprehensive multi-format benchmark')
    parser.add_argument('--output-dir', default='/tmp/benchmark-comprehensive',
                        help='Directory for benchmark output files')
    parser.add_argument('--binary', default=BENCHMARK_BINARY,
                        help='Path to tpch_benchmark binary')
    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check binary exists
    if not Path(args.binary).exists():
        print(f"ERROR: Benchmark binary not found: {args.binary}", file=sys.stderr)
        print(f"Please build with: cmake --build build -j$(nproc)", file=sys.stderr)
        sys.exit(1)

    # Check which formats are supported
    print("Checking format support...")
    supported_formats = check_format_support(args.binary)
    if not supported_formats:
        print("ERROR: Could not determine supported formats", file=sys.stderr)
        sys.exit(1)

    print(f"Supported formats: {', '.join(supported_formats)}")
    formats_to_test = [f for f in FORMATS if f in supported_formats]
    if not formats_to_test:
        print("ERROR: None of the target formats are supported!", file=sys.stderr)
        sys.exit(1)

    print(f"\nBenchmarking {len(formats_to_test)} formats × {len(TABLES)} tables × {RUNS} runs = {len(formats_to_test) * len(TABLES) * RUNS} benchmarks")
    print(f"Scale Factor: {SCALE_FACTOR}")
    print(f"Formats: {', '.join(formats_to_test)}")
    print(f"Tables: {', '.join(TABLES)}")
    print("="* 80)

    # Store results: results[format][table] = [run1, run2, run3]
    results = defaultdict(lambda: defaultdict(list))

    total_benchmarks = len(formats_to_test) * len(TABLES) * RUNS
    current = 0

    for format_name in formats_to_test:
        print(f"\n📊 Format: {format_name.upper()}")
        print("-" * 80)

        for table in TABLES:
            print(f"\n  Table: {table}")

            for run in range(1, RUNS + 1):
                current += 1
                print(f"    Run {run}/{RUNS} [{current}/{total_benchmarks}]...", end=" ", flush=True)

                throughput = run_benchmark(
                    args.binary,
                    table,
                    format_name,
                    SCALE_FACTOR,
                    str(output_dir / format_name / table / f"run{run}")
                )

                if throughput:
                    results[format_name][table].append(throughput)
                    print(f"✅ {throughput:,} rows/sec")
                else:
                    print()  # Newline after error message

    # Calculate averages and generate report
    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS - AVERAGES")
    print("=" * 80)

    summary = {}
    for format_name in sorted(formats_to_test):
        print(f"\n{format_name.upper()}:")
        summary[format_name] = {}

        for table in TABLES:
            runs = results[format_name][table]
            if runs:
                avg = sum(runs) / len(runs)
                min_val = min(runs)
                max_val = max(runs)
                stddev_pct = (max_val - min_val) / avg * 100 if avg > 0 else 0

                summary[format_name][table] = {
                    'runs': runs,
                    'average': avg,
                    'min': min_val,
                    'max': max_val,
                    'stddev_pct': stddev_pct
                }

                print(f"  {table:12s}: {max_val:>10,.0f} rows/sec (max)  (±{stddev_pct:>5.1f}%)")
            else:
                summary[format_name][table] = None
                print(f"  {table:12s}: FAILED")

    # Save results to JSON
    results_file = output_dir / "benchmark_results.json"
    with open(results_file, 'w') as f:
        json.dump({
            'metadata': {
                'scale_factor': SCALE_FACTOR,
                'tables': TABLES,
                'formats': formats_to_test,
                'runs': RUNS,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            },
            'raw_results': dict(results),
            'summary': summary
        }, f, indent=2)

    print(f"\n✅ Results saved to: {results_file}")
    print(f"✅ Benchmark outputs in: {output_dir}")

    return 0

if __name__ == '__main__':
    sys.exit(main())
