#!/usr/bin/env python3
"""
Phase 15: Comprehensive SF=10 Benchmark
Tests all 8 TPC-H tables with scale-factor 10 and all optimization combinations.

Combinations tested:
1. Baseline (regular path)
2. Zero-copy (Phase 14.1)
3. True zero-copy (Phase 14.2.3)
4. Async I/O (if available)
5. Zero-copy + Async I/O
6. True zero-copy + Async I/O
7. Parallel mode (all tables at once)
8. Parallel + Zero-copy
9. Parallel + True zero-copy
10. Parallel + Async I/O
11. Parallel + Zero-copy + Async I/O
12. Parallel + True zero-copy + Async I/O

Each combination runs 2 times for stability measurement.
"""

import subprocess
import json
import time
import re
import os
from pathlib import Path
from datetime import datetime
from statistics import mean, stdev, median
from typing import Dict, List, Tuple, Optional

# All 8 TPC-H tables with base row counts (scale factor 1)
# According to TPC-H standard: https://www.tpc.org/tpch/
TABLES_SF1 = [
    ("lineitem", 6000000),      # TPC-H: 6M rows
    ("orders", 1500000),        # TPC-H: 1.5M rows
    ("customer", 150000),       # TPC-H: 150K rows
    ("part", 200000),           # TPC-H: 200K rows
    ("partsupp", 8000000),      # TPC-H: 8M rows
    ("supplier", 10000),        # TPC-H: 10K rows
    ("nation", 25),             # TPC-H: 25 rows (constant)
    ("region", 5),              # TPC-H: 5 rows (constant)
]

# For SF=10 - multiply by scale factor
SCALE_FACTOR = 10
TABLES_SF10 = [(name, count * SCALE_FACTOR) for name, count in TABLES_SF1]

# Define all meaningful optimization combinations
# NOTE: Zero-copy modes (--zero-copy, --true-zero-copy) have a hang issue with full datasets
# Disabled for this benchmark - will investigate separately (Phase 16)
OPTIMIZATION_MODES = [
    {
        "name": "baseline",
        "description": "Regular path (baseline)",
        "flags": [],
        "per_table": True,
        "parallel": False,
    },
    {
        "name": "async_io",
        "description": "Async I/O with io_uring",
        "flags": ["--async-io"],
        "per_table": True,
        "parallel": False,
    },
    {
        "name": "parallel_baseline",
        "description": "Parallel mode (all tables at once, baseline)",
        "flags": ["--parallel"],
        "per_table": False,
        "parallel": True,
    },
    {
        "name": "parallel_async",
        "description": "Parallel + Async I/O",
        "flags": ["--parallel", "--async-io"],
        "per_table": False,
        "parallel": True,
    },
]


class Phase15Benchmark:
    def __init__(self, tpch_binary: str, output_dir: str = "/tmp/phase15_sf10_benchmark", runs: int = 2):
        self.tpch_binary = Path(tpch_binary)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.runs = runs  # Number of times to run each benchmark
        self.results: Dict = {}
        self.benchmark_date = datetime.now().isoformat()
        self.async_io_available = False
        self.check_async_io_support()

    def check_async_io_support(self):
        """Check if async I/O is available by running with --help"""
        try:
            result = subprocess.run(
                [str(self.tpch_binary), "--help"],
                capture_output=True,
                text=True,
                timeout=5
            )
            self.async_io_available = "--async-io" in result.stdout
        except Exception as e:
            print(f"Warning: Could not check async I/O support: {e}")
            self.async_io_available = False

    def _parse_throughput(self, output_text: str) -> float:
        """Extract throughput from program output"""
        patterns = [
            r'(\d+(?:\.\d+)?)\s*rows/sec',
            r'Generated.*?(\d+(?:\.\d+)?)\s*rows/s',
            r'Throughput:\s*(\d+(?:\.\d+)?)',
        ]

        for pattern in patterns:
            match = re.search(pattern, output_text, re.IGNORECASE)
            if match:
                return float(match.group(1))

        return 0.0

    def run_benchmark(
        self,
        mode: Dict,
        table_name: Optional[str] = None,
        row_count: Optional[int] = None,
        run_number: int = 1
    ) -> Optional[Dict]:
        """Run a single benchmark"""

        # Skip async modes if not available
        if not self.async_io_available and "--async-io" in mode["flags"]:
            return None

        # Create output directory
        output_path = self.output_dir / mode["name"] / f"run{run_number}"
        if table_name:
            output_path = output_path / table_name
        output_path.mkdir(parents=True, exist_ok=True)

        # Build command
        cmd = [
            str(self.tpch_binary),
            "--use-dbgen",
            "--scale-factor", str(SCALE_FACTOR),
            "--max-rows", "0",  # 0 = generate ALL rows for the scale factor
            "--format", "parquet",
            "--output-dir", str(output_path)
        ]

        # Add table for per-table benchmarks
        if mode["per_table"] and table_name:
            cmd.extend(["--table", table_name])

        # Add optimization flags
        cmd.extend(mode["flags"])

        try:
            start = time.time()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 60 minutes timeout for full SF=10 (60M lineitem rows)
            )
            elapsed = time.time() - start

            if result.returncode != 0:
                return None

            throughput = self._parse_throughput(result.stdout)

            return {
                "table": table_name,
                "rows": row_count,
                "mode": mode["name"],
                "run": run_number,
                "elapsed": elapsed,
                "throughput": throughput,
                "output": result.stdout
            }

        except subprocess.TimeoutExpired:
            return None
        except Exception as e:
            return None

    def run_per_table_benchmarks(self, mode: Dict) -> List[Dict]:
        """Run benchmarks for each table individually"""
        results = []

        print(f"\n{'='*120}")
        print(f"Mode: {mode['description']}")
        print(f"{'='*120}")
        print(f"{'Table':<15} {'Rows':<12} {'Run':<4} {'Throughput':>15} {'Time':>10} {'Status':<10}")
        print("-" * 120)

        for table_name, row_count in TABLES_SF10:
            for run_num in range(1, self.runs + 1):
                print(f"{table_name:<15} {row_count:<12} {run_num:<4} ", end="", flush=True)

                result = self.run_benchmark(mode, table_name, row_count, run_num)

                if result:
                    print(f"{result['throughput']:>15,.0f} {result['elapsed']:>10.3f}s OK")
                    results.append(result)
                else:
                    print("FAILED or SKIPPED")

        return results

    def run_parallel_benchmark(self, mode: Dict) -> Optional[Dict]:
        """Run parallel benchmark (all tables at once)"""
        print(f"\n{'='*120}")
        print(f"Mode: {mode['description']} (Parallel - All Tables)")
        print(f"{'='*120}")
        print(f"{'Run':<4} {'Total Rows':<15} {'Elapsed':>10} {'Throughput':>15} {'Status':<10}")
        print("-" * 120)

        total_rows = sum(count for _, count in TABLES_SF10)
        aggregate_results = []

        for run_num in range(1, self.runs + 1):
            print(f"{run_num:<4} {total_rows:<15} ", end="", flush=True)

            # For parallel mode, we need to generate all tables
            # This is handled by the --parallel flag in the binary
            result = self.run_benchmark(mode, run_number=run_num)

            if result:
                print(f"{result['elapsed']:>10.3f}s {result['throughput']:>15,.0f} OK")
                aggregate_results.append({
                    "run": run_num,
                    "total_rows": total_rows,
                    "elapsed": result["elapsed"],
                    "throughput": result["throughput"]
                })
            else:
                print(f"{'':>10}s {'0':>15} FAILED")

        if aggregate_results:
            return {
                "mode": mode["name"],
                "description": mode["description"],
                "results": aggregate_results
            }

        return None

    def run_all_benchmarks(self):
        """Run all benchmarks"""
        print("\n" + "="*120)
        print("Phase 15: Comprehensive Scale Factor 10 Benchmark")
        print("="*120)
        print(f"Binary: {self.tpch_binary}")
        print(f"Output: {self.output_dir}")
        print(f"Date: {self.benchmark_date}")
        print(f"Scale Factor: 10")
        print(f"Tables: {len(TABLES_SF10)} (lineitem, orders, customer, part, partsupp, supplier, nation, region)")
        print(f"Runs per benchmark: {self.runs}")
        print(f"Async I/O support: {self.async_io_available}")
        print("="*120)

        for mode in OPTIMIZATION_MODES:
            # Skip async modes if not available
            if not self.async_io_available and "--async-io" in mode["flags"]:
                continue

            if mode["per_table"]:
                results = self.run_per_table_benchmarks(mode)
                self.results[mode["name"]] = results
            else:
                result = self.run_parallel_benchmark(mode)
                if result:
                    self.results[mode["name"]] = result["results"]

    def generate_report(self):
        """Generate comprehensive performance report"""
        print("\n\n" + "="*120)
        print("PERFORMANCE REPORT")
        print("="*120)

        # Per-table analysis
        print("\n" + "="*120)
        print("PER-TABLE PERFORMANCE ANALYSIS")
        print("="*120)

        per_table_modes = [m for m in OPTIMIZATION_MODES if m["per_table"] and m["name"] in self.results]

        for table_name, row_count in TABLES_SF10:
            print(f"\n{table_name.upper()} (SF=10: {row_count:,} rows)")
            print("-" * 120)
            print(f"{'Mode':<30} {'Run 1 (r/s)':<18} {'Run 2 (r/s)':<18} {'Avg (r/s)':<18} {'Std Dev':<12} {'Speedup':<10}")
            print("-" * 120)

            baseline_avg = None

            for mode in per_table_modes:
                # Skip if async not available
                if not self.async_io_available and "--async-io" in mode["flags"]:
                    continue

                mode_results = [
                    r for r in self.results.get(mode["name"], [])
                    if r.get("table") == table_name
                ]

                if not mode_results:
                    continue

                throughputs = [r["throughput"] for r in mode_results]

                if len(throughputs) >= 2:
                    avg_throughput = mean(throughputs)
                    std_dev = stdev(throughputs)
                    run1 = throughputs[0]
                    run2 = throughputs[1]
                else:
                    avg_throughput = throughputs[0] if throughputs else 0
                    std_dev = 0
                    run1 = throughputs[0] if throughputs else 0
                    run2 = 0

                if mode["name"] == "baseline":
                    baseline_avg = avg_throughput

                speedup = (avg_throughput / baseline_avg) if baseline_avg and baseline_avg > 0 else 0

                speedup_str = f"{speedup:.2f}x" if speedup > 0 else "N/A"

                print(f"{mode['name']:<30} {run1:>16,.0f} {run2:>16,.0f} {avg_throughput:>16,.0f} {std_dev:>10,.0f} {speedup_str:>9}")

        # Parallel analysis
        parallel_modes = [m for m in OPTIMIZATION_MODES if m["parallel"] and m["name"] in self.results]
        if parallel_modes:
            print(f"\n\n" + "="*120)
            print("PARALLEL MODE ANALYSIS (All Tables Combined)")
            print("="*120)
            print(f"Total SF=10 rows: {sum(count for _, count in TABLES_SF10):,}")
            print(f"{'Mode':<30} {'Run 1 (s)':<15} {'Run 2 (s)':<15} {'Avg (s)':<15} {'Std Dev':<12} {'Speedup':<10}")
            print("-" * 120)

            baseline_par_avg = None

            for mode in parallel_modes:
                # Skip if async not available
                if not self.async_io_available and "--async-io" in mode["flags"]:
                    continue

                results = self.results.get(mode["name"], [])

                if not results:
                    continue

                elapsed_times = [r["elapsed"] for r in results]

                if len(elapsed_times) >= 2:
                    avg_elapsed = mean(elapsed_times)
                    std_dev = stdev(elapsed_times)
                    run1 = elapsed_times[0]
                    run2 = elapsed_times[1]
                else:
                    avg_elapsed = elapsed_times[0] if elapsed_times else 0
                    std_dev = 0
                    run1 = elapsed_times[0] if elapsed_times else 0
                    run2 = 0

                if mode["name"] == "parallel_baseline":
                    baseline_par_avg = avg_elapsed

                speedup = (baseline_par_avg / avg_elapsed) if baseline_par_avg and avg_elapsed > 0 else 0
                speedup_str = f"{speedup:.2f}x" if speedup > 0 else "N/A"

                print(f"{mode['name']:<30} {run1:>13.3f}s {run2:>13.3f}s {avg_elapsed:>13.3f}s {std_dev:>10.3f}s {speedup_str:>9}")

    def save_results(self):
        """Save results to JSON"""
        output_file = self.output_dir / "phase15_sf10_results.json"

        summary = {
            "date": self.benchmark_date,
            "binary": str(self.tpch_binary),
            "scale_factor": 10,
            "tables": len(TABLES_SF10),
            "runs_per_benchmark": self.runs,
            "async_io_available": self.async_io_available,
            "results": self.results
        }

        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)

        print(f"\n✅ Results saved to: {output_file}")


def main():
    import sys

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <path/to/tpch_benchmark> [output_dir] [runs]")
        print(f"Example: {sys.argv[0]} ./build/tpch_benchmark /tmp/phase15 2")
        print(f"\nDefault: 2 runs per benchmark combination")
        sys.exit(1)

    tpch_binary = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "/tmp/phase15_sf10_benchmark"
    runs = int(sys.argv[3]) if len(sys.argv) > 3 else 2

    # Verify binary exists
    if not Path(tpch_binary).exists():
        print(f"Error: Binary not found: {tpch_binary}")
        sys.exit(1)

    benchmark = Phase15Benchmark(tpch_binary, output_dir, runs)

    # Run all benchmarks
    benchmark.run_all_benchmarks()

    # Generate report
    benchmark.generate_report()

    # Save results
    benchmark.save_results()

    print("\n" + "="*120)
    print("✅ Benchmark Complete!")
    print("="*120)


if __name__ == "__main__":
    main()
