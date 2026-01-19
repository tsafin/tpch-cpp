#!/usr/bin/env python3
"""
Phase 16: Full Optimization Matrix Benchmark at SF=10
Tests all 8 TPC-H tables with comprehensive optimization modes.

PHASE 16 UPDATE: Zero-copy hang issues RESOLVED - all corner cases now pass.
Re-enabling zero-copy and true-zero-copy modes for complete benchmarking.

Combinations tested:
1. Baseline (regular path) - control
2. Zero-copy (Phase 14.1) - proven stable
3. True zero-copy (Phase 14.2.3) - proven stable
4. Parallel baseline (all 8 tables at once)
5. Parallel + Zero-copy
6. Parallel + True zero-copy
7. Parallel baseline + Async-IO (io_uring for concurrent I/O)
8. Parallel + Zero-copy + Async-IO
9. Parallel + True zero-copy + Async-IO

Each combination runs 2 times for stability measurement.
Full SF=10 dataset: 158.6M total rows across 8 tables

PHASE 16 UPDATE 2: Added async-IO support to parallel modes.
Async-IO uses io_uring for efficient concurrent I/O operations.
Previous Phase 15 testing showed 1.73x speedup with parallel + async-io.
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

# File formats to benchmark
FORMATS = ["orc", "parquet"]

# Define all meaningful optimization combinations
# Phase 16: Zero-copy modes are now STABLE - enable for comprehensive benchmarking
# Phase 16 Update 2: Added async-IO variants for parallel modes (io_uring support)
OPTIMIZATION_MODES = [
    {
        "name": "baseline",
        "description": "Regular path (baseline)",
        "flags": [],
        "per_table": True,
        "parallel": False,
    },
    {
        "name": "zero_copy",
        "description": "Zero-copy optimizations (Phase 14.1)",
        "flags": ["--zero-copy"],
        "per_table": True,
        "parallel": False,
    },
    {
        "name": "true_zero_copy",
        "description": "True zero-copy with Buffer::Wrap (Phase 14.2.3)",
        "flags": ["--true-zero-copy"],
        "per_table": True,
        "parallel": False,
    },
    {
        "name": "parallel_baseline",
        "description": "Parallel mode (all 8 tables, baseline)",
        "flags": ["--parallel"],
        "per_table": False,
        "parallel": True,
    },
    {
        "name": "parallel_zero_copy",
        "description": "Parallel + Zero-copy (Phase 14.1)",
        "flags": ["--parallel", "--zero-copy"],
        "per_table": False,
        "parallel": True,
    },
    {
        "name": "parallel_true_zero_copy",
        "description": "Parallel + True zero-copy (Phase 14.2.3)",
        "flags": ["--parallel", "--true-zero-copy"],
        "per_table": False,
        "parallel": True,
    },
    {
        "name": "parallel_baseline_async",
        "description": "Parallel baseline + Async-IO (io_uring, Phase 16)",
        "flags": ["--parallel", "--async-io"],
        "per_table": False,
        "parallel": True,
    },
    {
        "name": "parallel_zero_copy_async",
        "description": "Parallel + Zero-copy + Async-IO (Phase 16)",
        "flags": ["--parallel", "--zero-copy", "--async-io"],
        "per_table": False,
        "parallel": True,
    },
    {
        "name": "parallel_true_zero_copy_async",
        "description": "Parallel + True zero-copy + Async-IO (Phase 16)",
        "flags": ["--parallel", "--true-zero-copy", "--async-io"],
        "per_table": False,
        "parallel": True,
    },
]


class Phase15Benchmark:
    def __init__(self, tpch_binary: str, output_dir: str = "/tmp/phase16_sf10_benchmark", runs: int = 2):
        self.tpch_binary = Path(tpch_binary)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.runs = runs  # Number of times to run each benchmark
        self.results: Dict = {}
        self.benchmark_date = datetime.now().isoformat()

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
        format_type: str = "parquet",
        table_name: Optional[str] = None,
        row_count: Optional[int] = None,
        run_number: int = 1
    ) -> Optional[Dict]:
        """Run a single benchmark"""

        # Create output directory
        output_path = self.output_dir / format_type / mode["name"] / f"run{run_number}"
        if table_name:
            output_path = output_path / table_name
        output_path.mkdir(parents=True, exist_ok=True)

        # Build command
        cmd = [
            str(self.tpch_binary),
            "--use-dbgen",
            "--scale-factor", str(SCALE_FACTOR),
            "--max-rows", "0",  # 0 = generate ALL rows for the scale factor
            "--format", format_type,
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
                "format": format_type,
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

    def run_per_table_benchmarks(self, mode: Dict, format_type: str = "parquet") -> List[Dict]:
        """Run benchmarks for each table individually"""
        results = []

        print(f"\n{'='*120}")
        print(f"Format: {format_type.upper()} | Mode: {mode['description']}")
        print(f"{'='*120}")
        print(f"{'Table':<15} {'Rows':<12} {'Run':<4} {'Throughput':>15} {'Time':>10} {'Status':<10}")
        print("-" * 120)

        for table_name, row_count in TABLES_SF10:
            for run_num in range(1, self.runs + 1):
                print(f"{table_name:<15} {row_count:<12} {run_num:<4} ", end="", flush=True)

                result = self.run_benchmark(mode, format_type, table_name, row_count, run_num)

                if result:
                    print(f"{result['throughput']:>15,.0f} {result['elapsed']:>10.3f}s OK")
                    results.append(result)
                else:
                    print("FAILED or SKIPPED")

        return results

    def run_parallel_benchmark(self, mode: Dict, format_type: str = "parquet") -> Optional[Dict]:
        """Run parallel benchmark (all tables at once)"""
        print(f"\n{'='*120}")
        print(f"Format: {format_type.upper()} | Mode: {mode['description']} (Parallel - All Tables)")
        print(f"{'='*120}")
        print(f"{'Run':<4} {'Total Rows':<15} {'Elapsed':>10} {'Throughput':>15} {'Status':<10}")
        print("-" * 120)

        total_rows = sum(count for _, count in TABLES_SF10)
        aggregate_results = []

        for run_num in range(1, self.runs + 1):
            print(f"{run_num:<4} {total_rows:<15} ", end="", flush=True)

            # For parallel mode, we need to generate all tables
            # This is handled by the --parallel flag in the binary
            result = self.run_benchmark(mode, format_type, run_number=run_num)

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
        print("Phase 16+: Full Optimization Matrix Benchmark at SF=10 (ORC + Parquet, with Async-IO)")
        print("="*120)
        print(f"Binary: {self.tpch_binary}")
        print(f"Output: {self.output_dir}")
        print(f"Date: {self.benchmark_date}")
        print(f"Scale Factor: 10")
        print(f"Formats: {', '.join(FORMATS)}")
        print(f"Tables: {len(TABLES_SF10)} (lineitem, orders, customer, part, partsupp, supplier, nation, region)")
        print(f"Total Rows: {sum(count for _, count in TABLES_SF10):,}")
        print(f"Runs per benchmark: {self.runs}")
        print(f"Optimization modes: {len(OPTIMIZATION_MODES)} (3 sequential + 6 parallel variants with/without async-io)")
        print("="*120)

        for format_type in FORMATS:
            print(f"\n{'='*120}")
            print(f"Testing format: {format_type.upper()}")
            print(f"{'='*120}")

            for mode in OPTIMIZATION_MODES:
                result_key = f"{format_type}_{mode['name']}"
                if mode["per_table"]:
                    results = self.run_per_table_benchmarks(mode, format_type)
                    self.results[result_key] = results
                else:
                    result = self.run_parallel_benchmark(mode, format_type)
                    if result:
                        self.results[result_key] = result["results"]

    def generate_report(self):
        """Generate comprehensive performance report"""
        print("\n\n" + "="*120)
        print("PHASE 16+ PERFORMANCE REPORT - ORC vs PARQUET with FULL OPTIMIZATION MATRIX")
        print("="*120)

        # Per-table analysis by format
        print("\n" + "="*120)
        print("PER-TABLE PERFORMANCE ANALYSIS (SF=10)")
        print("="*120)

        for format_type in FORMATS:
            print(f"\n\n{'='*120}")
            print(f"FORMAT: {format_type.upper()}")
            print(f"{'='*120}")

            per_table_modes = [m for m in OPTIMIZATION_MODES if m["per_table"]]

            for table_name, row_count in TABLES_SF10:
                print(f"\n{table_name.upper()} (SF=10: {row_count:,} rows)")
                print("-" * 120)
                print(f"{'Mode':<30} {'Run 1 (r/s)':<18} {'Run 2 (r/s)':<18} {'Avg (r/s)':<18} {'Std Dev':<12} {'Speedup':<10}")
                print("-" * 120)

                baseline_avg = None

                for mode in per_table_modes:
                    result_key = f"{format_type}_{mode['name']}"
                    mode_results = [
                        r for r in self.results.get(result_key, [])
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

        # Parallel analysis by format
        for format_type in FORMATS:
            parallel_modes = [m for m in OPTIMIZATION_MODES if m["parallel"]]
            parallel_result_keys = [f"{format_type}_{m['name']}" for m in parallel_modes]
            has_parallel = any(k in self.results for k in parallel_result_keys)

            if has_parallel:
                print(f"\n\n" + "="*120)
                print(f"PARALLEL MODE ANALYSIS - {format_type.upper()} (All 8 Tables Combined)")
                print("="*120)
                total_rows = sum(count for _, count in TABLES_SF10)
                print(f"Total SF=10 rows: {total_rows:,}")
                print(f"\n{'Mode':<35} {'Run 1 (s)':<15} {'Run 2 (s)':<15} {'Avg (s)':<15} {'Std Dev':<12} {'Speedup':<10} {'Rows/sec':<15}")
                print("-" * 130)

                baseline_par_avg = None
                baseline_throughput = None

                for mode in parallel_modes:
                    result_key = f"{format_type}_{mode['name']}"
                    results = self.results.get(result_key, [])

                    if not results:
                        continue

                    elapsed_times = [r["elapsed"] for r in results]
                    throughputs = [r.get("throughput", 0) for r in results]

                    if len(elapsed_times) >= 2:
                        avg_elapsed = mean(elapsed_times)
                        std_dev = stdev(elapsed_times)
                        run1 = elapsed_times[0]
                        run2 = elapsed_times[1]
                        avg_throughput = mean(throughputs) if throughputs else 0
                    else:
                        avg_elapsed = elapsed_times[0] if elapsed_times else 0
                        std_dev = 0
                        run1 = elapsed_times[0] if elapsed_times else 0
                        run2 = 0
                        avg_throughput = throughputs[0] if throughputs else 0

                    if mode["name"] == "parallel_baseline":
                        baseline_par_avg = avg_elapsed
                        baseline_throughput = avg_throughput

                    speedup = (baseline_par_avg / avg_elapsed) if baseline_par_avg and avg_elapsed > 0 else 0
                    speedup_str = f"{speedup:.2f}x" if speedup > 0 else "N/A"

                    print(f"{mode['name']:<35} {run1:>13.3f}s {run2:>13.3f}s {avg_elapsed:>13.3f}s {std_dev:>10.3f}s {speedup_str:>9} {avg_throughput:>13,.0f}")

        # Summary statistics
        print(f"\n\n" + "="*120)
        print("OPTIMIZATION SUMMARY")
        print("="*120)

        # Async-IO specific analysis per format
        for format_type in FORMATS:
            async_modes = [m for m in OPTIMIZATION_MODES if "async" in m["name"]]
            parallel_modes = [m for m in OPTIMIZATION_MODES if m["parallel"]]

            async_result_keys = [f"{format_type}_{m['name']}" for m in async_modes]
            parallel_result_keys = [f"{format_type}_{m['name']}" for m in parallel_modes]
            has_async = any(k in self.results for k in async_result_keys)
            has_parallel = any(k in self.results for k in parallel_result_keys)

            if has_async and has_parallel:
                print(f"\nAsync-IO Impact Analysis - {format_type.upper()} (Parallel Mode):")
                print("-" * 120)

                # Compare async-io variants with their non-async counterparts
                async_comparisons = [
                    ("parallel_baseline", "parallel_baseline_async", "Parallel Baseline"),
                    ("parallel_zero_copy", "parallel_zero_copy_async", "Parallel Zero-Copy"),
                    ("parallel_true_zero_copy", "parallel_true_zero_copy_async", "Parallel True Zero-Copy"),
                ]

                for baseline_name, async_name, description in async_comparisons:
                    baseline_key = f"{format_type}_{baseline_name}"
                    async_key = f"{format_type}_{async_name}"
                    baseline_results = self.results.get(baseline_key, [])
                    async_results = self.results.get(async_key, [])

                    if baseline_results and async_results:
                        baseline_avg = mean([r["elapsed"] for r in baseline_results])
                        async_avg = mean([r["elapsed"] for r in async_results])
                        improvement = ((baseline_avg - async_avg) / baseline_avg) * 100 if baseline_avg > 0 else 0
                        speedup = baseline_avg / async_avg if async_avg > 0 else 0

                        status = "✓ IMPROVEMENT" if improvement > 0 else "✗ REGRESSION"
                        print(f"  {description:<40} {status:<20} {improvement:>7.2f}% faster with async-io ({speedup:.2f}x speedup)")

        # Per-table optimization impact by format
        for format_type in FORMATS:
            print(f"\nPer-Table Optimization Impact - {format_type.upper()}:")
            per_table_modes = [m for m in OPTIMIZATION_MODES if m["per_table"]]
            for mode in per_table_modes:
                if mode["name"] != "baseline":
                    result_key = f"{format_type}_{mode['name']}"
                    mode_results = self.results.get(result_key, [])
                    if mode_results:
                        # Calculate average speedup across all tables
                        speedups = []
                        for table_name, row_count in TABLES_SF10:
                            table_results = [r for r in mode_results if r.get("table") == table_name]
                            if table_results:
                                baseline_key = f"{format_type}_baseline"
                                baseline_results = [
                                    r for r in self.results.get(baseline_key, [])
                                    if r.get("table") == table_name
                                ]
                                if baseline_results:
                                    baseline_avg = mean([r["throughput"] for r in baseline_results])
                                    mode_avg = mean([r["throughput"] for r in table_results])
                                    speedup = mode_avg / baseline_avg if baseline_avg > 0 else 0
                                    speedups.append(speedup)

                        if speedups:
                            avg_speedup = mean(speedups)
                            min_speedup = min(speedups)
                            max_speedup = max(speedups)
                            print(f"  {mode['name']:<30} Avg: {avg_speedup:6.2f}x  (range: {min_speedup:.2f}x - {max_speedup:.2f}x)")

    def save_results(self):
        """Save results to JSON"""
        output_file = self.output_dir / "phase16_sf10_results.json"

        summary = {
            "date": self.benchmark_date,
            "binary": str(self.tpch_binary),
            "scale_factor": 10,
            "tables": len(TABLES_SF10),
            "total_rows": sum(count for _, count in TABLES_SF10),
            "runs_per_benchmark": self.runs,
            "optimization_modes": len(OPTIMIZATION_MODES),
            "results": self.results
        }

        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)

        print(f"\n✅ Results saved to: {output_file}")


def main():
    import sys

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <path/to/tpch_benchmark> [output_dir] [runs]")
        print(f"Example: {sys.argv[0]} ./build/tpch_benchmark /tmp/phase16 2")
        print(f"\nDefault: 2 runs per benchmark combination")
        sys.exit(1)

    tpch_binary = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "/tmp/phase16_sf10_benchmark"
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
    print("✅ Phase 16 Benchmark Complete!")
    print("="*120)


if __name__ == "__main__":
    main()
