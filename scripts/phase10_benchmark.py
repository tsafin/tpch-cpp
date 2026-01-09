#!/usr/bin/env python3
"""
Phase 10: Comprehensive TPC-H Performance Benchmarking Suite
Benchmarks all 6 working TPC-H tables with multiple scale factors and formats.
"""

import subprocess
import json
import time
import os
import sys
import re
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from datetime import datetime


@dataclass
class BenchmarkResult:
    """Single benchmark result"""
    table: str
    scale_factor: int
    max_rows: int
    format: str
    io_mode: str  # "sync" or "async"
    rows_written: int
    file_size: int
    elapsed_time: float
    throughput_rows_sec: float
    write_rate_mb_sec: float
    timestamp: str

    def __post_init__(self):
        if self.throughput_rows_sec == 0:
            self.throughput_rows_sec = self.rows_written / self.elapsed_time if self.elapsed_time > 0 else 0
        if self.write_rate_mb_sec == 0:
            self.write_rate_mb_sec = (self.file_size / 1024 / 1024) / self.elapsed_time if self.elapsed_time > 0 else 0


class Phase10Benchmarker:
    """Comprehensive benchmarking suite for TPC-H tables"""

    # TPC-H tables that work without segfault
    WORKING_TABLES = ["lineitem", "orders", "customer", "part", "partsupp", "supplier"]

    # Test configurations
    SCALE_FACTORS = [1, 10]  # SF=100 may be too large for quick testing
    FORMATS = ["parquet", "csv"]

    def __init__(self, benchmark_exe: str, output_dir: str = "benchmark-results"):
        self.benchmark_exe = benchmark_exe
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.results: List[BenchmarkResult] = []

    def run_single_benchmark(self, table: str, scale_factor: int,
                            max_rows: int, format: str, use_async_io: bool = False) -> Optional[BenchmarkResult]:
        """Run a single benchmark test"""
        cmd = [
            self.benchmark_exe,
            "--use-dbgen",
            "--table", table,
            "--scale-factor", str(scale_factor),
            "--max-rows", str(max_rows),
            "--format", format,
            "--output-dir", str(self.output_dir),
        ]

        if use_async_io:
            cmd.append("--async-io")

        print(f"  Running: {table} SF={scale_factor} rows={max_rows} format={format}")
        print(f"  Command: {' '.join(cmd)}")

        try:
            start_time = time.time()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            elapsed = time.time() - start_time

            if result.returncode != 0:
                print(f"    ERROR: Command failed with code {result.returncode}")
                print(f"    stderr: {result.stderr[:500]}")
                return None

            # Parse output
            output = result.stdout
            rows_written = self._extract_value(output, "Rows written:", int)
            file_size = self._extract_value(output, "File size:", int)
            throughput = self._extract_value(output, "Throughput:", float)
            write_rate = self._extract_value(output, "Write rate:", float)

            if rows_written is None or file_size is None:
                print(f"    ERROR: Could not parse output")
                return None

            result = BenchmarkResult(
                table=table,
                scale_factor=scale_factor,
                max_rows=max_rows,
                format=format,
                io_mode="async" if use_async_io else "sync",
                rows_written=rows_written,
                file_size=file_size,
                elapsed_time=elapsed,
                throughput_rows_sec=throughput or 0,
                write_rate_mb_sec=write_rate or 0,
                timestamp=datetime.now().isoformat()
            )

            print(f"    ✓ Success: {rows_written} rows, {file_size} bytes, {elapsed:.2f}s")
            return result

        except subprocess.TimeoutExpired:
            print(f"    ERROR: Timeout (300s exceeded)")
            return None
        except Exception as e:
            print(f"    ERROR: {e}")
            return None

    @staticmethod
    def _extract_value(output: str, key: str, dtype=str):
        """Extract a value from benchmark output"""
        for line in output.split('\n'):
            if key in line:
                # Extract the value part
                parts = line.split(key)
                if len(parts) > 1:
                    value_str = parts[1].strip()
                    # Remove units if present
                    value_str = value_str.split()[0]
                    try:
                        if dtype == int:
                            return int(value_str)
                        elif dtype == float:
                            return float(value_str)
                        else:
                            return value_str
                    except ValueError:
                        return None
        return None

    def benchmark_all_tables(self, use_async_io: bool = False):
        """Run comprehensive benchmarks on all tables with specified I/O mode"""
        total_tests = len(self.WORKING_TABLES) * len(self.SCALE_FACTORS) * len(self.FORMATS)
        completed = 0
        failed = 0
        io_mode_str = "Async (io_uring)" if use_async_io else "Synchronous"

        print("\n" + "="*80)
        print(f"PHASE 10: COMPREHENSIVE TPC-H PERFORMANCE BENCHMARKING [{io_mode_str}]")
        print("="*80)
        print(f"Tables: {', '.join(self.WORKING_TABLES)}")
        print(f"Scale factors: {self.SCALE_FACTORS}")
        print(f"Formats: {', '.join(self.FORMATS)}")
        print(f"I/O Mode: {io_mode_str}")
        print(f"Total tests: {total_tests}")
        print("="*80 + "\n")

        for table in self.WORKING_TABLES:
            print(f"\nBenchmarking table: {table}")
            print("-" * 60)

            for sf in self.SCALE_FACTORS:
                # Scale rows based on scale factor
                # TPC-H row counts at SF=1 (approximate)
                row_counts = {
                    "lineitem": 6_000_000,
                    "orders": 1_500_000,
                    "customer": 150_000,
                    "part": 200_000,
                    "partsupp": 800_000,
                    "supplier": 10_000,
                }

                base_rows = row_counts.get(table, 10_000)
                max_rows = base_rows * sf

                # For quick testing, limit very large scale factors
                if sf > 10 and max_rows > 100_000_000:
                    max_rows = 100_000_000
                    print(f"  Capping {table} SF={sf} to 100M rows for testing")

                for fmt in self.FORMATS:
                    result = self.run_single_benchmark(table, sf, max_rows, fmt, use_async_io)
                    completed += 1

                    if result:
                        self.results.append(result)
                        print(f"    Throughput: {result.throughput_rows_sec:,.0f} rows/sec")
                        print(f"    Write rate: {result.write_rate_mb_sec:.2f} MB/sec\n")
                    else:
                        failed += 1
                        print(f"    FAILED\n")

        print("\n" + "="*80)
        print(f"BENCHMARKING COMPLETE: {completed - failed}/{total_tests} tests passed")
        print("="*80 + "\n")

        return failed == 0

    def benchmark_both_io_modes(self):
        """Run comprehensive benchmarks for both synchronous and asynchronous I/O modes"""
        print("\n" + "#"*80)
        print("# RUNNING BENCHMARKS FOR BOTH SYNC AND ASYNC I/O MODES")
        print("#"*80)

        # Run synchronous benchmarks
        print("\n[1/2] SYNCHRONOUS I/O BENCHMARKS")
        sync_success = self.benchmark_all_tables(use_async_io=False)

        # Run asynchronous benchmarks
        print("\n[2/2] ASYNCHRONOUS I/O BENCHMARKS")
        async_success = self.benchmark_all_tables(use_async_io=True)

        return sync_success and async_success

    def generate_report(self):
        """Generate comprehensive report with sync/async comparison"""
        if not self.results:
            print("No results to report")
            return

        print("\n" + "="*80)
        print("BENCHMARK RESULTS SUMMARY")
        print("="*80 + "\n")

        # Check if we have both sync and async results
        io_modes = set(r.io_mode for r in self.results)
        has_both_modes = len(io_modes) == 2

        # Detailed results by I/O mode
        for io_mode in sorted(io_modes):
            mode_results = [r for r in self.results if r.io_mode == io_mode]
            mode_label = "ASYNCHRONOUS I/O (io_uring)" if io_mode == "async" else "SYNCHRONOUS I/O"

            print(f"\n{mode_label}")
            print("-" * 80)
            print(f"{'Table':<15} {'SF':>3} {'Format':<8} {'Rows':>12} {'Throughput':>15} {'Write Rate':>12}")
            print("-" * 80)

            for result in mode_results:
                print(f"{result.table:<15} {result.scale_factor:>3} {result.format:<8} "
                      f"{result.rows_written:>12,} {result.throughput_rows_sec:>12,.0f} r/s "
                      f"{result.write_rate_mb_sec:>10.2f} MB/s")

        print("\n" + "="*80)

        # Comparison analysis if we have both modes
        if has_both_modes:
            print("\nPERFORMANCE COMPARISON (Async vs Sync)")
            print("="*80)

            # Group by table and format
            comparison_data = {}
            for result in self.results:
                key = (result.table, result.scale_factor, result.format)
                if key not in comparison_data:
                    comparison_data[key] = {}
                comparison_data[key][result.io_mode] = result

            print(f"\n{'Table':<15} {'SF':>3} {'Format':<8} {'Sync r/s':>15} {'Async r/s':>15} {'Speedup':>10}")
            print("-" * 80)

            speedups = []
            for (table, sf, fmt) in sorted(comparison_data.keys()):
                modes = comparison_data[(table, sf, fmt)]
                if "sync" in modes and "async" in modes:
                    sync_result = modes["sync"]
                    async_result = modes["async"]
                    speedup = async_result.throughput_rows_sec / sync_result.throughput_rows_sec if sync_result.throughput_rows_sec > 0 else 0
                    speedups.append(speedup)
                    print(f"{table:<15} {sf:>3} {fmt:<8} {sync_result.throughput_rows_sec:>12,.0f}   "
                          f"{async_result.throughput_rows_sec:>12,.0f}   {speedup:>8.2f}x")

            if speedups:
                avg_speedup = sum(speedups) / len(speedups)
                print("-" * 80)
                print(f"{'Average speedup (Async / Sync)':<45} {avg_speedup:>8.2f}x")

        print("\n" + "="*80)

        # Summary statistics by table
        print("\nTHROUGHPUT BY TABLE (rows/sec):")
        print("-" * 60)

        tables_data = {}
        for result in self.results:
            if result.table not in tables_data:
                tables_data[result.table] = {}
            if result.io_mode not in tables_data[result.table]:
                tables_data[result.table][result.io_mode] = []
            tables_data[result.table][result.io_mode].append(result.throughput_rows_sec)

        for table in self.WORKING_TABLES:
            if table in tables_data:
                if has_both_modes:
                    sync_data = tables_data[table].get("sync", [])
                    async_data = tables_data[table].get("async", [])
                    sync_avg = sum(sync_data) / len(sync_data) if sync_data else 0
                    async_avg = sum(async_data) / len(async_data) if async_data else 0
                    print(f"{table:<15} Sync:  {sync_avg:>12,.0f}  |  Async: {async_avg:>12,.0f}")
                else:
                    for io_mode in sorted(tables_data[table].keys()):
                        throughputs = tables_data[table][io_mode]
                        avg = sum(throughputs) / len(throughputs)
                        print(f"{table:<15} [{io_mode:<5}] avg: {avg:>12,.0f}")

        print("\n" + "-" * 80)

        # Write rate by table
        print("\nWRITE RATE BY TABLE (MB/sec):")
        print("-" * 60)

        write_rates = {}
        for result in self.results:
            if result.table not in write_rates:
                write_rates[result.table] = {}
            if result.io_mode not in write_rates[result.table]:
                write_rates[result.table][result.io_mode] = []
            write_rates[result.table][result.io_mode].append(result.write_rate_mb_sec)

        for table in self.WORKING_TABLES:
            if table in write_rates:
                if has_both_modes:
                    sync_data = write_rates[table].get("sync", [])
                    async_data = write_rates[table].get("async", [])
                    sync_avg = sum(sync_data) / len(sync_data) if sync_data else 0
                    async_avg = sum(async_data) / len(async_data) if async_data else 0
                    print(f"{table:<15} Sync:  {sync_avg:>10.2f}  |  Async: {async_avg:>10.2f}")
                else:
                    for io_mode in sorted(write_rates[table].keys()):
                        rates = write_rates[table][io_mode]
                        avg = sum(rates) / len(rates)
                        print(f"{table:<15} [{io_mode:<5}] avg: {avg:>10.2f}")

        print("\n" + "-" * 80)

        # Format comparison
        print("\nFORMAT COMPARISON (average throughput):")
        print("-" * 60)

        format_data = {}
        for result in self.results:
            if result.format not in format_data:
                format_data[result.format] = {}
            if result.io_mode not in format_data[result.format]:
                format_data[result.format][result.io_mode] = []
            format_data[result.format][result.io_mode].append(result.throughput_rows_sec)

        for fmt in self.FORMATS:
            if fmt in format_data:
                if has_both_modes:
                    sync_data = format_data[fmt].get("sync", [])
                    async_data = format_data[fmt].get("async", [])
                    sync_avg = sum(sync_data) / len(sync_data) if sync_data else 0
                    async_avg = sum(async_data) / len(async_data) if async_data else 0
                    print(f"{fmt:<15} Sync:  {sync_avg:>12,.0f}  |  Async: {async_avg:>12,.0f}")
                else:
                    for io_mode in sorted(format_data[fmt].keys()):
                        throughputs = format_data[fmt][io_mode]
                        avg = sum(throughputs) / len(throughputs)
                        print(f"{fmt:<15} [{io_mode:<5}] {avg:>12,.0f} rows/sec")

        # Save detailed results to JSON
        json_file = self.output_dir / "phase10_results.json"
        with open(json_file, 'w') as f:
            json.dump([asdict(r) for r in self.results], f, indent=2)
        print(f"\nDetailed results saved to: {json_file}")

        print("\n" + "="*80 + "\n")


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Phase 10: Comprehensive TPC-H Performance Benchmarking")
    parser.add_argument("--exe", type=str, default=None, help="Path to tpch_benchmark executable")
    args = parser.parse_args()

    # Find benchmark executable
    exe_path = None
    if args.exe:
        exe_path = Path(args.exe)
    else:
        exe_path = Path("./build/tpch_benchmark")
        if not exe_path.exists():
            # Try alternative paths
            for candidate in [
                "./build/tpch_benchmark",
                "./tpch_benchmark",
                "/home/tsafin/src/tpch-cpp/build/tpch_benchmark"
            ]:
                if Path(candidate).exists():
                    exe_path = Path(candidate)
                    break

    if not exe_path or not exe_path.exists():
        print("ERROR: Could not find tpch_benchmark executable")
        print("Expected paths:")
        print("  - ./build/tpch_benchmark")
        print("  - ./tpch_benchmark")
        print("  - /home/tsafin/src/tpch-cpp/build/tpch_benchmark")
        if args.exe:
            print(f"  - {args.exe} (provided)")
        sys.exit(1)

    benchmarker = Phase10Benchmarker(str(exe_path))

    print("\n[INFO] Running comprehensive benchmarking for BOTH sync and async I/O modes")
    print("[INFO] Ensure the benchmark was compiled with -DTPCH_ENABLE_ASYNC_IO=ON for async tests\n")

    # Run benchmarks for both sync and async I/O modes
    success = benchmarker.benchmark_both_io_modes()

    # Generate comparison report
    benchmarker.generate_report()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
