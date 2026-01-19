#!/usr/bin/env python3
"""
ORC vs Parquet Performance Comparison Benchmark

Tests all 8 TPC-H tables across multiple scale factors (1, 10, 100) in both:
- ORC format (new support)
- Parquet format (baseline)

Measures:
- Throughput (rows/sec)
- Write rate (MB/sec)
- Elapsed time
- File size
- Compression ratio (file size / raw row data estimate)

Calculates speedup ratios and identifies format tradeoffs.
"""

import subprocess
import json
import time
import re
import os
from pathlib import Path
from datetime import datetime
from statistics import mean, stdev

# All 8 TPC-H tables with row counts per scale factor
# Format: (table_name, rows_sf1, rows_sf10, rows_sf100)
TABLES = [
    ("lineitem", 6001, 60175, 600572),      # 8/16 numeric (50%)
    ("orders", 1500, 15000, 150000),        # 4/9 numeric (44%)
    ("customer", 1500, 15000, 150000),      # 2/8 numeric (25%)
    ("part", 2000, 20000, 200000),          # 3/9 numeric (33%)
    ("partsupp", 8000, 80000, 800000),      # 4/5 numeric (80%) - numeric-heavy
    ("supplier", 100, 1000, 10000),         # 2/7 numeric (29%)
    ("nation", 25, 25, 25),                 # 2/4 numeric (50%)
    ("region", 5, 5, 5),                    # 1/3 numeric (33%)
]

class OrcVsParquetBenchmark:
    def __init__(self, tpch_binary, output_dir="/tmp/orc_vs_parquet_benchmark", scale_factors=None, formats=None):
        self.tpch_binary = Path(tpch_binary)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.scale_factors = scale_factors or [1, 10, 100]
        self.formats = formats or ["orc", "parquet"]
        self.results = {}
        self.benchmark_date = datetime.now().isoformat()

        # Initialize results structure
        for sf in self.scale_factors:
            self.results[f"sf{sf}"] = {}
            for fmt in self.formats:
                self.results[f"sf{sf}"][fmt] = {}

    def _parse_metrics(self, output_text):
        """Extract metrics from program output"""
        metrics = {
            "throughput": 0.0,
            "write_rate": 0.0,
            "elapsed_time": 0.0,
            "rows_written": 0
        }

        # Parse throughput (rows/sec)
        match = re.search(r'Throughput:\s*(\d+(?:\.\d+)?)\s*rows/sec', output_text)
        if match:
            metrics["throughput"] = float(match.group(1))

        # Parse write rate (MB/sec)
        match = re.search(r'Write rate:\s*(\d+(?:\.\d+)?)\s*MB/sec', output_text)
        if match:
            metrics["write_rate"] = float(match.group(1))

        # Parse elapsed time (seconds)
        match = re.search(r'Time elapsed:\s*(\d+(?:\.\d+)?)\s*seconds', output_text)
        if match:
            metrics["elapsed_time"] = float(match.group(1))

        # Parse rows written
        match = re.search(r'Rows written:\s*(\d+)', output_text)
        if match:
            metrics["rows_written"] = int(match.group(1))

        return metrics

    def run_benchmark(self, table_name, row_count, format_type, scale_factor):
        """Run a single benchmark for one table in one format"""
        output_path = self.output_dir / f"sf{scale_factor}" / format_type / table_name
        output_path.mkdir(parents=True, exist_ok=True)

        cmd = [
            str(self.tpch_binary),
            "--use-dbgen",
            "--table", table_name,
            "--max-rows", str(row_count),
            "--format", format_type,
            "--scale-factor", str(scale_factor),
            "--output-dir", str(output_path),
            "--true-zero-copy"  # Use best available optimization
        ]

        try:
            start = time.time()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout for large scale factors
                env={**os.environ, "OMP_NUM_THREADS": "8"}
            )
            elapsed = time.time() - start

            if result.returncode != 0:
                print(f"    FAILED: {result.stderr[:100]}")
                return None

            metrics = self._parse_metrics(result.stdout)

            # Verify output file was created and get file size
            file_path = output_path / f"{table_name}.{format_type}"
            file_size = 0
            if file_path.exists():
                file_size = file_path.stat().st_size

            # Estimate raw data size (rough approximation)
            # Average ~100 bytes per row for most tables
            estimated_raw_size = row_count * 100

            return {
                "table": table_name,
                "scale_factor": scale_factor,
                "rows": row_count,
                "format": format_type,
                "elapsed_time": metrics["elapsed_time"],
                "throughput": metrics["throughput"],
                "write_rate": metrics["write_rate"],
                "file_size": file_size,
                "estimated_raw_size": estimated_raw_size,
                "compression_ratio": file_size / estimated_raw_size if estimated_raw_size > 0 else 0
            }

        except subprocess.TimeoutExpired:
            print(f"    TIMEOUT (>600s)")
            return None
        except Exception as e:
            print(f"    ERROR: {str(e)}")
            return None

    def _get_row_count(self, table_name, scale_factor):
        """Get row count for a table at given scale factor"""
        for tname, sf1, sf10, sf100 in TABLES:
            if tname == table_name:
                if scale_factor == 1:
                    return sf1
                elif scale_factor == 10:
                    return sf10
                elif scale_factor == 100:
                    return sf100
        return 0

    def run_full_benchmark(self):
        """Run benchmark for all tables, formats, and scale factors"""
        print("\n" + "="*120)
        print(f"Format Comparison Benchmark ({', '.join(self.formats).upper()})")
        print("="*120)
        print(f"Binary: {self.tpch_binary}")
        print(f"Output: {self.output_dir}")
        print(f"Scale Factors: {self.scale_factors}")
        print(f"Formats: {self.formats}")
        print(f"Date: {self.benchmark_date}")
        print("="*120)

        for scale_factor in self.scale_factors:
            print(f"\n{'='*120}")
            print(f"Scale Factor: {scale_factor}")
            print(f"{'='*120}\n")

            for format_type in self.formats:
                print(f"Format: {format_type.upper()}")
                print(f"{'Table':<15} {'Rows':<12} {'Throughput':>15} {'Write Rate':>12} {'Time':>10} {'File Size':>12}")
                print("-" * 120)

                format_results = []

                for table_name, _, _, _ in TABLES:
                    row_count = self._get_row_count(table_name, scale_factor)
                    if row_count == 0:
                        continue

                    print(f"{table_name:<15} {row_count:<12} ", end="", flush=True)

                    result = self.run_benchmark(table_name, row_count, format_type, scale_factor)

                    if result:
                        throughput = result["throughput"]
                        write_rate = result["write_rate"]
                        elapsed = result["elapsed_time"]
                        file_size = result["file_size"]

                        print(f"{throughput:>15,.0f} {write_rate:>12.2f} MB/s {elapsed:>9.3f}s {file_size/1024/1024:>11.2f}M")

                        sf_key = f"sf{scale_factor}"
                        self.results[sf_key][format_type][table_name] = result
                        format_results.append(result)
                    else:
                        print("FAILED")

                if format_results:
                    avg_throughput = mean([r["throughput"] for r in format_results if r["throughput"] > 0])
                    avg_write_rate = mean([r["write_rate"] for r in format_results if r["write_rate"] > 0])
                    print(f"\n{'-'*120}")
                    print(f"{'Average':<15} {avg_throughput:>15,.0f} {avg_write_rate:>12.2f} MB/s")
                print()

    def compare_results(self):
        """Compare formats across all scale factors"""
        if len(self.formats) < 2:
            print("\nWarning: Need at least 2 formats to compare. Skipping comparison.")
            return

        print("\n" + "="*120)
        print(f"Format Comparison (Throughput)")
        print("="*120)

        fmt1, fmt2 = self.formats[0], self.formats[1]

        for scale_factor in self.scale_factors:
            sf_key = f"sf{scale_factor}"
            print(f"\n{'='*120}")
            print(f"Scale Factor {scale_factor}: {fmt1.upper()} vs {fmt2.upper()}")
            print(f"{'='*120}")
            print(f"{'Table':<15} {fmt1.upper():>15} {fmt2.upper():>15} {'Speedup':>12} {fmt1.upper()+' MB/s':>12} {fmt2.upper()+' MB/s':>12} {'Better':<10}")
            print("-" * 120)

            speedups = []
            format_wins = {fmt1: 0, fmt2: 0}

            for table_name, _, _, _ in TABLES:
                result1 = self.results[sf_key][fmt1].get(table_name)
                result2 = self.results[sf_key][fmt2].get(table_name)

                if result1 and result2:
                    tp1 = result1["throughput"]
                    tp2 = result2["throughput"]
                    wr1 = result1["write_rate"]
                    wr2 = result2["write_rate"]

                    if tp1 > 0 and tp2 > 0:
                        speedup = tp2 / tp1
                        speedups.append((table_name, speedup))

                        # Determine which format is better (higher throughput)
                        better = fmt2.upper() if tp2 > tp1 else fmt1.upper()
                        if tp2 > tp1:
                            format_wins[fmt2] += 1
                        else:
                            format_wins[fmt1] += 1

                        sign = "+" if speedup > 1 else ""
                        print(f"{table_name:<15} {tp1:>15,.0f} {tp2:>15,.0f} {sign}{(speedup-1)*100:>10.1f}% {wr1:>12.2f} {wr2:>12.2f} {better:<10}")
                else:
                    print(f"{table_name:<15} (missing data)")

            if speedups:
                avg_speedup = mean([s[1] for s in speedups])
                print("-" * 120)
                sign = "+" if avg_speedup > 1 else ""
                print(f"{'Average':>15} {sign}{(avg_speedup-1)*100:>23.1f}% ({fmt2.upper()} is {avg_speedup:.2f}x)")
                print(f"{'Win Count':<15} {fmt1.upper()}: {format_wins[fmt1]}, {fmt2.upper()}: {format_wins[fmt2]}")

    def analyze_scale_factor_effect(self):
        """Analyze how scale factor affects format performance"""
        if len(self.scale_factors) < 2:
            print("\nWarning: Need at least 2 scale factors for effect analysis. Skipping.")
            return

        print("\n" + "="*120)
        print("Scale Factor Impact Analysis")
        print("="*120)

        header_formats = "\t".join([f.upper() for f in self.formats])

        for table_name, _, _, _ in TABLES:
            print(f"\n{table_name}:")
            header_line = f"{'Scale Factor':<15}" + "\t".join([f"{f.upper()+' TP':>15}{f.upper()+' MB/s':>15}" for f in self.formats])
            print(header_line)
            print("-" * 80)

            for scale_factor in self.scale_factors:
                sf_key = f"sf{scale_factor}"
                line = f"SF {scale_factor:<13}"

                has_data = False
                for fmt in self.formats:
                    result = self.results[sf_key][fmt].get(table_name)
                    if result:
                        has_data = True
                        line += f"{result['throughput']:>15,.0f}{result['write_rate']:>15.2f}"

                if has_data:
                    print(line)

    def save_results(self):
        """Save detailed results to JSON"""
        output_file = self.output_dir / "benchmark_results.json"

        summary = {
            "date": self.benchmark_date,
            "binary": str(self.tpch_binary),
            "scale_factors": self.scale_factors,
            "results": self.results
        }

        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)

        print(f"\n\nDetailed results saved to: {output_file}")


def main():
    import sys

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <path/to/tpch_benchmark> [output_dir] [scale_factors] [formats]")
        print(f"Example: {sys.argv[0]} ./build/tpch_benchmark /tmp/benchmark 1,10,100 orc,csv")
        print(f"Formats: orc, parquet, csv (comma-separated, default: orc,parquet)")
        sys.exit(1)

    tpch_binary = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "/tmp/format_benchmark"
    scale_factors = [1, 10, 100]
    formats = ["orc", "parquet"]

    if len(sys.argv) > 3:
        try:
            scale_factors = [int(x.strip()) for x in sys.argv[3].split(",")]
        except ValueError:
            print("Error: scale_factors must be comma-separated integers")
            sys.exit(1)

    if len(sys.argv) > 4:
        formats = [x.strip().lower() for x in sys.argv[4].split(",")]

    # Verify binary exists
    if not Path(tpch_binary).exists():
        print(f"Error: Binary not found: {tpch_binary}")
        sys.exit(1)

    benchmark = OrcVsParquetBenchmark(tpch_binary, output_dir, scale_factors, formats)

    # Run all benchmarks
    benchmark.run_full_benchmark()

    # Analyze and compare results
    benchmark.compare_results()
    benchmark.analyze_scale_factor_effect()

    # Save results
    benchmark.save_results()

    print("\n" + "="*120)
    print("ORC vs Parquet Benchmark Complete!")
    print("="*120)


if __name__ == "__main__":
    main()
