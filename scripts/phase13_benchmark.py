#!/usr/bin/env python3
"""
Phase 13 Performance Regression Test Suite

Benchmarks all optimization phases:
- Baseline (no optimizations)
- Zero-copy only (13.4)
- All optimizations combined

Generates comparison report and validates improvements.
"""

import subprocess
import json
import time
import sys
from pathlib import Path
from typing import List, Dict, Any


class Phase13Benchmark:
    def __init__(self, tpch_binary: str, output_dir: str):
        self.tpch_binary = Path(tpch_binary)
        self.output_dir = Path(output_dir)
        self.results: List[Dict[str, Any]] = []

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def run_benchmark(self, name: str, extra_flags: List[str] = []) -> Dict[str, Any]:
        """Run single benchmark configuration"""
        cmd = [
            str(self.tpch_binary),
            "--use-dbgen",
            "--table", "lineitem",
            "--scale-factor", "1",
            "--max-rows", "50000",
            "--format", "parquet",
            "--output-dir", str(self.output_dir)
        ] + extra_flags

        print(f"\nRunning: {name}")
        print(f"  Command: {' '.join(cmd)}")

        # Run benchmark 3 times and take best result (reduce noise)
        best_time = float('inf')
        best_result = None

        for run in range(3):
            print(f"  Run {run + 1}/3...", end="", flush=True)

            start = time.time()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            elapsed = time.time() - start

            if elapsed < best_time:
                best_time = elapsed
                best_result = result

            print(f" {elapsed:.3f}s")

        if best_result is None:
            raise RuntimeError(f"Benchmark {name} failed")

        # Parse output for metrics
        throughput = self._parse_throughput(best_result.stdout)

        result_data = {
            "name": name,
            "elapsed_time": best_time,
            "throughput_rows_per_sec": throughput,
            "flags": extra_flags,
            "stdout": best_result.stdout,
            "stderr": best_result.stderr
        }

        self.results.append(result_data)

        print(f"  ✓ Best: {best_time:.3f}s ({throughput:,.0f} rows/sec)")

        return result_data

    def _parse_throughput(self, stdout: str) -> float:
        """Extract throughput from benchmark output"""
        for line in stdout.split('\n'):
            if 'rows/sec' in line.lower() or 'rows/s' in line.lower():
                # Try to find a number followed by "rows/sec" or "rows/s"
                import re
                match = re.search(r'([\d,]+(?:\.\d+)?)\s*rows/(?:sec|s)', line, re.IGNORECASE)
                if match:
                    return float(match.group(1).replace(',', ''))

        # If not found in output, calculate from elapsed time
        # Assume 50,000 rows
        for result in self.results:
            if result.get("elapsed_time"):
                return 50000 / result["elapsed_time"]

        return 0

    def run_all_benchmarks(self):
        """Execute full benchmark suite"""

        print("=" * 80)
        print("Phase 13 Performance Benchmark Suite")
        print("=" * 80)

        # Baseline (regular path, no optimizations)
        self.run_benchmark("Baseline (Regular Path)", [])

        # Zero-copy optimization only
        self.run_benchmark("Zero-Copy Only", ["--zero-copy"])

        # With parallel generation (if supported)
        try:
            self.run_benchmark("Parallel Generation (Regular)", ["--parallel"])
        except subprocess.TimeoutExpired:
            print("  ⚠️ Parallel generation timed out, skipping")
        except Exception as e:
            print(f"  ⚠️ Parallel generation failed: {e}")

        # Parallel + zero-copy
        try:
            self.run_benchmark("Parallel + Zero-Copy", ["--parallel", "--zero-copy"])
        except subprocess.TimeoutExpired:
            print("  ⚠️ Parallel + zero-copy timed out, skipping")
        except Exception as e:
            print(f"  ⚠️ Parallel + zero-copy failed: {e}")

    def generate_report(self):
        """Generate comparison report"""
        if not self.results:
            print("\n❌ No benchmark results to report")
            return

        baseline_throughput = self.results[0]["throughput_rows_per_sec"]
        baseline_time = self.results[0]["elapsed_time"]

        print("\n" + "=" * 80)
        print("Phase 13 Performance Report")
        print("=" * 80)
        print(f"{'Configuration':<35} {'Time (s)':>10} {'Throughput':>15} {'Speedup':>10}")
        print("-" * 80)

        for result in self.results:
            elapsed = result["elapsed_time"]
            throughput = result["throughput_rows_per_sec"]
            speedup = throughput / baseline_throughput if baseline_throughput > 0 else 0

            print(f"{result['name']:<35} {elapsed:>10.3f} {throughput:>12,.0f}/s {speedup:>9.2f}x")

        print("=" * 80)

        # Check if we met success criteria
        print("\nSuccess Criteria Validation:")
        print("-" * 80)

        # Find zero-copy result
        zero_copy_result = next(
            (r for r in self.results if "Zero-Copy" in r["name"] and "Parallel" not in r["name"]),
            None
        )

        if zero_copy_result:
            improvement = (baseline_time - zero_copy_result["elapsed_time"]) / baseline_time * 100
            target_improvement = 30  # 30% improvement target

            if improvement >= target_improvement:
                print(f"✅ Zero-copy improvement: {improvement:.1f}% (target: >{target_improvement}%)")
            else:
                print(f"⚠️ Zero-copy improvement: {improvement:.1f}% (target: >{target_improvement}%)")

        # Throughput target
        target_throughput = 1_000_000  # 1M rows/sec
        best_throughput = max(r["throughput_rows_per_sec"] for r in self.results)

        if best_throughput >= target_throughput:
            print(f"✅ Throughput achieved: {best_throughput:,.0f} rows/sec (target: >{target_throughput:,})")
        else:
            print(f"⚠️ Throughput achieved: {best_throughput:,.0f} rows/sec (target: >{target_throughput:,})")

        print("=" * 80)

        # Save JSON results
        output_file = self.output_dir / "phase13_results.json"
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\n✅ Results saved to: {output_file}")


def main():
    if len(sys.argv) < 2:
        print("Usage: phase13_benchmark.py <path-to-tpch_benchmark> [output-dir]")
        print("\nExample:")
        print("  ./scripts/phase13_benchmark.py ./build/tpch_benchmark")
        print("  ./scripts/phase13_benchmark.py ./build/tpch_benchmark ./benchmark-results")
        sys.exit(1)

    tpch_binary = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "benchmark-results"

    # Verify binary exists
    if not Path(tpch_binary).exists():
        print(f"❌ Error: Binary not found: {tpch_binary}")
        sys.exit(1)

    benchmark = Phase13Benchmark(
        tpch_binary=tpch_binary,
        output_dir=output_dir
    )

    try:
        benchmark.run_all_benchmarks()
        benchmark.generate_report()
    except KeyboardInterrupt:
        print("\n\n⚠️ Benchmark interrupted by user")
        if benchmark.results:
            benchmark.generate_report()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
