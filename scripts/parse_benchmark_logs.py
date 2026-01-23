#!/usr/bin/env python3

"""
Parse TPC-H Benchmark Logs and Generate JSON Summary

Parses benchmark output logs and extracts key metrics (throughput, timing, file size).
Generates a JSON summary suitable for GitHub Actions artifacts.

Usage:
  python3 parse_benchmark_logs.py <log_directory> [output_file]

Arguments:
  log_directory    Directory containing .log files from benchmarks
  output_file      Output JSON file (default: stdout)
"""

import json
import sys
import os
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class BenchmarkResult:
    """Represents a single benchmark result"""
    test_name: str
    format: str
    table: str
    mode: str
    scale_factor: int
    rows_written: Optional[int] = None
    throughput_rows_per_sec: Optional[int] = None
    write_rate_mb_per_sec: Optional[float] = None
    file_size_bytes: Optional[int] = None
    elapsed_time_ms: Optional[int] = None
    success: bool = True
    error_message: Optional[str] = None


class BenchmarkLogParser:
    """Parser for TPC-H benchmark output logs"""

    # Regex patterns for extracting metrics
    PATTERNS = {
        'rows_written': r'Rows written:\s+(\d+)',
        'throughput': r'Throughput:\s+(\d+)\s+rows/sec',
        'write_rate': r'Write rate:\s+([\d.]+)\s+MB/sec',
        'file_size': r'File size:\s+(\d+)\s+bytes',
        'elapsed_time': r'Elapsed time:\s+(\d+(?:\.\d+)?)\s+(?:sec|seconds)',
    }

    # Critical error patterns that indicate benchmark failure
    CRITICAL_ERRORS = {
        'crash': r'Segmentation fault|SIGSEGV',
        'oom': r'out of memory|ENOMEM|bad_alloc',
        'timeout': r'timeout|timed out',
        'fatal': r'FATAL|Fatal error',
    }

    def __init__(self, log_directory: str):
        self.log_directory = Path(log_directory)
        self.results: List[BenchmarkResult] = []

    def parse_log_file(self, log_file: Path) -> Optional[BenchmarkResult]:
        """Parse a single benchmark log file"""
        try:
            with open(log_file, 'r') as f:
                content = f.read()

            # Extract test name from filename
            # Format: {format}_{table}_{mode}.log
            stem = log_file.stem
            parts = stem.split('_')

            if len(parts) < 3:
                print(f"Warning: Could not parse filename: {log_file.name}", file=sys.stderr)
                return None

            # Handle table names that may contain underscores
            # Assume format is always first, mode is always last
            format_name = parts[0]
            mode_name = parts[-1]
            table_name = '_'.join(parts[1:-1])

            result = BenchmarkResult(
                test_name=stem,
                format=format_name,
                table=table_name,
                mode=mode_name,
                scale_factor=1,  # Default to SF=1 for CI
            )

            # Extract metrics using regex patterns
            for metric, pattern in self.PATTERNS.items():
                match = re.search(pattern, content)
                if match:
                    value = match.group(1)
                    if metric == 'rows_written':
                        result.rows_written = int(value)
                    elif metric == 'throughput':
                        result.throughput_rows_per_sec = int(value)
                    elif metric == 'write_rate':
                        result.write_rate_mb_per_sec = float(value)
                    elif metric == 'file_size':
                        result.file_size_bytes = int(value)
                    elif metric == 'elapsed_time':
                        # Convert seconds to milliseconds
                        result.elapsed_time_ms = int(float(value) * 1000)

            # Check for critical errors only
            for error_type, pattern in self.CRITICAL_ERRORS.items():
                if re.search(pattern, content, re.IGNORECASE):
                    result.success = False
                    result.error_message = f"Benchmark {error_type} detected"
                    return result

            # Check for empty output (indicates timeout or crash)
            if len(content) < 50:
                result.success = False
                result.error_message = "Incomplete output (possible timeout or crash)"
                return result

            # Success if we extracted at least some metrics
            if result.throughput_rows_per_sec is not None or result.rows_written is not None:
                result.success = True
            else:
                # If no metrics were extracted and no critical errors, still mark as incomplete
                result.success = False
                result.error_message = "Could not extract benchmark metrics"

            return result

        except Exception as e:
            print(f"Error parsing {log_file.name}: {e}", file=sys.stderr)
            return None

    def parse_all_logs(self) -> List[BenchmarkResult]:
        """Parse all log files in the directory"""
        log_files = sorted(self.log_directory.glob('*.log'))

        if not log_files:
            print(f"Warning: No log files found in {self.log_directory}", file=sys.stderr)
            return []

        for log_file in log_files:
            result = self.parse_log_file(log_file)
            if result:
                self.results.append(result)

        return self.results


def generate_summary(results: List[BenchmarkResult]) -> Dict[str, Any]:
    """Generate a comprehensive summary from results"""

    summary = {
        'timestamp': datetime.now().isoformat(),
        'total_benchmarks': len(results),
        'passed': sum(1 for r in results if r.success),
        'failed': sum(1 for r in results if not r.success),
        'benchmarks': [asdict(r) for r in results],
        'statistics': {
            'by_format': {},
            'by_table': {},
            'by_mode': {},
        }
    }

    # Aggregate statistics
    by_format: Dict[str, List[BenchmarkResult]] = {}
    by_table: Dict[str, List[BenchmarkResult]] = {}
    by_mode: Dict[str, List[BenchmarkResult]] = {}

    for result in results:
        if result.format not in by_format:
            by_format[result.format] = []
        by_format[result.format].append(result)

        if result.table not in by_table:
            by_table[result.table] = []
        by_table[result.table].append(result)

        if result.mode not in by_mode:
            by_mode[result.mode] = []
        by_mode[result.mode].append(result)

    # Calculate per-format statistics
    for format_name, format_results in by_format.items():
        successful = [r for r in format_results if r.success]
        summary['statistics']['by_format'][format_name] = {
            'count': len(format_results),
            'success': len(successful),
            'avg_throughput': sum(r.throughput_rows_per_sec for r in successful
                                    if r.throughput_rows_per_sec) // len(successful)
                              if successful else 0,
        }

    # Calculate per-table statistics
    for table_name, table_results in by_table.items():
        successful = [r for r in table_results if r.success]
        summary['statistics']['by_table'][table_name] = {
            'count': len(table_results),
            'success': len(successful),
            'avg_throughput': sum(r.throughput_rows_per_sec for r in successful
                                    if r.throughput_rows_per_sec) // len(successful)
                              if successful else 0,
        }

    # Calculate per-mode statistics
    for mode_name, mode_results in by_mode.items():
        successful = [r for r in mode_results if r.success]
        summary['statistics']['by_mode'][mode_name] = {
            'count': len(mode_results),
            'success': len(successful),
            'avg_throughput': sum(r.throughput_rows_per_sec for r in successful
                                    if r.throughput_rows_per_sec) // len(successful)
                              if successful else 0,
        }

    return summary


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 parse_benchmark_logs.py <log_directory> [output_file]", file=sys.stderr)
        sys.exit(1)

    log_dir = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    if not os.path.isdir(log_dir):
        print(f"Error: Directory not found: {log_dir}", file=sys.stderr)
        sys.exit(1)

    # Parse logs
    parser = BenchmarkLogParser(log_dir)
    results = parser.parse_all_logs()

    if not results:
        print("Warning: No benchmark results to summarize", file=sys.stderr)
        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_benchmarks': 0,
            'passed': 0,
            'failed': 0,
            'benchmarks': [],
            'statistics': {}
        }
    else:
        summary = generate_summary(results)

    # Output JSON
    json_output = json.dumps(summary, indent=2)

    if output_file:
        with open(output_file, 'w') as f:
            f.write(json_output)
        print(f"Summary written to: {output_file}", file=sys.stderr)
    else:
        print(json_output)

    return 0


if __name__ == '__main__':
    sys.exit(main())
