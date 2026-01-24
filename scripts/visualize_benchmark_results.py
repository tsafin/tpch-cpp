#!/usr/bin/env python3

"""
Visualize Benchmark Results - Generate HTML Report from ci_summary.json

Converts the JSON benchmark summary into an interactive HTML report with:
- Summary statistics (pass/fail counts)
- Throughput comparison charts
- Formatted tables by format, table, and mode
- Detailed individual results

Usage:
  python3 visualize_benchmark_results.py <ci_summary.json> [output.html]

"""

import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List


def format_throughput(value: int) -> str:
    """Format throughput value with thousands separator."""
    if value is None:
        return "N/A"
    return f"{value:,}"


def format_filesize(bytes_val: int) -> str:
    """Format file size in human-readable format."""
    if bytes_val is None:
        return "N/A"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f} TB"


def format_time(ms: int) -> str:
    """Format time in milliseconds."""
    if ms is None:
        return "N/A"
    if ms < 1000:
        return f"{ms} ms"
    return f"{ms/1000:.1f} sec"


def generate_html(summary: Dict[str, Any]) -> str:
    """Generate HTML report from summary data."""

    results = summary.get('benchmarks', [])
    stats = summary.get('statistics', {})
    timestamp = summary.get('timestamp', 'Unknown')

    passed = summary.get('passed', 0)
    failed = summary.get('failed', 0)
    total = summary.get('total_benchmarks', 0)

    # Build bar chart data for throughput by format
    by_format_data = stats.get('by_format', {})
    format_labels = list(by_format_data.keys())
    format_throughputs = [by_format_data[fmt].get('avg_throughput', 0) for fmt in format_labels]

    # Build bar chart data for throughput by table
    by_table_data = stats.get('by_table', {})
    table_labels = list(by_table_data.keys())
    table_throughputs = [by_table_data[tbl].get('avg_throughput', 0) for tbl in table_labels]

    # Build results table
    results_rows = ""
    for r in results:
        status = "✓ PASS" if r.get('success') else "✗ FAIL"
        status_class = "pass" if r.get('success') else "fail"

        results_rows += f"""
    <tr class="{status_class}">
        <td>{r.get('test_name', 'N/A')}</td>
        <td>{r.get('format', 'N/A')}</td>
        <td>{r.get('table', 'N/A')}</td>
        <td>{r.get('mode', 'N/A')}</td>
        <td class="numeric">{format_throughput(r.get('throughput_rows_per_sec'))}</td>
        <td class="numeric">{format_time(r.get('elapsed_time_ms'))}</td>
        <td class="numeric">{format_filesize(r.get('file_size_bytes'))}</td>
        <td>{status}</td>
    </tr>
"""

    # Build format statistics table
    format_stats_rows = ""
    for fmt in format_labels:
        data = by_format_data[fmt]
        format_stats_rows += f"""
    <tr>
        <td>{fmt}</td>
        <td class="numeric">{data.get('count', 0)}</td>
        <td class="numeric">{data.get('success', 0)}</td>
        <td class="numeric">{format_throughput(data.get('avg_throughput', 0))}</td>
    </tr>
"""

    # Build table statistics table
    table_stats_rows = ""
    for tbl in table_labels:
        data = by_table_data[tbl]
        table_stats_rows += f"""
    <tr>
        <td>{tbl}</td>
        <td class="numeric">{data.get('count', 0)}</td>
        <td class="numeric">{data.get('success', 0)}</td>
        <td class="numeric">{format_throughput(data.get('avg_throughput', 0))}</td>
    </tr>
"""

    # Build mode statistics table
    by_mode_data = stats.get('by_mode', {})
    mode_labels = list(by_mode_data.keys())
    mode_stats_rows = ""
    for mode in mode_labels:
        data = by_mode_data[mode]
        mode_stats_rows += f"""
    <tr>
        <td>{mode}</td>
        <td class="numeric">{data.get('count', 0)}</td>
        <td class="numeric">{data.get('success', 0)}</td>
        <td class="numeric">{format_throughput(data.get('avg_throughput', 0))}</td>
    </tr>
"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TPC-H Benchmark Results</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@3.9.1/dist/chart.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}

        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}

        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}

        .header p {{
            font-size: 1.1em;
            opacity: 0.9;
        }}

        .content {{
            padding: 30px;
        }}

        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}

        .summary-card {{
            background: #f5f5f5;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }}

        .summary-card.pass {{
            border-left-color: #10b981;
        }}

        .summary-card.fail {{
            border-left-color: #ef4444;
        }}

        .summary-card h3 {{
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            margin-bottom: 10px;
        }}

        .summary-card .value {{
            font-size: 2em;
            font-weight: bold;
            color: #333;
        }}

        .charts {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 30px;
            margin-bottom: 40px;
        }}

        .chart-container {{
            background: #f9f9f9;
            padding: 20px;
            border-radius: 8px;
            position: relative;
            height: 300px;
        }}

        .chart-container h3 {{
            margin-bottom: 15px;
            color: #333;
        }}

        .chart-container canvas {{
            max-height: 250px;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 40px;
            background: #fafafa;
        }}

        table h3 {{
            padding: 15px;
            background: #f0f0f0;
            color: #333;
        }}

        table thead {{
            background: #667eea;
            color: white;
        }}

        table th {{
            padding: 12px;
            text-align: left;
            font-weight: 600;
        }}

        table td {{
            padding: 12px;
            border-bottom: 1px solid #e0e0e0;
        }}

        table tr:hover {{
            background: #f5f5f5;
        }}

        table tr.pass {{
            background: #f0fdf4;
        }}

        table tr.fail {{
            background: #fef2f2;
        }}

        table td.numeric {{
            text-align: right;
            font-family: 'Courier New', monospace;
            color: #666;
        }}

        .section {{
            margin-bottom: 40px;
        }}

        .section h2 {{
            color: #333;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #667eea;
        }}

        .footer {{
            background: #f5f5f5;
            padding: 20px;
            text-align: center;
            color: #999;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 TPC-H Benchmark Report</h1>
            <p>Generated: {timestamp}</p>
        </div>

        <div class="content">
            <!-- Summary Statistics -->
            <div class="section">
                <div class="summary">
                    <div class="summary-card">
                        <h3>Total Benchmarks</h3>
                        <div class="value">{total}</div>
                    </div>
                    <div class="summary-card pass">
                        <h3>Passed</h3>
                        <div class="value">{passed}</div>
                    </div>
                    <div class="summary-card fail">
                        <h3>Failed</h3>
                        <div class="value">{failed}</div>
                    </div>
                    <div class="summary-card">
                        <h3>Success Rate</h3>
                        <div class="value">{100*passed//total if total > 0 else 0}%</div>
                    </div>
                </div>
            </div>

            <!-- Charts -->
            <div class="section">
                <h2>Performance Comparisons</h2>
                <div class="charts">
                    <div class="chart-container">
                        <h3>Throughput by Format</h3>
                        <canvas id="formatChart"></canvas>
                    </div>
                    <div class="chart-container">
                        <h3>Throughput by Table</h3>
                        <canvas id="tableChart"></canvas>
                    </div>
                </div>
            </div>

            <!-- Statistics Tables -->
            <div class="section">
                <h2>Statistics by Format</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Format</th>
                            <th>Total Tests</th>
                            <th>Passed</th>
                            <th>Avg Throughput (rows/sec)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {format_stats_rows}
                    </tbody>
                </table>
            </div>

            <div class="section">
                <h2>Statistics by Table</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Table</th>
                            <th>Total Tests</th>
                            <th>Passed</th>
                            <th>Avg Throughput (rows/sec)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_stats_rows}
                    </tbody>
                </table>
            </div>

            <div class="section">
                <h2>Statistics by Mode</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Mode</th>
                            <th>Total Tests</th>
                            <th>Passed</th>
                            <th>Avg Throughput (rows/sec)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {mode_stats_rows}
                    </tbody>
                </table>
            </div>

            <!-- Detailed Results -->
            <div class="section">
                <h2>Detailed Results</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Test</th>
                            <th>Format</th>
                            <th>Table</th>
                            <th>Mode</th>
                            <th>Throughput (rows/sec)</th>
                            <th>Time</th>
                            <th>File Size</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {results_rows}
                    </tbody>
                </table>
            </div>
        </div>

        <div class="footer">
            <p>TPC-H Benchmark Visualization | {total} tests | {passed} passed | {failed} failed</p>
        </div>
    </div>

    <script>
        // Format throughput chart
        const formatCtx = document.getElementById('formatChart').getContext('2d');
        new Chart(formatCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(format_labels)},
                datasets: [{{
                    label: 'Avg Throughput (rows/sec)',
                    data: {json.dumps(format_throughputs)},
                    backgroundColor: '#667eea',
                    borderColor: '#667eea',
                    borderWidth: 1
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return value.toLocaleString();
                            }}
                        }}
                    }}
                }},
                plugins: {{
                    legend: {{display: false}}
                }}
            }}
        }});

        // Table throughput chart
        const tableCtx = document.getElementById('tableChart').getContext('2d');
        new Chart(tableCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(table_labels)},
                datasets: [{{
                    label: 'Avg Throughput (rows/sec)',
                    data: {json.dumps(table_throughputs)},
                    backgroundColor: '#764ba2',
                    borderColor: '#764ba2',
                    borderWidth: 1
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return value.toLocaleString();
                            }}
                        }}
                    }}
                }},
                plugins: {{
                    legend: {{display: false}}
                }}
            }}
        }});
    </script>
</body>
</html>
"""
    return html


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 visualize_benchmark_results.py <ci_summary.json> [output.html]", file=sys.stderr)
        sys.exit(1)

    summary_file = Path(sys.argv[1])
    output_file = Path(sys.argv[2]) if len(sys.argv) > 2 else summary_file.parent / "report.html"

    if not summary_file.exists():
        print(f"Error: {summary_file} not found", file=sys.stderr)
        sys.exit(1)

    try:
        with open(summary_file, 'r') as f:
            summary = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}", file=sys.stderr)
        sys.exit(1)

    html = generate_html(summary)

    with open(output_file, 'w') as f:
        f.write(html)

    print(f"Report generated: {output_file}", file=sys.stderr)
    print(f"Open in browser to view interactive charts and tables")

    return 0


if __name__ == '__main__':
    sys.exit(main())
