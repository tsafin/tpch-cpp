#!/usr/bin/env python3
"""
Visualize Comprehensive Benchmark Results

Creates markdown tables and charts from benchmark results JSON.

Usage:
    python3 visualize_comprehensive_results.py benchmark_results.json
"""

import json
import sys
import argparse
from pathlib import Path
import subprocess

try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("Warning: matplotlib not available, skipping charts", file=sys.stderr)

def format_number(num):
    """Format number with comma separators"""
    if num is None:
        return "N/A"
    return f"{num:,.0f}"

def generate_markdown_table(summary, tables, formats):
    """Generate markdown table from summary data"""
    md = []
    md.append("\n## Consolidated Performance Table (SF=5, Zero-Copy Enabled, Maximum Values)\n")
    md.append("| Format | " + " | ".join(tables) + " | Avg Overall |")
    md.append("|--------|" + "|".join(["--------"] * len(tables)) + "|-------------|")

    overall_averages = {}

    for format_name in formats:
        row = [format_name.upper()]
        format_avg = []

        for table in tables:
            table_data = summary.get(format_name, {}).get(table)
            if table_data and table_data.get('max'):
                max_val = table_data['max']
                stddev = table_data.get('stddev_pct', 0)
                format_avg.append(max_val)
                cell = f"{format_number(max_val)} "
                if stddev > 15:
                    cell += "⚠️"
                row.append(cell)
            else:
                row.append("FAIL ❌")

        # Calculate overall average for this format
        if format_avg:
            overall = sum(format_avg) / len(format_avg)
            overall_averages[format_name] = overall
            row.append(f"**{format_number(overall)}**")
        else:
            overall_averages[format_name] = 0
            row.append("N/A")

        md.append("| " + " | ".join(row) + " |")

    md.append("\n*All values in rows/second. ⚠️ indicates >15% variance between runs.*\n")

    return "\n".join(md), overall_averages

def generate_detailed_table(summary, tables, formats):
    """Generate detailed table with min/max/stddev"""
    md = []
    md.append("\n## Detailed Performance Metrics\n")

    for table in tables:
        md.append(f"\n### {table.capitalize()} Table\n")
        md.append("| Format | Maximum | Min | Avg | StdDev % | Stability |")
        md.append("|--------|---------|-----|-----|----------|-----------|")

        for format_name in formats:
            table_data = summary.get(format_name, {}).get(table)
            if table_data and table_data.get('max'):
                avg = table_data['average']
                min_val = table_data['min']
                max_val = table_data['max']
                stddev = table_data['stddev_pct']

                # Stability rating
                if stddev < 5:
                    stability = "Excellent ✅"
                elif stddev < 15:
                    stability = "Good"
                elif stddev < 30:
                    stability = "Moderate ⚠️"
                else:
                    stability = "Poor ❌"

                md.append(f"| {format_name.upper()} | {format_number(max_val)} | "
                         f"{format_number(min_val)} | {format_number(avg)} | "
                         f"{stddev:.1f}% | {stability} |")
            else:
                md.append(f"| {format_name.upper()} | FAILED | - | - | - | - |")

    return "\n".join(md)

def create_performance_chart(summary, tables, formats, output_file):
    """Create bar chart comparing formats across tables"""
    if not HAS_MATPLOTLIB:
        return

    # Prepare data
    num_formats = len(formats)
    num_tables = len(tables)

    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

    # Chart 1: Per-table comparison
    x = np.arange(num_tables)
    width = 0.8 / num_formats

    for i, format_name in enumerate(formats):
        values = []
        for table in tables:
            table_data = summary.get(format_name, {}).get(table)
            if table_data and table_data.get('max'):
                values.append(table_data['max'])
            else:
                values.append(0)

        offset = width * (i - num_formats / 2 + 0.5)
        ax1.bar(x + offset, values, width, label=format_name.upper())

    ax1.set_xlabel('Table', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Throughput (rows/sec)', fontsize=12, fontweight='bold')
    ax1.set_title('Performance Comparison by Table (SF=5, Zero-Copy, Maximum Values)', fontsize=14, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels([t.capitalize() for t in tables])
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)

    # Format y-axis with commas
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

    # Chart 2: Overall maximum comparison
    overall_maxs = []
    for format_name in formats:
        format_max = []
        for table in tables:
            table_data = summary.get(format_name, {}).get(table)
            if table_data and table_data.get('max'):
                format_max.append(table_data['max'])

        if format_max:
            overall_maxs.append(sum(format_max) / len(format_max))
        else:
            overall_maxs.append(0)

    colors = plt.cm.Set3(np.linspace(0, 1, num_formats))
    bars = ax2.bar([f.upper() for f in formats], overall_maxs, color=colors)

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')

    ax2.set_ylabel('Average of Maximum Throughput (rows/sec)', fontsize=12, fontweight='bold')
    ax2.set_title('Overall Maximum Performance by Format', fontsize=14, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"✅ Chart saved to: {output_file}")

def create_stability_chart(summary, tables, formats, output_file):
    """Create chart showing stability (stddev %) for each format"""
    if not HAS_MATPLOTLIB:
        return

    fig, ax = plt.subplots(figsize=(12, 6))

    num_formats = len(formats)
    num_tables = len(tables)
    x = np.arange(num_tables)
    width = 0.8 / num_formats

    for i, format_name in enumerate(formats):
        stddevs = []
        for table in tables:
            table_data = summary.get(format_name, {}).get(table)
            if table_data and 'stddev_pct' in table_data:
                stddevs.append(table_data['stddev_pct'])
            else:
                stddevs.append(0)

        offset = width * (i - num_formats / 2 + 0.5)
        ax.bar(x + offset, stddevs, width, label=format_name.upper())

    ax.set_xlabel('Table', fontsize=12, fontweight='bold')
    ax.set_ylabel('Variance (% StdDev)', fontsize=12, fontweight='bold')
    ax.set_title('Stability Comparison Across Formats (Lower is Better)', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([t.capitalize() for t in tables])
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    # Add reference lines
    ax.axhline(y=5, color='green', linestyle='--', linewidth=1, alpha=0.5, label='Excellent (<5%)')
    ax.axhline(y=15, color='orange', linestyle='--', linewidth=1, alpha=0.5, label='Moderate (15%)')
    ax.axhline(y=30, color='red', linestyle='--', linewidth=1, alpha=0.5, label='Poor (30%)')

    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"✅ Stability chart saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Visualize benchmark results')
    parser.add_argument('results_file', help='Path to benchmark_results.json')
    parser.add_argument('--output', default='BENCHMARK_COMPREHENSIVE_RESULTS.md',
                        help='Output markdown file')
    args = parser.parse_args()

    results_file = Path(args.results_file)
    if not results_file.exists():
        print(f"ERROR: Results file not found: {results_file}", file=sys.stderr)
        sys.exit(1)

    # Load results
    with open(results_file) as f:
        data = json.load(f)

    metadata = data['metadata']
    summary = data['summary']
    raw_results = data['raw_results']

    tables = metadata['tables']
    formats = metadata['formats']
    scale_factor = metadata['scale_factor']
    timestamp = metadata['timestamp']

    # Generate markdown report
    md_lines = []
    md_lines.append(f"# Comprehensive Multi-Format Benchmark Results\n")
    md_lines.append(f"**Date**: {timestamp}\n")
    md_lines.append(f"**Scale Factor**: {scale_factor}\n")
    md_lines.append(f"**Tables Tested**: {', '.join(tables)}\n")
    md_lines.append(f"**Formats Tested**: {', '.join([f.upper() for f in formats])}\n")
    md_lines.append(f"**Runs per Benchmark**: {metadata['runs']}\n")
    md_lines.append(f"**Optimization**: Zero-copy enabled (recommended mode)\n")

    # Main summary table
    table_md, overall_averages = generate_markdown_table(summary, tables, formats)
    md_lines.append(table_md)

    # Ranking
    md_lines.append("\n## Format Rankings (by Overall Average)\n")
    sorted_formats = sorted(overall_averages.items(), key=lambda x: x[1], reverse=True)
    for i, (format_name, avg) in enumerate(sorted_formats, 1):
        if avg > 0:
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            md_lines.append(f"{i}. {medal} **{format_name.upper()}**: {format_number(avg)} rows/sec\n")

    # Detailed tables
    detailed_md = generate_detailed_table(summary, tables, formats)
    md_lines.append(detailed_md)

    # Key findings
    md_lines.append("\n## Key Findings\n")
    md_lines.append(f"- **Fastest Format**: {sorted_formats[0][0].upper()} ({format_number(sorted_formats[0][1])} rows/sec average)\n")

    # Most stable format (lowest average stddev)
    format_stability = {}
    for format_name in formats:
        stddevs = []
        for table in tables:
            table_data = summary.get(format_name, {}).get(table)
            if table_data and 'stddev_pct' in table_data:
                stddevs.append(table_data['stddev_pct'])
        if stddevs:
            format_stability[format_name] = sum(stddevs) / len(stddevs)

    if format_stability:
        most_stable = min(format_stability.items(), key=lambda x: x[1])
        md_lines.append(f"- **Most Stable**: {most_stable[0].upper()} ({most_stable[1]:.1f}% average variance)\n")

    md_lines.append(f"\n**Note**: All performance values show MAXIMUM throughput achieved across 3 runs.\n")

    # Write markdown file
    output_md = Path(args.output)
    with open(output_md, 'w') as f:
        f.write('\n'.join(md_lines))

    print(f"✅ Markdown report saved to: {output_md}")

    # Generate charts if matplotlib is available
    if HAS_MATPLOTLIB:
        chart_file = output_md.parent / "benchmark_performance_chart.png"
        create_performance_chart(summary, tables, formats, chart_file)

        stability_chart_file = output_md.parent / "benchmark_stability_chart.png"
        create_stability_chart(summary, tables, formats, stability_chart_file)

    return 0

if __name__ == '__main__':
    sys.exit(main())
