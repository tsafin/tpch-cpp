#!/usr/bin/env python3
"""
Performance Analysis Tool for TPC-H Benchmark

Parses perf report output and identifies hotspots, categorizing them by:
- Arrow/Parquet operations
- dbgen data generation
- Memory allocation
- String operations
- Other

Generates summary statistics and recommendations.
"""

import re
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict
from collections import defaultdict


@dataclass
class Hotspot:
    """Represents a single hotspot in the profile"""
    percentage: float
    symbol: str
    dso: str
    category: str


class PerformanceAnalyzer:
    """Analyzes perf report output and categorizes hotspots"""

    # Category patterns (ordered by priority)
    CATEGORIES = {
        'arrow_conversion': [
            r'arrow::.*Builder',
            r'append_.*_to_builders',
            r'dbgen_converter',
            r'arrow::RecordBatch',
            r'arrow::Array',
        ],
        'parquet_encoding': [
            r'parquet::.*Writer',
            r'parquet::.*Encoder',
            r'parquet::.*Compression',
            r'snappy::',
            r'ParquetWriter',
        ],
        'dbgen_generation': [
            r'mk_.*',  # dbgen functions like mk_order, mk_part
            r'dbgen',
            r'gen_.*',
            r'tpch_.*',
        ],
        'memory_allocation': [
            r'malloc',
            r'free',
            r'::operator new',
            r'::operator delete',
            r'::allocate',
            r'::deallocate',
            r'jemalloc',
            r'tcmalloc',
        ],
        'string_operations': [
            r'strlen',
            r'strcpy',
            r'strcat',
            r'memcpy',
            r'memset',
            r'memmove',
            r'std::.*string',
        ],
        'io_operations': [
            r'write',
            r'read',
            r'::write_async',
            r'::flush',
            r'fsync',
            r'fwrite',
        ],
    }

    def __init__(self, perf_report_path: Path):
        self.perf_report_path = perf_report_path
        self.hotspots: List[Hotspot] = []

    def parse_perf_report(self) -> None:
        """Parse perf report and extract hotspots"""

        with open(self.perf_report_path, 'r') as f:
            content = f.read()

        # Parse hotspot lines (format: "  12.34%  symbol_name  [dso]")
        pattern = r'^\s+([\d.]+)%\s+\S+\s+\[([^\]]+)\]\s+(.+)$'

        for line in content.split('\n'):
            match = re.match(pattern, line)
            if match:
                percentage = float(match.group(1))
                dso = match.group(2)
                symbol = match.group(3).strip()

                # Skip very small hotspots (<0.5%)
                if percentage < 0.5:
                    continue

                category = self._categorize_symbol(symbol, dso)

                hotspot = Hotspot(
                    percentage=percentage,
                    symbol=symbol,
                    dso=dso,
                    category=category
                )

                self.hotspots.append(hotspot)

    def _categorize_symbol(self, symbol: str, dso: str) -> str:
        """Categorize a symbol based on patterns"""

        # Check DSO first for library-specific categorization
        combined = f"{dso} {symbol}"

        for category, patterns in self.CATEGORIES.items():
            for pattern in patterns:
                if re.search(pattern, combined, re.IGNORECASE):
                    return category

        return 'other'

    def generate_report(self) -> str:
        """Generate formatted analysis report"""

        if not self.hotspots:
            return "No hotspots found. Did you parse the perf report?"

        # Aggregate by category
        category_totals: Dict[str, float] = defaultdict(float)
        for hotspot in self.hotspots:
            category_totals[hotspot.category] += hotspot.percentage

        # Sort categories by total percentage
        sorted_categories = sorted(
            category_totals.items(),
            key=lambda x: x[1],
            reverse=True
        )

        # Build report
        report = []
        report.append("=" * 80)
        report.append("Performance Analysis Report")
        report.append("=" * 80)
        report.append("")

        # Category summary
        report.append("## CPU Time by Category")
        report.append("")
        report.append(f"{'Category':<30} {'CPU Time':>12} {'Impact':>10}")
        report.append("-" * 80)

        total_cpu = sum(category_totals.values())

        for category, percentage in sorted_categories:
            impact = self._impact_level(percentage)
            report.append(f"{category:<30} {percentage:>11.2f}% {impact:>10}")

        report.append("-" * 80)
        report.append(f"{'Total Accounted':<30} {total_cpu:>11.2f}%")
        report.append("")

        # Top hotspots per category
        report.append("## Top Hotspots by Category")
        report.append("")

        for category, _ in sorted_categories:
            category_hotspots = [h for h in self.hotspots if h.category == category]
            if not category_hotspots:
                continue

            # Sort by percentage within category
            category_hotspots.sort(key=lambda h: h.percentage, reverse=True)

            report.append(f"### {category.upper().replace('_', ' ')}")
            report.append("")

            for hotspot in category_hotspots[:5]:  # Top 5
                report.append(f"  {hotspot.percentage:>6.2f}%  {hotspot.symbol[:70]}")

            report.append("")

        # Optimization recommendations
        report.append("## Optimization Recommendations")
        report.append("")
        report.append(self._generate_recommendations(category_totals))

        report.append("=" * 80)

        return "\n".join(report)

    def _impact_level(self, percentage: float) -> str:
        """Classify impact level based on CPU percentage"""
        if percentage >= 20:
            return "🔴 CRITICAL"
        elif percentage >= 10:
            return "🟠 HIGH"
        elif percentage >= 5:
            return "🟡 MEDIUM"
        else:
            return "🟢 LOW"

    def _generate_recommendations(self, category_totals: Dict[str, float]) -> str:
        """Generate optimization recommendations based on hotspots"""

        recommendations = []

        # Arrow conversion recommendations
        if category_totals.get('arrow_conversion', 0) >= 20:
            recommendations.append(
                "🔴 Arrow conversion is a CRITICAL bottleneck (>20% CPU)\n"
                "   ➜ Implement Phase 13.2: SIMD batch operations\n"
                "   ➜ Use AppendValues() instead of individual Append() calls\n"
                "   ➜ Pre-allocate builder capacity to avoid resizing\n"
            )
        elif category_totals.get('arrow_conversion', 0) >= 10:
            recommendations.append(
                "🟠 Arrow conversion is a HIGH priority optimization target\n"
                "   ➜ Consider batch append operations\n"
                "   ➜ Profile individual builder operations\n"
            )

        # Memory allocation recommendations
        if category_totals.get('memory_allocation', 0) >= 15:
            recommendations.append(
                "🔴 Memory allocation overhead is CRITICAL (>15% CPU)\n"
                "   ➜ Implement Phase 13.3: Memory pool optimizations\n"
                "   ➜ Use arena allocator for temporary buffers\n"
                "   ➜ Pre-allocate Arrow builders with capacity hints\n"
            )
        elif category_totals.get('memory_allocation', 0) >= 5:
            recommendations.append(
                "🟡 Memory allocation has room for optimization\n"
                "   ➜ Consider object pooling for frequently allocated types\n"
            )

        # Parquet encoding recommendations
        if category_totals.get('parquet_encoding', 0) >= 20:
            recommendations.append(
                "🔴 Parquet encoding is a CRITICAL bottleneck (>20% CPU)\n"
                "   ➜ Implement Phase 13.4: Parallel column writing\n"
                "   ➜ Enable Arrow's multi-threaded writer if available\n"
                "   ➜ Consider different compression algorithms (ZSTD vs SNAPPY)\n"
            )

        # String operations recommendations
        if category_totals.get('string_operations', 0) >= 10:
            recommendations.append(
                "🟠 String operations are HIGH priority\n"
                "   ➜ Use SIMD strlen (SSE4.2) for string builders\n"
                "   ➜ Batch string append operations where possible\n"
            )

        # I/O recommendations
        if category_totals.get('io_operations', 0) >= 15:
            recommendations.append(
                "🔴 I/O operations are bottleneck (>15% CPU)\n"
                "   ➜ Async I/O is already implemented - verify it's enabled\n"
                "   ➜ Consider larger write buffers\n"
                "   ➜ Profile to ensure I/O is actually async\n"
            )

        if not recommendations:
            recommendations.append(
                "✅ No critical bottlenecks identified!\n"
                "   Profile looks well-balanced. Consider:\n"
                "   ➜ Focus on algorithmic improvements in dbgen\n"
                "   ➜ Enable compiler optimizations (-O3 -march=native)\n"
            )

        return "\n".join(recommendations)


def main():
    if len(sys.argv) < 2:
        print("Usage: analyze_performance.py <perf_report.txt>")
        print("")
        print("Example:")
        print("  ./scripts/profile_benchmark.sh lineitem 1 100000")
        print("  ./scripts/analyze_performance.py /tmp/tpch-profiling/perf_report.txt")
        sys.exit(1)

    perf_report_path = Path(sys.argv[1])

    if not perf_report_path.exists():
        print(f"❌ Error: Perf report not found at {perf_report_path}")
        sys.exit(1)

    analyzer = PerformanceAnalyzer(perf_report_path)

    print("Parsing perf report...")
    analyzer.parse_perf_report()

    print(f"Found {len(analyzer.hotspots)} hotspots")
    print("")

    report = analyzer.generate_report()
    print(report)

    # Save report to file
    output_path = perf_report_path.parent / "performance_analysis.txt"
    with open(output_path, 'w') as f:
        f.write(report)

    print(f"\n✅ Analysis saved to: {output_path}")


if __name__ == "__main__":
    main()
