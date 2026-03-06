#!/usr/bin/env python3
"""
gen_dsts.py - Embed the TPC-DS binary distribution index (tpcds.idx) into a
              C source file as a static byte array.

Usage: gen_dsts.py <tpcds.idx> <output.c>

Background
----------
TPC-DS's dsdgen reads distribution data from a compiled binary file (tpcds.idx)
produced by the 'distcomp' tool.  At runtime, dist.c opens this file via:

    fopen(get_str("DISTRIBUTIONS"), "rb")

To avoid shipping tpcds.idx as a separate runtime file, we embed its bytes here
as a C uint8_t array.  DSDGenWrapper writes the embedded bytes to a tmpfile on
first use and points the DISTRIBUTIONS param at that tmpfile.

This mirrors the approach used by cmake/gen_dists.py for TPC-H's dists.dss.
"""

import sys
import os


def embed_binary(input_path: str, output_path: str) -> None:
    with open(input_path, "rb") as f:
        data = f.read()

    size = len(data)
    filename = os.path.basename(input_path)

    lines = []
    lines.append(
        "/* Auto-generated from {} by cmake/gen_dsts.py -- do not edit */".format(filename)
    )
    lines.append("")
    lines.append("#include <stddef.h>")
    lines.append("#include <stdint.h>")
    lines.append("")
    lines.append("/* Embedded binary content of {} ({} bytes) */".format(filename, size))
    lines.append("const uint8_t tpcds_idx_data[] = {")

    # 16 bytes per row for readability
    for i in range(0, size, 16):
        chunk = data[i : i + 16]
        hex_vals = ", ".join("0x{:02x}".format(b) for b in chunk)
        comma = "," if i + 16 < size else ""
        lines.append("    {}{}".format(hex_vals, comma))

    lines.append("};")
    lines.append("")
    lines.append(
        "const size_t tpcds_idx_size = {};".format(size)
    )
    lines.append("")

    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(output_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(
        "Embedded {} ({} bytes) -> {}".format(filename, size, os.path.basename(output_path))
    )


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: gen_dsts.py <tpcds.idx> <output.c>", file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    if not os.path.exists(input_path):
        print("Error: input file not found: {}".format(input_path), file=sys.stderr)
        sys.exit(1)

    embed_binary(input_path, output_path)


if __name__ == "__main__":
    main()
