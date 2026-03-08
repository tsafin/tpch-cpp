#!/usr/bin/env python3
"""
gen_dist_cache.py - Generate pre-parsed distribution cache C arrays from tpcds.idx.

Usage: gen_dist_cache.py <tpcds.idx> <output.c>

Background
----------
dsdgen's dist_op() fetches distribution values per row by looking up a string
in a flat char buffer via atoi() (TKN_INT) or strtodec() (TKN_DECIMAL).  In a
profiling run this appears as ~10% strlen_avx2 + ~9% strchr + ~8% strtoll.

This generator reads tpcds.idx at *build time*, parses every int/decimal value
set, and emits static const arrays.  dist.c then skips atoi/strtodec entirely
when EMBEDDED_DSDGEN is defined and points int_cache[]/dec_cache[] at these
read-only arrays.

Binary format of tpcds.idx
---------------------------
  [0..3]     int32_t entry_count (network byte order)
  ...distribution data blocks...
  [end - entry_count*IDX_SIZE .. end]  index table

IDX_SIZE = D_NAME_LEN(20) + 7 * sizeof(int32_t) = 48 bytes per entry.
Each index entry:
  name[20]        char  (null-padded)
  index           int32_t
  offset          int32_t  -- byte offset into file where dist data starts
  str_space       int32_t  -- bytes in the string pool
  length          int32_t  -- number of rows (entries)
  w_width         int32_t  -- number of weight sets
  v_width         int32_t  -- number of value sets
  name_space      int32_t  -- bytes in the name alias pool

Distribution data block at <offset>:
  type_vector[v_width]           int32_t each
  weight_sets[w_width][length]   int32_t each (cumulative, not needed here)
  value_sets[v_width][length]    int32_t each (byte offsets into strings[])
  names[name_space]              char bytes
  strings[str_space]             char bytes (null-terminated values)

Token types (from dcomp.h):
  TKN_VARCHAR = 6
  TKN_INT     = 7
  TKN_DATE    = 9
  TKN_DECIMAL = 10
"""

import sys
import os
import struct

# Token type constants (must match dcomp.h)
TKN_VARCHAR = 6
TKN_INT     = 7
TKN_DATE    = 9
TKN_DECIMAL = 10

# Index entry size (must match dist.h IDX_SIZE)
D_NAME_LEN = 20
IDX_SIZE = D_NAME_LEN + 7 * 4   # 48 bytes


def safe_c_ident(name: str) -> str:
    """Convert a distribution name to a valid C identifier."""
    return name.strip('\x00').replace('-', '_').replace(' ', '_')


def strtodec_py(s: str):
    """
    Replicate dsdgen's strtodec() logic in Python.
    Returns (flags, precision, scale, number) matching decimal_t.

    strtodec() sets:
      - flags = 0
      - if no decimal point: scale=len(int_str), number=int(s), precision=0
      - else: scale=len(int_part), number=int_part*10^frac_len+int_frac, precision=len(frac)
    Then if s starts with '-' and number > 0: number *= -1
    """
    flags = 0
    s = s.strip()
    dot = s.find('.')
    if dot == -1:
        scale = len(s)
        number = int(s) if s and s not in ('-', '+') else 0
        precision = 0
    else:
        int_part = s[:dot]
        frac_part = s[dot+1:]
        scale = len(int_part)
        base = int(int_part) if int_part and int_part not in ('-', '+') else 0
        frac_val = int(frac_part) if frac_part else 0
        precision = len(frac_part)
        number = base
        for _ in range(precision):
            number *= 10
        number += frac_val
    # sign correction: if string starts with '-' but number ended up positive
    if s.startswith('-') and number > 0:
        number = -number
    return (flags, precision, scale, number)


def parse_tpcds_idx(filepath: str):
    """
    Parse tpcds.idx and return a list of distribution dicts:
    {
        'name': str,
        'offset': int,
        'str_space': int,
        'length': int,
        'w_width': int,
        'v_width': int,
        'name_space': int,
        'type_vector': [int, ...],          # v_width entries
        'value_sets': [[int,...], ...],      # v_width x length offsets into strings
        'strings': bytes,                    # str_space bytes
    }
    """
    with open(filepath, 'rb') as f:
        data = f.read()

    file_size = len(data)
    offset = 0

    # Read entry_count from the start of the file
    entry_count, = struct.unpack_from('>i', data, 0)

    # Index table is at the end
    idx_table_offset = file_size - entry_count * IDX_SIZE

    dists = []
    for i in range(entry_count):
        base = idx_table_offset + i * IDX_SIZE
        name_raw = data[base:base + D_NAME_LEN]
        name = name_raw.split(b'\x00')[0].decode('ascii', errors='replace')
        (index, d_offset, str_space, length, w_width, v_width, name_space) = \
            struct.unpack_from('>7i', data, base + D_NAME_LEN)

        # Parse distribution data at d_offset
        pos = d_offset

        # type_vector
        type_vector = list(struct.unpack_from('>' + 'i' * v_width, data, pos))
        pos += v_width * 4

        # weight_sets (skip — not needed for value cache)
        pos += w_width * length * 4

        # value_sets: v_width x length offsets into strings[]
        value_sets = []
        for v in range(v_width):
            row = list(struct.unpack_from('>' + 'i' * length, data, pos))
            value_sets.append(row)
            pos += length * 4

        # names (skip for now)
        pos += name_space

        # strings
        strings = data[pos:pos + str_space]

        dists.append({
            'name':        name,
            'offset':      d_offset,
            'str_space':   str_space,
            'length':      length,
            'w_width':     w_width,
            'v_width':     v_width,
            'name_space':  name_space,
            'type_vector': type_vector,
            'value_sets':  value_sets,
            'strings':     strings,
        })

    return dists


def get_string(strings: bytes, offset: int) -> str:
    """Extract a null-terminated string from the strings pool."""
    end = strings.index(b'\x00', offset) if b'\x00' in strings[offset:] else len(strings)
    return strings[offset:end].decode('ascii', errors='replace')


def generate(input_path: str, output_path: str) -> None:
    dists = parse_tpcds_idx(input_path)

    lines = []
    lines.append("/* Auto-generated by cmake/gen_dist_cache.py -- do not edit */")
    lines.append("/* Pre-parsed TPC-DS distribution cache: eliminates per-row")
    lines.append("   atoi/strtodec overhead in dsdgen's dist_op() hot path. */")
    lines.append("")
    lines.append("#ifdef EMBEDDED_DSDGEN")
    lines.append("")
    lines.append("#include <stdint.h>")
    lines.append("#include <string.h>  /* strcmp */")
    lines.append("#include \"decimal.h\"")
    lines.append("")

    # Emit per-distribution per-vset arrays
    int_entries   = []   # (dist_name, vset_idx, c_array_name)
    dec_entries   = []   # (dist_name, vset_idx, c_array_name)

    for d in dists:
        cname = safe_c_ident(d['name'])
        length = d['length']
        strings = d['strings']

        for vi, typ in enumerate(d['type_vector']):
            offsets = d['value_sets'][vi]

            if typ == TKN_INT:
                arr_name = "tpcds_int_{}_v{}".format(cname, vi)
                vals = []
                for j in range(length):
                    s = get_string(strings, offsets[j])
                    try:
                        vals.append(int(s))
                    except ValueError:
                        vals.append(0)
                # emit array
                lines.append("static const int {}[{}] = {{".format(arr_name, length))
                # 16 values per row
                for chunk_start in range(0, length, 16):
                    chunk = vals[chunk_start:chunk_start+16]
                    comma = "," if chunk_start + 16 < length else ""
                    lines.append("    {}{}".format(", ".join(str(v) for v in chunk), comma))
                lines.append("};")
                lines.append("")
                int_entries.append((d['name'], vi, arr_name))

            elif typ == TKN_DECIMAL:
                arr_name = "tpcds_dec_{}_v{}".format(cname, vi)
                vals = []
                for j in range(length):
                    s = get_string(strings, offsets[j])
                    try:
                        fl, prec, sc, num = strtodec_py(s)
                    except Exception:
                        fl, prec, sc, num = 0, 0, 0, 0
                    vals.append((fl, prec, sc, num))
                lines.append("static const decimal_t {}[{}] = {{".format(arr_name, length))
                for j, (fl, prec, sc, num) in enumerate(vals):
                    comma = "," if j < length - 1 else ""
                    lines.append("    {{{}, {}, {}, {}LL}}{}".format(fl, prec, sc, num, comma))
                lines.append("};")
                lines.append("")
                dec_entries.append((d['name'], vi, arr_name))

    # Emit lookup tables
    lines.append("/* --- int cache lookup table --- */")
    lines.append("typedef struct { const char *name; int vset; const int *vals; } tpcds_int_entry_t;")
    lines.append("static const tpcds_int_entry_t tpcds_int_table[] = {")
    for (dname, vi, arr) in int_entries:
        lines.append('    {{"{}", {}, {}}},'.format(dname, vi, arr))
    lines.append("    {NULL, 0, NULL}")
    lines.append("};")
    lines.append("")

    lines.append("/* --- decimal cache lookup table --- */")
    lines.append("typedef struct { const char *name; int vset; const decimal_t *vals; } tpcds_dec_entry_t;")
    lines.append("static const tpcds_dec_entry_t tpcds_dec_table[] = {")
    for (dname, vi, arr) in dec_entries:
        lines.append('    {{"{}", {}, {}}},'.format(dname, vi, arr))
    lines.append("    {NULL, 0, NULL}")
    lines.append("};")
    lines.append("")

    # Emit lookup functions
    lines.append("const int *tpcds_lookup_int_cache(const char *dist_name, int vset);")
    lines.append("const decimal_t *tpcds_lookup_dec_cache(const char *dist_name, int vset);")
    lines.append("")
    lines.append("const int *tpcds_lookup_int_cache(const char *dist_name, int vset) {")
    lines.append("    const tpcds_int_entry_t *e = tpcds_int_table;")
    lines.append("    for (; e->name != NULL; ++e)")
    lines.append("        if (e->vset == vset && strcmp(e->name, dist_name) == 0)")
    lines.append("            return e->vals;")
    lines.append("    return NULL;")
    lines.append("}")
    lines.append("")
    lines.append("const decimal_t *tpcds_lookup_dec_cache(const char *dist_name, int vset) {")
    lines.append("    const tpcds_dec_entry_t *e = tpcds_dec_table;")
    lines.append("    for (; e->name != NULL; ++e)")
    lines.append("        if (e->vset == vset && strcmp(e->name, dist_name) == 0)")
    lines.append("            return e->vals;")
    lines.append("    return NULL;")
    lines.append("}")
    lines.append("")
    lines.append("#endif /* EMBEDDED_DSDGEN */")
    lines.append("")

    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(output_path, 'w') as f:
        f.write('\n'.join(lines) + '\n')

    n_int = len(int_entries)
    n_dec = len(dec_entries)
    n_dists = len(dists)
    print("Parsed {} distributions, {} int arrays, {} decimal arrays -> {}".format(
        n_dists, n_int, n_dec, os.path.basename(output_path)))


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: gen_dist_cache.py <tpcds.idx> <output.c>", file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    if not os.path.exists(input_path):
        print("Error: input file not found: {}".format(input_path), file=sys.stderr)
        sys.exit(1)

    generate(input_path, output_path)


if __name__ == "__main__":
    main()
