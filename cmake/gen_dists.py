#!/usr/bin/env python3
"""
gen_dists.py - Generate C source from dists.dss at CMake configure time.

Usage: gen_dists.py <input.dss> <output.c>

Reads all BEGIN...END blocks from dists.dss, pre-computes cumulative weights
(exactly as read_dist() in bm_utils.c does), and emits a C source file with:
  - static set_member[] arrays (one per distribution)
  - load_dists() that just assigns pointers -- no malloc, no file I/O
  - dbgen_reset_seeds() (replaces tpch_init.c entirely)

The generated load_dists() is idempotent: pointer assignments are safe to repeat.
"""

import sys
import os

# Mapping: dists.dss distribution name (lower-case) -> C global variable name.
# Only distributions whose C variables are actually defined in the compiled code
# (dbgen_stubs.c) are listed here.  Others (nouns, verbs, p_names, Q13a, Q13b)
# are either unused or have no corresponding C definition.
DIST_MAP = {
    "p_cntr":        "p_cntr_set",
    "colors":        "colors",
    "p_types":       "p_types_set",
    "nations":       "nations",
    "nations2":      "nations2",
    "regions":       "regions",
    "o_oprio":       "o_priority_set",
    "instruct":      "l_instruct_set",
    "smode":         "l_smode_set",
    "category":      "l_category_set",
    "rflag":         "l_rflag_set",
    "msegmnt":       "c_mseg_set",
    # text-generation distributions (defined in dbgen_stubs.c)
    "adverbs":       "adverbs",
    "articles":      "articles",
    "prepositions":  "prepositions",
    "auxillaries":   "auxillaries",
    "terminators":   "terminators",
    "adjectives":    "adjectives",
    "grammar":       "grammar",
    "np":            "np",
    "vp":            "vp",
}


def escape_c_string(s):
    """Escape a string for safe embedding in a C double-quoted literal."""
    return s.replace('\\', '\\\\').replace('"', '\\"')


def parse_dists(filepath):
    """
    Parse dists.dss.  Returns a dict:
        dist_name (lower-case) -> {
            'count': int,
            'max':   int,          # final cumulative weight (= target->max)
            'entries': [(cum_weight, token_text), ...],
            'var':   str,          # C variable name
        }

    Weight accumulation mirrors read_dist() in bm_utils.c exactly:
        target->max += weight;
        target->list[count].weight = target->max;  (cumulative!)
    """
    distributions = {}

    with open(filepath, 'r') as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i].rstrip('\n')

        # Strip comments (first '#' and everything after)
        if '#' in line:
            line = line[:line.index('#')]
        line = line.strip()

        if not line:
            i += 1
            continue

        # Look for "BEGIN <name>"
        parts = line.split()
        if len(parts) == 2 and parts[0].upper() == 'BEGIN':
            dist_name = parts[1].lower()
            entries = []
            count = None
            cumulative = 0

            i += 1
            while i < len(lines):
                inner = lines[i].rstrip('\n')
                if '#' in inner:
                    inner = inner[:inner.index('#')]
                inner = inner.strip()
                i += 1

                if not inner:
                    continue

                # END terminates the block
                if inner.upper().startswith('END'):
                    break

                # Expect "token|weight"
                if '|' not in inner:
                    continue

                pipe = inner.index('|')          # first '|', matching sscanf %[^|]
                token = inner[:pipe].strip()
                weight_str = inner[pipe + 1:].strip()

                try:
                    weight = int(weight_str)
                except ValueError:
                    continue

                if token.lower() == 'count':
                    count = weight
                    continue

                cumulative += weight
                entries.append((cumulative, token))

            if count is not None and dist_name in DIST_MAP:
                distributions[dist_name] = {
                    'count':   count,
                    'max':     cumulative,
                    'entries': entries,
                    'var':     DIST_MAP[dist_name],
                }
        else:
            i += 1

    return distributions


def gen_c_source(distributions, input_path):
    """Return the text of the generated C source file."""
    out = []

    out.append('/* Auto-generated from {} by cmake/gen_dists.py -- do not edit */'.format(
        os.path.basename(input_path)))
    out.append('')
    out.append('/* dss.h uses EXTERN which becomes "extern" unless DECLARER is set.')
    out.append(' * We do NOT set DECLARER here; distributions are defined in dbgen_stubs.c.')
    out.append(' * We only assign their fields inside load_dists(). */')
    out.append('')
    out.append('#include "dss.h"')
    out.append('#include "dsstypes.h"')
    out.append('')

    load_stmts = []

    # Emit one static array per distribution, sorted for deterministic output
    for dist_name in sorted(distributions.keys()):
        info = distributions[dist_name]
        var      = info['var']
        count    = info['count']
        max_val  = info['max']
        entries  = info['entries']
        arr_name = 'g_{}_list'.format(var)

        out.append('/* -- {} ({} entries, max={}) -- */'.format(
            dist_name, count, max_val))
        out.append('static set_member {}[{}] = {{'.format(arr_name, count))

        for idx, (cum_weight, token) in enumerate(entries):
            escaped = escape_c_string(token)
            comma = ',' if idx < len(entries) - 1 else ''
            out.append('    {{ {}L, (char*)"{}" }}{}'.format(
                cum_weight, escaped, comma))

        out.append('};')
        out.append('')

        load_stmts.append(
            '    {var}.count = {count}; {var}.max = {max}L;'.format(
                var=var, count=count, max=max_val))
        load_stmts.append(
            '    {var}.list = {arr}; {var}.permute = (long*)NULL;'.format(
                var=var, arr=arr_name))

    # Emit load_dists()
    out.append('void load_dists(void)')
    out.append('{')
    out.extend(load_stmts)
    out.append('}')
    out.append('')

    # Emit dbgen_reset_seeds() -- was in tpch_init.c
    out.append('/* dbgen_reset_seeds() -- replaces tpch_init.c */')
    out.append('extern seed_t Seed[];')
    out.append('')
    out.append('void dbgen_reset_seeds(void)')
    out.append('{')
    out.append('    int i;')
    out.append('    for (i = 0; i <= MAX_STREAM; i++) {')
    out.append('        Seed[i].usage = 0;')
    out.append('    }')
    out.append('}')
    out.append('')

    return '\n'.join(out)


def main():
    if len(sys.argv) != 3:
        print('Usage: gen_dists.py <input.dss> <output.c>', file=sys.stderr)
        sys.exit(1)

    input_path  = sys.argv[1]
    output_path = sys.argv[2]

    if not os.path.exists(input_path):
        print('Error: input file not found: {}'.format(input_path), file=sys.stderr)
        sys.exit(1)

    distributions = parse_dists(input_path)

    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    content = gen_c_source(distributions, input_path)

    with open(output_path, 'w') as f:
        f.write(content)

    total_entries = sum(info['count'] for info in distributions.values())
    print('Generated distributions from dists.dss ({} distributions, {} entries)'.format(
        len(distributions), total_entries))


if __name__ == '__main__':
    main()
