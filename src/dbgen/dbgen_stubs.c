/*
 * Stub implementations for dbgen embedded use
 * These provide necessary global variables and function stubs
 * for using dbgen as a library without the full distribution infrastructure
 */

#include <stdio.h>
#include <stdlib.h>
#include "../../../third_party/tpch/dbgen/dss.h"
#include "../../../third_party/tpch/dbgen/dsstypes.h"

/* Global variables accessed by dbgen - must be initialized/defined */
char *d_path = NULL;
long force = 0;
long scale = 1;
long verbose = 0;

/* Distribution definitions (normally in print.c, which we don't compile) */
/* These MUST be defined (not just declared) so they can be linked */
/* They are initialized to empty by default and populated by load_dists() */
distribution nations = {0};
distribution nations2 = {0};
distribution regions = {0};
distribution o_priority_set = {0};
distribution l_instruct_set = {0};
distribution l_smode_set = {0};
distribution l_category_set = {0};
distribution l_rflag_set = {0};
distribution c_mseg_set = {0};
distribution p_types_set = {0};
distribution p_cntr_set = {0};

/* Additional distributions used by load_dists */
distribution colors = {0};
distribution grammar = {0};
distribution np = {0};
distribution vp = {0};
distribution adjectives = {0};
distribution adverbs = {0};
distribution articles = {0};
distribution auxillaries = {0};
distribution prepositions = {0};
distribution terminators = {0};

/* Table definitions - properly initialized from driver.c */
/* Format: {filename, description, base_rows, print_func, seed_func, child_table, loaded_flag} */
tdef tdefs[10] = {
    {"part.tbl", "part table", 200000,
        NULL, NULL, 1, 0},
    {"partsupp.tbl", "partsupplier table", 200000,
        NULL, NULL, -1, 0},
    {"supplier.tbl", "suppliers table", 10000,
        NULL, NULL, -1, 0},
    {"customer.tbl", "customers table", 150000,
        NULL, NULL, -1, 0},
    {"orders.tbl", "order table", 150000,
        NULL, NULL, 5, 0},
    {"lineitem.tbl", "lineitem table", 150000,
        NULL, NULL, -1, 0},
    {"orders.tbl", "orders/lineitem tables", 150000,
        NULL, NULL, 5, 0},
    {"part.tbl", "part/partsupplier tables", 200000,
        NULL, NULL, 1, 0},
    {"nation.tbl", "nation table", 25,
        NULL, NULL, -1, 0},
    {"region.tbl", "region table", 25,
        NULL, NULL, -1, 0},
};
/* Note: Seed[] is already defined in rnd.c - no need to redefine here */

/* Called at the start of row generation for a table */
void row_start(int t) {
    /* No-op: we don't track row boundaries in embedded mode */
}

/* Called at the end of row generation for a table */
void row_stop(int t) {
    /* No-op: we don't track row boundaries in embedded mode */
    /* The real row_stop in rnd.c is disabled by EMBEDDED_DBGEN define */
}

/* Note: load_dists() is now provided by tpch_init.c */

/* Minimal dbg_text for nation/region comment fields */
/* We provide a simple implementation that doesn't require complex text generation */
#include <stdio.h>
#include <string.h>

extern unsigned long Seed[];

void dbg_text(char *tgt, int min, int max, int sd)
{
    /* Generate a simple deterministic comment based on seed and length */
    /* This is sufficient for nation and region comment fields */
    fprintf(stderr, "DEBUG: dbg_text called with min=%d max=%d sd=%d\n", min, max, sd);
    fflush(stderr);

    if (!tgt) {
        fprintf(stderr, "ERROR: dbg_text called with NULL target\n");
        return;
    }

    /* Clamp to reasonable length to avoid buffer overflow */
    /* Use N_CMNT_MAX from tpch_dbgen.h */
    if (min < 0) min = 0;
    if (max > N_CMNT_MAX) max = N_CMNT_MAX;
    if (max < min) max = min;

    int len = min;
    if (max > min) {
        len = min + ((Seed[sd] >> 8) % (max - min + 1));
    }
    if (len > N_CMNT_MAX) len = N_CMNT_MAX;
    if (len < 0) len = 0;

    fprintf(stderr, "DEBUG: dbg_text generating %d bytes\n", len);
    fflush(stderr);

    int i;
    for (i = 0; i < len; i++) {
        /* Generate printable ASCII characters (32-126) */
        Seed[sd] = Seed[sd] * 1103515245 + 12345;
        int c = 32 + ((Seed[sd] >> 8) % 95);
        tgt[i] = (char)c;
    }
    tgt[i] = '\0';
    fprintf(stderr, "DEBUG: dbg_text done\n");
    fflush(stderr);
}

/*
 * mk_ascdate() Fix for embedded mode
 *
 * NOTE: mk_ascdate() is now defined in bm_utils.c with internal caching.
 * The function uses a static variable to cache its result on first call,
 * ensuring all subsequent calls (from mk_order, mk_lineitem, etc.)
 * return the same pre-allocated pointer.
 *
 * This completely avoids the double-allocation problem that was causing
 * pointer corruption in Phase 9.1.
 *
 * dbgen_reset_seeds() is already implemented in tpch_init.c
 */
