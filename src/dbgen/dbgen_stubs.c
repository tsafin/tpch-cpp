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

/* dbg_text is called by mk_* functions to generate text descriptions */
/* We provide a stub that does nothing since we don't generate text */
void dbg_text(char *t, int min, int max, int s) {
    /* No-op: text generation not needed in embedded mode */
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
