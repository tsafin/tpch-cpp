/*
 * Stub implementations for dbgen embedded use
 * These provide necessary global variables and function stubs
 * for using dbgen as a library without the full distribution infrastructure
 */

#include <stdio.h>
#include <stdlib.h>

/* Global variables accessed by dbgen - must be initialized/defined */
char *d_path = NULL;
long force = 0;
long scale = 1;
long verbose = 0;

/* Distribution data structures - populated by load_dists() in normal operation */
/* For embedded use, these are stubs since we don't use print functions */
void *nations = NULL;
void *regions = NULL;
void *o_priority_set = NULL;
void *l_instruct_set = NULL;
void *l_smode_set = NULL;
void *l_category_set = NULL;
void *l_rflag_set = NULL;
void *c_mseg_set = NULL;
void *p_types_set = NULL;
void *p_cntr_set = NULL;

/* Table definitions - normally loaded by load_dists() */
/* Stub for embedded use - only needed if calling print functions */
void *tdefs = NULL;
void *set_seeds = NULL;
void *colors = NULL;

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
void dbg_text(int code, int i) {
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
