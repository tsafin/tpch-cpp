/**
 * tpcds_dsdgen.h — C++-safe entry point for TPC-DS dsdgen
 *
 * Instead of manually re-declaring struct definitions (which can silently
 * diverge from the real tpcds sources), this header directly includes the
 * canonical tpcds w_*.h table headers.  All struct layouts (CALL_CENTER_TBL,
 * ds_addr_t, etc.) therefore always match the generator implementation.
 *
 * Include this inside an  extern "C" { }  block from C++ translation units:
 *
 *   extern "C" {
 *   #include "tpcds_dsdgen.h"
 *   }
 *
 * The dsdgen_objs CMake target exposes the tpcds/tools/ source directory and
 * the build-time generated header directory (columns.h, tables.h, streams.h)
 * as PUBLIC include paths, so all includes resolve correctly.
 */

#ifndef TPCDS_DSDGEN_H
#define TPCDS_DSDGEN_H

#include <stdint.h>
#include <stddef.h>

/* -------------------------------------------------------------------------
 * Core tpcds types: ds_key_t, decimal_t, ds_pricing_t, ds_addr_t
 * decimal.h pulls in config.h → porting.h (ds_key_t) + mathops.h.
 * pricing.h pulls in decimal.h.
 * address.h pulls in constants.h.
 * ------------------------------------------------------------------------- */
#include "decimal.h"
#include "pricing.h"
#include "address.h"

/* -------------------------------------------------------------------------
 * All 24 W_ table struct definitions — use the canonical tpcds sources.
 * Each w_*.h provides the struct definition and mk_w_* / pr_w_* / ld_w_*
 * function declarations.
 * ------------------------------------------------------------------------- */
#include "w_store_sales.h"
#include "w_inventory.h"
#include "w_catalog_sales.h"
#include "w_web_sales.h"
#include "w_customer.h"
#include "w_item.h"
#include "w_datetbl.h"
#include "w_store_returns.h"
#include "w_catalog_returns.h"
#include "w_web_returns.h"
#include "w_call_center.h"
#include "w_catalog_page.h"
#include "w_web_page.h"
#include "w_web_site.h"
#include "w_warehouse.h"
#include "w_ship_mode.h"
#include "w_household_demographics.h"
#include "w_customer_demographics.h"
#include "w_customer_address.h"
#include "w_income_band.h"
#include "w_reason.h"
#include "w_timetbl.h"
#include "w_promotion.h"
#include "w_store.h"

/* -------------------------------------------------------------------------
 * Utility headers: scaling, params, RNG init
 * Note: tables.h (build-generated) defines plain macros like CALL_CENTER=0,
 * STORE=15, etc. that collide with C++ enum member names — do NOT include it
 * here.  The TPCDS_* constants below duplicate those values but are prefixed
 * to avoid collisions.  They match the generated tables.h and are stable
 * across TPC-DS versions.
 * ------------------------------------------------------------------------- */
#include "scaling.h"   /* get_rowcount(), getIDCount() */
#include "r_params.h"  /* set_str(), set_int(), init_params() */
#include "genrand.h"   /* init_rand() */

/* -------------------------------------------------------------------------
 * Table ID constants — match the values in the build-generated tables.h.
 * Prefixed TPCDS_* to avoid colliding with the bare macro names when this
 * header is included alongside other tpcds headers.
 * ------------------------------------------------------------------------- */
#define TPCDS_CALL_CENTER            0
#define TPCDS_CATALOG_PAGE           1
#define TPCDS_CATALOG_RETURNS        2
#define TPCDS_CATALOG_SALES          3
#define TPCDS_CUSTOMER               4
#define TPCDS_CUSTOMER_ADDRESS       5
#define TPCDS_CUSTOMER_DEMOGRAPHICS  6
#define TPCDS_DATE                   7
#define TPCDS_HOUSEHOLD_DEMOGRAPHICS 8
#define TPCDS_INCOME_BAND            9
#define TPCDS_INVENTORY             10
#define TPCDS_ITEM                  11
#define TPCDS_PROMOTION             12
#define TPCDS_REASON                13
#define TPCDS_SHIP_MODE             14
#define TPCDS_STORE                 15
#define TPCDS_STORE_RETURNS         16
#define TPCDS_STORE_SALES           17
#define TPCDS_TIME                  18
#define TPCDS_WAREHOUSE             19
#define TPCDS_WEB_PAGE              20
#define TPCDS_WEB_RETURNS           21
#define TPCDS_WEB_SALES             22
#define TPCDS_WEB_SITE              23

/* -------------------------------------------------------------------------
 * Embedded-mode callbacks — our additions to the tpcds C sources, compiled
 * in when EMBEDDED_DSDGEN is defined.  The callbacks replace pr_w_*() file
 * output with in-process row delivery for the master-detail tables.
 * ------------------------------------------------------------------------- */
#ifdef EMBEDDED_DSDGEN
extern void (*g_w_store_sales_callback)(const struct W_STORE_SALES_TBL *row, void *ctx);
extern void *g_w_store_sales_callback_ctx;
extern void (*g_w_catalog_sales_callback)(const struct W_CATALOG_SALES_TBL *row, void *ctx);
extern void *g_w_catalog_sales_callback_ctx;
extern void (*g_w_web_sales_callback)(const struct W_WEB_SALES_TBL *row, void *ctx);
extern void *g_w_web_sales_callback_ctx;
#endif /* EMBEDDED_DSDGEN */

/* Embedded distribution data (from dsts_generated.c) */
extern const uint8_t tpcds_idx_data[];
extern const size_t  tpcds_idx_size;

#endif /* TPCDS_DSDGEN_H */
