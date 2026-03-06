/**
 * tpcds_dsdgen.h — C++-safe forward declarations for TPC-DS dsdgen
 *
 * Provides the minimal type definitions and function declarations needed
 * to use dsdgen from C++ without pulling in the complex preprocessor
 * dependency chain (config.h → LINUX define → porting.h → ds_key_t).
 *
 * The actual dsdgen sources are compiled as C (via dsdgen_objs OBJECT library)
 * with LINUX=1 and TPCDS=1.  This header only provides what C++ wrappers need
 * to call into those objects.
 */

#ifndef TPCDS_DSDGEN_H
#define TPCDS_DSDGEN_H

#include <stdint.h>
#include <stddef.h>

/* On Linux, dsdgen's config.h sets HUGE_TYPE = int64_t, so ds_key_t = int64_t */
typedef int64_t ds_key_t;

/* Scaled-integer decimal type (decimal.h) */
typedef struct DECIMAL_T {
    int      flags;
    int      precision;
    int      scale;
    ds_key_t number;
} decimal_t;

/* Pricing aggregate used by fact tables (pricing.h) */
typedef struct DS_PRICING_T {
    decimal_t wholesale_cost;
    decimal_t list_price;
    decimal_t sales_price;
    int       quantity;
    decimal_t ext_discount_amt;
    decimal_t ext_sales_price;
    decimal_t ext_wholesale_cost;
    decimal_t ext_list_price;
    decimal_t tax_pct;
    decimal_t ext_tax;
    decimal_t coupon_amt;
    decimal_t ship_cost;
    decimal_t ext_ship_cost;
    decimal_t net_paid;
    decimal_t net_paid_inc_tax;
    decimal_t net_paid_inc_ship;
    decimal_t net_paid_inc_ship_tax;
    decimal_t net_profit;
    decimal_t refunded_cash;
    decimal_t reversed_charge;
    decimal_t store_credit;
    decimal_t fee;
    decimal_t net_loss;
} ds_pricing_t;

/* store_sales row (w_store_sales.h) */
struct W_STORE_SALES_TBL {
    ds_key_t     ss_sold_date_sk;
    ds_key_t     ss_sold_time_sk;
    ds_key_t     ss_sold_item_sk;
    ds_key_t     ss_sold_customer_sk;
    ds_key_t     ss_sold_cdemo_sk;
    ds_key_t     ss_sold_hdemo_sk;
    ds_key_t     ss_sold_addr_sk;
    ds_key_t     ss_sold_store_sk;
    ds_key_t     ss_sold_promo_sk;
    ds_key_t     ss_ticket_number;
    ds_pricing_t ss_pricing;
};

/* inventory row (w_inventory.h) */
struct W_INVENTORY_TBL {
    ds_key_t inv_date_sk;
    ds_key_t inv_item_sk;
    ds_key_t inv_warehouse_sk;
    int      inv_quantity_on_hand;
};

/* table ID constants (must match generated tables.h) */
#define TPCDS_STORE_SALES   17
#define TPCDS_INVENTORY     10

/* r_params.h — parameter access */
void  set_str(char* param, char* value);
void  set_int(char* var, char* val);
int   init_params(void);
char* get_str(char* var);
int   get_int(char* var);

/* genrand.h — RNG initialization */
void init_rand(void);

/* scaling.h — row count for scale factor */
ds_key_t get_rowcount(int table);

/* Table-specific row generators */
int mk_w_store_sales(void* pDest, ds_key_t kIndex);
int mk_w_inventory(void* pDest, ds_key_t kIndex);

/* Embedded-mode callback for store_sales (compiled in when EMBEDDED_DSDGEN is
 * defined). Set before calling mk_w_store_sales; called once per line item
 * with the fully-populated row; file output is suppressed when non-NULL. */
#ifdef EMBEDDED_DSDGEN
extern void (*g_w_store_sales_callback)(const struct W_STORE_SALES_TBL *row, void *ctx);
extern void *g_w_store_sales_callback_ctx;
#endif /* EMBEDDED_DSDGEN */

/* Embedded distribution data (from dsts_generated.c) */
extern const uint8_t tpcds_idx_data[];
extern const size_t  tpcds_idx_size;

#endif /* TPCDS_DSDGEN_H */
