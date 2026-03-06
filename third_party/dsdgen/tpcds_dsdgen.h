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

/* catalog_sales row (w_catalog_sales.h) */
struct W_CATALOG_SALES_TBL {
    ds_key_t     cs_sold_date_sk;
    ds_key_t     cs_sold_time_sk;
    ds_key_t     cs_ship_date_sk;
    ds_key_t     cs_bill_customer_sk;
    ds_key_t     cs_bill_cdemo_sk;
    ds_key_t     cs_bill_hdemo_sk;
    ds_key_t     cs_bill_addr_sk;
    ds_key_t     cs_ship_customer_sk;
    ds_key_t     cs_ship_cdemo_sk;
    ds_key_t     cs_ship_hdemo_sk;
    ds_key_t     cs_ship_addr_sk;
    ds_key_t     cs_call_center_sk;
    ds_key_t     cs_catalog_page_sk;
    ds_key_t     cs_ship_mode_sk;
    ds_key_t     cs_warehouse_sk;
    ds_key_t     cs_sold_item_sk;
    ds_key_t     cs_promo_sk;
    ds_key_t     cs_order_number;
    ds_pricing_t cs_pricing;
};

/* web_sales row (w_web_sales.h) */
struct W_WEB_SALES_TBL {
    ds_key_t     ws_sold_date_sk;
    ds_key_t     ws_sold_time_sk;
    ds_key_t     ws_ship_date_sk;
    ds_key_t     ws_item_sk;
    ds_key_t     ws_bill_customer_sk;
    ds_key_t     ws_bill_cdemo_sk;
    ds_key_t     ws_bill_hdemo_sk;
    ds_key_t     ws_bill_addr_sk;
    ds_key_t     ws_ship_customer_sk;
    ds_key_t     ws_ship_cdemo_sk;
    ds_key_t     ws_ship_hdemo_sk;
    ds_key_t     ws_ship_addr_sk;
    ds_key_t     ws_web_page_sk;
    ds_key_t     ws_web_site_sk;
    ds_key_t     ws_ship_mode_sk;
    ds_key_t     ws_warehouse_sk;
    ds_key_t     ws_promo_sk;
    ds_key_t     ws_order_number;
    ds_pricing_t ws_pricing;
};

/* customer row (w_customer.h) */
struct W_CUSTOMER_TBL {
    ds_key_t  c_customer_sk;
    char      c_customer_id[17];   /* RS_BKEY+1 */
    ds_key_t  c_current_cdemo_sk;
    ds_key_t  c_current_hdemo_sk;
    ds_key_t  c_current_addr_sk;
    int       c_first_shipto_date_id;
    int       c_first_sales_date_id;
    char     *c_salutation;
    char     *c_first_name;
    char     *c_last_name;
    int       c_preferred_cust_flag;
    int       c_birth_day;
    int       c_birth_month;
    int       c_birth_year;
    char     *c_birth_country;
    char      c_login[14];         /* RS_C_LOGIN+1 */
    char      c_email_address[51]; /* RS_C_EMAIL+1 */
    int       c_last_review_date;
};

/* item row (w_item.h) */
struct W_ITEM_TBL {
    ds_key_t  i_item_sk;
    char      i_item_id[17];
    ds_key_t  i_rec_start_date_id;
    ds_key_t  i_rec_end_date_id;
    char      i_item_desc[201];
    decimal_t i_current_price;
    decimal_t i_wholesale_cost;
    ds_key_t  i_brand_id;
    char      i_brand[51];
    ds_key_t  i_class_id;
    char     *i_class;
    ds_key_t  i_category_id;
    char     *i_category;
    ds_key_t  i_manufact_id;
    char      i_manufact[51];
    char     *i_size;
    char      i_formulation[21];
    char     *i_color;
    char     *i_units;
    char     *i_container;
    ds_key_t  i_manager_id;
    char      i_product_name[51];
    ds_key_t  i_promo_sk;
};

/* date_dim row (w_datetbl.h) */
struct W_DATE_TBL {
    ds_key_t  d_date_sk;
    char      d_date_id[17];
    int       d_month_seq;
    int       d_week_seq;
    int       d_quarter_seq;
    int       d_year;
    int       d_dow;
    int       d_moy;
    int       d_dom;
    int       d_qoy;
    int       d_fy_year;
    int       d_fy_quarter_seq;
    int       d_fy_week_seq;
    char     *d_day_name;
    int       d_holiday;
    int       d_weekend;
    int       d_following_holiday;
    int       d_first_dom;
    int       d_last_dom;
    int       d_same_day_ly;
    int       d_same_day_lq;
    int       d_current_day;
    int       d_current_week;
    int       d_current_month;
    int       d_current_quarter;
    int       d_current_year;
};

/* store_returns row (w_store_returns.h) */
struct W_STORE_RETURNS_TBL {
    ds_key_t     sr_returned_date_sk;
    ds_key_t     sr_returned_time_sk;
    ds_key_t     sr_item_sk;
    ds_key_t     sr_customer_sk;
    ds_key_t     sr_cdemo_sk;
    ds_key_t     sr_hdemo_sk;
    ds_key_t     sr_addr_sk;
    ds_key_t     sr_store_sk;
    ds_key_t     sr_reason_sk;
    ds_key_t     sr_ticket_number;
    ds_pricing_t sr_pricing;
};

/* catalog_returns row (w_catalog_returns.h) */
struct W_CATALOG_RETURNS_TBL {
    ds_key_t  cr_returned_date_sk;
    ds_key_t  cr_returned_time_sk;
    ds_key_t  cr_item_sk;
    ds_key_t  cr_refunded_customer_sk;
    ds_key_t  cr_refunded_cdemo_sk;
    ds_key_t  cr_refunded_hdemo_sk;
    ds_key_t  cr_refunded_addr_sk;
    ds_key_t  cr_returning_customer_sk;
    ds_key_t  cr_returning_cdemo_sk;
    ds_key_t  cr_returning_hdemo_sk;
    ds_key_t  cr_returning_addr_sk;
    ds_key_t  cr_call_center_sk;
    ds_key_t  cr_catalog_page_sk;
    ds_key_t  cr_ship_mode_sk;
    ds_key_t  cr_warehouse_sk;
    ds_key_t  cr_reason_sk;
    ds_key_t  cr_order_number;
    ds_pricing_t cr_pricing;
    decimal_t cr_fee;
    decimal_t cr_refunded_cash;
    decimal_t cr_reversed_charge;
    decimal_t cr_store_credit;
    decimal_t cr_net_loss;
};

/* web_returns row (w_web_returns.h) */
struct W_WEB_RETURNS_TBL {
    ds_key_t     wr_returned_date_sk;
    ds_key_t     wr_returned_time_sk;
    ds_key_t     wr_item_sk;
    ds_key_t     wr_refunded_customer_sk;
    ds_key_t     wr_refunded_cdemo_sk;
    ds_key_t     wr_refunded_hdemo_sk;
    ds_key_t     wr_refunded_addr_sk;
    ds_key_t     wr_returning_customer_sk;
    ds_key_t     wr_returning_cdemo_sk;
    ds_key_t     wr_returning_hdemo_sk;
    ds_key_t     wr_returning_addr_sk;
    ds_key_t     wr_web_page_sk;
    ds_key_t     wr_reason_sk;
    ds_key_t     wr_order_number;
    ds_pricing_t wr_pricing;
};

/* table ID constants (must match generated tables.h) */
#define TPCDS_STORE_SALES      17
#define TPCDS_INVENTORY        10
#define TPCDS_CATALOG_SALES     3
#define TPCDS_WEB_SALES        22
#define TPCDS_CUSTOMER          4
#define TPCDS_ITEM             11
#define TPCDS_DATE              7
#define TPCDS_STORE_RETURNS    16
#define TPCDS_CATALOG_RETURNS   2
#define TPCDS_WEB_RETURNS      21

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
int mk_w_catalog_sales(void* pDest, ds_key_t kIndex);
int mk_w_web_sales(void* pDest, ds_key_t kIndex);
int mk_w_customer(void* pDest, ds_key_t kIndex);
int mk_w_item(void* pDest, ds_key_t kIndex);
int mk_w_date(void* pDest, ds_key_t kIndex);
int mk_w_store_returns(void* pDest, ds_key_t kIndex);
int mk_w_catalog_returns(void* pDest, ds_key_t kIndex);
int mk_w_web_returns(void* pDest, ds_key_t kIndex);

/* Embedded-mode callback for store_sales (compiled in when EMBEDDED_DSDGEN is
 * defined). Set before calling mk_w_store_sales; called once per line item
 * with the fully-populated row; file output is suppressed when non-NULL. */
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
