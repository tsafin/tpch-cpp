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

/* Address type used in several dimension tables (address.h) */
typedef struct DS_ADDR_T {
    char  suite_num[11];   /* RS_CC_SUITE_NUM+1 */
    int   street_num;
    char *street_name1;
    char *street_name2;
    char *street_type;
    char *city;
    char *county;
    char *state;
    char  country[21];     /* RS_CC_COUNTRY+1 */
    int   zip;
    int   plus4;
    int   gmt_offset;
} ds_addr_t;

/* call_center row (w_call_center.h) */
struct CALL_CENTER_TBL {
    ds_key_t  cc_call_center_sk;
    char      cc_call_center_id[17];
    ds_key_t  cc_rec_start_date_id;
    ds_key_t  cc_rec_end_date_id;
    ds_key_t  cc_closed_date_id;
    ds_key_t  cc_open_date_id;
    char      cc_name[51];
    char     *cc_class;
    int       cc_employees;
    int       cc_sq_ft;
    char     *cc_hours;
    char      cc_manager[41];
    int       cc_market_id;
    char      cc_market_class[51];
    char      cc_market_desc[101];
    char      cc_market_manager[41];
    int       cc_division_id;
    char      cc_division_name[51];
    int       cc_company;
    char      cc_company_name[61];
    ds_addr_t cc_address;
    decimal_t cc_tax_percentage;
};

/* catalog_page row (w_catalog_page.h) */
struct CATALOG_PAGE_TBL {
    ds_key_t  cp_catalog_page_sk;
    char      cp_catalog_page_id[17];
    ds_key_t  cp_start_date_id;
    ds_key_t  cp_end_date_id;
    char      cp_department[21];
    int       cp_catalog_number;
    int       cp_catalog_page_number;
    char      cp_description[101];
    char     *cp_type;
};

/* web_page row (w_web_page.h) */
struct W_WEB_PAGE_TBL {
    ds_key_t  wp_page_sk;
    char      wp_page_id[17];
    char      wp_site_id[17];
    ds_key_t  wp_rec_start_date_id;
    ds_key_t  wp_rec_end_date_id;
    ds_key_t  wp_creation_date_sk;
    ds_key_t  wp_access_date_sk;
    int       wp_autogen_flag;
    ds_key_t  wp_customer_sk;
    char      wp_url[101];
    char     *wp_type;
    int       wp_char_count;
    int       wp_link_count;
    int       wp_image_count;
    int       wp_max_ad_count;
};

/* web_site row (w_web_site.h) */
struct W_WEB_SITE_TBL {
    ds_key_t  web_site_sk;
    char      web_site_id[17];
    ds_key_t  web_rec_start_date_id;
    ds_key_t  web_rec_end_date_id;
    char      web_name[51];
    ds_key_t  web_open_date;
    ds_key_t  web_close_date;
    char      web_class[51];
    char      web_manager[51];
    int       web_market_id;
    char      web_market_class[51];
    char      web_market_desc[101];
    char      web_market_manager[41];
    int       web_company_id;
    char      web_company_name[101];
    ds_addr_t web_address;
    decimal_t web_tax_percentage;
};

/* warehouse row (w_warehouse.h) */
struct W_WAREHOUSE_TBL {
    ds_key_t  w_warehouse_sk;
    char      w_warehouse_id[17];
    char      w_warehouse_name[21];
    int       w_warehouse_sq_ft;
    ds_addr_t w_address;
};

/* ship_mode row (w_ship_mode.h) */
struct W_SHIP_MODE_TBL {
    ds_key_t  sm_ship_mode_sk;
    char      sm_ship_mode_id[17];
    char     *sm_type;
    char     *sm_code;
    char     *sm_carrier;
    char      sm_contract[21];
};

/* household_demographics row (w_household_demographics.h) */
struct W_HOUSEHOLD_DEMOGRAPHICS_TBL {
    ds_key_t  hd_demo_sk;
    ds_key_t  hd_income_band_id;
    char     *hd_buy_potential;
    int       hd_dep_count;
    int       hd_vehicle_count;
};

/* customer_demographics row (w_customer_demographics.h) */
struct W_CUSTOMER_DEMOGRAPHICS_TBL {
    ds_key_t  cd_demo_sk;
    char     *cd_gender;
    char     *cd_marital_status;
    char     *cd_education_status;
    int       cd_purchase_estimate;
    char     *cd_credit_rating;
    int       cd_dep_count;
    int       cd_dep_employed_count;
    int       cd_dep_college_count;
};

/* customer_address row (w_customer_address.h) */
struct W_CUSTOMER_ADDRESS_TBL {
    ds_key_t  ca_addr_sk;
    char      ca_addr_id[17];
    ds_addr_t ca_address;
    char     *ca_location_type;
};

/* income_band row (w_income_band.h) */
struct W_INCOME_BAND_TBL {
    int ib_income_band_id;
    int ib_lower_bound;
    int ib_upper_bound;
};

/* reason row (w_reason.h) */
struct W_REASON_TBL {
    ds_key_t  r_reason_sk;
    char      r_reason_id[17];
    char     *r_reason_description;
};

/* time_dim row (w_timetbl.h) */
struct W_TIME_TBL {
    ds_key_t  t_time_sk;
    char      t_time_id[17];
    int       t_time;
    int       t_hour;
    int       t_minute;
    int       t_second;
    char     *t_am_pm;
    char     *t_shift;
    char     *t_sub_shift;
    char     *t_meal_time;
};

/* promotion row (w_promotion.h) */
struct W_PROMOTION_TBL {
    ds_key_t  p_promo_sk;
    char      p_promo_id[17];
    ds_key_t  p_start_date_id;
    ds_key_t  p_end_date_id;
    ds_key_t  p_item_sk;
    decimal_t p_cost;
    int       p_response_target;
    char      p_promo_name[51];
    int       p_channel_dmail;
    int       p_channel_email;
    int       p_channel_catalog;
    int       p_channel_tv;
    int       p_channel_radio;
    int       p_channel_press;
    int       p_channel_event;
    int       p_channel_demo;
    char      p_channel_details[101];
    char     *p_purpose;
    int       p_discount_active;
};

/* store row (w_store.h) */
struct W_STORE_TBL {
    ds_key_t  store_sk;
    char      store_id[17];
    ds_key_t  rec_start_date_id;
    ds_key_t  rec_end_date_id;
    ds_key_t  closed_date_id;
    char      store_name[51];
    int       employees;
    int       floor_space;
    char     *hours;
    char      store_manager[41];
    int       market_id;
    decimal_t dTaxPercentage;
    char     *geography_class;
    char      market_desc[101];
    char      market_manager[41];
    ds_key_t  division_id;
    char     *division_name;
    ds_key_t  company_id;
    char     *company_name;
    ds_addr_t address;
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
/* Phase 5 dimension tables */
#define TPCDS_CALL_CENTER       0
#define TPCDS_CATALOG_PAGE      1
#define TPCDS_CUSTOMER_ADDRESS  5
#define TPCDS_CUSTOMER_DEMOGRAPHICS 6
#define TPCDS_HOUSEHOLD_DEMOGRAPHICS 8
#define TPCDS_INCOME_BAND       9
#define TPCDS_PROMOTION        12
#define TPCDS_REASON           13
#define TPCDS_SHIP_MODE        14
#define TPCDS_STORE            15
#define TPCDS_TIME             18
#define TPCDS_WAREHOUSE        19
#define TPCDS_WEB_PAGE         20
#define TPCDS_WEB_SITE         23

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
/* Phase 5 dimension table generators */
int mk_w_call_center(void* pDest, ds_key_t kIndex);
int mk_w_catalog_page(void* pDest, ds_key_t kIndex);
int mk_w_web_page(void* pDest, ds_key_t kIndex);
int mk_w_web_site(void* pDest, ds_key_t kIndex);
int mk_w_warehouse(void* pDest, ds_key_t kIndex);
int mk_w_ship_mode(void* pDest, ds_key_t kIndex);
int mk_w_household_demographics(void* pDest, ds_key_t kIndex);
int mk_w_customer_demographics(void* pDest, ds_key_t kIndex);
int mk_w_customer_address(void* pDest, ds_key_t kIndex);
int mk_w_income_band(void* pDest, ds_key_t kIndex);
int mk_w_reason(void* pDest, ds_key_t kIndex);
int mk_w_time(void* pDest, ds_key_t kIndex);
int mk_w_promotion(void* pDest, ds_key_t kIndex);
int mk_w_store(void* pDest, ds_key_t kIndex);

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
