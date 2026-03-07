/**
 * dsdgen_converter.cpp — Convert dsdgen C structs to Arrow array builders.
 *
 * Uses dectof() to convert decimal_t (scaled integer) fields to double.
 * ds_key_t (= int64_t on Linux) is mapped to arrow::int64().
 */

#include "tpch/dsdgen_converter.hpp"

#include <stdexcept>
#include <arrow/builder.h>

extern "C" {
#include "tpcds_dsdgen.h"
}

namespace tpcds {

// ---------------------------------------------------------------------------
// Helper: decimal_t → double
//
// dsdgen stores decimals as scaled integers: number = value * 10^precision.
// Example: "12.34" → scale=2, precision=2, number=1234.
// Conversion: (double)number / 10^precision.
//
// NOTE: dectoflt() in decimal.c is buggy (divides by 10^(precision-1) and
// mutates the struct).  We implement the correct formula here.
// ---------------------------------------------------------------------------

static inline double dec_to_double(const decimal_t* d) {
    if (d->precision == 0) return static_cast<double>(d->number);
    double result = static_cast<double>(d->number);
    for (int i = 0; i < d->precision; ++i) {
        result /= 10.0;
    }
    return result;
}

// ---------------------------------------------------------------------------
// store_sales
// ---------------------------------------------------------------------------

void append_store_sales_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_STORE_SALES_TBL*>(row);

    // Surrogate keys (int64)
    static_cast<arrow::Int64Builder*>(builders["ss_sold_date_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_date_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_sold_time_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_time_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_item_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_item_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_store_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_store_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_promo_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_promo_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_ticket_number"].get())
        ->Append(static_cast<int64_t>(r->ss_ticket_number));

    // Quantity (int)
    static_cast<arrow::Int32Builder*>(builders["ss_quantity"].get())
        ->Append(static_cast<int32_t>(r->ss_pricing.quantity));

    // Decimal pricing fields → double
    const ds_pricing_t* p = &r->ss_pricing;

    static_cast<arrow::DoubleBuilder*>(builders["ss_wholesale_cost"].get())
        ->Append(dec_to_double(&p->wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ss_list_price"].get())
        ->Append(dec_to_double(&p->list_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_sales_price"].get())
        ->Append(dec_to_double(&p->sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_discount_amt"].get())
        ->Append(dec_to_double(&p->ext_discount_amt));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_sales_price"].get())
        ->Append(dec_to_double(&p->ext_sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_wholesale_cost"].get())
        ->Append(dec_to_double(&p->ext_wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_list_price"].get())
        ->Append(dec_to_double(&p->ext_list_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ss_coupon_amt"].get())
        ->Append(dec_to_double(&p->coupon_amt));
    static_cast<arrow::DoubleBuilder*>(builders["ss_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["ss_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ss_net_profit"].get())
        ->Append(dec_to_double(&p->net_profit));
}

// ---------------------------------------------------------------------------
// inventory
// ---------------------------------------------------------------------------

void append_inventory_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_INVENTORY_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["inv_date_sk"].get())
        ->Append(static_cast<int64_t>(r->inv_date_sk));
    static_cast<arrow::Int64Builder*>(builders["inv_item_sk"].get())
        ->Append(static_cast<int64_t>(r->inv_item_sk));
    static_cast<arrow::Int64Builder*>(builders["inv_warehouse_sk"].get())
        ->Append(static_cast<int64_t>(r->inv_warehouse_sk));
    static_cast<arrow::Int32Builder*>(builders["inv_quantity_on_hand"].get())
        ->Append(static_cast<int32_t>(r->inv_quantity_on_hand));
}

// ---------------------------------------------------------------------------
// catalog_sales
// ---------------------------------------------------------------------------

void append_catalog_sales_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_CATALOG_SALES_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["cs_sold_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_sold_date_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_sold_time_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_sold_time_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_date_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_bill_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_bill_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_bill_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_bill_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_bill_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_bill_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_bill_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_bill_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_call_center_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_call_center_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_catalog_page_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_catalog_page_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_mode_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_mode_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_warehouse_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_warehouse_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_item_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_sold_item_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_promo_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_promo_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_order_number"].get())
        ->Append(static_cast<int64_t>(r->cs_order_number));

    const ds_pricing_t* p = &r->cs_pricing;
    static_cast<arrow::Int32Builder*>(builders["cs_quantity"].get())
        ->Append(static_cast<int32_t>(p->quantity));
    static_cast<arrow::DoubleBuilder*>(builders["cs_wholesale_cost"].get())
        ->Append(dec_to_double(&p->wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["cs_list_price"].get())
        ->Append(dec_to_double(&p->list_price));
    static_cast<arrow::DoubleBuilder*>(builders["cs_sales_price"].get())
        ->Append(dec_to_double(&p->sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_discount_amt"].get())
        ->Append(dec_to_double(&p->ext_discount_amt));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_sales_price"].get())
        ->Append(dec_to_double(&p->ext_sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_wholesale_cost"].get())
        ->Append(dec_to_double(&p->ext_wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_list_price"].get())
        ->Append(dec_to_double(&p->ext_list_price));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["cs_coupon_amt"].get())
        ->Append(dec_to_double(&p->coupon_amt));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_ship_cost"].get())
        ->Append(dec_to_double(&p->ext_ship_cost));
    static_cast<arrow::DoubleBuilder*>(builders["cs_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["cs_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["cs_net_paid_inc_ship"].get())
        ->Append(dec_to_double(&p->net_paid_inc_ship));
    static_cast<arrow::DoubleBuilder*>(builders["cs_net_paid_inc_ship_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_ship_tax));
    static_cast<arrow::DoubleBuilder*>(builders["cs_net_profit"].get())
        ->Append(dec_to_double(&p->net_profit));
}

// ---------------------------------------------------------------------------
// web_sales
// ---------------------------------------------------------------------------

void append_web_sales_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_WEB_SALES_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["ws_sold_date_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_sold_date_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_sold_time_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_sold_time_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_date_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_date_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_item_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_item_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_bill_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_bill_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_bill_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_bill_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_bill_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_bill_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_bill_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_bill_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_web_page_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_web_page_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_web_site_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_web_site_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_mode_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_mode_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_warehouse_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_warehouse_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_promo_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_promo_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_order_number"].get())
        ->Append(static_cast<int64_t>(r->ws_order_number));

    const ds_pricing_t* p = &r->ws_pricing;
    static_cast<arrow::Int32Builder*>(builders["ws_quantity"].get())
        ->Append(static_cast<int32_t>(p->quantity));
    static_cast<arrow::DoubleBuilder*>(builders["ws_wholesale_cost"].get())
        ->Append(dec_to_double(&p->wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ws_list_price"].get())
        ->Append(dec_to_double(&p->list_price));
    static_cast<arrow::DoubleBuilder*>(builders["ws_sales_price"].get())
        ->Append(dec_to_double(&p->sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_discount_amt"].get())
        ->Append(dec_to_double(&p->ext_discount_amt));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_sales_price"].get())
        ->Append(dec_to_double(&p->ext_sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_wholesale_cost"].get())
        ->Append(dec_to_double(&p->ext_wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_list_price"].get())
        ->Append(dec_to_double(&p->ext_list_price));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ws_coupon_amt"].get())
        ->Append(dec_to_double(&p->coupon_amt));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_ship_cost"].get())
        ->Append(dec_to_double(&p->ext_ship_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ws_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["ws_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ws_net_paid_inc_ship"].get())
        ->Append(dec_to_double(&p->net_paid_inc_ship));
    static_cast<arrow::DoubleBuilder*>(builders["ws_net_paid_inc_ship_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_ship_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ws_net_profit"].get())
        ->Append(dec_to_double(&p->net_profit));
}

// ---------------------------------------------------------------------------
// customer
// ---------------------------------------------------------------------------

void append_customer_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_CUSTOMER_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["c_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->c_customer_sk));
    static_cast<arrow::StringBuilder*>(builders["c_customer_id"].get())
        ->Append(r->c_customer_id);
    static_cast<arrow::Int64Builder*>(builders["c_current_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->c_current_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["c_current_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->c_current_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["c_current_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->c_current_addr_sk));
    static_cast<arrow::Int32Builder*>(builders["c_first_shipto_date_id"].get())
        ->Append(static_cast<int32_t>(r->c_first_shipto_date_id));
    static_cast<arrow::Int32Builder*>(builders["c_first_sales_date_id"].get())
        ->Append(static_cast<int32_t>(r->c_first_sales_date_id));
    static_cast<arrow::StringBuilder*>(builders["c_salutation"].get())
        ->Append(r->c_salutation ? r->c_salutation : "");
    static_cast<arrow::StringBuilder*>(builders["c_first_name"].get())
        ->Append(r->c_first_name ? r->c_first_name : "");
    static_cast<arrow::StringBuilder*>(builders["c_last_name"].get())
        ->Append(r->c_last_name ? r->c_last_name : "");
    static_cast<arrow::Int32Builder*>(builders["c_preferred_cust_flag"].get())
        ->Append(static_cast<int32_t>(r->c_preferred_cust_flag));
    static_cast<arrow::Int32Builder*>(builders["c_birth_day"].get())
        ->Append(static_cast<int32_t>(r->c_birth_day));
    static_cast<arrow::Int32Builder*>(builders["c_birth_month"].get())
        ->Append(static_cast<int32_t>(r->c_birth_month));
    static_cast<arrow::Int32Builder*>(builders["c_birth_year"].get())
        ->Append(static_cast<int32_t>(r->c_birth_year));
    static_cast<arrow::StringBuilder*>(builders["c_birth_country"].get())
        ->Append(r->c_birth_country ? r->c_birth_country : "");
    static_cast<arrow::StringBuilder*>(builders["c_login"].get())
        ->Append(r->c_login);
    static_cast<arrow::StringBuilder*>(builders["c_email_address"].get())
        ->Append(r->c_email_address);
    static_cast<arrow::Int32Builder*>(builders["c_last_review_date"].get())
        ->Append(static_cast<int32_t>(r->c_last_review_date));
}

// ---------------------------------------------------------------------------
// item
// ---------------------------------------------------------------------------

void append_item_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_ITEM_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["i_item_sk"].get())
        ->Append(static_cast<int64_t>(r->i_item_sk));
    static_cast<arrow::StringBuilder*>(builders["i_item_id"].get())
        ->Append(r->i_item_id);
    static_cast<arrow::Int64Builder*>(builders["i_rec_start_date_id"].get())
        ->Append(static_cast<int64_t>(r->i_rec_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["i_rec_end_date_id"].get())
        ->Append(static_cast<int64_t>(r->i_rec_end_date_id));
    static_cast<arrow::StringBuilder*>(builders["i_item_desc"].get())
        ->Append(r->i_item_desc);
    static_cast<arrow::DoubleBuilder*>(builders["i_current_price"].get())
        ->Append(dec_to_double(&r->i_current_price));
    static_cast<arrow::DoubleBuilder*>(builders["i_wholesale_cost"].get())
        ->Append(dec_to_double(&r->i_wholesale_cost));
    static_cast<arrow::Int64Builder*>(builders["i_brand_id"].get())
        ->Append(static_cast<int64_t>(r->i_brand_id));
    static_cast<arrow::StringBuilder*>(builders["i_brand"].get())
        ->Append(r->i_brand);
    static_cast<arrow::Int64Builder*>(builders["i_class_id"].get())
        ->Append(static_cast<int64_t>(r->i_class_id));
    static_cast<arrow::StringBuilder*>(builders["i_class"].get())
        ->Append(r->i_class ? r->i_class : "");
    static_cast<arrow::Int64Builder*>(builders["i_category_id"].get())
        ->Append(static_cast<int64_t>(r->i_category_id));
    static_cast<arrow::StringBuilder*>(builders["i_category"].get())
        ->Append(r->i_category ? r->i_category : "");
    static_cast<arrow::Int64Builder*>(builders["i_manufact_id"].get())
        ->Append(static_cast<int64_t>(r->i_manufact_id));
    static_cast<arrow::StringBuilder*>(builders["i_manufact"].get())
        ->Append(r->i_manufact);
    static_cast<arrow::StringBuilder*>(builders["i_size"].get())
        ->Append(r->i_size ? r->i_size : "");
    static_cast<arrow::StringBuilder*>(builders["i_formulation"].get())
        ->Append(r->i_formulation);
    static_cast<arrow::StringBuilder*>(builders["i_color"].get())
        ->Append(r->i_color ? r->i_color : "");
    static_cast<arrow::StringBuilder*>(builders["i_units"].get())
        ->Append(r->i_units ? r->i_units : "");
    static_cast<arrow::StringBuilder*>(builders["i_container"].get())
        ->Append(r->i_container ? r->i_container : "");
    static_cast<arrow::Int64Builder*>(builders["i_manager_id"].get())
        ->Append(static_cast<int64_t>(r->i_manager_id));
    static_cast<arrow::StringBuilder*>(builders["i_product_name"].get())
        ->Append(r->i_product_name);
    static_cast<arrow::Int64Builder*>(builders["i_promo_sk"].get())
        ->Append(static_cast<int64_t>(r->i_promo_sk));
}

// ---------------------------------------------------------------------------
// date_dim
// ---------------------------------------------------------------------------

void append_date_dim_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_DATE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["d_date_sk"].get())
        ->Append(static_cast<int64_t>(r->d_date_sk));
    static_cast<arrow::StringBuilder*>(builders["d_date_id"].get())
        ->Append(r->d_date_id);
    static_cast<arrow::Int32Builder*>(builders["d_month_seq"].get())
        ->Append(static_cast<int32_t>(r->d_month_seq));
    static_cast<arrow::Int32Builder*>(builders["d_week_seq"].get())
        ->Append(static_cast<int32_t>(r->d_week_seq));
    static_cast<arrow::Int32Builder*>(builders["d_quarter_seq"].get())
        ->Append(static_cast<int32_t>(r->d_quarter_seq));
    static_cast<arrow::Int32Builder*>(builders["d_year"].get())
        ->Append(static_cast<int32_t>(r->d_year));
    static_cast<arrow::Int32Builder*>(builders["d_dow"].get())
        ->Append(static_cast<int32_t>(r->d_dow));
    static_cast<arrow::Int32Builder*>(builders["d_moy"].get())
        ->Append(static_cast<int32_t>(r->d_moy));
    static_cast<arrow::Int32Builder*>(builders["d_dom"].get())
        ->Append(static_cast<int32_t>(r->d_dom));
    static_cast<arrow::Int32Builder*>(builders["d_qoy"].get())
        ->Append(static_cast<int32_t>(r->d_qoy));
    static_cast<arrow::Int32Builder*>(builders["d_fy_year"].get())
        ->Append(static_cast<int32_t>(r->d_fy_year));
    static_cast<arrow::Int32Builder*>(builders["d_fy_quarter_seq"].get())
        ->Append(static_cast<int32_t>(r->d_fy_quarter_seq));
    static_cast<arrow::Int32Builder*>(builders["d_fy_week_seq"].get())
        ->Append(static_cast<int32_t>(r->d_fy_week_seq));
    static_cast<arrow::StringBuilder*>(builders["d_day_name"].get())
        ->Append(r->d_day_name ? r->d_day_name : "");
    static_cast<arrow::Int32Builder*>(builders["d_holiday"].get())
        ->Append(static_cast<int32_t>(r->d_holiday));
    static_cast<arrow::Int32Builder*>(builders["d_weekend"].get())
        ->Append(static_cast<int32_t>(r->d_weekend));
    static_cast<arrow::Int32Builder*>(builders["d_following_holiday"].get())
        ->Append(static_cast<int32_t>(r->d_following_holiday));
    static_cast<arrow::Int32Builder*>(builders["d_first_dom"].get())
        ->Append(static_cast<int32_t>(r->d_first_dom));
    static_cast<arrow::Int32Builder*>(builders["d_last_dom"].get())
        ->Append(static_cast<int32_t>(r->d_last_dom));
    static_cast<arrow::Int32Builder*>(builders["d_same_day_ly"].get())
        ->Append(static_cast<int32_t>(r->d_same_day_ly));
    static_cast<arrow::Int32Builder*>(builders["d_same_day_lq"].get())
        ->Append(static_cast<int32_t>(r->d_same_day_lq));
    static_cast<arrow::Int32Builder*>(builders["d_current_day"].get())
        ->Append(static_cast<int32_t>(r->d_current_day));
    static_cast<arrow::Int32Builder*>(builders["d_current_week"].get())
        ->Append(static_cast<int32_t>(r->d_current_week));
    static_cast<arrow::Int32Builder*>(builders["d_current_month"].get())
        ->Append(static_cast<int32_t>(r->d_current_month));
    static_cast<arrow::Int32Builder*>(builders["d_current_quarter"].get())
        ->Append(static_cast<int32_t>(r->d_current_quarter));
    static_cast<arrow::Int32Builder*>(builders["d_current_year"].get())
        ->Append(static_cast<int32_t>(r->d_current_year));
}

// ---------------------------------------------------------------------------
// store_returns
// ---------------------------------------------------------------------------

void append_store_returns_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_STORE_RETURNS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["sr_returned_date_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_returned_date_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_returned_time_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_returned_time_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_item_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_item_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_store_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_store_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_reason_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_reason_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_ticket_number"].get())
        ->Append(static_cast<int64_t>(r->sr_ticket_number));

    const ds_pricing_t* p = &r->sr_pricing;
    static_cast<arrow::Int32Builder*>(builders["sr_quantity"].get())
        ->Append(static_cast<int32_t>(p->quantity));
    static_cast<arrow::DoubleBuilder*>(builders["sr_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["sr_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["sr_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["sr_fee"].get())
        ->Append(dec_to_double(&p->fee));
    static_cast<arrow::DoubleBuilder*>(builders["sr_ext_ship_cost"].get())
        ->Append(dec_to_double(&p->ext_ship_cost));
    static_cast<arrow::DoubleBuilder*>(builders["sr_refunded_cash"].get())
        ->Append(dec_to_double(&p->refunded_cash));
    static_cast<arrow::DoubleBuilder*>(builders["sr_reversed_charge"].get())
        ->Append(dec_to_double(&p->reversed_charge));
    static_cast<arrow::DoubleBuilder*>(builders["sr_store_credit"].get())
        ->Append(dec_to_double(&p->store_credit));
    static_cast<arrow::DoubleBuilder*>(builders["sr_net_loss"].get())
        ->Append(dec_to_double(&p->net_loss));
}

// ---------------------------------------------------------------------------
// catalog_returns
// ---------------------------------------------------------------------------

void append_catalog_returns_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_CATALOG_RETURNS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["cr_returned_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returned_date_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_returned_time_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returned_time_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_item_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_item_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_refunded_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_refunded_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_refunded_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_refunded_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_refunded_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_refunded_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_refunded_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_refunded_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_returning_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returning_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_returning_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returning_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_returning_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returning_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_returning_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returning_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_call_center_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_call_center_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_catalog_page_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_catalog_page_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_ship_mode_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_ship_mode_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_warehouse_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_warehouse_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_reason_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_reason_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_order_number"].get())
        ->Append(static_cast<int64_t>(r->cr_order_number));

    const ds_pricing_t* p = &r->cr_pricing;
    static_cast<arrow::Int32Builder*>(builders["cr_quantity"].get())
        ->Append(static_cast<int32_t>(p->quantity));
    static_cast<arrow::DoubleBuilder*>(builders["cr_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["cr_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["cr_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["cr_fee"].get())
        ->Append(dec_to_double(&p->fee));
    static_cast<arrow::DoubleBuilder*>(builders["cr_ext_ship_cost"].get())
        ->Append(dec_to_double(&p->ext_ship_cost));
    static_cast<arrow::DoubleBuilder*>(builders["cr_refunded_cash"].get())
        ->Append(dec_to_double(&p->refunded_cash));
    static_cast<arrow::DoubleBuilder*>(builders["cr_reversed_charge"].get())
        ->Append(dec_to_double(&p->reversed_charge));
    static_cast<arrow::DoubleBuilder*>(builders["cr_store_credit"].get())
        ->Append(dec_to_double(&p->store_credit));
    static_cast<arrow::DoubleBuilder*>(builders["cr_net_loss"].get())
        ->Append(dec_to_double(&p->net_loss));
}

// ---------------------------------------------------------------------------
// web_returns
// ---------------------------------------------------------------------------

void append_web_returns_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_WEB_RETURNS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["wr_returned_date_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returned_date_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_returned_time_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returned_time_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_item_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_item_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_refunded_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_refunded_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_refunded_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_refunded_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_refunded_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_refunded_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_refunded_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_refunded_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_returning_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returning_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_returning_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returning_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_returning_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returning_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_returning_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returning_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_web_page_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_web_page_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_reason_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_reason_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_order_number"].get())
        ->Append(static_cast<int64_t>(r->wr_order_number));

    const ds_pricing_t* p = &r->wr_pricing;
    static_cast<arrow::Int32Builder*>(builders["wr_quantity"].get())
        ->Append(static_cast<int32_t>(p->quantity));
    static_cast<arrow::DoubleBuilder*>(builders["wr_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["wr_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["wr_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["wr_fee"].get())
        ->Append(dec_to_double(&p->fee));
    static_cast<arrow::DoubleBuilder*>(builders["wr_ext_ship_cost"].get())
        ->Append(dec_to_double(&p->ext_ship_cost));
    static_cast<arrow::DoubleBuilder*>(builders["wr_refunded_cash"].get())
        ->Append(dec_to_double(&p->refunded_cash));
    static_cast<arrow::DoubleBuilder*>(builders["wr_reversed_charge"].get())
        ->Append(dec_to_double(&p->reversed_charge));
    static_cast<arrow::DoubleBuilder*>(builders["wr_store_credit"].get())
        ->Append(dec_to_double(&p->store_credit));
    static_cast<arrow::DoubleBuilder*>(builders["wr_net_loss"].get())
        ->Append(dec_to_double(&p->net_loss));
}

// ---------------------------------------------------------------------------
// Helper: append ds_addr_t fields with given column-name prefix
// ---------------------------------------------------------------------------
//
// prefix_street_number, prefix_street_name, prefix_street_type,
// prefix_suite_number, prefix_city, prefix_county, prefix_state,
// prefix_zip (as string), prefix_country, prefix_gmt_offset
//
static void append_addr_fields(
    const ds_addr_t& addr,
    const std::string& pfx,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    static_cast<arrow::Int32Builder*>(builders[pfx + "street_number"].get())
        ->Append(addr.street_num);
    static_cast<arrow::StringBuilder*>(builders[pfx + "street_name"].get())
        ->Append(addr.street_name1 ? addr.street_name1 : "");
    static_cast<arrow::StringBuilder*>(builders[pfx + "street_type"].get())
        ->Append(addr.street_type ? addr.street_type : "");
    static_cast<arrow::StringBuilder*>(builders[pfx + "suite_number"].get())
        ->Append(addr.suite_num);
    static_cast<arrow::StringBuilder*>(builders[pfx + "city"].get())
        ->Append(addr.city ? addr.city : "");
    static_cast<arrow::StringBuilder*>(builders[pfx + "county"].get())
        ->Append(addr.county ? addr.county : "");
    static_cast<arrow::StringBuilder*>(builders[pfx + "state"].get())
        ->Append(addr.state ? addr.state : "");
    char zip_buf[12];
    std::snprintf(zip_buf, sizeof(zip_buf), "%05d", addr.zip);
    static_cast<arrow::StringBuilder*>(builders[pfx + "zip"].get())
        ->Append(zip_buf);
    static_cast<arrow::StringBuilder*>(builders[pfx + "country"].get())
        ->Append(addr.country);
    static_cast<arrow::DoubleBuilder*>(builders[pfx + "gmt_offset"].get())
        ->Append(static_cast<double>(addr.gmt_offset));
}

// ---------------------------------------------------------------------------
// call_center
// ---------------------------------------------------------------------------

void append_call_center_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct CALL_CENTER_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["cc_call_center_sk"].get())
        ->Append(static_cast<int64_t>(r->cc_call_center_sk));
    static_cast<arrow::StringBuilder*>(builders["cc_call_center_id"].get())
        ->Append(r->cc_call_center_id);
    static_cast<arrow::Int64Builder*>(builders["cc_rec_start_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cc_rec_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["cc_rec_end_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cc_rec_end_date_id));
    static_cast<arrow::Int64Builder*>(builders["cc_closed_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cc_closed_date_id));
    static_cast<arrow::Int64Builder*>(builders["cc_open_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cc_open_date_id));
    static_cast<arrow::StringBuilder*>(builders["cc_name"].get())
        ->Append(r->cc_name);
    static_cast<arrow::StringBuilder*>(builders["cc_class"].get())
        ->Append(r->cc_class ? r->cc_class : "");
    static_cast<arrow::Int32Builder*>(builders["cc_employees"].get())
        ->Append(static_cast<int32_t>(r->cc_employees));
    static_cast<arrow::Int32Builder*>(builders["cc_sq_ft"].get())
        ->Append(static_cast<int32_t>(r->cc_sq_ft));
    static_cast<arrow::StringBuilder*>(builders["cc_hours"].get())
        ->Append(r->cc_hours ? r->cc_hours : "");
    static_cast<arrow::StringBuilder*>(builders["cc_manager"].get())
        ->Append(r->cc_manager);
    static_cast<arrow::Int32Builder*>(builders["cc_mkt_id"].get())
        ->Append(static_cast<int32_t>(r->cc_market_id));
    static_cast<arrow::StringBuilder*>(builders["cc_mkt_class"].get())
        ->Append(r->cc_market_class);
    static_cast<arrow::StringBuilder*>(builders["cc_mkt_desc"].get())
        ->Append(r->cc_market_desc);
    static_cast<arrow::StringBuilder*>(builders["cc_market_manager"].get())
        ->Append(r->cc_market_manager);
    static_cast<arrow::Int32Builder*>(builders["cc_division"].get())
        ->Append(static_cast<int32_t>(r->cc_division_id));
    static_cast<arrow::StringBuilder*>(builders["cc_division_name"].get())
        ->Append(r->cc_division_name);
    static_cast<arrow::Int32Builder*>(builders["cc_company"].get())
        ->Append(static_cast<int32_t>(r->cc_company));
    static_cast<arrow::StringBuilder*>(builders["cc_company_name"].get())
        ->Append(r->cc_company_name);
    append_addr_fields(r->cc_address, "cc_", builders);
    static_cast<arrow::DoubleBuilder*>(builders["cc_tax_percentage"].get())
        ->Append(dec_to_double(&r->cc_tax_percentage));
}

// ---------------------------------------------------------------------------
// catalog_page
// ---------------------------------------------------------------------------

void append_catalog_page_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct CATALOG_PAGE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["cp_catalog_page_sk"].get())
        ->Append(static_cast<int64_t>(r->cp_catalog_page_sk));
    static_cast<arrow::StringBuilder*>(builders["cp_catalog_page_id"].get())
        ->Append(r->cp_catalog_page_id);
    static_cast<arrow::Int64Builder*>(builders["cp_start_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cp_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["cp_end_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cp_end_date_id));
    static_cast<arrow::StringBuilder*>(builders["cp_department"].get())
        ->Append(r->cp_department);
    static_cast<arrow::Int32Builder*>(builders["cp_catalog_number"].get())
        ->Append(static_cast<int32_t>(r->cp_catalog_number));
    static_cast<arrow::Int32Builder*>(builders["cp_catalog_page_number"].get())
        ->Append(static_cast<int32_t>(r->cp_catalog_page_number));
    static_cast<arrow::StringBuilder*>(builders["cp_description"].get())
        ->Append(r->cp_description);
    static_cast<arrow::StringBuilder*>(builders["cp_type"].get())
        ->Append(r->cp_type ? r->cp_type : "");
}

// ---------------------------------------------------------------------------
// web_page
// ---------------------------------------------------------------------------

void append_web_page_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_WEB_PAGE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["wp_web_page_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_page_sk));
    static_cast<arrow::StringBuilder*>(builders["wp_web_page_id"].get())
        ->Append(r->wp_page_id);
    static_cast<arrow::Int64Builder*>(builders["wp_rec_start_date_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_rec_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["wp_rec_end_date_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_rec_end_date_id));
    static_cast<arrow::Int64Builder*>(builders["wp_creation_date_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_creation_date_sk));
    static_cast<arrow::Int64Builder*>(builders["wp_access_date_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_access_date_sk));
    static_cast<arrow::Int32Builder*>(builders["wp_autogen_flag"].get())
        ->Append(static_cast<int32_t>(r->wp_autogen_flag));
    static_cast<arrow::Int64Builder*>(builders["wp_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_customer_sk));
    static_cast<arrow::StringBuilder*>(builders["wp_url"].get())
        ->Append(r->wp_url);
    static_cast<arrow::StringBuilder*>(builders["wp_type"].get())
        ->Append(r->wp_type ? r->wp_type : "");
    static_cast<arrow::Int32Builder*>(builders["wp_char_count"].get())
        ->Append(static_cast<int32_t>(r->wp_char_count));
    static_cast<arrow::Int32Builder*>(builders["wp_link_count"].get())
        ->Append(static_cast<int32_t>(r->wp_link_count));
    static_cast<arrow::Int32Builder*>(builders["wp_image_count"].get())
        ->Append(static_cast<int32_t>(r->wp_image_count));
    static_cast<arrow::Int32Builder*>(builders["wp_max_ad_count"].get())
        ->Append(static_cast<int32_t>(r->wp_max_ad_count));
}

// ---------------------------------------------------------------------------
// web_site
// ---------------------------------------------------------------------------

void append_web_site_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_WEB_SITE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["web_site_sk"].get())
        ->Append(static_cast<int64_t>(r->web_site_sk));
    static_cast<arrow::StringBuilder*>(builders["web_site_id"].get())
        ->Append(r->web_site_id);
    static_cast<arrow::Int64Builder*>(builders["web_rec_start_date_sk"].get())
        ->Append(static_cast<int64_t>(r->web_rec_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["web_rec_end_date_sk"].get())
        ->Append(static_cast<int64_t>(r->web_rec_end_date_id));
    static_cast<arrow::StringBuilder*>(builders["web_name"].get())
        ->Append(r->web_name);
    static_cast<arrow::Int64Builder*>(builders["web_open_date_sk"].get())
        ->Append(static_cast<int64_t>(r->web_open_date));
    static_cast<arrow::Int64Builder*>(builders["web_close_date_sk"].get())
        ->Append(static_cast<int64_t>(r->web_close_date));
    static_cast<arrow::StringBuilder*>(builders["web_class"].get())
        ->Append(r->web_class);
    static_cast<arrow::StringBuilder*>(builders["web_manager"].get())
        ->Append(r->web_manager);
    static_cast<arrow::Int32Builder*>(builders["web_mkt_id"].get())
        ->Append(static_cast<int32_t>(r->web_market_id));
    static_cast<arrow::StringBuilder*>(builders["web_mkt_class"].get())
        ->Append(r->web_market_class);
    static_cast<arrow::StringBuilder*>(builders["web_mkt_desc"].get())
        ->Append(r->web_market_desc);
    static_cast<arrow::StringBuilder*>(builders["web_market_manager"].get())
        ->Append(r->web_market_manager);
    static_cast<arrow::Int32Builder*>(builders["web_company_id"].get())
        ->Append(static_cast<int32_t>(r->web_company_id));
    static_cast<arrow::StringBuilder*>(builders["web_company_name"].get())
        ->Append(r->web_company_name);
    append_addr_fields(r->web_address, "web_", builders);
    static_cast<arrow::DoubleBuilder*>(builders["web_tax_percentage"].get())
        ->Append(dec_to_double(&r->web_tax_percentage));
}

// ---------------------------------------------------------------------------
// warehouse
// ---------------------------------------------------------------------------

void append_warehouse_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_WAREHOUSE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["w_warehouse_sk"].get())
        ->Append(static_cast<int64_t>(r->w_warehouse_sk));
    static_cast<arrow::StringBuilder*>(builders["w_warehouse_id"].get())
        ->Append(r->w_warehouse_id);
    static_cast<arrow::StringBuilder*>(builders["w_warehouse_name"].get())
        ->Append(r->w_warehouse_name);
    static_cast<arrow::Int32Builder*>(builders["w_warehouse_sq_ft"].get())
        ->Append(static_cast<int32_t>(r->w_warehouse_sq_ft));
    append_addr_fields(r->w_address, "w_", builders);
}

// ---------------------------------------------------------------------------
// ship_mode
// ---------------------------------------------------------------------------

void append_ship_mode_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_SHIP_MODE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["sm_ship_mode_sk"].get())
        ->Append(static_cast<int64_t>(r->sm_ship_mode_sk));
    static_cast<arrow::StringBuilder*>(builders["sm_ship_mode_id"].get())
        ->Append(r->sm_ship_mode_id);
    static_cast<arrow::StringBuilder*>(builders["sm_type"].get())
        ->Append(r->sm_type ? r->sm_type : "");
    static_cast<arrow::StringBuilder*>(builders["sm_code"].get())
        ->Append(r->sm_code ? r->sm_code : "");
    static_cast<arrow::StringBuilder*>(builders["sm_carrier"].get())
        ->Append(r->sm_carrier ? r->sm_carrier : "");
    static_cast<arrow::StringBuilder*>(builders["sm_contract"].get())
        ->Append(r->sm_contract);
}

// ---------------------------------------------------------------------------
// household_demographics
// ---------------------------------------------------------------------------

void append_household_demographics_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_HOUSEHOLD_DEMOGRAPHICS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["hd_demo_sk"].get())
        ->Append(static_cast<int64_t>(r->hd_demo_sk));
    static_cast<arrow::Int64Builder*>(builders["hd_income_band_sk"].get())
        ->Append(static_cast<int64_t>(r->hd_income_band_id));
    static_cast<arrow::StringBuilder*>(builders["hd_buy_potential"].get())
        ->Append(r->hd_buy_potential ? r->hd_buy_potential : "");
    static_cast<arrow::Int32Builder*>(builders["hd_dep_count"].get())
        ->Append(static_cast<int32_t>(r->hd_dep_count));
    static_cast<arrow::Int32Builder*>(builders["hd_vehicle_count"].get())
        ->Append(static_cast<int32_t>(r->hd_vehicle_count));
}

// ---------------------------------------------------------------------------
// customer_demographics
// ---------------------------------------------------------------------------

void append_customer_demographics_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_CUSTOMER_DEMOGRAPHICS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["cd_demo_sk"].get())
        ->Append(static_cast<int64_t>(r->cd_demo_sk));
    static_cast<arrow::StringBuilder*>(builders["cd_gender"].get())
        ->Append(r->cd_gender ? r->cd_gender : "");
    static_cast<arrow::StringBuilder*>(builders["cd_marital_status"].get())
        ->Append(r->cd_marital_status ? r->cd_marital_status : "");
    static_cast<arrow::StringBuilder*>(builders["cd_education_status"].get())
        ->Append(r->cd_education_status ? r->cd_education_status : "");
    static_cast<arrow::Int32Builder*>(builders["cd_purchase_estimate"].get())
        ->Append(static_cast<int32_t>(r->cd_purchase_estimate));
    static_cast<arrow::StringBuilder*>(builders["cd_credit_rating"].get())
        ->Append(r->cd_credit_rating ? r->cd_credit_rating : "");
    static_cast<arrow::Int32Builder*>(builders["cd_dep_count"].get())
        ->Append(static_cast<int32_t>(r->cd_dep_count));
    static_cast<arrow::Int32Builder*>(builders["cd_dep_employed_count"].get())
        ->Append(static_cast<int32_t>(r->cd_dep_employed_count));
    static_cast<arrow::Int32Builder*>(builders["cd_dep_college_count"].get())
        ->Append(static_cast<int32_t>(r->cd_dep_college_count));
}

// ---------------------------------------------------------------------------
// customer_address
// ---------------------------------------------------------------------------

void append_customer_address_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_CUSTOMER_ADDRESS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["ca_address_sk"].get())
        ->Append(static_cast<int64_t>(r->ca_addr_sk));
    static_cast<arrow::StringBuilder*>(builders["ca_address_id"].get())
        ->Append(r->ca_addr_id);
    append_addr_fields(r->ca_address, "ca_", builders);
    static_cast<arrow::StringBuilder*>(builders["ca_location_type"].get())
        ->Append(r->ca_location_type ? r->ca_location_type : "");
}

// ---------------------------------------------------------------------------
// income_band
// ---------------------------------------------------------------------------

void append_income_band_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_INCOME_BAND_TBL*>(row);

    static_cast<arrow::Int32Builder*>(builders["ib_income_band_id"].get())
        ->Append(static_cast<int32_t>(r->ib_income_band_id));
    static_cast<arrow::Int32Builder*>(builders["ib_lower_bound"].get())
        ->Append(static_cast<int32_t>(r->ib_lower_bound));
    static_cast<arrow::Int32Builder*>(builders["ib_upper_bound"].get())
        ->Append(static_cast<int32_t>(r->ib_upper_bound));
}

// ---------------------------------------------------------------------------
// reason
// ---------------------------------------------------------------------------

void append_reason_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_REASON_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["r_reason_sk"].get())
        ->Append(static_cast<int64_t>(r->r_reason_sk));
    static_cast<arrow::StringBuilder*>(builders["r_reason_id"].get())
        ->Append(r->r_reason_id);
    static_cast<arrow::StringBuilder*>(builders["r_reason_desc"].get())
        ->Append(r->r_reason_description ? r->r_reason_description : "");
}

// ---------------------------------------------------------------------------
// time_dim
// ---------------------------------------------------------------------------

void append_time_dim_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_TIME_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["t_time_sk"].get())
        ->Append(static_cast<int64_t>(r->t_time_sk));
    static_cast<arrow::StringBuilder*>(builders["t_time_id"].get())
        ->Append(r->t_time_id);
    static_cast<arrow::Int32Builder*>(builders["t_time"].get())
        ->Append(static_cast<int32_t>(r->t_time));
    static_cast<arrow::Int32Builder*>(builders["t_hour"].get())
        ->Append(static_cast<int32_t>(r->t_hour));
    static_cast<arrow::Int32Builder*>(builders["t_minute"].get())
        ->Append(static_cast<int32_t>(r->t_minute));
    static_cast<arrow::Int32Builder*>(builders["t_second"].get())
        ->Append(static_cast<int32_t>(r->t_second));
    static_cast<arrow::StringBuilder*>(builders["t_am_pm"].get())
        ->Append(r->t_am_pm ? r->t_am_pm : "");
    static_cast<arrow::StringBuilder*>(builders["t_shift"].get())
        ->Append(r->t_shift ? r->t_shift : "");
    static_cast<arrow::StringBuilder*>(builders["t_sub_shift"].get())
        ->Append(r->t_sub_shift ? r->t_sub_shift : "");
    static_cast<arrow::StringBuilder*>(builders["t_meal_time"].get())
        ->Append(r->t_meal_time ? r->t_meal_time : "");
}

// ---------------------------------------------------------------------------
// promotion
// ---------------------------------------------------------------------------

void append_promotion_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_PROMOTION_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["p_promo_sk"].get())
        ->Append(static_cast<int64_t>(r->p_promo_sk));
    static_cast<arrow::StringBuilder*>(builders["p_promo_id"].get())
        ->Append(r->p_promo_id);
    static_cast<arrow::Int64Builder*>(builders["p_start_date_sk"].get())
        ->Append(static_cast<int64_t>(r->p_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["p_end_date_sk"].get())
        ->Append(static_cast<int64_t>(r->p_end_date_id));
    static_cast<arrow::Int64Builder*>(builders["p_item_sk"].get())
        ->Append(static_cast<int64_t>(r->p_item_sk));
    static_cast<arrow::DoubleBuilder*>(builders["p_cost"].get())
        ->Append(dec_to_double(&r->p_cost));
    static_cast<arrow::Int32Builder*>(builders["p_response_target"].get())
        ->Append(static_cast<int32_t>(r->p_response_target));
    static_cast<arrow::StringBuilder*>(builders["p_promo_name"].get())
        ->Append(r->p_promo_name);
    static_cast<arrow::Int32Builder*>(builders["p_channel_dmail"].get())
        ->Append(static_cast<int32_t>(r->p_channel_dmail));
    static_cast<arrow::Int32Builder*>(builders["p_channel_email"].get())
        ->Append(static_cast<int32_t>(r->p_channel_email));
    static_cast<arrow::Int32Builder*>(builders["p_channel_catalog"].get())
        ->Append(static_cast<int32_t>(r->p_channel_catalog));
    static_cast<arrow::Int32Builder*>(builders["p_channel_tv"].get())
        ->Append(static_cast<int32_t>(r->p_channel_tv));
    static_cast<arrow::Int32Builder*>(builders["p_channel_radio"].get())
        ->Append(static_cast<int32_t>(r->p_channel_radio));
    static_cast<arrow::Int32Builder*>(builders["p_channel_press"].get())
        ->Append(static_cast<int32_t>(r->p_channel_press));
    static_cast<arrow::Int32Builder*>(builders["p_channel_event"].get())
        ->Append(static_cast<int32_t>(r->p_channel_event));
    static_cast<arrow::Int32Builder*>(builders["p_channel_demo"].get())
        ->Append(static_cast<int32_t>(r->p_channel_demo));
    static_cast<arrow::StringBuilder*>(builders["p_channel_details"].get())
        ->Append(r->p_channel_details);
    static_cast<arrow::StringBuilder*>(builders["p_purpose"].get())
        ->Append(r->p_purpose ? r->p_purpose : "");
    static_cast<arrow::Int32Builder*>(builders["p_discount_active"].get())
        ->Append(static_cast<int32_t>(r->p_discount_active));
}

// ---------------------------------------------------------------------------
// store
// ---------------------------------------------------------------------------

void append_store_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const struct W_STORE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["s_store_sk"].get())
        ->Append(static_cast<int64_t>(r->store_sk));
    static_cast<arrow::StringBuilder*>(builders["s_store_id"].get())
        ->Append(r->store_id);
    static_cast<arrow::Int64Builder*>(builders["s_rec_start_date"].get())
        ->Append(static_cast<int64_t>(r->rec_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["s_rec_end_date"].get())
        ->Append(static_cast<int64_t>(r->rec_end_date_id));
    static_cast<arrow::Int64Builder*>(builders["s_closed_date_sk"].get())
        ->Append(static_cast<int64_t>(r->closed_date_id));
    static_cast<arrow::StringBuilder*>(builders["s_store_name"].get())
        ->Append(r->store_name);
    static_cast<arrow::Int32Builder*>(builders["s_number_employees"].get())
        ->Append(static_cast<int32_t>(r->employees));
    static_cast<arrow::Int32Builder*>(builders["s_floor_space"].get())
        ->Append(static_cast<int32_t>(r->floor_space));
    static_cast<arrow::StringBuilder*>(builders["s_hours"].get())
        ->Append(r->hours ? r->hours : "");
    static_cast<arrow::StringBuilder*>(builders["s_manager"].get())
        ->Append(r->store_manager);
    static_cast<arrow::Int32Builder*>(builders["s_market_id"].get())
        ->Append(static_cast<int32_t>(r->market_id));
    static_cast<arrow::StringBuilder*>(builders["s_geography_class"].get())
        ->Append(r->geography_class ? r->geography_class : "");
    static_cast<arrow::StringBuilder*>(builders["s_market_desc"].get())
        ->Append(r->market_desc);
    static_cast<arrow::StringBuilder*>(builders["s_market_manager"].get())
        ->Append(r->market_manager);
    static_cast<arrow::Int64Builder*>(builders["s_division_id"].get())
        ->Append(static_cast<int64_t>(r->division_id));
    static_cast<arrow::StringBuilder*>(builders["s_division_name"].get())
        ->Append(r->division_name ? r->division_name : "");
    static_cast<arrow::Int64Builder*>(builders["s_company_id"].get())
        ->Append(static_cast<int64_t>(r->company_id));
    static_cast<arrow::StringBuilder*>(builders["s_company_name"].get())
        ->Append(r->company_name ? r->company_name : "");
    append_addr_fields(r->address, "s_", builders);
    static_cast<arrow::DoubleBuilder*>(builders["s_tax_percentage"].get())
        ->Append(dec_to_double(&r->dTaxPercentage));
}

// ---------------------------------------------------------------------------
// Generic dispatcher
// ---------------------------------------------------------------------------

void append_dsdgen_row_to_builders(
    const std::string& tbl_name,
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    if (tbl_name == "store_sales") {
        append_store_sales_to_builders(row, builders);
    } else if (tbl_name == "inventory") {
        append_inventory_to_builders(row, builders);
    } else if (tbl_name == "catalog_sales") {
        append_catalog_sales_to_builders(row, builders);
    } else if (tbl_name == "web_sales") {
        append_web_sales_to_builders(row, builders);
    } else if (tbl_name == "customer") {
        append_customer_to_builders(row, builders);
    } else if (tbl_name == "item") {
        append_item_to_builders(row, builders);
    } else if (tbl_name == "date_dim") {
        append_date_dim_to_builders(row, builders);
    } else if (tbl_name == "store_returns") {
        append_store_returns_to_builders(row, builders);
    } else if (tbl_name == "catalog_returns") {
        append_catalog_returns_to_builders(row, builders);
    } else if (tbl_name == "web_returns") {
        append_web_returns_to_builders(row, builders);
    } else if (tbl_name == "call_center") {
        append_call_center_to_builders(row, builders);
    } else if (tbl_name == "catalog_page") {
        append_catalog_page_to_builders(row, builders);
    } else if (tbl_name == "web_page") {
        append_web_page_to_builders(row, builders);
    } else if (tbl_name == "web_site") {
        append_web_site_to_builders(row, builders);
    } else if (tbl_name == "warehouse") {
        append_warehouse_to_builders(row, builders);
    } else if (tbl_name == "ship_mode") {
        append_ship_mode_to_builders(row, builders);
    } else if (tbl_name == "household_demographics") {
        append_household_demographics_to_builders(row, builders);
    } else if (tbl_name == "customer_demographics") {
        append_customer_demographics_to_builders(row, builders);
    } else if (tbl_name == "customer_address") {
        append_customer_address_to_builders(row, builders);
    } else if (tbl_name == "income_band") {
        append_income_band_to_builders(row, builders);
    } else if (tbl_name == "reason") {
        append_reason_to_builders(row, builders);
    } else if (tbl_name == "time_dim") {
        append_time_dim_to_builders(row, builders);
    } else if (tbl_name == "promotion") {
        append_promotion_to_builders(row, builders);
    } else if (tbl_name == "store") {
        append_store_to_builders(row, builders);
    } else {
        throw std::invalid_argument("append_dsdgen_row_to_builders: unknown table: " + tbl_name);
    }
}

}  // namespace tpcds
