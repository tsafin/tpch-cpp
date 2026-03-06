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
    } else {
        throw std::invalid_argument("append_dsdgen_row_to_builders: unknown table: " + tbl_name);
    }
}

}  // namespace tpcds
