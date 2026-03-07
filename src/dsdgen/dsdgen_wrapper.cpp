/**
 * dsdgen_wrapper.cpp — C++ wrapper around TPC-DS dsdgen
 *
 * Initialises dsdgen global state using the embedded tpcds.idx binary
 * (compiled into dsts_generated.c) and provides per-table generation methods.
 */

#include "tpch/dsdgen_wrapper.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <unistd.h>

// dsdgen C types and functions — single wrapper header
extern "C" {
#include "tpcds_dsdgen.h"
}

// Embedded distribution data (compiled from tpcds.idx by cmake/gen_dsts.py)
extern "C" {
    extern const uint8_t tpcds_idx_data[];
    extern const size_t  tpcds_idx_size;
}

namespace tpcds {

// ---------------------------------------------------------------------------
// Static helpers
// ---------------------------------------------------------------------------

int DSDGenWrapper::table_id(TableType t) {
    return static_cast<int>(t);
}

std::string DSDGenWrapper::table_name(TableType t) {
    switch (t) {
        case TableType::CALL_CENTER:           return "call_center";
        case TableType::CATALOG_PAGE:          return "catalog_page";
        case TableType::CATALOG_RETURNS:       return "catalog_returns";
        case TableType::CATALOG_SALES:         return "catalog_sales";
        case TableType::CUSTOMER:              return "customer";
        case TableType::CUSTOMER_ADDRESS:      return "customer_address";
        case TableType::CUSTOMER_DEMOGRAPHICS: return "customer_demographics";
        case TableType::DATE_DIM:              return "date_dim";
        case TableType::HOUSEHOLD_DEMOGRAPHICS:return "household_demographics";
        case TableType::INCOME_BAND:           return "income_band";
        case TableType::INVENTORY:             return "inventory";
        case TableType::ITEM:                  return "item";
        case TableType::PROMOTION:             return "promotion";
        case TableType::REASON:                return "reason";
        case TableType::SHIP_MODE:             return "ship_mode";
        case TableType::STORE:                 return "store";
        case TableType::STORE_RETURNS:         return "store_returns";
        case TableType::STORE_SALES:           return "store_sales";
        case TableType::TIME_DIM:              return "time_dim";
        case TableType::WAREHOUSE:             return "warehouse";
        case TableType::WEB_PAGE:              return "web_page";
        case TableType::WEB_RETURNS:           return "web_returns";
        case TableType::WEB_SALES:             return "web_sales";
        case TableType::WEB_SITE:              return "web_site";
        default:                               return "unknown";
    }
}

// ---------------------------------------------------------------------------
// Arrow schemas
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> DSDGenWrapper::get_schema(TableType t) {
    switch (t) {
        case TableType::STORE_SALES:
            return arrow::schema({
                arrow::field("ss_sold_date_sk",         arrow::int64()),
                arrow::field("ss_sold_time_sk",         arrow::int64()),
                arrow::field("ss_item_sk",              arrow::int64()),
                arrow::field("ss_customer_sk",          arrow::int64()),
                arrow::field("ss_cdemo_sk",             arrow::int64()),
                arrow::field("ss_hdemo_sk",             arrow::int64()),
                arrow::field("ss_addr_sk",              arrow::int64()),
                arrow::field("ss_store_sk",             arrow::int64()),
                arrow::field("ss_promo_sk",             arrow::int64()),
                arrow::field("ss_ticket_number",        arrow::int64()),
                arrow::field("ss_quantity",             arrow::int32()),
                arrow::field("ss_wholesale_cost",       arrow::float64()),
                arrow::field("ss_list_price",           arrow::float64()),
                arrow::field("ss_sales_price",          arrow::float64()),
                arrow::field("ss_ext_discount_amt",     arrow::float64()),
                arrow::field("ss_ext_sales_price",      arrow::float64()),
                arrow::field("ss_ext_wholesale_cost",   arrow::float64()),
                arrow::field("ss_ext_list_price",       arrow::float64()),
                arrow::field("ss_ext_tax",              arrow::float64()),
                arrow::field("ss_coupon_amt",           arrow::float64()),
                arrow::field("ss_net_paid",             arrow::float64()),
                arrow::field("ss_net_paid_inc_tax",     arrow::float64()),
                arrow::field("ss_net_profit",           arrow::float64()),
            });

        case TableType::INVENTORY:
            return arrow::schema({
                arrow::field("inv_date_sk",             arrow::int64()),
                arrow::field("inv_item_sk",             arrow::int64()),
                arrow::field("inv_warehouse_sk",        arrow::int64()),
                arrow::field("inv_quantity_on_hand",    arrow::int32()),
            });

        case TableType::CATALOG_SALES:
            return arrow::schema({
                arrow::field("cs_sold_date_sk",              arrow::int64()),
                arrow::field("cs_sold_time_sk",              arrow::int64()),
                arrow::field("cs_ship_date_sk",              arrow::int64()),
                arrow::field("cs_bill_customer_sk",          arrow::int64()),
                arrow::field("cs_bill_cdemo_sk",             arrow::int64()),
                arrow::field("cs_bill_hdemo_sk",             arrow::int64()),
                arrow::field("cs_bill_addr_sk",              arrow::int64()),
                arrow::field("cs_ship_customer_sk",          arrow::int64()),
                arrow::field("cs_ship_cdemo_sk",             arrow::int64()),
                arrow::field("cs_ship_hdemo_sk",             arrow::int64()),
                arrow::field("cs_ship_addr_sk",              arrow::int64()),
                arrow::field("cs_call_center_sk",            arrow::int64()),
                arrow::field("cs_catalog_page_sk",           arrow::int64()),
                arrow::field("cs_ship_mode_sk",              arrow::int64()),
                arrow::field("cs_warehouse_sk",              arrow::int64()),
                arrow::field("cs_item_sk",                   arrow::int64()),
                arrow::field("cs_promo_sk",                  arrow::int64()),
                arrow::field("cs_order_number",              arrow::int64()),
                arrow::field("cs_quantity",                  arrow::int32()),
                arrow::field("cs_wholesale_cost",            arrow::float64()),
                arrow::field("cs_list_price",                arrow::float64()),
                arrow::field("cs_sales_price",               arrow::float64()),
                arrow::field("cs_ext_discount_amt",          arrow::float64()),
                arrow::field("cs_ext_sales_price",           arrow::float64()),
                arrow::field("cs_ext_wholesale_cost",        arrow::float64()),
                arrow::field("cs_ext_list_price",            arrow::float64()),
                arrow::field("cs_ext_tax",                   arrow::float64()),
                arrow::field("cs_coupon_amt",                arrow::float64()),
                arrow::field("cs_ext_ship_cost",             arrow::float64()),
                arrow::field("cs_net_paid",                  arrow::float64()),
                arrow::field("cs_net_paid_inc_tax",          arrow::float64()),
                arrow::field("cs_net_paid_inc_ship",         arrow::float64()),
                arrow::field("cs_net_paid_inc_ship_tax",     arrow::float64()),
                arrow::field("cs_net_profit",                arrow::float64()),
            });

        case TableType::WEB_SALES:
            return arrow::schema({
                arrow::field("ws_sold_date_sk",              arrow::int64()),
                arrow::field("ws_sold_time_sk",              arrow::int64()),
                arrow::field("ws_ship_date_sk",              arrow::int64()),
                arrow::field("ws_item_sk",                   arrow::int64()),
                arrow::field("ws_bill_customer_sk",          arrow::int64()),
                arrow::field("ws_bill_cdemo_sk",             arrow::int64()),
                arrow::field("ws_bill_hdemo_sk",             arrow::int64()),
                arrow::field("ws_bill_addr_sk",              arrow::int64()),
                arrow::field("ws_ship_customer_sk",          arrow::int64()),
                arrow::field("ws_ship_cdemo_sk",             arrow::int64()),
                arrow::field("ws_ship_hdemo_sk",             arrow::int64()),
                arrow::field("ws_ship_addr_sk",              arrow::int64()),
                arrow::field("ws_web_page_sk",               arrow::int64()),
                arrow::field("ws_web_site_sk",               arrow::int64()),
                arrow::field("ws_ship_mode_sk",              arrow::int64()),
                arrow::field("ws_warehouse_sk",              arrow::int64()),
                arrow::field("ws_promo_sk",                  arrow::int64()),
                arrow::field("ws_order_number",              arrow::int64()),
                arrow::field("ws_quantity",                  arrow::int32()),
                arrow::field("ws_wholesale_cost",            arrow::float64()),
                arrow::field("ws_list_price",                arrow::float64()),
                arrow::field("ws_sales_price",               arrow::float64()),
                arrow::field("ws_ext_discount_amt",          arrow::float64()),
                arrow::field("ws_ext_sales_price",           arrow::float64()),
                arrow::field("ws_ext_wholesale_cost",        arrow::float64()),
                arrow::field("ws_ext_list_price",            arrow::float64()),
                arrow::field("ws_ext_tax",                   arrow::float64()),
                arrow::field("ws_coupon_amt",                arrow::float64()),
                arrow::field("ws_ext_ship_cost",             arrow::float64()),
                arrow::field("ws_net_paid",                  arrow::float64()),
                arrow::field("ws_net_paid_inc_tax",          arrow::float64()),
                arrow::field("ws_net_paid_inc_ship",         arrow::float64()),
                arrow::field("ws_net_paid_inc_ship_tax",     arrow::float64()),
                arrow::field("ws_net_profit",                arrow::float64()),
            });

        case TableType::CUSTOMER:
            return arrow::schema({
                arrow::field("c_customer_sk",            arrow::int64()),
                arrow::field("c_customer_id",            arrow::utf8()),
                arrow::field("c_current_cdemo_sk",       arrow::int64()),
                arrow::field("c_current_hdemo_sk",       arrow::int64()),
                arrow::field("c_current_addr_sk",        arrow::int64()),
                arrow::field("c_first_shipto_date_id",   arrow::int32()),
                arrow::field("c_first_sales_date_id",    arrow::int32()),
                arrow::field("c_salutation",             arrow::utf8()),
                arrow::field("c_first_name",             arrow::utf8()),
                arrow::field("c_last_name",              arrow::utf8()),
                arrow::field("c_preferred_cust_flag",    arrow::int32()),
                arrow::field("c_birth_day",              arrow::int32()),
                arrow::field("c_birth_month",            arrow::int32()),
                arrow::field("c_birth_year",             arrow::int32()),
                arrow::field("c_birth_country",          arrow::utf8()),
                arrow::field("c_login",                  arrow::utf8()),
                arrow::field("c_email_address",          arrow::utf8()),
                arrow::field("c_last_review_date",       arrow::int32()),
            });

        case TableType::ITEM:
            return arrow::schema({
                arrow::field("i_item_sk",           arrow::int64()),
                arrow::field("i_item_id",           arrow::utf8()),
                arrow::field("i_rec_start_date_id", arrow::int64()),
                arrow::field("i_rec_end_date_id",   arrow::int64()),
                arrow::field("i_item_desc",         arrow::utf8()),
                arrow::field("i_current_price",     arrow::float64()),
                arrow::field("i_wholesale_cost",    arrow::float64()),
                arrow::field("i_brand_id",          arrow::int64()),
                arrow::field("i_brand",             arrow::utf8()),
                arrow::field("i_class_id",          arrow::int64()),
                arrow::field("i_class",             arrow::utf8()),
                arrow::field("i_category_id",       arrow::int64()),
                arrow::field("i_category",          arrow::utf8()),
                arrow::field("i_manufact_id",       arrow::int64()),
                arrow::field("i_manufact",          arrow::utf8()),
                arrow::field("i_size",              arrow::utf8()),
                arrow::field("i_formulation",       arrow::utf8()),
                arrow::field("i_color",             arrow::utf8()),
                arrow::field("i_units",             arrow::utf8()),
                arrow::field("i_container",         arrow::utf8()),
                arrow::field("i_manager_id",        arrow::int64()),
                arrow::field("i_product_name",      arrow::utf8()),
                arrow::field("i_promo_sk",          arrow::int64()),
            });

        case TableType::DATE_DIM:
            return arrow::schema({
                arrow::field("d_date_sk",           arrow::int64()),
                arrow::field("d_date_id",           arrow::utf8()),
                arrow::field("d_month_seq",         arrow::int32()),
                arrow::field("d_week_seq",          arrow::int32()),
                arrow::field("d_quarter_seq",       arrow::int32()),
                arrow::field("d_year",              arrow::int32()),
                arrow::field("d_dow",               arrow::int32()),
                arrow::field("d_moy",               arrow::int32()),
                arrow::field("d_dom",               arrow::int32()),
                arrow::field("d_qoy",               arrow::int32()),
                arrow::field("d_fy_year",           arrow::int32()),
                arrow::field("d_fy_quarter_seq",    arrow::int32()),
                arrow::field("d_fy_week_seq",       arrow::int32()),
                arrow::field("d_day_name",          arrow::utf8()),
                arrow::field("d_holiday",           arrow::int32()),
                arrow::field("d_weekend",           arrow::int32()),
                arrow::field("d_following_holiday", arrow::int32()),
                arrow::field("d_first_dom",         arrow::int32()),
                arrow::field("d_last_dom",          arrow::int32()),
                arrow::field("d_same_day_ly",       arrow::int32()),
                arrow::field("d_same_day_lq",       arrow::int32()),
                arrow::field("d_current_day",       arrow::int32()),
                arrow::field("d_current_week",      arrow::int32()),
                arrow::field("d_current_month",     arrow::int32()),
                arrow::field("d_current_quarter",   arrow::int32()),
                arrow::field("d_current_year",      arrow::int32()),
            });

        case TableType::STORE_RETURNS:
            return arrow::schema({
                arrow::field("sr_returned_date_sk",  arrow::int64()),
                arrow::field("sr_returned_time_sk",  arrow::int64()),
                arrow::field("sr_item_sk",           arrow::int64()),
                arrow::field("sr_customer_sk",       arrow::int64()),
                arrow::field("sr_cdemo_sk",          arrow::int64()),
                arrow::field("sr_hdemo_sk",          arrow::int64()),
                arrow::field("sr_addr_sk",           arrow::int64()),
                arrow::field("sr_store_sk",          arrow::int64()),
                arrow::field("sr_reason_sk",         arrow::int64()),
                arrow::field("sr_ticket_number",     arrow::int64()),
                arrow::field("sr_quantity",          arrow::int32()),
                arrow::field("sr_net_paid",          arrow::float64()),
                arrow::field("sr_ext_tax",           arrow::float64()),
                arrow::field("sr_net_paid_inc_tax",  arrow::float64()),
                arrow::field("sr_fee",               arrow::float64()),
                arrow::field("sr_ext_ship_cost",     arrow::float64()),
                arrow::field("sr_refunded_cash",     arrow::float64()),
                arrow::field("sr_reversed_charge",   arrow::float64()),
                arrow::field("sr_store_credit",      arrow::float64()),
                arrow::field("sr_net_loss",          arrow::float64()),
            });

        case TableType::CATALOG_RETURNS:
            return arrow::schema({
                arrow::field("cr_returned_date_sk",      arrow::int64()),
                arrow::field("cr_returned_time_sk",      arrow::int64()),
                arrow::field("cr_item_sk",               arrow::int64()),
                arrow::field("cr_refunded_customer_sk",  arrow::int64()),
                arrow::field("cr_refunded_cdemo_sk",     arrow::int64()),
                arrow::field("cr_refunded_hdemo_sk",     arrow::int64()),
                arrow::field("cr_refunded_addr_sk",      arrow::int64()),
                arrow::field("cr_returning_customer_sk", arrow::int64()),
                arrow::field("cr_returning_cdemo_sk",    arrow::int64()),
                arrow::field("cr_returning_hdemo_sk",    arrow::int64()),
                arrow::field("cr_returning_addr_sk",     arrow::int64()),
                arrow::field("cr_call_center_sk",        arrow::int64()),
                arrow::field("cr_catalog_page_sk",       arrow::int64()),
                arrow::field("cr_ship_mode_sk",          arrow::int64()),
                arrow::field("cr_warehouse_sk",          arrow::int64()),
                arrow::field("cr_reason_sk",             arrow::int64()),
                arrow::field("cr_order_number",          arrow::int64()),
                arrow::field("cr_quantity",              arrow::int32()),
                arrow::field("cr_net_paid",              arrow::float64()),
                arrow::field("cr_ext_tax",               arrow::float64()),
                arrow::field("cr_net_paid_inc_tax",      arrow::float64()),
                arrow::field("cr_fee",                   arrow::float64()),
                arrow::field("cr_ext_ship_cost",         arrow::float64()),
                arrow::field("cr_refunded_cash",         arrow::float64()),
                arrow::field("cr_reversed_charge",       arrow::float64()),
                arrow::field("cr_store_credit",          arrow::float64()),
                arrow::field("cr_net_loss",              arrow::float64()),
            });

        case TableType::WEB_RETURNS:
            return arrow::schema({
                arrow::field("wr_returned_date_sk",      arrow::int64()),
                arrow::field("wr_returned_time_sk",      arrow::int64()),
                arrow::field("wr_item_sk",               arrow::int64()),
                arrow::field("wr_refunded_customer_sk",  arrow::int64()),
                arrow::field("wr_refunded_cdemo_sk",     arrow::int64()),
                arrow::field("wr_refunded_hdemo_sk",     arrow::int64()),
                arrow::field("wr_refunded_addr_sk",      arrow::int64()),
                arrow::field("wr_returning_customer_sk", arrow::int64()),
                arrow::field("wr_returning_cdemo_sk",    arrow::int64()),
                arrow::field("wr_returning_hdemo_sk",    arrow::int64()),
                arrow::field("wr_returning_addr_sk",     arrow::int64()),
                arrow::field("wr_web_page_sk",           arrow::int64()),
                arrow::field("wr_reason_sk",             arrow::int64()),
                arrow::field("wr_order_number",          arrow::int64()),
                arrow::field("wr_quantity",              arrow::int32()),
                arrow::field("wr_net_paid",              arrow::float64()),
                arrow::field("wr_ext_tax",               arrow::float64()),
                arrow::field("wr_net_paid_inc_tax",      arrow::float64()),
                arrow::field("wr_fee",                   arrow::float64()),
                arrow::field("wr_ext_ship_cost",         arrow::float64()),
                arrow::field("wr_refunded_cash",         arrow::float64()),
                arrow::field("wr_reversed_charge",       arrow::float64()),
                arrow::field("wr_store_credit",          arrow::float64()),
                arrow::field("wr_net_loss",              arrow::float64()),
            });

        case TableType::CALL_CENTER:
            return arrow::schema({
                arrow::field("cc_call_center_sk",  arrow::int64()),
                arrow::field("cc_call_center_id",  arrow::utf8()),
                arrow::field("cc_rec_start_date_sk", arrow::int64()),
                arrow::field("cc_rec_end_date_sk", arrow::int64()),
                arrow::field("cc_closed_date_sk",  arrow::int64()),
                arrow::field("cc_open_date_sk",    arrow::int64()),
                arrow::field("cc_name",            arrow::utf8()),
                arrow::field("cc_class",           arrow::utf8()),
                arrow::field("cc_employees",       arrow::int32()),
                arrow::field("cc_sq_ft",           arrow::int32()),
                arrow::field("cc_hours",           arrow::utf8()),
                arrow::field("cc_manager",         arrow::utf8()),
                arrow::field("cc_mkt_id",          arrow::int32()),
                arrow::field("cc_mkt_class",       arrow::utf8()),
                arrow::field("cc_mkt_desc",        arrow::utf8()),
                arrow::field("cc_market_manager",  arrow::utf8()),
                arrow::field("cc_division",        arrow::int32()),
                arrow::field("cc_division_name",   arrow::utf8()),
                arrow::field("cc_company",         arrow::int32()),
                arrow::field("cc_company_name",    arrow::utf8()),
                arrow::field("cc_street_number",   arrow::int32()),
                arrow::field("cc_street_name",     arrow::utf8()),
                arrow::field("cc_street_type",     arrow::utf8()),
                arrow::field("cc_suite_number",    arrow::utf8()),
                arrow::field("cc_city",            arrow::utf8()),
                arrow::field("cc_county",          arrow::utf8()),
                arrow::field("cc_state",           arrow::utf8()),
                arrow::field("cc_zip",             arrow::utf8()),
                arrow::field("cc_country",         arrow::utf8()),
                arrow::field("cc_gmt_offset",      arrow::float64()),
                arrow::field("cc_tax_percentage",  arrow::float64()),
            });

        case TableType::CATALOG_PAGE:
            return arrow::schema({
                arrow::field("cp_catalog_page_sk",     arrow::int64()),
                arrow::field("cp_catalog_page_id",     arrow::utf8()),
                arrow::field("cp_start_date_sk",       arrow::int64()),
                arrow::field("cp_end_date_sk",         arrow::int64()),
                arrow::field("cp_department",          arrow::utf8()),
                arrow::field("cp_catalog_number",      arrow::int32()),
                arrow::field("cp_catalog_page_number", arrow::int32()),
                arrow::field("cp_description",         arrow::utf8()),
                arrow::field("cp_type",                arrow::utf8()),
            });

        case TableType::WEB_PAGE:
            return arrow::schema({
                arrow::field("wp_web_page_sk",       arrow::int64()),
                arrow::field("wp_web_page_id",       arrow::utf8()),
                arrow::field("wp_rec_start_date_sk", arrow::int64()),
                arrow::field("wp_rec_end_date_sk",   arrow::int64()),
                arrow::field("wp_creation_date_sk",  arrow::int64()),
                arrow::field("wp_access_date_sk",    arrow::int64()),
                arrow::field("wp_autogen_flag",      arrow::int32()),
                arrow::field("wp_customer_sk",       arrow::int64()),
                arrow::field("wp_url",               arrow::utf8()),
                arrow::field("wp_type",              arrow::utf8()),
                arrow::field("wp_char_count",        arrow::int32()),
                arrow::field("wp_link_count",        arrow::int32()),
                arrow::field("wp_image_count",       arrow::int32()),
                arrow::field("wp_max_ad_count",      arrow::int32()),
            });

        case TableType::WEB_SITE:
            return arrow::schema({
                arrow::field("web_site_sk",          arrow::int64()),
                arrow::field("web_site_id",          arrow::utf8()),
                arrow::field("web_rec_start_date_sk", arrow::int64()),
                arrow::field("web_rec_end_date_sk",  arrow::int64()),
                arrow::field("web_name",             arrow::utf8()),
                arrow::field("web_open_date_sk",     arrow::int64()),
                arrow::field("web_close_date_sk",    arrow::int64()),
                arrow::field("web_class",            arrow::utf8()),
                arrow::field("web_manager",          arrow::utf8()),
                arrow::field("web_mkt_id",           arrow::int32()),
                arrow::field("web_mkt_class",        arrow::utf8()),
                arrow::field("web_mkt_desc",         arrow::utf8()),
                arrow::field("web_market_manager",   arrow::utf8()),
                arrow::field("web_company_id",       arrow::int32()),
                arrow::field("web_company_name",     arrow::utf8()),
                arrow::field("web_street_number",    arrow::int32()),
                arrow::field("web_street_name",      arrow::utf8()),
                arrow::field("web_street_type",      arrow::utf8()),
                arrow::field("web_suite_number",     arrow::utf8()),
                arrow::field("web_city",             arrow::utf8()),
                arrow::field("web_county",           arrow::utf8()),
                arrow::field("web_state",            arrow::utf8()),
                arrow::field("web_zip",              arrow::utf8()),
                arrow::field("web_country",          arrow::utf8()),
                arrow::field("web_gmt_offset",       arrow::float64()),
                arrow::field("web_tax_percentage",   arrow::float64()),
            });

        case TableType::WAREHOUSE:
            return arrow::schema({
                arrow::field("w_warehouse_sk",    arrow::int64()),
                arrow::field("w_warehouse_id",    arrow::utf8()),
                arrow::field("w_warehouse_name",  arrow::utf8()),
                arrow::field("w_warehouse_sq_ft", arrow::int32()),
                arrow::field("w_street_number",   arrow::int32()),
                arrow::field("w_street_name",     arrow::utf8()),
                arrow::field("w_street_type",     arrow::utf8()),
                arrow::field("w_suite_number",    arrow::utf8()),
                arrow::field("w_city",            arrow::utf8()),
                arrow::field("w_county",          arrow::utf8()),
                arrow::field("w_state",           arrow::utf8()),
                arrow::field("w_zip",             arrow::utf8()),
                arrow::field("w_country",         arrow::utf8()),
                arrow::field("w_gmt_offset",      arrow::float64()),
            });

        case TableType::SHIP_MODE:
            return arrow::schema({
                arrow::field("sm_ship_mode_sk", arrow::int64()),
                arrow::field("sm_ship_mode_id", arrow::utf8()),
                arrow::field("sm_type",         arrow::utf8()),
                arrow::field("sm_code",         arrow::utf8()),
                arrow::field("sm_carrier",      arrow::utf8()),
                arrow::field("sm_contract",     arrow::utf8()),
            });

        case TableType::HOUSEHOLD_DEMOGRAPHICS:
            return arrow::schema({
                arrow::field("hd_demo_sk",        arrow::int64()),
                arrow::field("hd_income_band_sk", arrow::int64()),
                arrow::field("hd_buy_potential",  arrow::utf8()),
                arrow::field("hd_dep_count",      arrow::int32()),
                arrow::field("hd_vehicle_count",  arrow::int32()),
            });

        case TableType::CUSTOMER_DEMOGRAPHICS:
            return arrow::schema({
                arrow::field("cd_demo_sk",             arrow::int64()),
                arrow::field("cd_gender",              arrow::utf8()),
                arrow::field("cd_marital_status",      arrow::utf8()),
                arrow::field("cd_education_status",    arrow::utf8()),
                arrow::field("cd_purchase_estimate",   arrow::int32()),
                arrow::field("cd_credit_rating",       arrow::utf8()),
                arrow::field("cd_dep_count",           arrow::int32()),
                arrow::field("cd_dep_employed_count",  arrow::int32()),
                arrow::field("cd_dep_college_count",   arrow::int32()),
            });

        case TableType::CUSTOMER_ADDRESS:
            return arrow::schema({
                arrow::field("ca_address_sk",    arrow::int64()),
                arrow::field("ca_address_id",    arrow::utf8()),
                arrow::field("ca_street_number", arrow::int32()),
                arrow::field("ca_street_name",   arrow::utf8()),
                arrow::field("ca_street_type",   arrow::utf8()),
                arrow::field("ca_suite_number",  arrow::utf8()),
                arrow::field("ca_city",          arrow::utf8()),
                arrow::field("ca_county",        arrow::utf8()),
                arrow::field("ca_state",         arrow::utf8()),
                arrow::field("ca_zip",           arrow::utf8()),
                arrow::field("ca_country",       arrow::utf8()),
                arrow::field("ca_gmt_offset",    arrow::float64()),
                arrow::field("ca_location_type", arrow::utf8()),
            });

        case TableType::INCOME_BAND:
            return arrow::schema({
                arrow::field("ib_income_band_id", arrow::int32()),
                arrow::field("ib_lower_bound",    arrow::int32()),
                arrow::field("ib_upper_bound",    arrow::int32()),
            });

        case TableType::REASON:
            return arrow::schema({
                arrow::field("r_reason_sk",   arrow::int64()),
                arrow::field("r_reason_id",   arrow::utf8()),
                arrow::field("r_reason_desc", arrow::utf8()),
            });

        case TableType::TIME_DIM:
            return arrow::schema({
                arrow::field("t_time_sk",   arrow::int64()),
                arrow::field("t_time_id",   arrow::utf8()),
                arrow::field("t_time",      arrow::int32()),
                arrow::field("t_hour",      arrow::int32()),
                arrow::field("t_minute",    arrow::int32()),
                arrow::field("t_second",    arrow::int32()),
                arrow::field("t_am_pm",     arrow::utf8()),
                arrow::field("t_shift",     arrow::utf8()),
                arrow::field("t_sub_shift", arrow::utf8()),
                arrow::field("t_meal_time", arrow::utf8()),
            });

        case TableType::PROMOTION:
            return arrow::schema({
                arrow::field("p_promo_sk",        arrow::int64()),
                arrow::field("p_promo_id",         arrow::utf8()),
                arrow::field("p_start_date_sk",    arrow::int64()),
                arrow::field("p_end_date_sk",      arrow::int64()),
                arrow::field("p_item_sk",          arrow::int64()),
                arrow::field("p_cost",             arrow::float64()),
                arrow::field("p_response_target",  arrow::int32()),
                arrow::field("p_promo_name",       arrow::utf8()),
                arrow::field("p_channel_dmail",    arrow::int32()),
                arrow::field("p_channel_email",    arrow::int32()),
                arrow::field("p_channel_catalog",  arrow::int32()),
                arrow::field("p_channel_tv",       arrow::int32()),
                arrow::field("p_channel_radio",    arrow::int32()),
                arrow::field("p_channel_press",    arrow::int32()),
                arrow::field("p_channel_event",    arrow::int32()),
                arrow::field("p_channel_demo",     arrow::int32()),
                arrow::field("p_channel_details",  arrow::utf8()),
                arrow::field("p_purpose",          arrow::utf8()),
                arrow::field("p_discount_active",  arrow::int32()),
            });

        case TableType::STORE:
            return arrow::schema({
                arrow::field("s_store_sk",       arrow::int64()),
                arrow::field("s_store_id",       arrow::utf8()),
                arrow::field("s_rec_start_date", arrow::int64()),
                arrow::field("s_rec_end_date",   arrow::int64()),
                arrow::field("s_closed_date_sk", arrow::int64()),
                arrow::field("s_store_name",     arrow::utf8()),
                arrow::field("s_number_employees", arrow::int32()),
                arrow::field("s_floor_space",    arrow::int32()),
                arrow::field("s_hours",          arrow::utf8()),
                arrow::field("s_manager",        arrow::utf8()),
                arrow::field("s_market_id",      arrow::int32()),
                arrow::field("s_geography_class", arrow::utf8()),
                arrow::field("s_market_desc",    arrow::utf8()),
                arrow::field("s_market_manager", arrow::utf8()),
                arrow::field("s_division_id",    arrow::int64()),
                arrow::field("s_division_name",  arrow::utf8()),
                arrow::field("s_company_id",     arrow::int64()),
                arrow::field("s_company_name",   arrow::utf8()),
                arrow::field("s_street_number",  arrow::int32()),
                arrow::field("s_street_name",    arrow::utf8()),
                arrow::field("s_street_type",    arrow::utf8()),
                arrow::field("s_suite_number",   arrow::utf8()),
                arrow::field("s_city",           arrow::utf8()),
                arrow::field("s_county",         arrow::utf8()),
                arrow::field("s_state",          arrow::utf8()),
                arrow::field("s_zip",            arrow::utf8()),
                arrow::field("s_country",        arrow::utf8()),
                arrow::field("s_gmt_offset",     arrow::float64()),
                arrow::field("s_tax_percentage", arrow::float64()),
            });

        default:
            throw std::invalid_argument(
                "DSDGenWrapper::get_schema: schema not yet implemented for table " +
                table_name(t));
    }
}

// ---------------------------------------------------------------------------
// Constructor / destructor
// ---------------------------------------------------------------------------

DSDGenWrapper::DSDGenWrapper(long scale_factor, bool verbose)
    : scale_factor_(scale_factor), verbose_(verbose), initialized_(false) {
    if (scale_factor <= 0) {
        throw std::invalid_argument("scale_factor must be positive");
    }
}

DSDGenWrapper::~DSDGenWrapper() {
    if (!tmp_dist_path_.empty()) {
        ::unlink(tmp_dist_path_.c_str());
    }
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

void DSDGenWrapper::init_dsdgen() {
    if (initialized_) return;

    // 1. Write embedded tpcds.idx to a temp file (dsdgen opens it by path).
    char tmp_tmpl[] = "/tmp/tpcds_idx_XXXXXX";
    int fd = ::mkstemp(tmp_tmpl);
    if (fd < 0) {
        throw std::runtime_error("DSDGenWrapper: mkstemp failed for tpcds.idx");
    }
    const uint8_t* data = tpcds_idx_data;
    size_t remaining    = tpcds_idx_size;
    while (remaining > 0) {
        ssize_t written = ::write(fd, data, remaining);
        if (written <= 0) {
            ::close(fd);
            ::unlink(tmp_tmpl);
            throw std::runtime_error("DSDGenWrapper: write to tmp tpcds.idx failed");
        }
        data      += written;
        remaining -= static_cast<size_t>(written);
    }
    ::close(fd);
    tmp_dist_path_ = tmp_tmpl;

    // 2. Initialise dsdgen parameter table and override relevant params.
    init_params();

    // 3. Point DISTRIBUTIONS at the temp file we just wrote.
    set_str(const_cast<char*>("DISTRIBUTIONS"),
            const_cast<char*>(tmp_dist_path_.c_str()));

    // 4. Set scale factor.
    char scale_buf[32];
    std::snprintf(scale_buf, sizeof(scale_buf), "%ld", scale_factor_);
    set_int(const_cast<char*>("SCALE"), scale_buf);

    // 5. Seed the RNG (must happen after init_params so streams are set up).
    init_rand();

    initialized_ = true;

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: initialized (SF=%ld, dist=%s)\n",
            scale_factor_, tmp_dist_path_.c_str());
    }
}

// ---------------------------------------------------------------------------
// get_row_count
// ---------------------------------------------------------------------------

long DSDGenWrapper::get_row_count(TableType t) const {
    // get_rowcount() reads the global scale factor set in init_dsdgen().
    // const_cast is safe: we only call this after initialization.
    const_cast<DSDGenWrapper*>(this)->init_dsdgen();
    return static_cast<long>(get_rowcount(table_id(t)));
}

// ---------------------------------------------------------------------------
// generate_store_sales
// ---------------------------------------------------------------------------
//
// store_sales is a master-detail table: each call to mk_w_store_sales(NULL, i)
// generates one "ticket" (master) with 8-16 line items (details). Each detail
// row is emitted via the callback g_w_store_sales_callback, which is the only
// way to capture the fully-populated rows (including pricing fields that live
// in the global g_w_store_sales, not in the caller-supplied struct).
//
// get_rowcount(STORE_SALES) returns the number of TICKETS (master rows).
// The total number of line-item rows emitted will be higher (8-16×).
// ---------------------------------------------------------------------------

// C-linkage trampolines for master-detail tables
namespace {
struct StoreSalesCtx {
    std::function<void(const void*)>* cb;
    long max_rows;
    long emitted;
};

extern "C" void store_sales_trampoline(
    const struct W_STORE_SALES_TBL* row, void* ctx)
{
    auto* c = static_cast<StoreSalesCtx*>(ctx);
    if (c->max_rows > 0 && c->emitted >= c->max_rows) return;
    (*c->cb)(static_cast<const void*>(row));
    ++c->emitted;
}

struct CatalogSalesCtx {
    std::function<void(const void*)>* cb;
    long max_rows;
    long emitted;
};

extern "C" void catalog_sales_trampoline(
    const struct W_CATALOG_SALES_TBL* row, void* ctx)
{
    auto* c = static_cast<CatalogSalesCtx*>(ctx);
    if (c->max_rows > 0 && c->emitted >= c->max_rows) return;
    (*c->cb)(static_cast<const void*>(row));
    ++c->emitted;
}

struct WebSalesCtx {
    std::function<void(const void*)>* cb;
    long max_rows;
    long emitted;
};

extern "C" void web_sales_trampoline(
    const struct W_WEB_SALES_TBL* row, void* ctx)
{
    auto* c = static_cast<WebSalesCtx*>(ctx);
    if (c->max_rows > 0 && c->emitted >= c->max_rows) return;
    (*c->cb)(static_cast<const void*>(row));
    ++c->emitted;
}
} // anonymous namespace

void DSDGenWrapper::generate_store_sales(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    ds_key_t n_tickets = get_rowcount(TPCDS_STORE_SALES);

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating store_sales from %lld tickets\n",
            static_cast<long long>(n_tickets));
    }

    StoreSalesCtx ctx{&callback, max_rows, 0L};
    g_w_store_sales_callback     = store_sales_trampoline;
    g_w_store_sales_callback_ctx = &ctx;

    for (ds_key_t i = 1; i <= n_tickets; ++i) {
        if (max_rows > 0 && ctx.emitted >= max_rows) break;
        mk_w_store_sales(nullptr, i);
    }

    // Always clear the callback to avoid dangling pointer
    g_w_store_sales_callback     = nullptr;
    g_w_store_sales_callback_ctx = nullptr;

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: emitted %ld store_sales rows\n", ctx.emitted);
    }
}

// ---------------------------------------------------------------------------
// generate_inventory
// ---------------------------------------------------------------------------

void DSDGenWrapper::generate_inventory(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    ds_key_t total = get_rowcount(TPCDS_INVENTORY);
    if (max_rows > 0 && static_cast<ds_key_t>(max_rows) < total) {
        total = static_cast<ds_key_t>(max_rows);
    }

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating %lld inventory rows\n",
            static_cast<long long>(total));
    }

    W_INVENTORY_TBL row;
    for (ds_key_t i = 1; i <= total; ++i) {
        mk_w_inventory(&row, i);
        callback(&row);
    }
}

// ---------------------------------------------------------------------------
// generate_catalog_sales
// ---------------------------------------------------------------------------

void DSDGenWrapper::generate_catalog_sales(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    ds_key_t n_tickets = get_rowcount(TPCDS_CATALOG_SALES);

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating catalog_sales from %lld tickets\n",
            static_cast<long long>(n_tickets));
    }

    CatalogSalesCtx ctx{&callback, max_rows, 0L};
    g_w_catalog_sales_callback     = catalog_sales_trampoline;
    g_w_catalog_sales_callback_ctx = &ctx;

    for (ds_key_t i = 1; i <= n_tickets; ++i) {
        if (max_rows > 0 && ctx.emitted >= max_rows) break;
        mk_w_catalog_sales(nullptr, i);
    }

    g_w_catalog_sales_callback     = nullptr;
    g_w_catalog_sales_callback_ctx = nullptr;

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: emitted %ld catalog_sales rows\n", ctx.emitted);
    }
}

// ---------------------------------------------------------------------------
// generate_web_sales
// ---------------------------------------------------------------------------

void DSDGenWrapper::generate_web_sales(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    ds_key_t n_tickets = get_rowcount(TPCDS_WEB_SALES);

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating web_sales from %lld tickets\n",
            static_cast<long long>(n_tickets));
    }

    WebSalesCtx ctx{&callback, max_rows, 0L};
    g_w_web_sales_callback     = web_sales_trampoline;
    g_w_web_sales_callback_ctx = &ctx;

    for (ds_key_t i = 1; i <= n_tickets; ++i) {
        if (max_rows > 0 && ctx.emitted >= max_rows) break;
        mk_w_web_sales(nullptr, i);
    }

    g_w_web_sales_callback     = nullptr;
    g_w_web_sales_callback_ctx = nullptr;

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: emitted %ld web_sales rows\n", ctx.emitted);
    }
}

// ---------------------------------------------------------------------------
// generate_customer
// ---------------------------------------------------------------------------

void DSDGenWrapper::generate_customer(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    ds_key_t total = get_rowcount(TPCDS_CUSTOMER);
    if (max_rows > 0 && static_cast<ds_key_t>(max_rows) < total) {
        total = static_cast<ds_key_t>(max_rows);
    }

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating %lld customer rows\n",
            static_cast<long long>(total));
    }

    W_CUSTOMER_TBL row;
    for (ds_key_t i = 1; i <= total; ++i) {
        mk_w_customer(&row, i);
        callback(&row);
    }
}

// ---------------------------------------------------------------------------
// generate_item
// ---------------------------------------------------------------------------

void DSDGenWrapper::generate_item(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    ds_key_t total = get_rowcount(TPCDS_ITEM);
    if (max_rows > 0 && static_cast<ds_key_t>(max_rows) < total) {
        total = static_cast<ds_key_t>(max_rows);
    }

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating %lld item rows\n",
            static_cast<long long>(total));
    }

    W_ITEM_TBL row;
    for (ds_key_t i = 1; i <= total; ++i) {
        mk_w_item(&row, i);
        callback(&row);
    }
}

// ---------------------------------------------------------------------------
// generate_date_dim
// ---------------------------------------------------------------------------

void DSDGenWrapper::generate_date_dim(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    ds_key_t total = get_rowcount(TPCDS_DATE);
    if (max_rows > 0 && static_cast<ds_key_t>(max_rows) < total) {
        total = static_cast<ds_key_t>(max_rows);
    }

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating %lld date_dim rows\n",
            static_cast<long long>(total));
    }

    W_DATE_TBL row;
    for (ds_key_t i = 1; i <= total; ++i) {
        mk_w_date(&row, i);
        callback(&row);
    }
}

// ---------------------------------------------------------------------------
// generate_store_returns
// ---------------------------------------------------------------------------
//
// store_returns is generated as a side effect of store_sales: each sales row
// has a SR_RETURN_PCT (10%) chance of producing a return.  The returns table
// has no standalone row count (get_rowcount returns -1).
//
// We drive generation through the store_sales ticket loop: for each ticket
// index we call mk_w_store_sales to populate g_w_store_sales, then call
// mk_w_store_returns to produce the corresponding return row.  This gives
// correct referential integrity (the return references the just-generated
// sale).  The 10% probability is NOT applied here — every sale generates a
// return row — which is intentional for benchmarking (avoids random skipping).
// ---------------------------------------------------------------------------

void DSDGenWrapper::generate_store_returns(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    // Use store_sales ticket count as the driver (returns have no own rowcount).
    ds_key_t n_tickets = get_rowcount(TPCDS_STORE_SALES);
    if (max_rows > 0 && static_cast<ds_key_t>(max_rows) < n_tickets) {
        n_tickets = static_cast<ds_key_t>(max_rows);
    }

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating store_returns from %lld sales tickets\n",
            static_cast<long long>(n_tickets));
    }

    // Use a no-op callback to suppress sales output while still populating g_w_store_sales.
    g_w_store_sales_callback     = [](const struct W_STORE_SALES_TBL*, void*) {};
    g_w_store_sales_callback_ctx = nullptr;

    W_STORE_RETURNS_TBL row;
    long emitted = 0;
    for (ds_key_t i = 1; i <= n_tickets; ++i) {
        // Populate g_w_store_sales so mk_w_store_returns has valid sale context.
        // The no-op callback suppresses stdout printing.
        mk_w_store_sales(nullptr, i);
        mk_w_store_returns(&row, i);
        callback(&row);
        ++emitted;
        if (max_rows > 0 && emitted >= max_rows) break;
    }

    g_w_store_sales_callback     = nullptr;
    g_w_store_sales_callback_ctx = nullptr;

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: emitted %ld store_returns rows\n", emitted);
    }
}

// ---------------------------------------------------------------------------
// generate_catalog_returns
// ---------------------------------------------------------------------------
//
// Same approach as generate_store_returns but driven by catalog_sales tickets.
// ---------------------------------------------------------------------------

void DSDGenWrapper::generate_catalog_returns(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    ds_key_t n_tickets = get_rowcount(TPCDS_CATALOG_SALES);
    if (max_rows > 0 && static_cast<ds_key_t>(max_rows) < n_tickets) {
        n_tickets = static_cast<ds_key_t>(max_rows);
    }

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating catalog_returns from %lld sales tickets\n",
            static_cast<long long>(n_tickets));
    }

    // Use a no-op callback to suppress sales output while still populating g_w_catalog_sales.
    g_w_catalog_sales_callback     = [](const struct W_CATALOG_SALES_TBL*, void*) {};
    g_w_catalog_sales_callback_ctx = nullptr;

    W_CATALOG_RETURNS_TBL row;
    long emitted = 0;
    for (ds_key_t i = 1; i <= n_tickets; ++i) {
        // Populate g_w_catalog_sales so mk_w_catalog_returns has valid sale context.
        mk_w_catalog_sales(nullptr, i);
        mk_w_catalog_returns(&row, i);
        callback(&row);
        ++emitted;
        if (max_rows > 0 && emitted >= max_rows) break;
    }

    g_w_catalog_sales_callback     = nullptr;
    g_w_catalog_sales_callback_ctx = nullptr;

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: emitted %ld catalog_returns rows\n", emitted);
    }
}

// ---------------------------------------------------------------------------
// generate_web_returns
// ---------------------------------------------------------------------------
//
// Same approach as generate_store_returns but driven by web_sales tickets.
// ---------------------------------------------------------------------------

void DSDGenWrapper::generate_web_returns(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    ds_key_t n_tickets = get_rowcount(TPCDS_WEB_SALES);
    if (max_rows > 0 && static_cast<ds_key_t>(max_rows) < n_tickets) {
        n_tickets = static_cast<ds_key_t>(max_rows);
    }

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating web_returns from %lld sales tickets\n",
            static_cast<long long>(n_tickets));
    }

    // Use a no-op callback to suppress sales output while still populating g_w_web_sales.
    g_w_web_sales_callback     = [](const struct W_WEB_SALES_TBL*, void*) {};
    g_w_web_sales_callback_ctx = nullptr;

    W_WEB_RETURNS_TBL row;
    long emitted = 0;
    for (ds_key_t i = 1; i <= n_tickets; ++i) {
        // Populate g_w_web_sales so mk_w_web_returns has valid sale context.
        mk_w_web_sales(nullptr, i);
        mk_w_web_returns(&row, i);
        callback(&row);
        ++emitted;
        if (max_rows > 0 && emitted >= max_rows) break;
    }

    g_w_web_sales_callback     = nullptr;
    g_w_web_sales_callback_ctx = nullptr;

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: emitted %ld web_returns rows\n", emitted);
    }
}

// ---------------------------------------------------------------------------
// Phase 5 dimension table generators (simple direct-struct pattern)
// ---------------------------------------------------------------------------

#define TPCDS_SIMPLE_GENERATE(funcname, TBL_TYPE, TPCDS_CONST, mk_func, log_name) \
void DSDGenWrapper::funcname(                                                       \
    std::function<void(const void* row)> callback,                                 \
    long max_rows)                                                                  \
{                                                                                   \
    init_dsdgen();                                                                  \
    ds_key_t total = get_rowcount(TPCDS_CONST);                                    \
    if (max_rows > 0 && static_cast<ds_key_t>(max_rows) < total)                   \
        total = static_cast<ds_key_t>(max_rows);                                   \
    if (verbose_) {                                                                 \
        std::fprintf(stderr,                                                        \
            "DSDGenWrapper: generating %lld " log_name " rows\n",                  \
            static_cast<long long>(total));                                         \
    }                                                                               \
    TBL_TYPE row;                                                                   \
    for (ds_key_t i = 1; i <= total; ++i) {                                        \
        mk_func(&row, i);                                                           \
        callback(&row);                                                             \
    }                                                                               \
}

TPCDS_SIMPLE_GENERATE(generate_call_center, struct CALL_CENTER_TBL,
    TPCDS_CALL_CENTER, mk_w_call_center, "call_center")

TPCDS_SIMPLE_GENERATE(generate_catalog_page, struct CATALOG_PAGE_TBL,
    TPCDS_CATALOG_PAGE, mk_w_catalog_page, "catalog_page")

TPCDS_SIMPLE_GENERATE(generate_web_page, struct W_WEB_PAGE_TBL,
    TPCDS_WEB_PAGE, mk_w_web_page, "web_page")

TPCDS_SIMPLE_GENERATE(generate_web_site, struct W_WEB_SITE_TBL,
    TPCDS_WEB_SITE, mk_w_web_site, "web_site")

TPCDS_SIMPLE_GENERATE(generate_warehouse, struct W_WAREHOUSE_TBL,
    TPCDS_WAREHOUSE, mk_w_warehouse, "warehouse")

TPCDS_SIMPLE_GENERATE(generate_ship_mode, struct W_SHIP_MODE_TBL,
    TPCDS_SHIP_MODE, mk_w_ship_mode, "ship_mode")

TPCDS_SIMPLE_GENERATE(generate_household_demographics, struct W_HOUSEHOLD_DEMOGRAPHICS_TBL,
    TPCDS_HOUSEHOLD_DEMOGRAPHICS, mk_w_household_demographics, "household_demographics")

TPCDS_SIMPLE_GENERATE(generate_customer_demographics, struct W_CUSTOMER_DEMOGRAPHICS_TBL,
    TPCDS_CUSTOMER_DEMOGRAPHICS, mk_w_customer_demographics, "customer_demographics")

TPCDS_SIMPLE_GENERATE(generate_customer_address, struct W_CUSTOMER_ADDRESS_TBL,
    TPCDS_CUSTOMER_ADDRESS, mk_w_customer_address, "customer_address")

TPCDS_SIMPLE_GENERATE(generate_income_band, struct W_INCOME_BAND_TBL,
    TPCDS_INCOME_BAND, mk_w_income_band, "income_band")

TPCDS_SIMPLE_GENERATE(generate_reason, struct W_REASON_TBL,
    TPCDS_REASON, mk_w_reason, "reason")

TPCDS_SIMPLE_GENERATE(generate_time_dim, struct W_TIME_TBL,
    TPCDS_TIME, mk_w_time, "time_dim")

TPCDS_SIMPLE_GENERATE(generate_promotion, struct W_PROMOTION_TBL,
    TPCDS_PROMOTION, mk_w_promotion, "promotion")

TPCDS_SIMPLE_GENERATE(generate_store, struct W_STORE_TBL,
    TPCDS_STORE, mk_w_store, "store")

}  // namespace tpcds
