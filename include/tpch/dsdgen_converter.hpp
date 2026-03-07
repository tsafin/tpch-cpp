#pragma once

#include <memory>
#include <vector>
#include <string>
#include <arrow/builder.h>

namespace tpcds {

using BuilderMap = std::vector<std::shared_ptr<arrow::ArrayBuilder>>;

/**
 * Convert dsdgen C struct rows to Arrow array builders.
 *
 * Each function casts void* to the appropriate dsdgen struct, extracts
 * fields, and appends to the matching Arrow builders.
 */

/**
 * Append a store_sales row (W_STORE_SALES_TBL*) to Arrow builders.
 * Schema matches DSDGenWrapper::get_schema(TableType::STORE_SALES).
 */
void append_store_sales_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append an inventory row (W_INVENTORY_TBL*) to Arrow builders.
 * Schema matches DSDGenWrapper::get_schema(TableType::INVENTORY).
 */
void append_inventory_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a catalog_sales row (W_CATALOG_SALES_TBL*) to Arrow builders.
 */
void append_catalog_sales_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a web_sales row (W_WEB_SALES_TBL*) to Arrow builders.
 */
void append_web_sales_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a customer row (W_CUSTOMER_TBL*) to Arrow builders.
 */
void append_customer_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append an item row (W_ITEM_TBL*) to Arrow builders.
 */
void append_item_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a date_dim row (W_DATE_TBL*) to Arrow builders.
 */
void append_date_dim_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a store_returns row (W_STORE_RETURNS_TBL*) to Arrow builders.
 */
void append_store_returns_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a catalog_returns row (W_CATALOG_RETURNS_TBL*) to Arrow builders.
 */
void append_catalog_returns_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a web_returns row (W_WEB_RETURNS_TBL*) to Arrow builders.
 */
void append_web_returns_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a call_center row (CALL_CENTER_TBL*) to Arrow builders.
 */
void append_call_center_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a catalog_page row (CATALOG_PAGE_TBL*) to Arrow builders.
 */
void append_catalog_page_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a web_page row (W_WEB_PAGE_TBL*) to Arrow builders.
 */
void append_web_page_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a web_site row (W_WEB_SITE_TBL*) to Arrow builders.
 */
void append_web_site_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a warehouse row (W_WAREHOUSE_TBL*) to Arrow builders.
 */
void append_warehouse_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a ship_mode row (W_SHIP_MODE_TBL*) to Arrow builders.
 */
void append_ship_mode_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a household_demographics row (W_HOUSEHOLD_DEMOGRAPHICS_TBL*) to Arrow builders.
 */
void append_household_demographics_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a customer_demographics row (W_CUSTOMER_DEMOGRAPHICS_TBL*) to Arrow builders.
 */
void append_customer_demographics_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a customer_address row (W_CUSTOMER_ADDRESS_TBL*) to Arrow builders.
 */
void append_customer_address_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append an income_band row (W_INCOME_BAND_TBL*) to Arrow builders.
 */
void append_income_band_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a reason row (W_REASON_TBL*) to Arrow builders.
 */
void append_reason_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a time_dim row (W_TIME_TBL*) to Arrow builders.
 */
void append_time_dim_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a promotion row (W_PROMOTION_TBL*) to Arrow builders.
 */
void append_promotion_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Append a store row (W_STORE_TBL*) to Arrow builders.
 */
void append_store_to_builders(
    const void* row,
    BuilderMap& builders);

/**
 * Generic dispatcher by table name.
 */
void append_dsdgen_row_to_builders(
    const std::string& table_name,
    const void* row,
    BuilderMap& builders);

/**
 * Returns static dictionary Arrow array for dict8-encoded columns, or nullptr.
 */
std::shared_ptr<arrow::Array> get_dict_for_field(const std::string& field_name);

}  // namespace tpcds
