#pragma once

#include <memory>
#include <map>
#include <string>
#include <arrow/builder.h>

namespace tpcds {

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
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Append an inventory row (W_INVENTORY_TBL*) to Arrow builders.
 * Schema matches DSDGenWrapper::get_schema(TableType::INVENTORY).
 */
void append_inventory_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Generic dispatcher by table name.
 */
void append_dsdgen_row_to_builders(
    const std::string& table_name,
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

}  // namespace tpcds
