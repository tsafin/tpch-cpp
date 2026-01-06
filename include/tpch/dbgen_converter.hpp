#pragma once

#include <memory>
#include <map>
#include <string>
#include <arrow/builder.h>

namespace tpch {

/**
 * Convert dbgen C struct rows to Arrow builders
 *
 * Each function casts void* to the appropriate C struct,
 * extracts fields, and appends to Arrow builders.
 */

void append_lineitem_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_orders_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_customer_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_part_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_partsupp_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_supplier_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_nation_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_region_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Generic dispatcher based on table name
 */
void append_row_to_builders(
    const std::string& table_name,
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

}  // namespace tpch
