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
 *
 * These converters are called once per row during data generation.
 * The builders accumulate rows until finish() is called to create an Arrow RecordBatch.
 */

/**
 * Append a lineitem row from dbgen to Arrow builders
 * Converts line_t* to Arrow columns
 */
void append_lineitem_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Append an orders row from dbgen to Arrow builders
 * Converts order_t* to Arrow columns
 */
void append_orders_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Append a customer row from dbgen to Arrow builders
 * Converts customer_t* to Arrow columns
 */
void append_customer_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Append a part row from dbgen to Arrow builders
 * Converts part_t* to Arrow columns
 */
void append_part_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Append a partsupp row from dbgen to Arrow builders
 * Converts partsupp_t* to Arrow columns
 */
void append_partsupp_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Append a supplier row from dbgen to Arrow builders
 * Converts supplier_t* to Arrow columns
 */
void append_supplier_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Append a nation row from dbgen to Arrow builders
 * Converts code_t* to Arrow columns (for nation table)
 */
void append_nation_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Append a region row from dbgen to Arrow builders
 * Converts code_t* to Arrow columns (for region table)
 */
void append_region_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Generic dispatcher based on table name
 * Routes to the appropriate append_*_to_builders() function
 */
void append_row_to_builders(
    const std::string& table_name,
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

}  // namespace tpch
