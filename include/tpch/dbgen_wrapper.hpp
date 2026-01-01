#pragma once

#include <memory>
#include <functional>
#include <string>
#include <vector>

#include <arrow/api.h>

namespace tpch {

enum class TableType {
    LINEITEM,
    ORDERS,
    CUSTOMER,
    PART,
    PARTSUPP,
    SUPPLIER,
    NATION,
    REGION,
    COUNT_
};

/**
 * Get table name for display/debugging
 */
std::string table_type_name(TableType table);

/**
 * Get expected row count for a table at given scale factor
 */
long get_row_count(TableType table, long scale_factor);

/**
 * C++ wrapper around TPC-H dbgen reference implementation
 *
 * Provides iterator-style data generation with Arrow-compatible output.
 * Uses official dbgen C code as backend.
 */
class DBGenWrapper {
public:
    /**
     * Initialize DBGenWrapper with scale factor
     *
     * @param scale_factor TPC-H scale factor (1 = 1GB baseline)
     */
    explicit DBGenWrapper(long scale_factor);

    ~DBGenWrapper();

    // Delete copy operations (C code globals not thread-safe)
    DBGenWrapper(const DBGenWrapper&) = delete;
    DBGenWrapper& operator=(const DBGenWrapper&) = delete;

    // Allow moves
    DBGenWrapper(DBGenWrapper&& other) noexcept;
    DBGenWrapper& operator=(DBGenWrapper&& other) noexcept;

    /**
     * Generate all tables with callbacks
     *
     * Useful for parallel generation of multiple tables
     */
    void generate_all_tables(
        std::function<void(const char* table_name, const void* row)> callback);

    /**
     * Generate lineitem table
     *
     * @param callback Function called for each generated row (line_t*)
     * @param max_rows Optional limit on rows to generate
     */
    void generate_lineitem(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate orders table
     *
     * @param callback Function called for each generated row (order_t*)
     * @param max_rows Optional limit on rows to generate
     */
    void generate_orders(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate customer table
     *
     * @param callback Function called for each generated row (customer_t*)
     * @param max_rows Optional limit on rows to generate
     */
    void generate_customer(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate part table
     *
     * @param callback Function called for each generated row (part_t*)
     * @param max_rows Optional limit on rows to generate
     */
    void generate_part(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate partsupp table
     *
     * @param callback Function called for each generated row (partsupp_t*)
     * @param max_rows Optional limit on rows to generate
     */
    void generate_partsupp(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate supplier table
     *
     * @param callback Function called for each generated row (supplier_t*)
     * @param max_rows Optional limit on rows to generate
     */
    void generate_supplier(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate nation table (25 rows)
     *
     * @param callback Function called for each generated row (code_t*)
     */
    void generate_nation(
        std::function<void(const void* row)> callback);

    /**
     * Generate region table (5 rows)
     *
     * @param callback Function called for each generated row (code_t*)
     */
    void generate_region(
        std::function<void(const void* row)> callback);

    /**
     * Get scale factor
     */
    long scale_factor() const { return scale_factor_; }

    /**
     * Create Arrow schema for a table type
     */
    static std::shared_ptr<arrow::Schema> get_schema(TableType table);

private:
    long scale_factor_;
    bool initialized_;

    /**
     * Initialize dbgen global state
     * Not thread-safe - dbgen uses global variables
     */
    void init_dbgen();

    /**
     * Get TPC-H seed for a table
     */
    unsigned long get_seed(int table_id);
};

}  // namespace tpch
