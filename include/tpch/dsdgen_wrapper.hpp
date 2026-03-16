#pragma once

#include <memory>
#include <functional>
#include <string>
#include <cstdint>

#include <arrow/api.h>

namespace tpcds {

/**
 * TPC-DS table identifiers for the 24 standard W_ (warehouse) tables.
 *
 * The integer values are pinned to the generated `tables.h` constants and
 * verified via static_asserts in the implementation TU.
 */
enum class TableType {
    CallCenter           = 0,
    CatalogPage          = 1,
    CatalogReturns       = 2,
    CatalogSales         = 3,
    Customer             = 4,
    CustomerAddress      = 5,
    CustomerDemographics = 6,
    DateDim              = 7,
    HouseholdDemographics = 8,
    IncomeBand           = 9,
    Inventory            = 10,
    Item                 = 11,
    Promotion            = 12,
    Reason               = 13,
    ShipMode             = 14,
    Store                = 15,
    StoreReturns         = 16,
    StoreSales           = 17,
    TimeDim              = 18,
    Warehouse            = 19,
    WebPage              = 20,
    WebReturns           = 21,
    WebSales             = 22,
    WebSite              = 23,
    Count_
};

/**
 * C++ wrapper around the TPC-DS dsdgen reference implementation.
 *
 * Initializes dsdgen global state (embedded distribution data, scale factor,
 * RNG seeds) and provides per-table generation methods with callback API.
 *
 * THREAD-SAFETY: NOT thread-safe. dsdgen uses global mutable state.
 * Use one DSDGenWrapper per process, generate tables sequentially.
 */
class DSDGenWrapper {
public:
    /**
     * Construct wrapper for the given scale factor.
     * @param scale_factor TPC-DS scale factor (1 = ~1GB baseline)
     * @param verbose      Print verbose diagnostic messages
     */
    explicit DSDGenWrapper(long scale_factor, bool verbose = false);
    ~DSDGenWrapper();

    DSDGenWrapper(const DSDGenWrapper&) = delete;
    DSDGenWrapper& operator=(const DSDGenWrapper&) = delete;

    /**
     * Generate store_sales rows.
     * Calls callback once per row with a const W_STORE_SALES_TBL*.
     * @param callback  Invoked for each generated row.
     * @param max_rows  Limit; -1 or 0 means generate all rows.
     */
    void generate_store_sales(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate inventory rows.
     * Calls callback once per row with a const W_INVENTORY_TBL*.
     */
    void generate_inventory(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate catalog_sales rows (master-detail via callback).
     * Calls callback once per line item with a const W_CATALOG_SALES_TBL*.
     */
    void generate_catalog_sales(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate web_sales rows (master-detail via callback).
     * Calls callback once per line item with a const W_WEB_SALES_TBL*.
     */
    void generate_web_sales(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate customer rows.
     * Calls callback once per row with a const W_CUSTOMER_TBL*.
     */
    void generate_customer(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate item rows.
     * Calls callback once per row with a const W_ITEM_TBL*.
     */
    void generate_item(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate date_dim rows.
     * Calls callback once per row with a const W_DATE_TBL*.
     */
    void generate_date_dim(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate store_returns rows.
     * Calls callback once per row with a const W_STORE_RETURNS_TBL*.
     */
    void generate_store_returns(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate catalog_returns rows.
     * Calls callback once per row with a const W_CATALOG_RETURNS_TBL*.
     */
    void generate_catalog_returns(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    /**
     * Generate web_returns rows.
     * Calls callback once per row with a const W_WEB_RETURNS_TBL*.
     */
    void generate_web_returns(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    // -----------------------------------------------------------------------
    // Phase 5 dimension table generators
    // -----------------------------------------------------------------------

    void generate_call_center(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_catalog_page(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_web_page(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_web_site(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_warehouse(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_ship_mode(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_household_demographics(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_customer_demographics(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_customer_address(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_income_band(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_reason(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_time_dim(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_promotion(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_store(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    long scale_factor() const { return scale_factor_; }

    /**
     * Load distributions and seed RNG in the parent process before fork().
     * Children inherit the fully-initialised state via COW.
     * Must be called before any fork(); call clear_tmp_path() in each child.
     */
    void prepare_for_fork();

    /**
     * Clear the temp-file path so this process's destructor does not unlink it.
     * Call immediately after fork() in each child process.
     * The parent retains the path and unlinks the file after all children exit.
     */
    void clear_tmp_path();

    /**
     * Return the Arrow schema for a table type.
     */
    static std::shared_ptr<arrow::Schema> get_schema(TableType table, double scale_factor = 1.0);

    /**
     * Return expected row count for a table at the given scale factor.
     * Uses dsdgen's get_rowcount() after initialization.
     */
    long get_row_count(TableType table) const;

    /**
     * Return the dsdgen integer table ID for a TableType.
     */
    static int table_id(TableType table);

    /**
     * Return the canonical lower-case table name string.
     */
    static std::string table_name(TableType table);

private:
    long scale_factor_;
    bool verbose_;
    bool initialized_;
    std::string tmp_dist_path_;  // path to temporary tpcds.idx file

    void init_dsdgen();
};

}  // namespace tpcds
