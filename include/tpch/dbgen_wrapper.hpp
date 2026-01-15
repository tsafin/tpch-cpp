#pragma once

#include <memory>
#include <functional>
#include <string>
#include <vector>
#include <cstdint>
#include <mutex>
#include <span>

#include <arrow/api.h>

// Include dbgen types and API
extern "C" {
#include "tpch_dbgen.h"
}

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
 * Batch result from dbgen - owns memory, provides span views
 *
 * This structure owns the generated data and provides zero-copy
 * access via std::span. Useful for Phase 13.4 zero-copy optimizations.
 */
template<typename T>
struct DBGenBatch {
    std::vector<T> rows;  // Owns the data

    /**
     * Get zero-copy span view over rows
     */
    std::span<const T> span() const {
        return std::span<const T>(rows);
    }

    size_t size() const { return rows.size(); }
    bool empty() const { return rows.empty(); }
};

/**
 * C++ wrapper around TPC-H dbgen reference implementation
 *
 * Provides iterator-style data generation with Arrow-compatible output.
 * Uses official dbgen C code as backend.
 *
 * **THREAD-SAFETY WARNING**:
 *
 * DBGenWrapper is NOT thread-safe for concurrent generation. The underlying
 * dbgen implementation uses global mutable state (RNG seeds, configuration).
 *
 * Safe usage patterns:
 * 1. Single-threaded: One DBGenWrapper per process/thread
 * 2. Multi-process: Fork separate processes (OS provides memory isolation)
 * 3. Sequential: Generate one table completely before next
 *
 * UNSAFE patterns:
 * - Calling generate_*() from multiple threads concurrently
 * - Multiple DBGenWrapper instances generating simultaneously
 *
 * For parallel generation, use multiple processes instead of threads.
 * Distributions are loaded once and are read-only (safe for concurrent reads).
 */
class DBGenWrapper {
public:
    /**
     * Initialize DBGenWrapper with scale factor
     *
     * @param scale_factor TPC-H scale factor (1 = 1GB baseline)
     * @param verbose Enable verbose debug output
     *
     * Note: Distribution loading is thread-safe (one-time initialization)
     */
    explicit DBGenWrapper(long scale_factor, bool verbose = false);

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

    /**
     * Set skip initialization flag (for use after global init)
     *
     * When set to true, the DBGenWrapper will not call init_dbgen()
     * because it assumes initialization was already done globally.
     */
    void set_skip_init(bool skip) { skip_init_ = skip; }

    // =======================================================================
    // Phase 13.4: Batch generation interfaces for zero-copy optimization
    // =======================================================================

    /**
     * Batch iterator for lineitem rows (zero-copy friendly)
     *
     * Generates lineitem rows in batches instead of one-by-one callbacks.
     * This enables zero-copy conversion using std::span views.
     *
     * Example usage:
     * ```cpp
     * auto iter = dbgen.generate_lineitem_batches(10000, 100000);
     * while (iter.has_next()) {
     *     auto batch = iter.next();
     *     auto span = batch.span();  // Zero-copy span view
     *     // Process span...
     * }
     * ```
     */
    class LineitemBatchIterator {
    public:
        using Batch = DBGenBatch<line_t>;

        LineitemBatchIterator(DBGenWrapper* wrapper, size_t batch_size, size_t max_rows);

        bool has_next() const { return remaining_ > 0; }
        Batch next();

    private:
        DBGenWrapper* wrapper_;
        size_t batch_size_;
        size_t remaining_;
        size_t current_order_;
    };

    /**
     * Create a batch iterator for lineitem generation
     *
     * @param batch_size Number of rows per batch
     * @param max_rows Maximum total rows to generate
     * @return Iterator over batches
     */
    LineitemBatchIterator generate_lineitem_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for orders rows (zero-copy friendly)
     */
    class OrdersBatchIterator {
    public:
        using Batch = DBGenBatch<order_t>;

        OrdersBatchIterator(DBGenWrapper* wrapper, size_t batch_size, size_t max_rows);

        bool has_next() const { return remaining_ > 0; }
        Batch next();

    private:
        DBGenWrapper* wrapper_;
        size_t batch_size_;
        size_t remaining_;
        size_t current_row_;
    };

    OrdersBatchIterator generate_orders_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for customer rows (zero-copy friendly)
     */
    class CustomerBatchIterator {
    public:
        using Batch = DBGenBatch<customer_t>;

        CustomerBatchIterator(DBGenWrapper* wrapper, size_t batch_size, size_t max_rows);

        bool has_next() const { return remaining_ > 0; }
        Batch next();

    private:
        DBGenWrapper* wrapper_;
        size_t batch_size_;
        size_t remaining_;
        size_t current_row_;
    };

    CustomerBatchIterator generate_customer_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for part rows (zero-copy friendly)
     */
    class PartBatchIterator {
    public:
        using Batch = DBGenBatch<part_t>;

        PartBatchIterator(DBGenWrapper* wrapper, size_t batch_size, size_t max_rows);

        bool has_next() const { return remaining_ > 0; }
        Batch next();

    private:
        DBGenWrapper* wrapper_;
        size_t batch_size_;
        size_t remaining_;
        size_t current_row_;
    };

    PartBatchIterator generate_part_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for partsupp rows (zero-copy friendly)
     */
    class PartsuppBatchIterator {
    public:
        using Batch = DBGenBatch<partsupp_t>;

        PartsuppBatchIterator(DBGenWrapper* wrapper, size_t batch_size, size_t max_rows);

        bool has_next() const { return remaining_ > 0; }
        Batch next();

    private:
        DBGenWrapper* wrapper_;
        size_t batch_size_;
        size_t remaining_;
        size_t current_row_;
    };

    PartsuppBatchIterator generate_partsupp_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for supplier rows (zero-copy friendly)
     */
    class SupplierBatchIterator {
    public:
        using Batch = DBGenBatch<supplier_t>;

        SupplierBatchIterator(DBGenWrapper* wrapper, size_t batch_size, size_t max_rows);

        bool has_next() const { return remaining_ > 0; }
        Batch next();

    private:
        DBGenWrapper* wrapper_;
        size_t batch_size_;
        size_t remaining_;
        size_t current_row_;
    };

    SupplierBatchIterator generate_supplier_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for nation rows (zero-copy friendly)
     */
    class NationBatchIterator {
    public:
        using Batch = DBGenBatch<code_t>;

        NationBatchIterator(DBGenWrapper* wrapper, size_t batch_size, size_t max_rows);

        bool has_next() const { return remaining_ > 0; }
        Batch next();

    private:
        DBGenWrapper* wrapper_;
        size_t batch_size_;
        size_t remaining_;
        size_t current_row_;
    };

    NationBatchIterator generate_nation_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for region rows (zero-copy friendly)
     */
    class RegionBatchIterator {
    public:
        using Batch = DBGenBatch<code_t>;

        RegionBatchIterator(DBGenWrapper* wrapper, size_t batch_size, size_t max_rows);

        bool has_next() const { return remaining_ > 0; }
        Batch next();

    private:
        DBGenWrapper* wrapper_;
        size_t batch_size_;
        size_t remaining_;
        size_t current_row_;
    };

    RegionBatchIterator generate_region_batches(size_t batch_size, size_t max_rows);

private:
    long scale_factor_;
    bool initialized_;
    bool verbose_;
    bool skip_init_;  // Skip initialization (global init already done)
    char** asc_dates_;  // Date array cache for orders/lineitem generation

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

/**
 * Global initialization for dbgen (call ONCE before fork())
 *
 * Performs heavy one-time initialization:
 * - Loads distributions from dists.dss file
 * - Pre-caches date array (2557 strings)
 * - Optionally pre-warms text pool (300MB)
 * - Sets global dbgen configuration
 *
 * After calling this, child processes can skip initialization
 * by using DBGenWrapper::set_skip_init(true).
 *
 * Thread-safety: Must be called from parent process before fork().
 * Child processes inherit all initialized state via copy-on-write.
 *
 * @param scale_factor TPC-H scale factor (1 = 1GB baseline)
 * @param verbose_flag Enable verbose debug output
 */
void dbgen_init_global(long scale_factor, bool verbose_flag);

/**
 * Check if global dbgen initialization was completed
 *
 * @return true if dbgen_init_global() was called
 */
bool dbgen_is_initialized();

}  // namespace tpch
