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

// Forward declaration for RAII session helper
class DBGenWrapper;

/**
 * RAII helper for DBGen generation sessions.
 * Keeps basic session state together; concrete behavior is implemented
 * in the .cpp file and this type is intentionally lightweight here
 * so it can be introduced without touching existing generation logic.
 */
class DBGenSession {
   public:
    DBGenSession(DBGenWrapper* wrapper, int table_id, long start_row, long stop_row);
    ~DBGenSession();

    // Non-copyable
    DBGenSession(const DBGenSession&) = delete;
    DBGenSession& operator=(const DBGenSession&) = delete;

   private:
    DBGenWrapper* wrapper_;
    int table_id_;
    long start_row_;
    long stop_row_;
};

// Lightweight per-table traits used for future generic generators.
// They only expose the concrete row type and table identifier.
struct OrdersTraits {
    using Row = order_t;
    static constexpr TableType table = TableType::ORDERS;
};
struct LineitemTraits {
    using Row = line_t;
    static constexpr TableType table = TableType::LINEITEM;
};
struct CustomerTraits {
    using Row = customer_t;
    static constexpr TableType table = TableType::CUSTOMER;
};
struct PartTraits {
    using Row = part_t;
    static constexpr TableType table = TableType::PART;
};
struct PartsuppTraits {
    using Row = partsupp_t;
    static constexpr TableType table = TableType::PARTSUPP;
};
struct SupplierTraits {
    using Row = supplier_t;
    static constexpr TableType table = TableType::SUPPLIER;
};
struct NationTraits {
    using Row = code_t;
    static constexpr TableType table = TableType::NATION;
};
struct RegionTraits {
    using Row = code_t;
    static constexpr TableType table = TableType::REGION;
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
     * Generic generator dispatcher that forwards to the table-specific
     * generator implementation. This member template allows incremental
     * migration of the per-table generate_* methods to a single generic
     * entrypoint without changing external APIs.
     */
    template<typename Traits>
    void generate_generic(std::function<void(const void* row)> callback, long max_rows = -1);

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

    // Helper used in static_assert for unreachable branches
    template <typename>
    struct always_false : std::false_type {};

    // Generic batch iterator implementation used by simple tables.
    // Defined as a nested template so it can access DBGenWrapper's
    // initialization helpers and private members.
    template<typename Traits>
    class BatchIteratorImpl {
    public:
        using Row = typename Traits::Row;
        using Batch = DBGenBatch<Row>;

        BatchIteratorImpl(DBGenWrapper* wrapper, size_t batch_size, size_t max_rows)
            : wrapper_(wrapper), batch_size_(batch_size) {
            // Determine total remaining rows
            size_t total = static_cast<size_t>(get_row_count(Traits::table, wrapper_->scale_factor_));
            remaining_ = (max_rows == 0) ? total : std::min(static_cast<size_t>(max_rows), total);
            current_row_ = 1;

            if (!wrapper_->initialized_) {
                wrapper_->init_dbgen();
            }

            dbgen_reset_seeds();
            if constexpr (Traits::table == TableType::ORDERS) {
                row_start(DBGEN_ORDER);
            } else if constexpr (Traits::table == TableType::CUSTOMER) {
                row_start(DBGEN_CUST);
            } else if constexpr (Traits::table == TableType::PART) {
                row_start(DBGEN_PART);
            } else if constexpr (Traits::table == TableType::SUPPLIER) {
                row_start(DBGEN_SUPP);
            } else if constexpr (Traits::table == TableType::NATION) {
                row_start(DBGEN_NATION);
            } else if constexpr (Traits::table == TableType::LINEITEM) {
                // Lineitem generation is driven by orders
                row_start(DBGEN_LINE);
            } else if constexpr (Traits::table == TableType::PARTSUPP) {
                // Partsupp generation is driven by part rows
                row_start(DBGEN_PSUPP);
            } else if constexpr (Traits::table == TableType::REGION) {
                row_start(DBGEN_REGION);
            }
        }

        bool has_next() const { return remaining_ > 0; }

        Batch next() {
            Batch batch;
            if (remaining_ == 0) return batch;

            batch.rows.reserve(std::min(batch_size_, remaining_));

            size_t total_rows;
            if constexpr (Traits::table == TableType::LINEITEM) {
                // LINEITEM rows are produced while iterating ORDERS
                total_rows = static_cast<size_t>(get_row_count(TableType::ORDERS, wrapper_->scale_factor_));
            } else if constexpr (Traits::table == TableType::PARTSUPP) {
                // PARTSUPP rows are produced while iterating PART
                total_rows = static_cast<size_t>(get_row_count(TableType::PART, wrapper_->scale_factor_));
            } else {
                total_rows = static_cast<size_t>(get_row_count(Traits::table, wrapper_->scale_factor_));
            }

            while (batch.rows.size() < batch_size_ && remaining_ > 0 && current_row_ <= total_rows) {
                Row r{};
                if constexpr (Traits::table == TableType::ORDERS) {
                    if (mk_order(static_cast<DSS_HUGE>(current_row_), &r, 0) < 0) break;
                } else if constexpr (Traits::table == TableType::CUSTOMER) {
                    if (mk_cust(static_cast<DSS_HUGE>(current_row_), &r) < 0) break;
                } else if constexpr (Traits::table == TableType::PART) {
                    if (mk_part(static_cast<DSS_HUGE>(current_row_), &r) < 0) break;
                } else if constexpr (Traits::table == TableType::SUPPLIER) {
                    if (mk_supp(static_cast<DSS_HUGE>(current_row_), &r) < 0) break;
                } else if constexpr (Traits::table == TableType::NATION) {
                    if (mk_nation(static_cast<DSS_HUGE>(current_row_), &r) < 0) break;
                } else if constexpr (Traits::table == TableType::REGION) {
                    if (mk_region(static_cast<DSS_HUGE>(current_row_), &r) < 0) break;
                } else if constexpr (Traits::table == TableType::LINEITEM) {
                    // Generate next order and emit its lineitems
                    order_t ord{};
                    if (mk_order(static_cast<DSS_HUGE>(current_row_), &ord, 0) < 0) break;

                    for (int j = 0; j < (int)ord.lines && j < O_LCNT_MAX; ++j) {
                        if (batch.rows.size() >= batch_size_) break;
                        batch.rows.push_back(ord.l[j]);
                        remaining_--;
                    }
                    current_row_++;
                    // continue to next iteration (we already pushed rows)
                    if (batch.rows.size() >= batch_size_ || remaining_ == 0) break;
                    else continue;
                } else if constexpr (Traits::table == TableType::PARTSUPP) {
                    // Generate next part and emit its partsupp rows
                    part_t prt{};
                    if (mk_part(static_cast<DSS_HUGE>(current_row_), &prt) < 0) break;

                    for (int j = 0; j < SUPP_PER_PART; ++j) {
                        if (batch.rows.size() >= batch_size_) break;
                        batch.rows.push_back(prt.s[j]);
                        remaining_--;
                    }
                    current_row_++;
                    if (batch.rows.size() >= batch_size_ || remaining_ == 0) break;
                    else continue;
                } else {
                    static_assert(always_false<Traits>::value, "Unsupported batch iterator table");
                }
                batch.rows.push_back(r);
                remaining_--;
                current_row_++;
            }

            // Stop generation if complete
            if (remaining_ == 0 || current_row_ > total_rows) {
                if constexpr (Traits::table == TableType::ORDERS) {
                    row_stop(DBGEN_ORDER);
                } else if constexpr (Traits::table == TableType::CUSTOMER) {
                    row_stop(DBGEN_CUST);
                } else if constexpr (Traits::table == TableType::PART) {
                    row_stop(DBGEN_PART);
                } else if constexpr (Traits::table == TableType::SUPPLIER) {
                    row_stop(DBGEN_SUPP);
                } else if constexpr (Traits::table == TableType::NATION) {
                    row_stop(DBGEN_NATION);
                } else if constexpr (Traits::table == TableType::REGION) {
                    row_stop(DBGEN_REGION);
                } else if constexpr (Traits::table == TableType::LINEITEM) {
                    row_stop(DBGEN_LINE);
                } else if constexpr (Traits::table == TableType::PARTSUPP) {
                    row_stop(DBGEN_PSUPP);
                }
            }

            return batch;
        }

    private:
        DBGenWrapper* wrapper_;
        size_t batch_size_;
        size_t remaining_;
        size_t current_row_;
    };

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
    using LineitemBatchIterator = BatchIteratorImpl<LineitemTraits>;
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
    using OrdersBatchIterator = BatchIteratorImpl<OrdersTraits>;
    OrdersBatchIterator generate_orders_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for customer rows (zero-copy friendly)
     */
    using CustomerBatchIterator = BatchIteratorImpl<CustomerTraits>;
    CustomerBatchIterator generate_customer_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for part rows (zero-copy friendly)
     */
    using PartBatchIterator = BatchIteratorImpl<PartTraits>;
    PartBatchIterator generate_part_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for partsupp rows (zero-copy friendly)
     */
    using PartsuppBatchIterator = BatchIteratorImpl<PartsuppTraits>;
    PartsuppBatchIterator generate_partsupp_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for supplier rows (zero-copy friendly)
     */
    using SupplierBatchIterator = BatchIteratorImpl<SupplierTraits>;
    SupplierBatchIterator generate_supplier_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for nation rows (zero-copy friendly)
     */
    using NationBatchIterator = BatchIteratorImpl<NationTraits>;
    NationBatchIterator generate_nation_batches(size_t batch_size, size_t max_rows);

    /**
     * Batch iterator for region rows (zero-copy friendly)
     */
    using RegionBatchIterator = BatchIteratorImpl<RegionTraits>;
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


template<typename Traits>
void DBGenWrapper::generate_generic(std::function<void(const void* row)> callback, long max_rows) {
    if constexpr (Traits::table == TableType::ORDERS) {
        generate_orders(callback, max_rows);
    } else if constexpr (Traits::table == TableType::CUSTOMER) {
        generate_customer(callback, max_rows);
    } else if constexpr (Traits::table == TableType::PART) {
        generate_part(callback, max_rows);
    } else if constexpr (Traits::table == TableType::PARTSUPP) {
        generate_partsupp(callback, max_rows);
    } else if constexpr (Traits::table == TableType::SUPPLIER) {
        generate_supplier(callback, max_rows);
    } else if constexpr (Traits::table == TableType::NATION) {
        generate_nation(callback);
    } else if constexpr (Traits::table == TableType::REGION) {
        generate_region(callback);
    } else if constexpr (Traits::table == TableType::LINEITEM) {
        generate_lineitem(callback, max_rows);
    } else {
        static_assert(always_false<Traits>::value, "Unsupported table in generate_generic");
    }
}

void dbgen_init_global(long scale_factor, bool verbose_flag);

/**
 * Check if global dbgen initialization was completed
 *
 * @return true if dbgen_init_global() was called
 */
bool dbgen_is_initialized();

}  // namespace tpch
