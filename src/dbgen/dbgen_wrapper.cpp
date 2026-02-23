#include "tpch/dbgen_wrapper.hpp"

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <stdexcept>

// dbgen C types and functions are already declared in tpch_dbgen.h
// which is included via dbgen_wrapper.hpp

// Additional dbgen globals we need to set
extern "C" {
    extern long scale;
    extern long verbose;
    extern long force;
    extern char *d_path;
}

namespace tpch {

std::string table_type_name(TableType table) {
    switch (table) {
        case TableType::LINEITEM: return "lineitem";
        case TableType::ORDERS: return "orders";
        case TableType::CUSTOMER: return "customer";
        case TableType::PART: return "part";
        case TableType::PARTSUPP: return "partsupp";
        case TableType::SUPPLIER: return "supplier";
        case TableType::NATION: return "nation";
        case TableType::REGION: return "region";
        case TableType::COUNT_: return "unknown";
    }
    return "unknown";
}

long get_row_count(TableType table, long scale_factor) {
    // TPC-H row count formulas based on scale factor (in GB)
    // These are from the TPC-H specification
    switch (table) {
        case TableType::PART:
            return 200000L * scale_factor;
        case TableType::SUPPLIER:
            return 10000L * scale_factor;
        case TableType::PARTSUPP:
            return 800000L * scale_factor;
        case TableType::CUSTOMER:
            return 150000L * scale_factor;
        case TableType::ORDERS:
            return 1500000L * scale_factor;
        case TableType::LINEITEM:
            return 6000000L * scale_factor;
        case TableType::NATION:
            return 25;  // Fixed
        case TableType::REGION:
            return 5;   // Fixed
        case TableType::COUNT_:
            return 0;
    }
    return 0;
}

DBGenWrapper::DBGenWrapper(long scale_factor, bool verbose)
    : scale_factor_(scale_factor), initialized_(false), verbose_(verbose), skip_init_(false), asc_dates_(nullptr) {
    if (scale_factor <= 0) {
        throw std::invalid_argument("Scale factor must be positive");
    }
}

DBGenWrapper::~DBGenWrapper() = default;

DBGenWrapper::DBGenWrapper(DBGenWrapper&& other) noexcept
    : scale_factor_(other.scale_factor_), initialized_(other.initialized_), verbose_(other.verbose_) {
    other.initialized_ = false;
}

DBGenWrapper& DBGenWrapper::operator=(DBGenWrapper&& other) noexcept {
    if (this != &other) {
        scale_factor_ = other.scale_factor_;
        initialized_ = other.initialized_;
        verbose_ = other.verbose_;
        other.initialized_ = false;
    }
    return *this;
}

void tpch::DBGenWrapper::init_dbgen() {
    // If skip_init_ is true, global init was already done
    // Just mark as initialized and return
    if (skip_init_) {
        initialized_ = true;
        return;
    }

    // Set global dbgen state
    // dbgen uses global variables for configuration
    scale = scale_factor_;
    verbose = verbose_ ? 1 : 0;  // Set based on verbose flag
    force = 0;
    d_path = nullptr;  // Use current directory

    // Pre-cache the date array using the caching wrapper in dbgen_stubs.c
    // This ensures all subsequent calls to mk_ascdate() (from mk_order, mk_lineitem, etc.)
    // will reuse the same pre-allocated 2557-element array instead of allocating new ones
    if (asc_dates_ == nullptr) {
        asc_dates_ = mk_ascdate();
        if (asc_dates_ == nullptr) {
            throw std::runtime_error("Failed to allocate date array for dbgen");
        }
    }

    // Load distribution data (required for data generation, not just printing)
    // This populates the distribution structures used by mk_order, mk_lineitem, etc.
    if (verbose_) {
        fprintf(stderr, "DEBUG: Calling load_dists()...\n");
        fflush(stderr);
    }
    load_dists();
    if (verbose_) {
        fprintf(stderr, "DEBUG: load_dists() completed\n");
        fflush(stderr);
    }

    initialized_ = true;
}

unsigned long tpch::DBGenWrapper::get_seed(int table_id) {
    // TPC-H seed generation based on table ID
    // Each table gets a deterministic seed based on its type
    return (123456789UL + table_id * 999999991UL) % (1UL << 31);
}

void tpch::DBGenWrapper::generate_lineitem(
    std::function<void(const void* row)> callback,
    long max_rows) {

    if (!initialized_) {
        init_dbgen();
    }

    // Reset RNG state before generating rows
    dbgen_reset_seeds();
    /* Capture the initial RNG seed snapshot so other generators can restore it
       and reproduce the same sequence */
    dbgen_capture_seed_snapshot();

    row_start(DBGEN_LINE);

    long rows_generated = 0;

    // LineItem rows are generated implicitly via order generation
    // Each order has between 1-7 line items
    for (DSS_HUGE i = 1; i <= get_row_count(TableType::ORDERS, scale_factor_); ++i) {
        order_t order{};
        if (mk_order(i, &order, 0) < 0) {
            break;
        }

        // Extract each lineitem from the order
        for (int j = 0; j < (int)order.lines && j < O_LCNT_MAX; ++j) {
            if (callback) {
                callback(&order.l[j]);
            }
            rows_generated++;

            if (max_rows > 0 && rows_generated >= max_rows) {
                row_stop(DBGEN_LINE);
                return;
            }
        }
    }

    row_stop(DBGEN_LINE);
}

void tpch::DBGenWrapper::generate_orders(
    std::function<void(const void* row)> callback,
    long max_rows) {
    auto batch_iter = generate_orders_batches(10000, max_rows);
    while (batch_iter.has_next()) {
        auto batch = batch_iter.next();
        for (size_t i = 0; i < batch.size(); ++i) {
            if (callback) {
                callback(&batch.rows[i]);
            }
        }
    }
}

void tpch::DBGenWrapper::generate_customer(
    std::function<void(const void* row)> callback,
    long max_rows) {
    auto batch_iter = generate_customer_batches(10000, max_rows);
    while (batch_iter.has_next()) {
        auto batch = batch_iter.next();
        for (size_t i = 0; i < batch.size(); ++i) {
            if (callback) {
                callback(&batch.rows[i]);
            }
        }
    }
}

void tpch::DBGenWrapper::generate_part(
    std::function<void(const void* row)> callback,
    long max_rows) {
    auto batch_iter = generate_part_batches(10000, max_rows);
    while (batch_iter.has_next()) {
        auto batch = batch_iter.next();
        for (size_t i = 0; i < batch.size(); ++i) {
            if (callback) {
                callback(&batch.rows[i]);
            }
        }
    }
}

void tpch::DBGenWrapper::generate_partsupp(
    std::function<void(const void* row)> callback,
    long max_rows) {

    if (!initialized_) {
        init_dbgen();
    }

    dbgen_reset_seeds();
    row_start(DBGEN_PSUPP);

    long rows_generated = 0;
    long total_rows_part = get_row_count(TableType::PART, scale_factor_);

    for (DSS_HUGE i = 1; i <= total_rows_part; ++i) {
        part_t part{};
        if (mk_part(i, &part) < 0) {
            break;
        }

        // Extract partsupp rows from part (part has SUPP_PER_PART partsupp entries)
        for (int j = 0; j < SUPP_PER_PART; ++j) {
            if (callback) {
                callback(&part.s[j]);
            }
            rows_generated++;

            if (max_rows > 0 && rows_generated >= max_rows) {
                row_stop(DBGEN_PSUPP);
                return;
            }
        }
    }

    row_stop(DBGEN_PSUPP);
}

void tpch::DBGenWrapper::generate_supplier(
    std::function<void(const void* row)> callback,
    long max_rows) {
    auto batch_iter = generate_supplier_batches(10000, max_rows);
    while (batch_iter.has_next()) {
        auto batch = batch_iter.next();
        for (size_t i = 0; i < batch.size(); ++i) {
            if (callback) {
                callback(&batch.rows[i]);
            }
        }
    }
}

void tpch::DBGenWrapper::generate_nation(
    std::function<void(const void* row)> callback) {
    auto batch_iter = generate_nation_batches(1024, -1);
    while (batch_iter.has_next()) {
        auto batch = batch_iter.next();
        for (size_t i = 0; i < batch.size(); ++i) {
            callback(&batch.rows[i]);
        }
    }
}

void tpch::DBGenWrapper::generate_region(
    std::function<void(const void* row)> callback) {
    auto batch_iter = generate_region_batches(1024, -1);
    while (batch_iter.has_next()) {
        auto batch = batch_iter.next();
        for (size_t i = 0; i < batch.size(); ++i) {
            callback(&batch.rows[i]);
        }
    }
}

void tpch::DBGenWrapper::generate_all_tables(
    std::function<void(const char* table_name, const void* row)> callback) {

    // Generate each table
    generate_customer([this, &callback](const void* row) {
        callback("customer", row);
    });

    generate_supplier([this, &callback](const void* row) {
        callback("supplier", row);
    });

    generate_part([this, &callback](const void* row) {
        callback("part", row);
    });

    generate_partsupp([this, &callback](const void* row) {
        callback("partsupp", row);
    });

    generate_nation([this, &callback](const void* row) {
        callback("nation", row);
    });

    generate_region([this, &callback](const void* row) {
        callback("region", row);
    });

    generate_orders([this, &callback](const void* row) {
        callback("orders", row);
    });

    generate_lineitem([this, &callback](const void* row) {
        callback("lineitem", row);
    });
}

// Helper: create an Arrow field with a pre-computed cardinality hint for Lance.
// Lance reads this "lance.cardinality" metadata and skips HyperLogLogPlus computation,
// using the exact value instead.  Only use for fields with known-bounded cardinality
// from the TPC-H specification (dists.dss and TPC-H v3 spec).
static std::shared_ptr<arrow::Field> tpch_field(
    const std::string& name,
    std::shared_ptr<arrow::DataType> type,
    int64_t known_cardinality = -1)
{
    if (known_cardinality > 0) {
        auto meta = arrow::key_value_metadata(
            std::vector<std::string>{"lance.cardinality"},
            std::vector<std::string>{std::to_string(known_cardinality)});
        return arrow::field(name, type, /*nullable=*/true, meta);
    }
    return arrow::field(name, type);
}

std::shared_ptr<arrow::Schema> tpch::DBGenWrapper::get_schema(TableType table) {
    using arrow::int64;
    using arrow::float64;
    using arrow::utf8;

    // Dictionary type shorthand for low-cardinality string columns.
    // Lance routes these to DictionaryDataBlock where compute_stat() is a no-op
    // (zero HLL/XXH3 overhead). Indices stored as int8 (up to 127 values).
    auto dict8 = arrow::dictionary(arrow::int8(), utf8());

    switch (table) {
        case TableType::LINEITEM:
            // Low-cardinality columns use dict8 → zero statistics overhead in Lance.
            // Date fields keep utf8+hint (2556 values exceed int8 range).
            return arrow::schema({
                tpch_field("l_orderkey",       int64()),
                tpch_field("l_partkey",        int64()),
                tpch_field("l_suppkey",        int64()),
                tpch_field("l_linenumber",     int64()),
                tpch_field("l_quantity",       float64()),
                tpch_field("l_extendedprice",  float64()),
                tpch_field("l_discount",       float64()),
                tpch_field("l_tax",            float64()),
                tpch_field("l_returnflag",     dict8),       // 3 values: A/N/R
                tpch_field("l_linestatus",     dict8),       // 2 values: F/O
                tpch_field("l_commitdate",     utf8(),  2556),
                tpch_field("l_shipdate",       utf8(),  2556),
                tpch_field("l_receiptdate",    utf8(),  2556),
                tpch_field("l_shipinstruct",   dict8),       // 4 values
                tpch_field("l_shipmode",       dict8),       // 7 values
                tpch_field("l_comment",        utf8()),      // high cardinality
            });

        case TableType::ORDERS:
            return arrow::schema({
                tpch_field("o_orderkey",      int64()),
                tpch_field("o_custkey",       int64()),
                tpch_field("o_orderstatus",   dict8),       // 3 values: F/O/P
                tpch_field("o_totalprice",    float64()),
                tpch_field("o_orderdate",     utf8(),  2556),
                tpch_field("o_orderpriority", dict8),       // 5 values: 1-URGENT..5-LOW
                tpch_field("o_clerk",         utf8()),      // high cardinality
                tpch_field("o_shippriority",  int64()),
                tpch_field("o_comment",       utf8()),      // high cardinality
            });

        case TableType::CUSTOMER:
            return arrow::schema({
                tpch_field("c_custkey",     int64()),
                tpch_field("c_name",        utf8()),        // high cardinality
                tpch_field("c_address",     utf8()),        // high cardinality
                tpch_field("c_nationkey",   int64()),
                tpch_field("c_phone",       utf8()),        // unique per customer
                tpch_field("c_acctbal",     float64()),
                tpch_field("c_mktsegment",  dict8),        // 5 values
                tpch_field("c_comment",     utf8()),        // high cardinality
            });

        case TableType::PART:
            // p_type has 150 values (exceeds int8 range) — keep utf8 with hint.
            return arrow::schema({
                tpch_field("p_partkey",     int64()),
                tpch_field("p_name",        utf8()),        // high cardinality
                tpch_field("p_mfgr",        dict8),        // 5 values
                tpch_field("p_brand",       dict8),        // 25 values
                tpch_field("p_type",        utf8(),  150), // 150 values, keep hint
                tpch_field("p_size",        int64()),
                tpch_field("p_container",   dict8),        // 40 values
                tpch_field("p_retailprice", float64()),
                tpch_field("p_comment",     utf8()),        // high cardinality
            });

        case TableType::PARTSUPP:
            return arrow::schema({
                tpch_field("ps_partkey",    int64()),
                tpch_field("ps_suppkey",    int64()),
                tpch_field("ps_availqty",   int64()),
                tpch_field("ps_supplycost", float64()),
                tpch_field("ps_comment",    utf8()),   // high cardinality
            });

        case TableType::SUPPLIER:
            // nations: 25 values
            return arrow::schema({
                tpch_field("s_suppkey",   int64()),
                tpch_field("s_name",      utf8()),    // Supplier#XXXXXXXXX - high cardinality
                tpch_field("s_address",   utf8()),    // high cardinality
                tpch_field("s_nationkey", int64()),
                tpch_field("s_phone",     utf8()),    // unique per supplier
                tpch_field("s_acctbal",   float64()),
                tpch_field("s_comment",   utf8()),    // high cardinality
            });

        case TableType::NATION:
            // nations: 25 values
            return arrow::schema({
                tpch_field("n_nationkey", int64()),
                tpch_field("n_name",      utf8(),  25),
                tpch_field("n_regionkey", int64()),
                tpch_field("n_comment",   utf8()),
            });

        case TableType::REGION:
            // regions: 5 values
            return arrow::schema({
                tpch_field("r_regionkey", int64()),
                tpch_field("r_name",      utf8(),  5),
                tpch_field("r_comment",   utf8()),
            });

        case TableType::COUNT_:
            break;
    }

    return nullptr;
}

// ============================================================================
// Global initialization functions for fork-after-init pattern (Phase 12.6)
// ============================================================================

namespace {
    // Track whether global initialization was performed
    bool g_dbgen_initialized = false;
}

void dbgen_init_global(long scale_factor, bool verbose_flag) {
    if (g_dbgen_initialized) {
        // Already initialized - this is safe to call multiple times
        return;
    }

    // Set global dbgen configuration
    scale = scale_factor;
    verbose = verbose_flag ? 1 : 0;
    force = 0;
    d_path = nullptr;

    // Load distributions from dists.dss (expensive I/O and parsing)
    // This is the heaviest part of initialization
    if (verbose_flag) {
        fprintf(stderr, "dbgen_init_global: Loading distributions...\n");
        fflush(stderr);
    }
    load_dists();
    if (verbose_flag) {
        fprintf(stderr, "dbgen_init_global: Distributions loaded\n");
        fflush(stderr);
    }

    // Pre-cache date array (2557 date strings)
    if (verbose_flag) {
        fprintf(stderr, "dbgen_init_global: Pre-caching date array...\n");
        fflush(stderr);
    }
    char** dates = mk_ascdate();
    if (dates == nullptr) {
        throw std::runtime_error("Failed to allocate date array in global init");
    }
    if (verbose_flag) {
        fprintf(stderr, "dbgen_init_global: Date array cached\n");
        fflush(stderr);
    }

    // Optional: Pre-warm text pool (300MB)
    // This uses Seed[5] which is marked {NONE} - not used by any table
    // Uncomment if you want to pre-generate the text pool before forking
    // char dummy[256];
    // dbg_text(dummy, 100, 200, 5);

    g_dbgen_initialized = true;

    if (verbose_flag) {
        fprintf(stderr, "dbgen_init_global: Initialization complete\n");
        fflush(stderr);
    }
}

bool dbgen_is_initialized() {
    return g_dbgen_initialized;
}

// ============================================================================
// Phase 13.4: Batch generation implementation for zero-copy optimizations
// ============================================================================

DBGenWrapper::LineitemBatchIterator
DBGenWrapper::generate_lineitem_batches(size_t batch_size, size_t max_rows) {
    return LineitemBatchIterator(this, batch_size, max_rows);
}

// Orders batch iterator: implementation provided by BatchIteratorImpl in header
DBGenWrapper::OrdersBatchIterator
DBGenWrapper::generate_orders_batches(size_t batch_size, size_t max_rows) {
    return OrdersBatchIterator(this, batch_size, max_rows);
}

// Customer batch iterator: implementation provided by BatchIteratorImpl in header
DBGenWrapper::CustomerBatchIterator
DBGenWrapper::generate_customer_batches(size_t batch_size, size_t max_rows) {
    return CustomerBatchIterator(this, batch_size, max_rows);
}

// Part batch iterator: implementation provided by BatchIteratorImpl in header
DBGenWrapper::PartBatchIterator
DBGenWrapper::generate_part_batches(size_t batch_size, size_t max_rows) {
    return PartBatchIterator(this, batch_size, max_rows);
}

DBGenWrapper::PartsuppBatchIterator
DBGenWrapper::generate_partsupp_batches(size_t batch_size, size_t max_rows) {
    return PartsuppBatchIterator(this, batch_size, max_rows);
}

// Supplier batch iterator: implementation provided by BatchIteratorImpl in header
DBGenWrapper::SupplierBatchIterator
DBGenWrapper::generate_supplier_batches(size_t batch_size, size_t max_rows) {
    return SupplierBatchIterator(this, batch_size, max_rows);
}

// Nation batch iterator: implementation provided by BatchIteratorImpl in header
DBGenWrapper::NationBatchIterator
DBGenWrapper::generate_nation_batches(size_t batch_size, size_t max_rows) {
    return NationBatchIterator(this, batch_size, max_rows);
}

// Region batch iterator: implementation provided by BatchIteratorImpl in header
DBGenWrapper::RegionBatchIterator
DBGenWrapper::generate_region_batches(size_t batch_size, size_t max_rows) {
    return RegionBatchIterator(this, batch_size, max_rows);
}

}  // namespace tpch

