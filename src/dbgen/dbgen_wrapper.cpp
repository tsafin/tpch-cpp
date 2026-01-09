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
    : scale_factor_(scale_factor), initialized_(false), verbose_(verbose), asc_dates_(nullptr) {
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

    row_start(DBGEN_LINE);

    long rows_generated = 0;

    // LineItem rows are generated implicitly via order generation
    // Each order has between 1-7 line items
    order_t order;
    for (DSS_HUGE i = 1; i <= get_row_count(TableType::ORDERS, scale_factor_); ++i) {
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

    if (!initialized_) {
        init_dbgen();
    }

    dbgen_reset_seeds();
    row_start(DBGEN_ORDER);

    long rows_generated = 0;
    long total_rows = get_row_count(TableType::ORDERS, scale_factor_);

    order_t order;
    for (DSS_HUGE i = 1; i <= total_rows; ++i) {
        if (mk_order(i, &order, 0) < 0) {
            break;
        }

        if (callback) {
            callback(&order);
        }
        rows_generated++;

        if (max_rows > 0 && rows_generated >= max_rows) {
            row_stop(DBGEN_ORDER);
            return;
        }
    }

    row_stop(DBGEN_ORDER);
}

void tpch::DBGenWrapper::generate_customer(
    std::function<void(const void* row)> callback,
    long max_rows) {

    if (!initialized_) {
        init_dbgen();
    }

    dbgen_reset_seeds();
    row_start(DBGEN_CUST);

    long rows_generated = 0;
    long total_rows = get_row_count(TableType::CUSTOMER, scale_factor_);

    customer_t customer;
    for (DSS_HUGE i = 1; i <= total_rows; ++i) {
        if (mk_cust(i, &customer) < 0) {
            break;
        }

        if (callback) {
            callback(&customer);
        }
        rows_generated++;

        if (max_rows > 0 && rows_generated >= max_rows) {
            row_stop(DBGEN_CUST);
            return;
        }
    }

    row_stop(DBGEN_CUST);
}

void tpch::DBGenWrapper::generate_part(
    std::function<void(const void* row)> callback,
    long max_rows) {

    if (!initialized_) {
        init_dbgen();
    }

    dbgen_reset_seeds();
    row_start(DBGEN_PART);

    long rows_generated = 0;
    long total_rows = get_row_count(TableType::PART, scale_factor_);

    part_t part;
    for (DSS_HUGE i = 1; i <= total_rows; ++i) {
        if (mk_part(i, &part) < 0) {
            break;
        }

        if (callback) {
            callback(&part);
        }
        rows_generated++;

        if (max_rows > 0 && rows_generated >= max_rows) {
            row_stop(DBGEN_PART);
            return;
        }
    }

    row_stop(DBGEN_PART);
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

    part_t part;
    for (DSS_HUGE i = 1; i <= total_rows_part; ++i) {
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

    if (!initialized_) {
        init_dbgen();
    }

    dbgen_reset_seeds();
    row_start(DBGEN_SUPP);

    long rows_generated = 0;
    long total_rows = get_row_count(TableType::SUPPLIER, scale_factor_);

    supplier_t supplier;
    for (DSS_HUGE i = 1; i <= total_rows; ++i) {
        if (mk_supp(i, &supplier) < 0) {
            break;
        }

        if (callback) {
            callback(&supplier);
        }
        rows_generated++;

        if (max_rows > 0 && rows_generated >= max_rows) {
            row_stop(DBGEN_SUPP);
            return;
        }
    }

    row_stop(DBGEN_SUPP);
}

void tpch::DBGenWrapper::generate_nation(
    std::function<void(const void* row)> callback) {

    if (!initialized_) {
        init_dbgen();
    }

    dbgen_reset_seeds();
    row_start(DBGEN_NATION);

    code_t nation;
    for (DSS_HUGE i = 1; i <= 25; ++i) {
        if (mk_nation(i, &nation) < 0) {
            break;
        }

        if (callback) {
            callback(&nation);
        }
    }

    row_stop(DBGEN_NATION);
}

void tpch::DBGenWrapper::generate_region(
    std::function<void(const void* row)> callback) {

    if (!initialized_) {
        init_dbgen();
    }

    dbgen_reset_seeds();
    row_start(DBGEN_REGION);

    code_t region;
    for (DSS_HUGE i = 1; i <= 5; ++i) {
        if (mk_region(i, &region) < 0) {
            break;
        }

        if (callback) {
            callback(&region);
        }
    }

    row_stop(DBGEN_REGION);
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

std::shared_ptr<arrow::Schema> tpch::DBGenWrapper::get_schema(TableType table) {
    using arrow::field;
    using arrow::int64;
    using arrow::float64;
    using arrow::utf8;

    switch (table) {
        case TableType::LINEITEM:
            return arrow::schema({
                field("l_orderkey", int64()),
                field("l_partkey", int64()),
                field("l_suppkey", int64()),
                field("l_linenumber", int64()),
                field("l_quantity", float64()),
                field("l_extendedprice", float64()),
                field("l_discount", float64()),
                field("l_tax", float64()),
                field("l_returnflag", utf8()),
                field("l_linestatus", utf8()),
                field("l_commitdate", utf8()),
                field("l_shipdate", utf8()),
                field("l_receiptdate", utf8()),
                field("l_shipinstruct", utf8()),
                field("l_shipmode", utf8()),
                field("l_comment", utf8()),
            });

        case TableType::ORDERS:
            return arrow::schema({
                field("o_orderkey", int64()),
                field("o_custkey", int64()),
                field("o_orderstatus", utf8()),
                field("o_totalprice", float64()),
                field("o_orderdate", utf8()),
                field("o_orderpriority", utf8()),
                field("o_clerk", utf8()),
                field("o_shippriority", int64()),
                field("o_comment", utf8()),
            });

        case TableType::CUSTOMER:
            return arrow::schema({
                field("c_custkey", int64()),
                field("c_name", utf8()),
                field("c_address", utf8()),
                field("c_nationkey", int64()),
                field("c_phone", utf8()),
                field("c_acctbal", float64()),
                field("c_mktsegment", utf8()),
                field("c_comment", utf8()),
            });

        case TableType::PART:
            return arrow::schema({
                field("p_partkey", int64()),
                field("p_name", utf8()),
                field("p_mfgr", utf8()),
                field("p_brand", utf8()),
                field("p_type", utf8()),
                field("p_size", int64()),
                field("p_container", utf8()),
                field("p_retailprice", float64()),
                field("p_comment", utf8()),
            });

        case TableType::PARTSUPP:
            return arrow::schema({
                field("ps_partkey", int64()),
                field("ps_suppkey", int64()),
                field("ps_availqty", int64()),
                field("ps_supplycost", float64()),
                field("ps_comment", utf8()),
            });

        case TableType::SUPPLIER:
            return arrow::schema({
                field("s_suppkey", int64()),
                field("s_name", utf8()),
                field("s_address", utf8()),
                field("s_nationkey", int64()),
                field("s_phone", utf8()),
                field("s_acctbal", float64()),
                field("s_comment", utf8()),
            });

        case TableType::NATION:
            return arrow::schema({
                field("n_nationkey", int64()),
                field("n_name", utf8()),
                field("n_regionkey", int64()),
                field("n_comment", utf8()),
            });

        case TableType::REGION:
            return arrow::schema({
                field("r_regionkey", int64()),
                field("r_name", utf8()),
                field("r_comment", utf8()),
            });

        case TableType::COUNT_:
            break;
    }

    return nullptr;
}

}  // namespace tpch

