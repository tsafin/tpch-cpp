#include "tpch/dbgen_wrapper.hpp"

#include <cstdint>
#include <iostream>
#include <stdexcept>

// Declare dbgen C types and functions (avoiding full header includes)
// These are the public interfaces from dbgen
extern "C" {

// Type definitions from dsstypes.h
typedef long long DSS_HUGE;

#define DATE_LEN 11
#define PHONE_LEN 15
#define C_NAME_LEN 18
#define C_ADDR_MAX 40
#define MAXAGG_LEN 14
#define C_CMNT_MAX 117
#define L_CMNT_MAX 44
#define O_CLRK_LEN 15
#define O_LCNT_MAX 7
#define O_CMNT_MAX 79
#define PS_CMNT_MAX 124
#define P_NAME_LEN 55
#define P_MFG_LEN 25
#define P_BRND_LEN 10
#define P_TYPE_LEN 25
#define P_CNTR_LEN 10
#define P_CMNT_MAX 23
#define S_NAME_LEN 25
#define S_ADDR_MAX 40
#define S_CMNT_MAX 101
#define N_CMNT_MAX 114
#define SUPP_PER_PART 4

// From dss.h constants
#define PART 0
#define PSUPP 1
#define SUPP 2
#define CUST 3
#define ORDER 4
#define LINE 5
#define NATION 8
#define REGION 9

// Customer structure
typedef struct {
    DSS_HUGE custkey;
    char name[C_NAME_LEN + 3];
    char address[C_ADDR_MAX + 1];
    int alen;
    DSS_HUGE nation_code;
    char phone[PHONE_LEN + 1];
    DSS_HUGE acctbal;
    char mktsegment[MAXAGG_LEN + 1];
    char comment[C_CMNT_MAX + 1];
    int clen;
} customer_t;

// Lineitem structure
typedef struct {
    DSS_HUGE okey;
    DSS_HUGE partkey;
    DSS_HUGE suppkey;
    DSS_HUGE lcnt;
    DSS_HUGE quantity;
    DSS_HUGE eprice;
    DSS_HUGE discount;
    DSS_HUGE tax;
    char rflag[1];
    char lstatus[1];
    char cdate[DATE_LEN];
    char sdate[DATE_LEN];
    char rdate[DATE_LEN];
    char shipinstruct[MAXAGG_LEN + 1];
    char shipmode[MAXAGG_LEN + 1];
    char comment[L_CMNT_MAX + 1];
    int clen;
} line_t;

// Order structure
typedef struct {
    DSS_HUGE okey;
    DSS_HUGE custkey;
    char orderstatus;
    DSS_HUGE totalprice;
    char odate[DATE_LEN];
    char opriority[MAXAGG_LEN + 1];
    char clerk[O_CLRK_LEN + 1];
    long spriority;
    DSS_HUGE lines;
    char comment[O_CMNT_MAX + 1];
    int clen;
    line_t l[O_LCNT_MAX];
} order_t;

// Part structure
typedef struct {
    DSS_HUGE partkey;
    char name[P_NAME_LEN + 1];
    int nlen;
    char mfgr[P_MFG_LEN + 1];
    char brand[P_BRND_LEN + 1];
    char type[P_TYPE_LEN + 1];
    int tlen;
    DSS_HUGE size;
    char container[P_CNTR_LEN + 1];
    DSS_HUGE retailprice;
    char comment[P_CMNT_MAX + 1];
    int clen;
} part_t;

// Partsupp structure
typedef struct {
    DSS_HUGE partkey;
    DSS_HUGE suppkey;
    DSS_HUGE qty;
    DSS_HUGE scost;
    char comment[PS_CMNT_MAX + 1];
    int clen;
} partsupp_t;

// Supplier structure
typedef struct {
    DSS_HUGE suppkey;
    char name[S_NAME_LEN + 1];
    char address[S_ADDR_MAX + 1];
    int alen;
    DSS_HUGE nation_code;
    char phone[PHONE_LEN + 1];
    DSS_HUGE acctbal;
    char comment[S_CMNT_MAX + 1];
    int clen;
} supplier_t;

// Code structure (for nation/region)
typedef struct {
    DSS_HUGE code;
    char *text;
    long join;
    char comment[N_CMNT_MAX + 1];
    int clen;
} code_t;

// dbgen global variables
extern long scale;
extern int verbose;
extern long force;
extern char *d_path;

// dbgen functions
extern void dss_random(DSS_HUGE *tgt, DSS_HUGE min, DSS_HUGE max, long seed);
extern void row_start(int t);
extern void row_stop(int t);

// Table building functions
extern long mk_cust(DSS_HUGE n_cust, customer_t *c);
extern long mk_order(DSS_HUGE index, order_t *o, long upd_num);
extern long mk_part(DSS_HUGE index, part_t *p);
extern long mk_supp(DSS_HUGE index, supplier_t *s);
extern long mk_nation(DSS_HUGE i, code_t *c);
extern long mk_region(DSS_HUGE i, code_t *c);

// Partsupp building is done inside mk_part, so we extract from part_t

}  // extern "C"

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

DBGenWrapper::DBGenWrapper(long scale_factor)
    : scale_factor_(scale_factor), initialized_(false) {
    if (scale_factor <= 0) {
        throw std::invalid_argument("Scale factor must be positive");
    }
}

DBGenWrapper::~DBGenWrapper() = default;

DBGenWrapper::DBGenWrapper(DBGenWrapper&& other) noexcept
    : scale_factor_(other.scale_factor_), initialized_(other.initialized_) {
    other.initialized_ = false;
}

DBGenWrapper& DBGenWrapper::operator=(DBGenWrapper&& other) noexcept {
    if (this != &other) {
        scale_factor_ = other.scale_factor_;
        initialized_ = other.initialized_;
        other.initialized_ = false;
    }
    return *this;
}

void DBGenWrapper::init_dbgen() {
    // Set global dbgen state
    // dbgen uses global variables for configuration
    scale = scale_factor_;
    verbose = 0;  // Suppress dbgen output
    force = 0;
    d_path = nullptr;  // Use current directory
    initialized_ = true;
}

unsigned long DBGenWrapper::get_seed(int table_id) {
    // TPC-H seed generation based on table ID
    // Each table gets a deterministic seed based on its type
    return (123456789UL + table_id * 999999991UL) % (1UL << 31);
}

void DBGenWrapper::generate_lineitem(
    std::function<void(const void* row)> callback,
    long max_rows) {

    if (!initialized_) {
        init_dbgen();
    }

    row_start(LINE);

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
                row_stop(LINE);
                return;
            }
        }
    }

    row_stop(LINE);
}

void DBGenWrapper::generate_orders(
    std::function<void(const void* row)> callback,
    long max_rows) {

    if (!initialized_) {
        init_dbgen();
    }

    row_start(ORDER);

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
            row_stop(ORDER);
            return;
        }
    }

    row_stop(ORDER);
}

void DBGenWrapper::generate_customer(
    std::function<void(const void* row)> callback,
    long max_rows) {

    if (!initialized_) {
        init_dbgen();
    }

    row_start(CUST);

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
            row_stop(CUST);
            return;
        }
    }

    row_stop(CUST);
}

void DBGenWrapper::generate_part(
    std::function<void(const void* row)> callback,
    long max_rows) {

    if (!initialized_) {
        init_dbgen();
    }

    row_start(PART);

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
            row_stop(PART);
            return;
        }
    }

    row_stop(PART);
}

void DBGenWrapper::generate_partsupp(
    std::function<void(const void* row)> callback,
    long max_rows) {

    if (!initialized_) {
        init_dbgen();
    }

    row_start(PSUPP);

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
                row_stop(PSUPP);
                return;
            }
        }
    }

    row_stop(PSUPP);
}

void DBGenWrapper::generate_supplier(
    std::function<void(const void* row)> callback,
    long max_rows) {

    if (!initialized_) {
        init_dbgen();
    }

    row_start(SUPP);

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
            row_stop(SUPP);
            return;
        }
    }

    row_stop(SUPP);
}

void DBGenWrapper::generate_nation(
    std::function<void(const void* row)> callback) {

    if (!initialized_) {
        init_dbgen();
    }

    row_start(NATION);

    code_t nation;
    for (DSS_HUGE i = 0; i < 25; ++i) {
        if (mk_nation(i, &nation) < 0) {
            break;
        }

        if (callback) {
            callback(&nation);
        }
    }

    row_stop(NATION);
}

void DBGenWrapper::generate_region(
    std::function<void(const void* row)> callback) {

    if (!initialized_) {
        init_dbgen();
    }

    row_start(REGION);

    code_t region;
    for (DSS_HUGE i = 0; i < 5; ++i) {
        if (mk_region(i, &region) < 0) {
            break;
        }

        if (callback) {
            callback(&region);
        }
    }

    row_stop(REGION);
}

void DBGenWrapper::generate_all_tables(
    std::function<void(const char* table_name, const void* row)> callback) {

    // Generate each table
    generate_customer([this, callback](const void* row) {
        callback("customer", row);
    });

    generate_supplier([this, callback](const void* row) {
        callback("supplier", row);
    });

    generate_part([this, callback](const void* row) {
        callback("part", row);
    });

    generate_partsupp([this, callback](const void* row) {
        callback("partsupp", row);
    });

    generate_nation([this, callback](const void* row) {
        callback("nation", row);
    });

    generate_region([this, callback](const void* row) {
        callback("region", row);
    });

    generate_orders([this, callback](const void* row) {
        callback("orders", row);
    });

    generate_lineitem([this, callback](const void* row) {
        callback("lineitem", row);
    });
}

std::shared_ptr<arrow::Schema> DBGenWrapper::get_schema(TableType table) {
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
