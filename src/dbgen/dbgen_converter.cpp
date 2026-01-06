#include "tpch/dbgen_converter.hpp"

// Define constants needed by dsstypes.h
#define DSS_HUGE long long
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

// Forward declare the structs from dbgen
extern "C" {
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
    } order_t;

    typedef struct {
        DSS_HUGE partkey;
        DSS_HUGE suppkey;
        DSS_HUGE qty;
        DSS_HUGE scost;
        char comment[PS_CMNT_MAX + 1];
        int clen;
    } partsupp_t;

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
        partsupp_t s[SUPP_PER_PART];
    } part_t;

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

    typedef struct {
        DSS_HUGE code;
        char *text;
        long join;
        char comment[N_CMNT_MAX + 1];
        int clen;
    } code_t;
}

#include <stdexcept>
#include <string>

namespace tpch {

void append_lineitem_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* line = static_cast<const line_t*>(row);

    // Append each field to corresponding builder
    static_cast<arrow::Int64Builder*>(builders["l_orderkey"].get())
        ->Append(line->okey);
    static_cast<arrow::Int64Builder*>(builders["l_partkey"].get())
        ->Append(line->partkey);
    static_cast<arrow::Int64Builder*>(builders["l_suppkey"].get())
        ->Append(line->suppkey);
    static_cast<arrow::Int64Builder*>(builders["l_linenumber"].get())
        ->Append(line->lcnt);

    // Quantity: convert to double (dbgen stores as cents)
    static_cast<arrow::DoubleBuilder*>(builders["l_quantity"].get())
        ->Append(static_cast<double>(line->quantity) / 100.0);

    // Extended price, discount, tax: convert from cents
    static_cast<arrow::DoubleBuilder*>(builders["l_extendedprice"].get())
        ->Append(static_cast<double>(line->eprice) / 100.0);
    static_cast<arrow::DoubleBuilder*>(builders["l_discount"].get())
        ->Append(static_cast<double>(line->discount) / 100.0);
    static_cast<arrow::DoubleBuilder*>(builders["l_tax"].get())
        ->Append(static_cast<double>(line->tax) / 100.0);

    // String fields: convert char arrays to UTF8
    auto* rflag_builder = static_cast<arrow::StringBuilder*>(builders["l_returnflag"].get());
    rflag_builder->Append(std::string(line->rflag, 1));

    auto* lstatus_builder = static_cast<arrow::StringBuilder*>(builders["l_linestatus"].get());
    lstatus_builder->Append(std::string(line->lstatus, 1));

    // Date fields: extract null-terminated strings
    auto* commitdate_builder = static_cast<arrow::StringBuilder*>(builders["l_commitdate"].get());
    commitdate_builder->Append(std::string(line->cdate));

    auto* shipdate_builder = static_cast<arrow::StringBuilder*>(builders["l_shipdate"].get());
    shipdate_builder->Append(std::string(line->sdate));

    auto* receiptdate_builder = static_cast<arrow::StringBuilder*>(builders["l_receiptdate"].get());
    receiptdate_builder->Append(std::string(line->rdate));

    // Ship instructions and mode
    auto* shipinstruct_builder = static_cast<arrow::StringBuilder*>(builders["l_shipinstruct"].get());
    shipinstruct_builder->Append(std::string(line->shipinstruct));

    auto* shipmode_builder = static_cast<arrow::StringBuilder*>(builders["l_shipmode"].get());
    shipmode_builder->Append(std::string(line->shipmode));

    // Comment: use clen to extract exact string length
    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["l_comment"].get());
    comment_builder->Append(std::string(line->comment, line->clen));
}

void append_orders_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* order = static_cast<const order_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["o_orderkey"].get())
        ->Append(order->okey);
    static_cast<arrow::Int64Builder*>(builders["o_custkey"].get())
        ->Append(order->custkey);

    auto* orderstatus_builder = static_cast<arrow::StringBuilder*>(builders["o_orderstatus"].get());
    orderstatus_builder->Append(std::string(&order->orderstatus, 1));

    static_cast<arrow::DoubleBuilder*>(builders["o_totalprice"].get())
        ->Append(static_cast<double>(order->totalprice) / 100.0);

    auto* odate_builder = static_cast<arrow::StringBuilder*>(builders["o_orderdate"].get());
    odate_builder->Append(std::string(order->odate));

    auto* priority_builder = static_cast<arrow::StringBuilder*>(builders["o_orderpriority"].get());
    priority_builder->Append(std::string(order->opriority));

    auto* clerk_builder = static_cast<arrow::StringBuilder*>(builders["o_clerk"].get());
    clerk_builder->Append(std::string(order->clerk));

    static_cast<arrow::Int64Builder*>(builders["o_shippriority"].get())
        ->Append(order->spriority);

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["o_comment"].get());
    comment_builder->Append(std::string(order->comment, order->clen));
}

void append_customer_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* cust = static_cast<const customer_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["c_custkey"].get())
        ->Append(cust->custkey);

    auto* name_builder = static_cast<arrow::StringBuilder*>(builders["c_name"].get());
    name_builder->Append(std::string(cust->name));

    auto* address_builder = static_cast<arrow::StringBuilder*>(builders["c_address"].get());
    address_builder->Append(std::string(cust->address, cust->alen));

    static_cast<arrow::Int64Builder*>(builders["c_nationkey"].get())
        ->Append(cust->nation_code);

    auto* phone_builder = static_cast<arrow::StringBuilder*>(builders["c_phone"].get());
    phone_builder->Append(std::string(cust->phone));

    static_cast<arrow::DoubleBuilder*>(builders["c_acctbal"].get())
        ->Append(static_cast<double>(cust->acctbal) / 100.0);

    auto* mktseg_builder = static_cast<arrow::StringBuilder*>(builders["c_mktsegment"].get());
    mktseg_builder->Append(std::string(cust->mktsegment));

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["c_comment"].get());
    comment_builder->Append(std::string(cust->comment, cust->clen));
}

void append_part_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* part = static_cast<const part_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["p_partkey"].get())
        ->Append(part->partkey);

    auto* name_builder = static_cast<arrow::StringBuilder*>(builders["p_name"].get());
    name_builder->Append(std::string(part->name, part->nlen));

    auto* mfgr_builder = static_cast<arrow::StringBuilder*>(builders["p_mfgr"].get());
    mfgr_builder->Append(std::string(part->mfgr));

    auto* brand_builder = static_cast<arrow::StringBuilder*>(builders["p_brand"].get());
    brand_builder->Append(std::string(part->brand));

    auto* type_builder = static_cast<arrow::StringBuilder*>(builders["p_type"].get());
    type_builder->Append(std::string(part->type, part->tlen));

    static_cast<arrow::Int64Builder*>(builders["p_size"].get())
        ->Append(part->size);

    auto* container_builder = static_cast<arrow::StringBuilder*>(builders["p_container"].get());
    container_builder->Append(std::string(part->container));

    static_cast<arrow::DoubleBuilder*>(builders["p_retailprice"].get())
        ->Append(static_cast<double>(part->retailprice) / 100.0);

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["p_comment"].get());
    comment_builder->Append(std::string(part->comment, part->clen));
}

void append_partsupp_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* ps = static_cast<const partsupp_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["ps_partkey"].get())
        ->Append(ps->partkey);
    static_cast<arrow::Int64Builder*>(builders["ps_suppkey"].get())
        ->Append(ps->suppkey);
    static_cast<arrow::Int64Builder*>(builders["ps_availqty"].get())
        ->Append(ps->qty);

    static_cast<arrow::DoubleBuilder*>(builders["ps_supplycost"].get())
        ->Append(static_cast<double>(ps->scost) / 100.0);

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["ps_comment"].get());
    comment_builder->Append(std::string(ps->comment, ps->clen));
}

void append_supplier_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* supp = static_cast<const supplier_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["s_suppkey"].get())
        ->Append(supp->suppkey);

    auto* name_builder = static_cast<arrow::StringBuilder*>(builders["s_name"].get());
    name_builder->Append(std::string(supp->name));

    auto* address_builder = static_cast<arrow::StringBuilder*>(builders["s_address"].get());
    address_builder->Append(std::string(supp->address, supp->alen));

    static_cast<arrow::Int64Builder*>(builders["s_nationkey"].get())
        ->Append(supp->nation_code);

    auto* phone_builder = static_cast<arrow::StringBuilder*>(builders["s_phone"].get());
    phone_builder->Append(std::string(supp->phone));

    static_cast<arrow::DoubleBuilder*>(builders["s_acctbal"].get())
        ->Append(static_cast<double>(supp->acctbal) / 100.0);

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["s_comment"].get());
    comment_builder->Append(std::string(supp->comment, supp->clen));
}

void append_nation_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* code = static_cast<const code_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["n_nationkey"].get())
        ->Append(code->code);

    auto* name_builder = static_cast<arrow::StringBuilder*>(builders["n_name"].get());
    if (code->text) {
        (void)name_builder->Append(std::string(code->text));
    } else {
        (void)name_builder->AppendNull();
    }

    static_cast<arrow::Int64Builder*>(builders["n_regionkey"].get())
        ->Append(code->join);

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["n_comment"].get());
    (void)comment_builder->Append(std::string(code->comment, code->clen));
}

void append_region_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* region_code = static_cast<const code_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["r_regionkey"].get())
        ->Append(region_code->code);

    auto* name_builder = static_cast<arrow::StringBuilder*>(builders["r_name"].get());
    if (region_code->text) {
        (void)name_builder->Append(std::string(region_code->text));
    } else {
        (void)name_builder->AppendNull();
    }

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["r_comment"].get());
    (void)comment_builder->Append(std::string(region_code->comment, region_code->clen));
}

void append_row_to_builders(
    const std::string& table_name,
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    if (table_name == "lineitem") {
        append_lineitem_to_builders(row, builders);
    } else if (table_name == "orders") {
        append_orders_to_builders(row, builders);
    } else if (table_name == "customer") {
        append_customer_to_builders(row, builders);
    } else if (table_name == "part") {
        append_part_to_builders(row, builders);
    } else if (table_name == "partsupp") {
        append_partsupp_to_builders(row, builders);
    } else if (table_name == "supplier") {
        append_supplier_to_builders(row, builders);
    } else if (table_name == "nation") {
        append_nation_to_builders(row, builders);
    } else if (table_name == "region") {
        append_region_to_builders(row, builders);
    } else {
        throw std::invalid_argument("Unknown table: " + table_name);
    }
}

}  // namespace tpch
