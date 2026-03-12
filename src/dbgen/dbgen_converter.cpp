#include "tpch/dbgen_converter.hpp"
#include "tpch/dbgen_wrapper.hpp"
#include "tpch/performance_counters.hpp"
#include "tpch/simd_string_utils.hpp"
#include "tpch/zero_copy_converter.hpp"  // Phase 3.3: encode functions for dict columns

#include <string>
#include <stdexcept>
#include <arrow/builder.h>

// Include the embeddable tpch_dbgen.h header which defines all types
extern "C" {
#include "tpch_dbgen.h"
}

namespace tpch {

void append_lineitem_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    TPCH_SCOPED_TIMER("arrow_append_lineitem");

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

    // Quantity: convert to double (dbgen stores as integer, divide by 100 for cents)
    static_cast<arrow::DoubleBuilder*>(builders["l_quantity"].get())
        ->Append(static_cast<double>(line->quantity) / 100.0);

    // Extended price, discount, tax: already double-compatible values (divide by 100)
    static_cast<arrow::DoubleBuilder*>(builders["l_extendedprice"].get())
        ->Append(static_cast<double>(line->eprice) / 100.0);
    static_cast<arrow::DoubleBuilder*>(builders["l_discount"].get())
        ->Append(static_cast<double>(line->discount) / 100.0);
    static_cast<arrow::DoubleBuilder*>(builders["l_tax"].get())
        ->Append(static_cast<double>(line->tax) / 100.0);

    // Dict-encoded low-cardinality fields (Phase 3.3)
    static_cast<arrow::Int8Builder*>(builders["l_returnflag"].get())
        ->Append(tpch::ZeroCopyConverter::encode_returnflag(line->rflag[0]));
    static_cast<arrow::Int8Builder*>(builders["l_linestatus"].get())
        ->Append(tpch::ZeroCopyConverter::encode_linestatus(line->lstatus[0]));

    // Date fields: dict16-encoded (2556 unique values, fit in int16)
    static_cast<arrow::Int16Builder*>(builders["l_commitdate"].get())
        ->Append(tpch::ZeroCopyConverter::encode_date(line->cdate));
    static_cast<arrow::Int16Builder*>(builders["l_shipdate"].get())
        ->Append(tpch::ZeroCopyConverter::encode_date(line->sdate));
    static_cast<arrow::Int16Builder*>(builders["l_receiptdate"].get())
        ->Append(tpch::ZeroCopyConverter::encode_date(line->rdate));

    // Dict-encoded ship instruction and mode
    static_cast<arrow::Int8Builder*>(builders["l_shipinstruct"].get())
        ->Append(tpch::ZeroCopyConverter::encode_shipinstruct(line->shipinstruct));
    static_cast<arrow::Int8Builder*>(builders["l_shipmode"].get())
        ->Append(tpch::ZeroCopyConverter::encode_shipmode(line->shipmode));

    // Comment: utf8 (high cardinality)
    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["l_comment"].get());
    comment_builder->Append(std::string(line->comment, line->clen));
}

void append_orders_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    TPCH_SCOPED_TIMER("arrow_append_orders");

    auto* order = static_cast<const order_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["o_orderkey"].get())
        ->Append(order->okey);
    static_cast<arrow::Int64Builder*>(builders["o_custkey"].get())
        ->Append(order->custkey);

    static_cast<arrow::Int8Builder*>(builders["o_orderstatus"].get())
        ->Append(tpch::ZeroCopyConverter::encode_orderstatus(order->orderstatus));

    static_cast<arrow::DoubleBuilder*>(builders["o_totalprice"].get())
        ->Append(static_cast<double>(order->totalprice) / 100.0);

    // orderdate: dict16-encoded (2556 unique values, fit in int16)
    static_cast<arrow::Int16Builder*>(builders["o_orderdate"].get())
        ->Append(tpch::ZeroCopyConverter::encode_date(order->odate));

    static_cast<arrow::Int8Builder*>(builders["o_orderpriority"].get())
        ->Append(tpch::ZeroCopyConverter::encode_orderpriority(order->opriority));

    auto* clerk_builder = static_cast<arrow::StringBuilder*>(builders["o_clerk"].get());
    clerk_builder->Append(order->clerk, simd::strlen_sse42_unaligned(order->clerk));

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
    name_builder->Append(cust->name, simd::strlen_sse42_unaligned(cust->name));

    auto* addr_builder = static_cast<arrow::StringBuilder*>(builders["c_address"].get());
    addr_builder->Append(cust->address, cust->alen);

    static_cast<arrow::Int64Builder*>(builders["c_nationkey"].get())
        ->Append(cust->nation_code);

    auto* phone_builder = static_cast<arrow::StringBuilder*>(builders["c_phone"].get());
    phone_builder->Append(cust->phone, simd::strlen_sse42_unaligned(cust->phone));

    static_cast<arrow::DoubleBuilder*>(builders["c_acctbal"].get())
        ->Append(static_cast<double>(cust->acctbal) / 100.0);

    static_cast<arrow::Int8Builder*>(builders["c_mktsegment"].get())
        ->Append(tpch::ZeroCopyConverter::encode_mktsegment(cust->mktsegment));

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
    name_builder->Append(part->name, simd::strlen_sse42_unaligned(part->name));

    static_cast<arrow::Int8Builder*>(builders["p_mfgr"].get())
        ->Append(tpch::ZeroCopyConverter::encode_mfgr(part->mfgr));

    static_cast<arrow::Int8Builder*>(builders["p_brand"].get())
        ->Append(tpch::ZeroCopyConverter::encode_brand(part->brand));

    // p_type: dict16-encoded (150 values)
    static_cast<arrow::Int16Builder*>(builders["p_type"].get())
        ->Append(tpch::ZeroCopyConverter::encode_ptype(part->type));

    static_cast<arrow::Int64Builder*>(builders["p_size"].get())
        ->Append(part->size);

    static_cast<arrow::Int8Builder*>(builders["p_container"].get())
        ->Append(tpch::ZeroCopyConverter::encode_container(part->container));

    static_cast<arrow::DoubleBuilder*>(builders["p_retailprice"].get())
        ->Append(static_cast<double>(part->retailprice) / 100.0);

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["p_comment"].get());
    comment_builder->Append(std::string(part->comment, part->clen));
}

void append_partsupp_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* psupp = static_cast<const partsupp_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["ps_partkey"].get())
        ->Append(psupp->partkey);

    static_cast<arrow::Int64Builder*>(builders["ps_suppkey"].get())
        ->Append(psupp->suppkey);

    static_cast<arrow::Int64Builder*>(builders["ps_availqty"].get())
        ->Append(psupp->qty);

    static_cast<arrow::DoubleBuilder*>(builders["ps_supplycost"].get())
        ->Append(static_cast<double>(psupp->scost) / 100.0);

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["ps_comment"].get());
    comment_builder->Append(std::string(psupp->comment, psupp->clen));
}

void append_supplier_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* supp = static_cast<const supplier_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["s_suppkey"].get())
        ->Append(supp->suppkey);

    auto* name_builder = static_cast<arrow::StringBuilder*>(builders["s_name"].get());
    name_builder->Append(supp->name, simd::strlen_sse42_unaligned(supp->name));

    auto* addr_builder = static_cast<arrow::StringBuilder*>(builders["s_address"].get());
    addr_builder->Append(supp->address, supp->alen);

    static_cast<arrow::Int64Builder*>(builders["s_nationkey"].get())
        ->Append(supp->nation_code);

    auto* phone_builder = static_cast<arrow::StringBuilder*>(builders["s_phone"].get());
    phone_builder->Append(supp->phone, simd::strlen_sse42_unaligned(supp->phone));

    static_cast<arrow::DoubleBuilder*>(builders["s_acctbal"].get())
        ->Append(static_cast<double>(supp->acctbal) / 100.0);

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["s_comment"].get());
    comment_builder->Append(std::string(supp->comment, supp->clen));
}

void append_nation_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* nation = static_cast<const code_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["n_nationkey"].get())
        ->Append(nation->code);

    auto* name_builder = static_cast<arrow::StringBuilder*>(builders["n_name"].get());
    if (nation->text) {
        name_builder->Append(nation->text, simd::strlen_sse42_unaligned(nation->text));
    } else {
        name_builder->AppendNull();
    }

    static_cast<arrow::Int64Builder*>(builders["n_regionkey"].get())
        ->Append(nation->join);

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["n_comment"].get());
    comment_builder->Append(std::string(nation->comment, nation->clen));
}

void append_region_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* region = static_cast<const code_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["r_regionkey"].get())
        ->Append(region->code);

    auto* name_builder = static_cast<arrow::StringBuilder*>(builders["r_name"].get());
    if (region->text) {
        name_builder->Append(region->text, simd::strlen_sse42_unaligned(region->text));
    } else {
        name_builder->AppendNull();
    }

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["r_comment"].get());
    comment_builder->Append(std::string(region->comment, region->clen));
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
