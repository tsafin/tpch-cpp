#include "tpch/dbgen_converter.hpp"
#include "tpch/dbgen_wrapper.hpp"

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

    // String fields: convert char arrays to UTF8
    auto* rflag_builder = static_cast<arrow::StringBuilder*>(builders["l_returnflag"].get());
    rflag_builder->Append(std::string(line->rflag, 1));

    auto* lstatus_builder = static_cast<arrow::StringBuilder*>(builders["l_linestatus"].get());
    lstatus_builder->Append(std::string(line->lstatus, 1));

    // Date fields: extract null-terminated strings
    auto* cdate_builder = static_cast<arrow::StringBuilder*>(builders["l_commitdate"].get());
    cdate_builder->Append(std::string(line->cdate));

    auto* sdate_builder = static_cast<arrow::StringBuilder*>(builders["l_shipdate"].get());
    sdate_builder->Append(std::string(line->sdate));

    auto* rdate_builder = static_cast<arrow::StringBuilder*>(builders["l_receiptdate"].get());
    rdate_builder->Append(std::string(line->rdate));

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

    auto* addr_builder = static_cast<arrow::StringBuilder*>(builders["c_address"].get());
    addr_builder->Append(std::string(cust->address, cust->alen));

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
    name_builder->Append(std::string(supp->name));

    auto* addr_builder = static_cast<arrow::StringBuilder*>(builders["s_address"].get());
    addr_builder->Append(std::string(supp->address, supp->alen));

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

    auto* nation = static_cast<const code_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["n_nationkey"].get())
        ->Append(nation->code);

    auto* name_builder = static_cast<arrow::StringBuilder*>(builders["n_name"].get());
    if (nation->text) {
        name_builder->Append(std::string(nation->text));
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
        name_builder->Append(std::string(region->text));
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
