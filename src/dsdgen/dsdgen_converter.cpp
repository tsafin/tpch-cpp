/**
 * dsdgen_converter.cpp — Convert dsdgen C structs to Arrow array builders.
 *
 * Uses dectof() to convert decimal_t (scaled integer) fields to double.
 * ds_key_t (= int64_t on Linux) is mapped to arrow::int64().
 */

#include "tpch/dsdgen_converter.hpp"

#include <stdexcept>
#include <arrow/builder.h>

extern "C" {
#include "tpcds_dsdgen.h"
}

namespace tpcds {

// ---------------------------------------------------------------------------
// Helper: decimal_t → double
//
// dsdgen stores decimals as scaled integers: number = value * 10^precision.
// Example: "12.34" → scale=2, precision=2, number=1234.
// Conversion: (double)number / 10^precision.
//
// NOTE: dectoflt() in decimal.c is buggy (divides by 10^(precision-1) and
// mutates the struct).  We implement the correct formula here.
// ---------------------------------------------------------------------------

static inline double dec_to_double(const decimal_t* d) {
    if (d->precision == 0) return static_cast<double>(d->number);
    double result = static_cast<double>(d->number);
    for (int i = 0; i < d->precision; ++i) {
        result /= 10.0;
    }
    return result;
}

// ---------------------------------------------------------------------------
// store_sales
// ---------------------------------------------------------------------------

void append_store_sales_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_STORE_SALES_TBL*>(row);

    // Surrogate keys (int64)
    static_cast<arrow::Int64Builder*>(builders["ss_sold_date_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_date_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_sold_time_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_time_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_item_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_item_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_store_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_store_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_promo_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_promo_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_ticket_number"].get())
        ->Append(static_cast<int64_t>(r->ss_ticket_number));

    // Quantity (int)
    static_cast<arrow::Int32Builder*>(builders["ss_quantity"].get())
        ->Append(static_cast<int32_t>(r->ss_pricing.quantity));

    // Decimal pricing fields → double
    const ds_pricing_t* p = &r->ss_pricing;

    static_cast<arrow::DoubleBuilder*>(builders["ss_wholesale_cost"].get())
        ->Append(dec_to_double(&p->wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ss_list_price"].get())
        ->Append(dec_to_double(&p->list_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_sales_price"].get())
        ->Append(dec_to_double(&p->sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_discount_amt"].get())
        ->Append(dec_to_double(&p->ext_discount_amt));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_sales_price"].get())
        ->Append(dec_to_double(&p->ext_sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_wholesale_cost"].get())
        ->Append(dec_to_double(&p->ext_wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_list_price"].get())
        ->Append(dec_to_double(&p->ext_list_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ss_coupon_amt"].get())
        ->Append(dec_to_double(&p->coupon_amt));
    static_cast<arrow::DoubleBuilder*>(builders["ss_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["ss_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ss_net_profit"].get())
        ->Append(dec_to_double(&p->net_profit));
}

// ---------------------------------------------------------------------------
// inventory
// ---------------------------------------------------------------------------

void append_inventory_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    auto* r = static_cast<const W_INVENTORY_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["inv_date_sk"].get())
        ->Append(static_cast<int64_t>(r->inv_date_sk));
    static_cast<arrow::Int64Builder*>(builders["inv_item_sk"].get())
        ->Append(static_cast<int64_t>(r->inv_item_sk));
    static_cast<arrow::Int64Builder*>(builders["inv_warehouse_sk"].get())
        ->Append(static_cast<int64_t>(r->inv_warehouse_sk));
    static_cast<arrow::Int32Builder*>(builders["inv_quantity_on_hand"].get())
        ->Append(static_cast<int32_t>(r->inv_quantity_on_hand));
}

// ---------------------------------------------------------------------------
// Generic dispatcher
// ---------------------------------------------------------------------------

void append_dsdgen_row_to_builders(
    const std::string& tbl_name,
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    if (tbl_name == "store_sales") {
        append_store_sales_to_builders(row, builders);
    } else if (tbl_name == "inventory") {
        append_inventory_to_builders(row, builders);
    } else {
        throw std::invalid_argument("append_dsdgen_row_to_builders: unknown table: " + tbl_name);
    }
}

}  // namespace tpcds
