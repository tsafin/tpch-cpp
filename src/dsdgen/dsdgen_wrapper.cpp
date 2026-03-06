/**
 * dsdgen_wrapper.cpp — C++ wrapper around TPC-DS dsdgen
 *
 * Initialises dsdgen global state using the embedded tpcds.idx binary
 * (compiled into dsts_generated.c) and provides per-table generation methods.
 */

#include "tpch/dsdgen_wrapper.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <unistd.h>

// dsdgen C types and functions — single wrapper header
extern "C" {
#include "tpcds_dsdgen.h"
}

// Embedded distribution data (compiled from tpcds.idx by cmake/gen_dsts.py)
extern "C" {
    extern const uint8_t tpcds_idx_data[];
    extern const size_t  tpcds_idx_size;
}

namespace tpcds {

// ---------------------------------------------------------------------------
// Static helpers
// ---------------------------------------------------------------------------

int DSDGenWrapper::table_id(TableType t) {
    return static_cast<int>(t);
}

std::string DSDGenWrapper::table_name(TableType t) {
    switch (t) {
        case TableType::CALL_CENTER:           return "call_center";
        case TableType::CATALOG_PAGE:          return "catalog_page";
        case TableType::CATALOG_RETURNS:       return "catalog_returns";
        case TableType::CATALOG_SALES:         return "catalog_sales";
        case TableType::CUSTOMER:              return "customer";
        case TableType::CUSTOMER_ADDRESS:      return "customer_address";
        case TableType::CUSTOMER_DEMOGRAPHICS: return "customer_demographics";
        case TableType::DATE_DIM:              return "date_dim";
        case TableType::HOUSEHOLD_DEMOGRAPHICS:return "household_demographics";
        case TableType::INCOME_BAND:           return "income_band";
        case TableType::INVENTORY:             return "inventory";
        case TableType::ITEM:                  return "item";
        case TableType::PROMOTION:             return "promotion";
        case TableType::REASON:                return "reason";
        case TableType::SHIP_MODE:             return "ship_mode";
        case TableType::STORE:                 return "store";
        case TableType::STORE_RETURNS:         return "store_returns";
        case TableType::STORE_SALES:           return "store_sales";
        case TableType::TIME_DIM:              return "time_dim";
        case TableType::WAREHOUSE:             return "warehouse";
        case TableType::WEB_PAGE:              return "web_page";
        case TableType::WEB_RETURNS:           return "web_returns";
        case TableType::WEB_SALES:             return "web_sales";
        case TableType::WEB_SITE:              return "web_site";
        default:                               return "unknown";
    }
}

// ---------------------------------------------------------------------------
// Arrow schemas
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> DSDGenWrapper::get_schema(TableType t) {
    switch (t) {
        case TableType::STORE_SALES:
            return arrow::schema({
                arrow::field("ss_sold_date_sk",         arrow::int64()),
                arrow::field("ss_sold_time_sk",         arrow::int64()),
                arrow::field("ss_item_sk",              arrow::int64()),
                arrow::field("ss_customer_sk",          arrow::int64()),
                arrow::field("ss_cdemo_sk",             arrow::int64()),
                arrow::field("ss_hdemo_sk",             arrow::int64()),
                arrow::field("ss_addr_sk",              arrow::int64()),
                arrow::field("ss_store_sk",             arrow::int64()),
                arrow::field("ss_promo_sk",             arrow::int64()),
                arrow::field("ss_ticket_number",        arrow::int64()),
                arrow::field("ss_quantity",             arrow::int32()),
                arrow::field("ss_wholesale_cost",       arrow::float64()),
                arrow::field("ss_list_price",           arrow::float64()),
                arrow::field("ss_sales_price",          arrow::float64()),
                arrow::field("ss_ext_discount_amt",     arrow::float64()),
                arrow::field("ss_ext_sales_price",      arrow::float64()),
                arrow::field("ss_ext_wholesale_cost",   arrow::float64()),
                arrow::field("ss_ext_list_price",       arrow::float64()),
                arrow::field("ss_ext_tax",              arrow::float64()),
                arrow::field("ss_coupon_amt",           arrow::float64()),
                arrow::field("ss_net_paid",             arrow::float64()),
                arrow::field("ss_net_paid_inc_tax",     arrow::float64()),
                arrow::field("ss_net_profit",           arrow::float64()),
            });

        case TableType::INVENTORY:
            return arrow::schema({
                arrow::field("inv_date_sk",             arrow::int64()),
                arrow::field("inv_item_sk",             arrow::int64()),
                arrow::field("inv_warehouse_sk",        arrow::int64()),
                arrow::field("inv_quantity_on_hand",    arrow::int32()),
            });

        default:
            throw std::invalid_argument(
                "DSDGenWrapper::get_schema: schema not yet implemented for table " +
                table_name(t));
    }
}

// ---------------------------------------------------------------------------
// Constructor / destructor
// ---------------------------------------------------------------------------

DSDGenWrapper::DSDGenWrapper(long scale_factor, bool verbose)
    : scale_factor_(scale_factor), verbose_(verbose), initialized_(false) {
    if (scale_factor <= 0) {
        throw std::invalid_argument("scale_factor must be positive");
    }
}

DSDGenWrapper::~DSDGenWrapper() {
    if (!tmp_dist_path_.empty()) {
        ::unlink(tmp_dist_path_.c_str());
    }
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

void DSDGenWrapper::init_dsdgen() {
    if (initialized_) return;

    // 1. Write embedded tpcds.idx to a temp file (dsdgen opens it by path).
    char tmp_tmpl[] = "/tmp/tpcds_idx_XXXXXX";
    int fd = ::mkstemp(tmp_tmpl);
    if (fd < 0) {
        throw std::runtime_error("DSDGenWrapper: mkstemp failed for tpcds.idx");
    }
    const uint8_t* data = tpcds_idx_data;
    size_t remaining    = tpcds_idx_size;
    while (remaining > 0) {
        ssize_t written = ::write(fd, data, remaining);
        if (written <= 0) {
            ::close(fd);
            ::unlink(tmp_tmpl);
            throw std::runtime_error("DSDGenWrapper: write to tmp tpcds.idx failed");
        }
        data      += written;
        remaining -= static_cast<size_t>(written);
    }
    ::close(fd);
    tmp_dist_path_ = tmp_tmpl;

    // 2. Initialise dsdgen parameter table and override relevant params.
    init_params();

    // 3. Point DISTRIBUTIONS at the temp file we just wrote.
    set_str(const_cast<char*>("DISTRIBUTIONS"),
            const_cast<char*>(tmp_dist_path_.c_str()));

    // 4. Set scale factor.
    char scale_buf[32];
    std::snprintf(scale_buf, sizeof(scale_buf), "%ld", scale_factor_);
    set_int(const_cast<char*>("SCALE"), scale_buf);

    // 5. Seed the RNG (must happen after init_params so streams are set up).
    init_rand();

    initialized_ = true;

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: initialized (SF=%ld, dist=%s)\n",
            scale_factor_, tmp_dist_path_.c_str());
    }
}

// ---------------------------------------------------------------------------
// get_row_count
// ---------------------------------------------------------------------------

long DSDGenWrapper::get_row_count(TableType t) const {
    // get_rowcount() reads the global scale factor set in init_dsdgen().
    // const_cast is safe: we only call this after initialization.
    const_cast<DSDGenWrapper*>(this)->init_dsdgen();
    return static_cast<long>(get_rowcount(table_id(t)));
}

// ---------------------------------------------------------------------------
// generate_store_sales
// ---------------------------------------------------------------------------
//
// store_sales is a master-detail table: each call to mk_w_store_sales(NULL, i)
// generates one "ticket" (master) with 8-16 line items (details). Each detail
// row is emitted via the callback g_w_store_sales_callback, which is the only
// way to capture the fully-populated rows (including pricing fields that live
// in the global g_w_store_sales, not in the caller-supplied struct).
//
// get_rowcount(STORE_SALES) returns the number of TICKETS (master rows).
// The total number of line-item rows emitted will be higher (8-16×).
// ---------------------------------------------------------------------------

// C-linkage trampoline — set as g_w_store_sales_callback before generation
namespace {
struct StoreSalesCtx {
    std::function<void(const void*)>* cb;
    long max_rows;
    long emitted;
};

extern "C" void store_sales_trampoline(
    const struct W_STORE_SALES_TBL* row, void* ctx)
{
    auto* c = static_cast<StoreSalesCtx*>(ctx);
    if (c->max_rows > 0 && c->emitted >= c->max_rows) return;
    (*c->cb)(static_cast<const void*>(row));
    ++c->emitted;
}
} // anonymous namespace

void DSDGenWrapper::generate_store_sales(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    ds_key_t n_tickets = get_rowcount(TPCDS_STORE_SALES);

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating store_sales from %lld tickets\n",
            static_cast<long long>(n_tickets));
    }

    StoreSalesCtx ctx{&callback, max_rows, 0L};
    g_w_store_sales_callback     = store_sales_trampoline;
    g_w_store_sales_callback_ctx = &ctx;

    for (ds_key_t i = 1; i <= n_tickets; ++i) {
        if (max_rows > 0 && ctx.emitted >= max_rows) break;
        mk_w_store_sales(nullptr, i);
    }

    // Always clear the callback to avoid dangling pointer
    g_w_store_sales_callback     = nullptr;
    g_w_store_sales_callback_ctx = nullptr;

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: emitted %ld store_sales rows\n", ctx.emitted);
    }
}

// ---------------------------------------------------------------------------
// generate_inventory
// ---------------------------------------------------------------------------

void DSDGenWrapper::generate_inventory(
    std::function<void(const void* row)> callback,
    long max_rows)
{
    init_dsdgen();

    ds_key_t total = get_rowcount(TPCDS_INVENTORY);
    if (max_rows > 0 && static_cast<ds_key_t>(max_rows) < total) {
        total = static_cast<ds_key_t>(max_rows);
    }

    if (verbose_) {
        std::fprintf(stderr,
            "DSDGenWrapper: generating %lld inventory rows\n",
            static_cast<long long>(total));
    }

    W_INVENTORY_TBL row;
    for (ds_key_t i = 1; i <= total; ++i) {
        mk_w_inventory(&row, i);
        callback(&row);
    }
}

}  // namespace tpcds
