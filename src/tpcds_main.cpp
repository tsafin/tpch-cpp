/**
 * tpcds_main.cpp — TPC-DS data generator benchmark executable
 *
 * Generates TPC-DS benchmark data in multiple formats (Parquet, CSV, ORC,
 * Lance, Paimon, Iceberg) using the official TPC-DS dsdgen generator.
 *
 * CLI mirrors tpch_benchmark:
 *   ./tpcds_benchmark --format parquet --table store_sales --scale-factor 1
 *   ./tpcds_benchmark --format parquet --table inventory   --scale-factor 5
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <cctype>
#include <vector>
#include <getopt.h>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <arrow/api.h>
#include <arrow/builder.h>

#include "tpch/writer_interface.hpp"
#include "tpch/csv_writer.hpp"
#include "tpch/parquet_writer.hpp"
#include "tpch/dsdgen_wrapper.hpp"
#include "tpch/dsdgen_converter.hpp"

#ifdef TPCH_ENABLE_ORC
#include "tpch/orc_writer.hpp"
#endif
#ifdef TPCH_ENABLE_PAIMON
#include "tpch/paimon_writer.hpp"
#endif
#ifdef TPCH_ENABLE_ICEBERG
#include "tpch/iceberg_writer.hpp"
#endif
#ifdef TPCH_ENABLE_LANCE
#include "tpch/lance_writer.hpp"
#endif

namespace {

struct Options {
    long        scale_factor    = 1;
    std::string format          = "parquet";
    std::string output_dir      = "/tmp";
    long        max_rows        = 1000;
    std::string table           = "store_sales";
    std::string compression     = "zstd";    // snappy, zstd, none
    bool        verbose         = false;
    bool        zero_copy       = false;     // streaming mode: O(batch) memory instead of O(total)
    std::string zero_copy_mode  = "sync";    // sync, auto, async (lance-specific selection)
    bool        parallel        = false;     // generate all tables in parallel
    int         parallel_tables = 0;         // max concurrent tables; 0 = all
};

void print_usage(const char* prog) {
    fprintf(stderr,
        "Usage: %s [OPTIONS]\n"
        "\n"
        "Options:\n"
        "  --format <fmt>         Output format: parquet, csv"
#ifdef TPCH_ENABLE_ORC
        ", orc"
#endif
#ifdef TPCH_ENABLE_PAIMON
        ", paimon"
#endif
#ifdef TPCH_ENABLE_ICEBERG
        ", iceberg"
#endif
#ifdef TPCH_ENABLE_LANCE
        ", lance"
#endif
        " (default: parquet)\n"
        "  --table <name>         TPC-DS table name (default: store_sales)\n"
        "  --scale-factor <sf>    Scale factor (default: 1)\n"
        "  --output-dir <dir>     Output directory (default: /tmp)\n"
        "  --max-rows <n>         Max rows to generate (0=all, default: 1000)\n"
        "  --compression <c>      Parquet compression: zstd (default), snappy, none\n"
        "  --zero-copy            Streaming mode: flush each batch immediately (O(batch) RAM)\n"
        "  --zero-copy-mode <m>   Zero-copy mode for Lance: sync, auto, async (default: sync)\n"
#ifdef TPCH_ENABLE_LANCE
#endif
        "  --parallel             Generate all tables in parallel (fork-after-init)\n"
        "  --parallel-tables <N>  Max concurrent tables (default: all)\n"
        "  --verbose              Verbose output\n"
        "  --help                 Show this help\n"
        "\n"
        "TPC-DS tables (implemented):\n"
        "  Fact:      store_sales, inventory, catalog_sales, web_sales,\n"
        "             store_returns, catalog_returns, web_returns\n"
        "  Dimension: customer, item, date_dim,\n"
        "             call_center, catalog_page, web_page, web_site,\n"
        "             warehouse, ship_mode, household_demographics,\n"
        "             customer_demographics, customer_address, income_band,\n"
        "             reason, time_dim, promotion, store\n",
        prog);
}

Options parse_args(int argc, char* argv[]) {
    Options opts;

    enum {
        OPT_COMPRESSION    = 1000,
        OPT_ZERO_COPY,
        OPT_ZERO_COPY_MODE,
        OPT_PARALLEL,
        OPT_PARALLEL_TABLES
    };
    static struct option long_opts[] = {
        {"format",          required_argument, nullptr, 'f'},
        {"table",           required_argument, nullptr, 't'},
        {"scale-factor",    required_argument, nullptr, 's'},
        {"output-dir",      required_argument, nullptr, 'o'},
        {"max-rows",        required_argument, nullptr, 'm'},
        {"compression",     required_argument, nullptr, OPT_COMPRESSION},
        {"zero-copy",       no_argument,       nullptr, OPT_ZERO_COPY},
        {"zero-copy-mode",  required_argument, nullptr, OPT_ZERO_COPY_MODE},
        {"parallel",        no_argument,       nullptr, OPT_PARALLEL},
        {"parallel-tables", required_argument, nullptr, OPT_PARALLEL_TABLES},
        {"verbose",         no_argument,       nullptr, 'v'},
        {"help",            no_argument,       nullptr, 'h'},
        {nullptr, 0, nullptr, 0}
    };

    int c;
    while ((c = getopt_long(argc, argv, "f:t:s:o:m:vzh", long_opts, nullptr)) != -1) {
        switch (c) {
            case 'f': opts.format       = optarg; break;
            case 't': opts.table        = optarg; break;
            case 's': opts.scale_factor = std::stol(optarg); break;
            case 'o': opts.output_dir   = optarg; break;
            case 'm': opts.max_rows     = std::stol(optarg); break;
            case OPT_COMPRESSION:    opts.compression     = optarg; break;
            case OPT_ZERO_COPY:      opts.zero_copy       = true;   break;
            case OPT_ZERO_COPY_MODE: opts.zero_copy_mode  = optarg; break;
            case OPT_PARALLEL:       opts.parallel        = true;   break;
            case OPT_PARALLEL_TABLES:
                opts.parallel_tables = std::stoi(optarg);
                if (opts.parallel_tables <= 0)
                    throw std::invalid_argument("--parallel-tables must be > 0");
                break;
            case 'z': opts.zero_copy    = true;   break;
            case 'v': opts.verbose      = true;   break;
            case 'h': print_usage(argv[0]); exit(0);
            default:  print_usage(argv[0]); exit(1);
        }
    }
    return opts;
}

std::string normalize_zero_copy_mode(std::string mode) {
    for (char& c : mode) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return mode;
}

// Create writer for the given format and output path.
// When zero_copy=true, enables streaming write mode: each batch is flushed
// immediately to disk, capping RAM usage at O(batch_size) instead of O(total_rows).
std::unique_ptr<tpch::WriterInterface> create_writer(
    const std::string& format,
    const std::string& filepath,
    const std::string& compression,
    bool zero_copy = false,
    bool lance_async_streaming = false)
{
    if (format == "csv") {
        return std::make_unique<tpch::CSVWriter>(filepath);
    } else if (format == "parquet") {
        auto w = std::make_unique<tpch::ParquetWriter>(filepath);
        w->set_compression(compression);
        if (zero_copy) {
            w->enable_streaming_write();
        }
        return w;
    }
#ifdef TPCH_ENABLE_ORC
    else if (format == "orc") {
        return std::make_unique<tpch::ORCWriter>(filepath);
    }
#endif
#ifdef TPCH_ENABLE_PAIMON
    else if (format == "paimon") {
        return std::make_unique<tpch::PaimonWriter>(filepath);
    }
#endif
#ifdef TPCH_ENABLE_ICEBERG
    else if (format == "iceberg") {
        return std::make_unique<tpch::IcebergWriter>(filepath);
    }
#endif
#ifdef TPCH_ENABLE_LANCE
    else if (format == "lance") {
        auto w = std::make_unique<tpch::LanceWriter>(filepath);
        if (zero_copy && lance_async_streaming) {
            w->enable_streaming_write(true);
        }
        return w;
    }
#endif
    throw std::invalid_argument("Unknown format: " + format);
}

// Build Arrow array builders from schema (int32, int64, float64, string)
tpcds::BuilderMap
create_builders(std::shared_ptr<arrow::Schema> schema, int64_t capacity)
{
    tpcds::BuilderMap builders;
    builders.reserve(static_cast<size_t>(schema->num_fields()));

    for (const auto& field : schema->fields()) {
        switch (field->type()->id()) {
            case arrow::Type::INT64: {
                auto b = std::make_shared<arrow::Int64Builder>();
                (void)b->Reserve(capacity);
                builders.push_back(b);
                break;
            }
            case arrow::Type::INT32: {
                auto b = std::make_shared<arrow::Int32Builder>();
                (void)b->Reserve(capacity);
                builders.push_back(b);
                break;
            }
            case arrow::Type::DOUBLE: {
                auto b = std::make_shared<arrow::DoubleBuilder>();
                (void)b->Reserve(capacity);
                builders.push_back(b);
                break;
            }
            case arrow::Type::STRING: {
                auto b = std::make_shared<arrow::StringBuilder>();
                (void)b->Reserve(capacity);
                (void)b->ReserveData(capacity * 32);
                builders.push_back(b);
                break;
            }
            case arrow::Type::DICTIONARY: {
                auto b = std::make_shared<arrow::Int8Builder>();
                (void)b->Reserve(capacity);
                builders.push_back(b);
                break;
            }
            default:
                throw std::runtime_error(
                    "Unsupported Arrow type: " + field->type()->ToString());
        }
    }
    return builders;
}

// Finish builders → RecordBatch, then reset
std::shared_ptr<arrow::RecordBatch>
finish_batch(
    std::shared_ptr<arrow::Schema> schema,
    tpcds::BuilderMap& builders,
    size_t num_rows)
{
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(schema->num_fields());
    for (int i = 0; i < schema->num_fields(); ++i) {
        const auto& field = schema->field(i);
        std::shared_ptr<arrow::Array> array;
        arrow::Status finish_status =
            builders[static_cast<size_t>(i)]->Finish(&array);
        if (!finish_status.ok()) {
            throw std::runtime_error(
                "Failed to finish Arrow builder for field '" +
                field->name() + "': " + finish_status.ToString());
        }
        // Convert Int8 indices to DictionaryArray for DICTIONARY fields
        if (field->type()->id() == arrow::Type::DICTIONARY) {
            auto dict = tpcds::get_dict_for_field(field->name());
            if (dict) {
                auto dict_result =
                    arrow::DictionaryArray::FromArrays(field->type(), array, dict);
                if (!dict_result.ok()) {
                    throw std::runtime_error(
                        "Failed to build dictionary array for field '" +
                        field->name() + "': " + dict_result.status().ToString());
                }
                array = dict_result.ValueOrDie();
            }
        }
        arrays.push_back(array);
    }
    return arrow::RecordBatch::Make(schema, static_cast<int64_t>(num_rows), arrays);
}

void reset_builders(tpcds::BuilderMap& builders) {
    for (auto& b : builders) { b->Reset(); }
}

// ---------------------------------------------------------------------------
// main generation loop (row-by-row callback → batched Arrow writes)
// ---------------------------------------------------------------------------

template<typename GenerateFn>
size_t run_generation(
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    GenerateFn generate_fn)
{
    // 8192 = Lance max_rows_per_group default — aligns C++ batches to Lance row-group
    // boundaries so the streaming encoder never sees split/leftover rows at group edges.
    // This also benefits Parquet (common row-group granularity) and ORC stripe alignment.
    const size_t batch_size = 8192;
    size_t rows_in_batch = 0;
    size_t total_rows = 0;

    auto builders = create_builders(schema, static_cast<int64_t>(batch_size));

    auto callback = [&](const void* row) {
        tpcds::append_dsdgen_row_to_builders(opts.table, row, builders);
        ++rows_in_batch;
        ++total_rows;

        if (rows_in_batch >= batch_size) {
            writer->write_batch(finish_batch(schema, builders, rows_in_batch));
            reset_builders(builders);
            rows_in_batch = 0;

            if (opts.verbose && (total_rows % 100000 == 0)) {
                fprintf(stderr, "  Generated %zu rows...\n", total_rows);
            }
        }
    };

    generate_fn(callback);

    // Flush final partial batch
    if (rows_in_batch > 0) {
        writer->write_batch(finish_batch(schema, builders, rows_in_batch));
    }

    return total_rows;
}

// Map table name → TableType enum
tpcds::TableType parse_table(const std::string& name) {
    if (name == "store_sales")              return tpcds::TableType::StoreSales;
    if (name == "inventory")               return tpcds::TableType::Inventory;
    if (name == "catalog_sales")           return tpcds::TableType::CatalogSales;
    if (name == "web_sales")               return tpcds::TableType::WebSales;
    if (name == "customer")                return tpcds::TableType::Customer;
    if (name == "item")                    return tpcds::TableType::Item;
    if (name == "date_dim")                return tpcds::TableType::DateDim;
    if (name == "store_returns")           return tpcds::TableType::StoreReturns;
    if (name == "catalog_returns")         return tpcds::TableType::CatalogReturns;
    if (name == "web_returns")             return tpcds::TableType::WebReturns;
    if (name == "call_center")             return tpcds::TableType::CallCenter;
    if (name == "catalog_page")            return tpcds::TableType::CatalogPage;
    if (name == "web_page")                return tpcds::TableType::WebPage;
    if (name == "web_site")                return tpcds::TableType::WebSite;
    if (name == "warehouse")               return tpcds::TableType::Warehouse;
    if (name == "ship_mode")               return tpcds::TableType::ShipMode;
    if (name == "household_demographics")  return tpcds::TableType::HouseholdDemographics;
    if (name == "customer_demographics")   return tpcds::TableType::CustomerDemographics;
    if (name == "customer_address")        return tpcds::TableType::CustomerAddress;
    if (name == "income_band")             return tpcds::TableType::IncomeBand;
    if (name == "reason")                  return tpcds::TableType::Reason;
    if (name == "time_dim")                return tpcds::TableType::TimeDim;
    if (name == "promotion")               return tpcds::TableType::Promotion;
    if (name == "store")                   return tpcds::TableType::Store;
    throw std::invalid_argument("Table '" + name + "' not found. Use --help for list.");
}

// Extension for a given format
std::string file_extension(const std::string& fmt) {
    if (fmt == "parquet") return ".parquet";
    if (fmt == "csv")     return ".csv";
    if (fmt == "orc")     return ".orc";
    if (fmt == "paimon")  return ".paimon";
    if (fmt == "iceberg") return ".iceberg";
    if (fmt == "lance")   return ".lance";
    return "." + fmt;
}

// ---------------------------------------------------------------------------
// dispatch_generation — maps TableType to the correct DSDGenWrapper method
// ---------------------------------------------------------------------------

size_t dispatch_generation(
    const Options& opts,
    tpcds::TableType table_type,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    tpcds::DSDGenWrapper& dsdgen)
{
    if (table_type == tpcds::TableType::StoreSales)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_store_sales(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::Inventory)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_inventory(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::CatalogSales)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_catalog_sales(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::WebSales)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_web_sales(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::Customer)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_customer(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::Item)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_item(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::DateDim)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_date_dim(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::StoreReturns)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_store_returns(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::CatalogReturns)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_catalog_returns(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::WebReturns)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_web_returns(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::CallCenter)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_call_center(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::CatalogPage)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_catalog_page(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::WebPage)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_web_page(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::WebSite)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_web_site(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::Warehouse)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_warehouse(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::ShipMode)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_ship_mode(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::HouseholdDemographics)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_household_demographics(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::CustomerDemographics)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_customer_demographics(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::CustomerAddress)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_customer_address(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::IncomeBand)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_income_band(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::Reason)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_reason(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::TimeDim)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_time_dim(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::Promotion)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_promotion(cb, opts.max_rows); });
    if (table_type == tpcds::TableType::Store)
        return run_generation(opts, schema, writer,
            [&](auto cb) { dsdgen.generate_store(cb, opts.max_rows); });
    throw std::invalid_argument("dispatch_generation: unhandled table type");
}

// ---------------------------------------------------------------------------
// Parallel generation — DS-10.1
// ---------------------------------------------------------------------------

// All 24 implemented TPC-DS tables, ordered: small dimensions first so they
// complete quickly and their slots open for the heavier fact tables.
static const std::vector<std::pair<std::string, tpcds::TableType>> ALL_TPCDS_TABLES = {
    // tiny dimensions (< 100 rows at any SF)
    {"income_band",             tpcds::TableType::IncomeBand},
    {"ship_mode",               tpcds::TableType::ShipMode},
    {"warehouse",               tpcds::TableType::Warehouse},
    {"reason",                  tpcds::TableType::Reason},
    {"call_center",             tpcds::TableType::CallCenter},
    // small dimensions
    {"web_site",                tpcds::TableType::WebSite},
    {"web_page",                tpcds::TableType::WebPage},
    {"catalog_page",            tpcds::TableType::CatalogPage},
    {"household_demographics",  tpcds::TableType::HouseholdDemographics},
    {"promotion",               tpcds::TableType::Promotion},
    {"store",                   tpcds::TableType::Store},
    // medium dimensions
    {"item",                    tpcds::TableType::Item},
    {"date_dim",                tpcds::TableType::DateDim},
    {"time_dim",                tpcds::TableType::TimeDim},
    {"customer_demographics",   tpcds::TableType::CustomerDemographics},
    // large dimensions
    {"customer_address",        tpcds::TableType::CustomerAddress},
    {"customer",                tpcds::TableType::Customer},
    // fact tables (heaviest last so all slots are warm when they start)
    {"inventory",               tpcds::TableType::Inventory},
    {"web_returns",             tpcds::TableType::WebReturns},
    {"catalog_returns",         tpcds::TableType::CatalogReturns},
    {"store_returns",           tpcds::TableType::StoreReturns},
    {"web_sales",               tpcds::TableType::WebSales},
    {"catalog_sales",           tpcds::TableType::CatalogSales},
    {"store_sales",             tpcds::TableType::StoreSales},
};

// Child process: generate one table, write output, exit.
// dsdgen is already initialised (inherited via COW from parent).
// Returns exit code (0 = success).
static int run_table_child(
    const Options& opts,
    tpcds::TableType table_type,
    tpcds::DSDGenWrapper& dsdgen)
{
    const std::string tname = tpcds::DSDGenWrapper::table_name(table_type);
    const std::string filepath = opts.output_dir + "/" + tname + file_extension(opts.format);

    bool lance_async = (opts.format == "lance" && opts.zero_copy &&
                        opts.zero_copy_mode == "async");

    std::unique_ptr<tpch::WriterInterface> writer;
    try {
        writer = create_writer(opts.format, filepath, opts.compression,
                               opts.zero_copy, lance_async);
    } catch (const std::exception& e) {
        fprintf(stderr, "[%s] failed to create writer: %s\n", tname.c_str(), e.what());
        return 1;
    }

#ifdef TPCH_ENABLE_LANCE
    if (opts.format == "lance") {
        if (auto* lw = dynamic_cast<tpch::LanceWriter*>(writer.get())) {
            if (opts.zero_copy && !lance_async)
                lw->set_buffered_flush_config(128, 1'048'576);
        }
    }
#endif

    // run_generation uses opts.table for append_dsdgen_row_to_builders dispatch
    Options child_opts  = opts;
    child_opts.table    = tname;

    auto schema = tpcds::DSDGenWrapper::get_schema(table_type, opts.scale_factor);

    auto t0 = std::chrono::steady_clock::now();
    size_t rows = 0;
    try {
        rows = dispatch_generation(child_opts, table_type, schema, writer, dsdgen);
    } catch (const std::exception& e) {
        fprintf(stderr, "[%s] generation error: %s\n", tname.c_str(), e.what());
        return 1;
    }
    writer->close();

    double elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t0).count();
    printf("tpcds_benchmark: %-28s  SF=%ld  rows=%zu  elapsed=%.2fs  rate=%.0f rows/s\n",
           tname.c_str(), opts.scale_factor, rows,
           elapsed, elapsed > 0 ? rows / elapsed : 0.0);
    printf("  output: %s\n", filepath.c_str());
    fflush(stdout);
    return 0;
}

// Fork-after-init parallel generation with rolling N-slot window.
// Returns 0 if all children succeeded, 1 if any failed.
static int generate_all_tables_parallel(const Options& opts)
{
    const size_t ntables = ALL_TPCDS_TABLES.size();
    const size_t slot_limit = (opts.parallel_tables > 0)
        ? static_cast<size_t>(opts.parallel_tables)
        : ntables;

    // Initialise dsdgen ONCE in the parent.  All children inherit the loaded
    // distributions and seeded RNG streams via COW — no re-init needed.
    tpcds::DSDGenWrapper parent_dsdgen(opts.scale_factor, opts.verbose);
    parent_dsdgen.prepare_for_fork();

    auto t_wall = std::chrono::steady_clock::now();

    fprintf(stderr,
        "tpcds_benchmark: parallel  SF=%ld  tables=%zu  slots=%zu  format=%s\n",
        opts.scale_factor, ntables, slot_limit, opts.format.c_str());

    // pid → table index map so we can report which table finished
    std::vector<pid_t>  pids(ntables, -1);
    std::vector<size_t> slot_table(slot_limit, SIZE_MAX); // slot → table index
    size_t next   = 0;   // index of next table to fork
    size_t active = 0;   // number of live children
    int    failed = 0;

    auto fork_next = [&](size_t slot) {
        if (next >= ntables) return;
        const auto& [tname, ttype] = ALL_TPCDS_TABLES[next];

        pid_t pid = ::fork();
        if (pid < 0) {
            perror("fork");
            ++failed;
            ++next;
            return;
        }
        if (pid == 0) {
            // Child: temp file belongs to parent — don't unlink on exit.
            parent_dsdgen.clear_tmp_path();
            int rc = run_table_child(opts, ttype, parent_dsdgen);
            std::exit(rc);
        }
        // Parent
        pids[next]       = pid;
        slot_table[slot] = next;
        ++next;
        ++active;
    };

    // Fill initial slots
    for (size_t s = 0; s < slot_limit && next < ntables; ++s)
        fork_next(s);

    // Rolling wait: as each child finishes, fork the next table into its slot
    while (active > 0) {
        int status;
        pid_t done = ::waitpid(-1, &status, 0);
        if (done < 0) { perror("waitpid"); break; }

        // Find which slot this pid occupied
        size_t freed_slot = SIZE_MAX;
        for (size_t s = 0; s < slot_limit; ++s) {
            if (slot_table[s] < ntables && pids[slot_table[s]] == done) {
                freed_slot = s;
                break;
            }
        }

        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            const char* tname = (freed_slot < slot_limit && slot_table[freed_slot] < ntables)
                ? ALL_TPCDS_TABLES[slot_table[freed_slot]].first.c_str()
                : "unknown";
            fprintf(stderr, "tpcds_benchmark: [%s] child failed (pid=%d status=%d)\n",
                    tname, done, status);
            ++failed;
        }
        --active;

        if (freed_slot != SIZE_MAX)
            fork_next(freed_slot);
    }

    // Parent owns the temp distribution file — destructor unlinks it.
    double wall = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t_wall).count();
    fprintf(stderr,
        "tpcds_benchmark: parallel done  SF=%ld  %zu tables  wall=%.2fs  %s\n",
        opts.scale_factor, ntables, wall,
        failed ? "SOME TABLES FAILED" : "all ok");

    return failed ? 1 : 0;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }

    Options opts;
    try {
        opts = parse_args(argc, argv);
    } catch (const std::exception& e) {
        fprintf(stderr, "Error parsing arguments: %s\n", e.what());
        return 1;
    }
    if (opts.scale_factor <= 0) {
        fprintf(stderr, "tpcds_benchmark: --scale-factor must be > 0\n");
        return 1;
    }
    opts.zero_copy_mode = normalize_zero_copy_mode(opts.zero_copy_mode);
    if (opts.zero_copy_mode != "auto" && opts.zero_copy_mode != "sync" && opts.zero_copy_mode != "async") {
        fprintf(stderr, "tpcds_benchmark: --zero-copy-mode must be one of: auto, sync, async\n");
        return 1;
    }

    // Parallel mode: generate all tables and return immediately
    if (opts.parallel)
        return generate_all_tables_parallel(opts);

    // Resolve table (single-table path)
    tpcds::TableType table_type;
    try {
        table_type = parse_table(opts.table);
    } catch (const std::invalid_argument& e) {
        fprintf(stderr, "tpcds_benchmark: %s\n", e.what());
        return 1;
    }

    // Build output path
    std::string filepath = opts.output_dir + "/" + opts.table + file_extension(opts.format);

    // single-table tpcds_benchmark: synchronous bounded path is default.
    bool lance_async_streaming =
        (opts.format == "lance" && opts.zero_copy && opts.zero_copy_mode == "async");

    if (opts.verbose) {
        fprintf(stderr,
            "tpcds_benchmark: table=%s  format=%s  SF=%ld  max_rows=%ld  zero_copy=%s mode=%s\n"
            "  output: %s\n",
            opts.table.c_str(), opts.format.c_str(),
            opts.scale_factor, opts.max_rows,
            opts.zero_copy ? "yes" : "no",
            opts.zero_copy_mode.c_str(),
            filepath.c_str());
    }

    // Create writer
    std::unique_ptr<tpch::WriterInterface> writer;
    try {
        writer = create_writer(
            opts.format,
            filepath,
            opts.compression,
            opts.zero_copy,
            lance_async_streaming);
    } catch (const std::exception& e) {
        fprintf(stderr, "tpcds_benchmark: failed to create writer: %s\n", e.what());
        return 1;
    }

#ifdef TPCH_ENABLE_LANCE
    if (opts.format == "lance") {
        if (auto* lw = dynamic_cast<tpch::LanceWriter*>(writer.get())) {
            if (opts.zero_copy && !lance_async_streaming) {
                // Keep sync zero-copy bounded, but avoid tiny ~65K-row fragments that
                // amplify Lance append/commit overhead at higher scale factors.
                lw->set_buffered_flush_config(128, 1'048'576);
            }
        }
    }
#endif

    // Get Arrow schema
    auto schema = tpcds::DSDGenWrapper::get_schema(table_type, opts.scale_factor);

    // Build dsdgen wrapper
    tpcds::DSDGenWrapper dsdgen(opts.scale_factor, opts.verbose);

    auto t_start = std::chrono::steady_clock::now();

    // Generate
    size_t actual_rows = 0;
    try {
        actual_rows = dispatch_generation(opts, table_type, schema, writer, dsdgen);
    } catch (const std::exception& e) {
        fprintf(stderr, "tpcds_benchmark: generation error: %s\n", e.what());
        return 1;
    }

    writer->close();

    auto t_end = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();

    // Report: use actual emitted row count (avoids -1 for tables with no standalone rowcount)
    long actual = static_cast<long>(actual_rows);

    printf("tpcds_benchmark: %s  SF=%ld  rows=%ld  elapsed=%.2fs  rate=%.0f rows/s\n",
           opts.table.c_str(), opts.scale_factor, actual,
           elapsed, (elapsed > 0) ? actual / elapsed : 0.0);
    printf("  output: %s\n", filepath.c_str());

    return 0;
}
