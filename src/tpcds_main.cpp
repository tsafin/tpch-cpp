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
#include <getopt.h>
#include <sys/stat.h>

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
    long        scale_factor = 1;
    std::string format       = "parquet";
    std::string output_dir   = "/tmp";
    long        max_rows     = 1000;
    std::string table        = "store_sales";
    std::string compression  = "snappy";  // snappy, lz4, zstd, none
    bool        verbose      = false;
    bool        zero_copy    = false;     // streaming mode: O(batch) memory instead of O(total)
    std::string zero_copy_mode = "sync";  // sync, auto, async (lance-specific selection)
    long        lance_stream_queue = 4;   // bounded C++ -> Rust stream queue depth
    long        lance_max_blocking_threads = 8;
    bool        lance_mem_profile = false;
    long        lance_mem_profile_every = 100;
    long        lance_sg_batches = 1;
    long        lance_sg_queue_chunks = 4;
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
        "  --compression <c>      Parquet compression: snappy (default), zstd, none\n"
        "  --zero-copy            Streaming mode: flush each batch immediately (O(batch) RAM)\n"
        "  --zero-copy-mode <m>   Zero-copy mode for Lance: sync, auto, async (default: sync)\n"
#ifdef TPCH_ENABLE_LANCE
        "  --lance-stream-queue <n> Lance streaming queue depth (default: 4)\n"
        "  --lance-max-blocking-threads <n> Cap Tokio blocking threads for Lance (default: 8)\n"
        "  --lance-mem-profile     Enable Rust-side stage/batch RSS logging\n"
        "  --lance-mem-every <n>   RSS log cadence in batches (default: 100)\n"
        "  --lance-sg-batches <n>  Scatter/gather chunk size in batches (default: 1=off)\n"
        "  --lance-sg-queue-chunks <n> Scatter/gather queue size in chunks (default: 4)\n"
#endif
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
        OPT_COMPRESSION = 1000,
        OPT_ZERO_COPY,
        OPT_ZERO_COPY_MODE,
        OPT_LANCE_STREAM_QUEUE,
        OPT_LANCE_MAX_BLOCKING_THREADS,
        OPT_LANCE_MEM_PROFILE,
        OPT_LANCE_MEM_EVERY,
        OPT_LANCE_SG_BATCHES,
        OPT_LANCE_SG_QUEUE_CHUNKS
    };
    static struct option long_opts[] = {
        {"format",       required_argument, nullptr, 'f'},
        {"table",        required_argument, nullptr, 't'},
        {"scale-factor", required_argument, nullptr, 's'},
        {"output-dir",   required_argument, nullptr, 'o'},
        {"max-rows",     required_argument, nullptr, 'm'},
        {"compression",  required_argument, nullptr, OPT_COMPRESSION},
        {"zero-copy",    no_argument,       nullptr, OPT_ZERO_COPY},
        {"zero-copy-mode", required_argument, nullptr, OPT_ZERO_COPY_MODE},
        {"lance-stream-queue", required_argument, nullptr, OPT_LANCE_STREAM_QUEUE},
        {"lance-max-blocking-threads", required_argument, nullptr, OPT_LANCE_MAX_BLOCKING_THREADS},
        {"lance-mem-profile", no_argument, nullptr, OPT_LANCE_MEM_PROFILE},
        {"lance-mem-every", required_argument, nullptr, OPT_LANCE_MEM_EVERY},
        {"lance-sg-batches", required_argument, nullptr, OPT_LANCE_SG_BATCHES},
        {"lance-sg-queue-chunks", required_argument, nullptr, OPT_LANCE_SG_QUEUE_CHUNKS},
        {"verbose",      no_argument,       nullptr, 'v'},
        {"help",         no_argument,       nullptr, 'h'},
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
            case OPT_COMPRESSION: opts.compression = optarg; break;
            case OPT_ZERO_COPY: opts.zero_copy = true; break;
            case OPT_ZERO_COPY_MODE: opts.zero_copy_mode = optarg; break;
            case OPT_LANCE_STREAM_QUEUE: opts.lance_stream_queue = std::stol(optarg); break;
            case OPT_LANCE_MAX_BLOCKING_THREADS: opts.lance_max_blocking_threads = std::stol(optarg); break;
            case OPT_LANCE_MEM_PROFILE: opts.lance_mem_profile = true; break;
            case OPT_LANCE_MEM_EVERY: opts.lance_mem_profile_every = std::stol(optarg); break;
            case OPT_LANCE_SG_BATCHES: opts.lance_sg_batches = std::stol(optarg); break;
            case OPT_LANCE_SG_QUEUE_CHUNKS: opts.lance_sg_queue_chunks = std::stol(optarg); break;
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
        auto array = builders[static_cast<size_t>(i)]->Finish().ValueOrDie();
        // Convert Int8 indices to DictionaryArray for DICTIONARY fields
        if (field->type()->id() == arrow::Type::DICTIONARY) {
            auto dict = tpcds::get_dict_for_field(field->name());
            if (dict) {
                array = arrow::DictionaryArray::FromArrays(field->type(), array, dict).ValueOrDie();
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
    opts.zero_copy_mode = normalize_zero_copy_mode(opts.zero_copy_mode);
    if (opts.zero_copy_mode != "auto" && opts.zero_copy_mode != "sync" && opts.zero_copy_mode != "async") {
        fprintf(stderr, "tpcds_benchmark: --zero-copy-mode must be one of: auto, sync, async\n");
        return 1;
    }

    // Resolve table
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

    if (opts.lance_stream_queue < 1) {
        fprintf(stderr, "tpcds_benchmark: --lance-stream-queue must be >= 1\n");
        return 1;
    }
    if (opts.lance_max_blocking_threads < 1) {
        fprintf(stderr, "tpcds_benchmark: --lance-max-blocking-threads must be >= 1\n");
        return 1;
    }
    if (opts.lance_mem_profile_every < 1) {
        fprintf(stderr, "tpcds_benchmark: --lance-mem-every must be >= 1\n");
        return 1;
    }
    if (opts.lance_sg_batches < 1) {
        fprintf(stderr, "tpcds_benchmark: --lance-sg-batches must be >= 1\n");
        return 1;
    }
    if (opts.lance_sg_queue_chunks < 1) {
        fprintf(stderr, "tpcds_benchmark: --lance-sg-queue-chunks must be >= 1\n");
        return 1;
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
            lw->set_stream_queue_depth(static_cast<size_t>(opts.lance_stream_queue));
            lw->set_runtime_config(static_cast<int>(opts.lance_max_blocking_threads));
            lw->set_profile_config(
                opts.lance_mem_profile,
                static_cast<size_t>(opts.lance_mem_profile_every));
            lw->set_scatter_gather_config(
                static_cast<size_t>(opts.lance_sg_batches),
                static_cast<size_t>(opts.lance_sg_queue_chunks));
            if (opts.zero_copy && !lance_async_streaming) {
                // bounded synchronous path to cap memory without Tokio background streaming
                lw->set_buffered_flush_config(8, 65'536);
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
        if (table_type == tpcds::TableType::StoreSales) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_store_sales(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::Inventory) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_inventory(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::CatalogSales) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_catalog_sales(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::WebSales) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_web_sales(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::Customer) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_customer(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::Item) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_item(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::DateDim) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_date_dim(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::StoreReturns) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_store_returns(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::CatalogReturns) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_catalog_returns(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::WebReturns) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_web_returns(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::CallCenter) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_call_center(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::CatalogPage) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_catalog_page(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::WebPage) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_web_page(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::WebSite) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_web_site(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::Warehouse) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_warehouse(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::ShipMode) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_ship_mode(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::HouseholdDemographics) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_household_demographics(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::CustomerDemographics) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_customer_demographics(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::CustomerAddress) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_customer_address(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::IncomeBand) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_income_band(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::Reason) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_reason(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::TimeDim) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_time_dim(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::Promotion) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_promotion(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::Store) {
            actual_rows = run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_store(cb, opts.max_rows); });
        }
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
