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
#include <map>
#include <string>
#include <chrono>
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
    bool        verbose      = false;
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
        "  --verbose              Verbose output\n"
        "  --help                 Show this help\n"
        "\n"
        "TPC-DS tables (Phase 2 — implemented):\n"
        "  store_sales, inventory\n"
        "\n"
        "TPC-DS tables (planned Phase 3+):\n"
        "  Fact:      catalog_sales, web_sales, store_returns, catalog_returns,\n"
        "             web_returns\n"
        "  Dimension: customer, customer_address, customer_demographics,\n"
        "             date_dim, time_dim, item, store, call_center,\n"
        "             catalog_page, web_page, web_site, warehouse,\n"
        "             ship_mode, household_demographics, income_band,\n"
        "             reason, promotion\n",
        prog);
}

Options parse_args(int argc, char* argv[]) {
    Options opts;

    static struct option long_opts[] = {
        {"format",       required_argument, nullptr, 'f'},
        {"table",        required_argument, nullptr, 't'},
        {"scale-factor", required_argument, nullptr, 's'},
        {"output-dir",   required_argument, nullptr, 'o'},
        {"max-rows",     required_argument, nullptr, 'm'},
        {"verbose",      no_argument,       nullptr, 'v'},
        {"help",         no_argument,       nullptr, 'h'},
        {nullptr, 0, nullptr, 0}
    };

    int c;
    while ((c = getopt_long(argc, argv, "f:t:s:o:m:vh", long_opts, nullptr)) != -1) {
        switch (c) {
            case 'f': opts.format       = optarg; break;
            case 't': opts.table        = optarg; break;
            case 's': opts.scale_factor = std::stol(optarg); break;
            case 'o': opts.output_dir   = optarg; break;
            case 'm': opts.max_rows     = std::stol(optarg); break;
            case 'v': opts.verbose      = true;   break;
            case 'h': print_usage(argv[0]); exit(0);
            default:  print_usage(argv[0]); exit(1);
        }
    }
    return opts;
}

// Create writer for the given format and output path
std::unique_ptr<tpch::WriterInterface> create_writer(
    const std::string& format,
    const std::string& filepath)
{
    if (format == "csv") {
        return std::make_unique<tpch::CSVWriter>(filepath);
    } else if (format == "parquet") {
        return std::make_unique<tpch::ParquetWriter>(filepath);
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
        return std::make_unique<tpch::LanceWriter>(filepath);
    }
#endif
    throw std::invalid_argument("Unknown format: " + format);
}

// Build Arrow array builders from schema (int32, int64, float64, string)
std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>
create_builders(std::shared_ptr<arrow::Schema> schema)
{
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>> builders;
    const int64_t capacity = 10000;

    for (const auto& field : schema->fields()) {
        switch (field->type()->id()) {
            case arrow::Type::INT64: {
                auto b = std::make_shared<arrow::Int64Builder>();
                (void)b->Reserve(capacity);
                builders[field->name()] = b;
                break;
            }
            case arrow::Type::INT32: {
                auto b = std::make_shared<arrow::Int32Builder>();
                (void)b->Reserve(capacity);
                builders[field->name()] = b;
                break;
            }
            case arrow::Type::DOUBLE: {
                auto b = std::make_shared<arrow::DoubleBuilder>();
                (void)b->Reserve(capacity);
                builders[field->name()] = b;
                break;
            }
            case arrow::Type::STRING: {
                auto b = std::make_shared<arrow::StringBuilder>();
                (void)b->Reserve(capacity);
                (void)b->ReserveData(capacity * 32);
                builders[field->name()] = b;
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
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders,
    size_t num_rows)
{
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(schema->num_fields());
    for (const auto& field : schema->fields()) {
        arrays.push_back(builders[field->name()]->Finish().ValueOrDie());
    }
    return arrow::RecordBatch::Make(schema, static_cast<int64_t>(num_rows), arrays);
}

void reset_builders(std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {
    for (auto& [name, b] : builders) { b->Reset(); }
}

// ---------------------------------------------------------------------------
// main generation loop (row-by-row callback → batched Arrow writes)
// ---------------------------------------------------------------------------

template<typename GenerateFn>
void run_generation(
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    GenerateFn generate_fn)
{
    const size_t batch_size = 10000;
    size_t rows_in_batch = 0;
    size_t total_rows = 0;

    auto builders = create_builders(schema);

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
}

// Map table name → TableType enum
tpcds::TableType parse_table(const std::string& name) {
    if (name == "store_sales")  return tpcds::TableType::STORE_SALES;
    if (name == "inventory")    return tpcds::TableType::INVENTORY;
    throw std::invalid_argument(
        "Table '" + name + "' not yet implemented (Phase 3+).\n"
        "Available in Phase 2: store_sales, inventory");
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

    if (opts.verbose) {
        fprintf(stderr,
            "tpcds_benchmark: table=%s  format=%s  SF=%ld  max_rows=%ld\n"
            "  output: %s\n",
            opts.table.c_str(), opts.format.c_str(),
            opts.scale_factor, opts.max_rows,
            filepath.c_str());
    }

    // Create writer
    std::unique_ptr<tpch::WriterInterface> writer;
    try {
        writer = create_writer(opts.format, filepath);
    } catch (const std::exception& e) {
        fprintf(stderr, "tpcds_benchmark: failed to create writer: %s\n", e.what());
        return 1;
    }

    // Get Arrow schema
    auto schema = tpcds::DSDGenWrapper::get_schema(table_type);

    // Build dsdgen wrapper
    tpcds::DSDGenWrapper dsdgen(opts.scale_factor, opts.verbose);

    auto t_start = std::chrono::steady_clock::now();

    // Generate
    try {
        if (table_type == tpcds::TableType::STORE_SALES) {
            run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_store_sales(cb, opts.max_rows); });
        } else if (table_type == tpcds::TableType::INVENTORY) {
            run_generation(opts, schema, writer,
                [&](auto cb) { dsdgen.generate_inventory(cb, opts.max_rows); });
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "tpcds_benchmark: generation error: %s\n", e.what());
        return 1;
    }

    writer->close();

    auto t_end = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();

    // Report
    long row_count = dsdgen.get_row_count(table_type);
    long actual    = (opts.max_rows > 0 && opts.max_rows < row_count)
                     ? opts.max_rows : row_count;
    if (opts.max_rows == 1000 && opts.max_rows < row_count) {
        // default 1000-row limit
        actual = opts.max_rows;
    }

    printf("tpcds_benchmark: %s  SF=%ld  rows≈%ld  elapsed=%.2fs  rate=%.0f rows/s\n",
           opts.table.c_str(), opts.scale_factor, actual,
           elapsed, (elapsed > 0) ? actual / elapsed : 0.0);
    printf("  output: %s\n", filepath.c_str());

    return 0;
}
