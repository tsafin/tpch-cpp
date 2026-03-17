#include <iostream>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <map>
#include <getopt.h>
#include <iomanip>
#include <sys/stat.h>
#include <filesystem>
#include <sys/wait.h>
#include <unistd.h>
#include <cstdlib>

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/builder.h>

#include "tpch/writer_interface.hpp"
#include "tpch/csv_writer.hpp"
#include "tpch/parquet_writer.hpp"
#include "tpch/dbgen_wrapper.hpp"
#include "tpch/dbgen_converter.hpp"
#include "tpch/zero_copy_converter.hpp"  // Phase 13.4: Zero-copy optimizations
#include "tpch/performance_counters.hpp"
#include "tpch/io_uring_pool.hpp"
#include "tpch/io_uring_output_stream.hpp"
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
    long scale_factor = 1;
    std::string format = "parquet";
    std::string output_dir = "/tmp";
    long max_rows = 1000;
    bool verbose = false;
    bool parallel = false;
    int  parallel_tables = 0;  // max concurrent children; 0 = all
    bool zero_copy = false;
    std::string zero_copy_mode = "sync";  // sync, auto, async (Lance-specific)
    std::string compression = "zstd";     // snappy, zstd, none
    std::string table = "lineitem";
    long lance_rows_per_file = 0;
    long lance_rows_per_group = 0;
    long lance_max_bytes_per_file = 0;
    bool lance_skip_auto_cleanup = false;
    long lance_stream_queue = 16;
    std::string lance_stats_level;
    double lance_cardinality_sample_rate = 1.0;  // Phase 3.1: Sampling-based cardinality
    bool io_uring = false;  // use io_uring for disk writes (Parquet: IoUringOutputStream; Lance: Rust io_uring)
};

constexpr int OPT_LANCE_ROWS_PER_FILE = 1000;
constexpr int OPT_LANCE_ROWS_PER_GROUP = 1001;
constexpr int OPT_LANCE_MAX_BYTES_PER_FILE = 1002;
constexpr int OPT_LANCE_SKIP_AUTO_CLEANUP = 1003;
constexpr int OPT_LANCE_STREAM_QUEUE = 1004;
constexpr int OPT_LANCE_STATS_LEVEL = 1005;
constexpr int OPT_LANCE_CARDINALITY_SAMPLE_RATE = 1006;  // Phase 3.1
constexpr int OPT_PARALLEL_TABLES = 1007;
constexpr int OPT_ZERO_COPY_MODE = 1008;
constexpr int OPT_COMPRESSION   = 1009;
constexpr int OPT_IO_URING       = 1010;

constexpr size_t DBGEN_BATCH_SIZE = 8192;  // aligned with Lance max_rows_per_group

void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  --scale-factor <SF>   TPC-H scale factor (default: 1)\n"
              << "  --format <format>     Output format: parquet, csv"
#ifdef TPCH_ENABLE_ORC
              << ", orc"
#endif
#ifdef TPCH_ENABLE_PAIMON
              << ", paimon"
#endif
#ifdef TPCH_ENABLE_ICEBERG
              << ", iceberg"
#endif
#ifdef TPCH_ENABLE_LANCE
              << ", lance"
#endif
              << " (default: parquet)\n"
              << "  --output-dir <dir>    Output directory (default: /tmp)\n"
              << "  --max-rows <N>        Maximum rows to generate (default: 1000, 0=all)\n"
              << "  --table <name>        TPC-H table: lineitem, orders, customer, part,\n"
              << "                        partsupp, supplier, nation, region (default: lineitem)\n"
              << "  --parallel            Generate all 8 tables in parallel\n"
              << "  --parallel-tables <N> Max concurrent table children (default: all)\n"
              << "  --zero-copy           Enable zero-copy streaming writes (O(batch) RAM)\n"
              << "  --zero-copy-mode <m>  Zero-copy mode for Lance: sync (default), auto, async\n"
              << "  --compression <c>     Parquet compression: zstd (default), snappy, none\n"
              << "  --io-uring            Use io_uring for disk writes (Parquet: kernel async I/O;\n"
              << "                        Lance: delegated to Rust runtime)\n"
#ifdef TPCH_ENABLE_LANCE
              << "  --lance-rows-per-file <N>   Lance max rows per file (default: use Lance defaults)\n"
              << "  --lance-rows-per-group <N>  Lance max rows per group (default: use Lance defaults)\n"
              << "  --lance-max-bytes-per-file <N>  Lance max bytes per file (default: use Lance defaults)\n"
              << "  --lance-skip-auto-cleanup   Skip Lance auto cleanup during commit\n"
              << "  --lance-stream-queue <N>    Lance streaming queue depth (default: 16)\n"
              << "  --lance-stats-level <full|light>  Lance stats level (default: full)\n"
              << "  --lance-cardinality-sample-rate <0.0-1.0>  Cardinality sampling rate (Phase 3.1)\n"
              << "                               Controls HyperLogLog sampling: 1.0=100% (default),\n"
              << "                               0.5=50%, 0.1=10%. Smaller rates = faster writes.\n"
#endif
              << "  --verbose             Verbose output\n"
              << "  --help                Show this help message\n";
}

Options parse_args(int argc, char* argv[]) {
    Options opts;

    static struct option long_options[] = {
        {"scale-factor", required_argument, nullptr, 's'},
        {"format", required_argument, nullptr, 'f'},
        {"output-dir", required_argument, nullptr, 'o'},
        {"max-rows", required_argument, nullptr, 'm'},
        {"table", required_argument, nullptr, 't'},
        {"parallel", no_argument, nullptr, 'p'},
        {"parallel-tables", required_argument, nullptr, OPT_PARALLEL_TABLES},
        {"zero-copy", no_argument, nullptr, 'z'},
        {"zero-copy-mode", required_argument, nullptr, OPT_ZERO_COPY_MODE},
        {"compression",  required_argument, nullptr, OPT_COMPRESSION},
#ifdef TPCH_ENABLE_LANCE
        {"lance-rows-per-file", required_argument, nullptr, OPT_LANCE_ROWS_PER_FILE},
        {"lance-rows-per-group", required_argument, nullptr, OPT_LANCE_ROWS_PER_GROUP},
        {"lance-max-bytes-per-file", required_argument, nullptr, OPT_LANCE_MAX_BYTES_PER_FILE},
        {"lance-skip-auto-cleanup", no_argument, nullptr, OPT_LANCE_SKIP_AUTO_CLEANUP},
        {"lance-stream-queue", required_argument, nullptr, OPT_LANCE_STREAM_QUEUE},
        {"lance-stats-level", required_argument, nullptr, OPT_LANCE_STATS_LEVEL},
        {"lance-cardinality-sample-rate", required_argument, nullptr, OPT_LANCE_CARDINALITY_SAMPLE_RATE},
#endif
        {"io-uring", no_argument, nullptr, OPT_IO_URING},
        {"verbose", no_argument, nullptr, 'v'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0}
    };

    int c;
    while ((c = getopt_long(argc, argv, "s:f:o:m:t:pzvh", long_options, nullptr)) != -1) {
        switch (c) {
            case 's':
                opts.scale_factor = std::stol(optarg);
                break;
            case 'f':
                opts.format = optarg;
                break;
            case 'o':
                opts.output_dir = optarg;
                break;
            case 'm':
                opts.max_rows = std::stol(optarg);
                break;
            case 't':
                opts.table = optarg;
                break;
            case 'p':
                opts.parallel = true;
                break;
            case OPT_PARALLEL_TABLES:
                opts.parallel_tables = std::stoi(optarg);
                if (opts.parallel_tables <= 0) {
                    std::cerr << "Error: --parallel-tables must be > 0\n";
                    exit(1);
                }
                break;
            case 'z':
                opts.zero_copy = true;
                break;
            case OPT_ZERO_COPY_MODE:
                opts.zero_copy_mode = optarg;
                if (opts.zero_copy_mode != "sync" && opts.zero_copy_mode != "auto" && opts.zero_copy_mode != "async") {
                    std::cerr << "Error: --zero-copy-mode must be sync, auto, or async\n";
                    exit(1);
                }
                break;
            case OPT_COMPRESSION:
                opts.compression = optarg;
                break;
            case OPT_LANCE_ROWS_PER_FILE:
                opts.lance_rows_per_file = std::stol(optarg);
                break;
            case OPT_LANCE_ROWS_PER_GROUP:
                opts.lance_rows_per_group = std::stol(optarg);
                break;
            case OPT_LANCE_MAX_BYTES_PER_FILE:
                opts.lance_max_bytes_per_file = std::stol(optarg);
                break;
            case OPT_LANCE_SKIP_AUTO_CLEANUP:
                opts.lance_skip_auto_cleanup = true;
                break;
            case OPT_LANCE_STREAM_QUEUE:
                opts.lance_stream_queue = std::stol(optarg);
                break;
            case OPT_LANCE_STATS_LEVEL:
                opts.lance_stats_level = optarg;
                break;
            case OPT_LANCE_CARDINALITY_SAMPLE_RATE:
                opts.lance_cardinality_sample_rate = std::stod(optarg);
                if (opts.lance_cardinality_sample_rate < 0.01 || opts.lance_cardinality_sample_rate > 1.0) {
                    std::cerr << "Error: cardinality-sample-rate must be between 0.01 and 1.0\n";
                    exit(1);
                }
                break;
            case OPT_IO_URING:
                opts.io_uring = true;
                break;
            case 'v':
                opts.verbose = true;
                break;
            case 'h':
                print_usage(argv[0]);
                exit(0);
            default:
                print_usage(argv[0]);
                exit(1);
        }
    }

    return opts;
}

std::string get_output_filename(
    const std::string& output_dir,
    const std::string& format,
    const std::string& table = "") {
    std::string filename;
    if (!table.empty()) {
        filename = table + "." + format;
    } else {
        filename = "sample_data." + format;
    }
    if (!output_dir.empty() && output_dir.back() == '/') {
        return output_dir + filename;
    }
    return output_dir + "/" + filename;
}

long get_file_size(const std::string& filename) {
    namespace fs = std::filesystem;
    std::error_code ec;
    if (!fs::exists(filename, ec)) {
        return -1;
    }
    if (fs::is_regular_file(filename, ec)) {
        auto size = fs::file_size(filename, ec);
        return ec ? -1 : static_cast<long>(size);
    }
    if (fs::is_directory(filename, ec)) {
        uintmax_t total = 0;
        for (const auto& entry : fs::recursive_directory_iterator(filename, ec)) {
            if (ec) {
                return -1;
            }
            if (entry.is_regular_file(ec)) {
                total += entry.file_size(ec);
                if (ec) {
                    return -1;
                }
            }
        }
        return static_cast<long>(total);
    }
    return -1;
}

std::unique_ptr<tpch::WriterInterface> create_writer(
    const std::string& format,
    const std::string& filepath,
    const std::string& compression = "zstd",
    bool zero_copy = false) {
    if (format == "csv") {
        return std::make_unique<tpch::CSVWriter>(filepath);
    } else if (format == "parquet") {
        auto w = std::make_unique<tpch::ParquetWriter>(filepath);
        w->set_compression(compression);
        if (zero_copy) w->enable_streaming_write();
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
        return std::make_unique<tpch::LanceWriter>(filepath);
    }
#endif
    else {
        throw std::invalid_argument("Unknown format: " + format);
    }
}

std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>
create_builders_from_schema(std::shared_ptr<arrow::Schema> schema) {
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>> builders;

    // Pre-allocate capacity for batch size (10000 rows)
    // This reduces memory allocation overhead by avoiding incremental growth
    const int64_t capacity = 10000;

    for (const auto& field : schema->fields()) {
        if (field->type()->id() == arrow::Type::INT64) {
            auto builder = std::make_shared<arrow::Int64Builder>();
            (void)builder->Reserve(capacity);
            builders[field->name()] = builder;
        } else if (field->type()->id() == arrow::Type::DOUBLE) {
            auto builder = std::make_shared<arrow::DoubleBuilder>();
            (void)builder->Reserve(capacity);
            builders[field->name()] = builder;
        } else if (field->type()->id() == arrow::Type::STRING) {
            auto builder = std::make_shared<arrow::StringBuilder>();
            (void)builder->Reserve(capacity);
            (void)builder->ReserveData(capacity * 50);
            builders[field->name()] = builder;
        } else if (field->type()->id() == arrow::Type::DICTIONARY) {
            const auto& dict_type = static_cast<const arrow::DictionaryType&>(*field->type());
            if (dict_type.index_type()->id() == arrow::Type::INT16) {
                // dict16: date fields (2556 values) and p_type (150 values)
                auto builder = std::make_shared<arrow::Int16Builder>();
                (void)builder->Reserve(capacity);
                builders[field->name()] = builder;
            } else {
                // INT8 (default for low-cardinality columns, up to 127 values)
                auto builder = std::make_shared<arrow::Int8Builder>();
                (void)builder->Reserve(capacity);
                builders[field->name()] = builder;
            }
        } else {
            throw std::runtime_error("Unsupported type: " + field->type()->ToString());
        }
    }

    return builders;
}

std::shared_ptr<arrow::RecordBatch> finish_batch(
    std::shared_ptr<arrow::Schema> schema,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders,
    size_t num_rows) {

    std::vector<std::shared_ptr<arrow::Array>> arrays;

    for (const auto& field : schema->fields()) {
        auto it = builders.find(field->name());
        if (it == builders.end()) {
            throw std::runtime_error("Missing builder for: " + field->name());
        }
        auto array = it->second->Finish().ValueOrDie();
        if (field->type()->id() == arrow::Type::DICTIONARY) {
            // Wrap int8 indices in DictionaryArray with the static string dictionary
            auto dict = tpch::ZeroCopyConverter::get_dict_for_field(field->name());
            array = arrow::DictionaryArray::FromArrays(
                field->type(), array, dict).ValueOrDie();
        }
        arrays.push_back(array);
    }

    return arrow::RecordBatch::Make(schema, num_rows, arrays);
}

void reset_builders(
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {
    for (auto& [name, builder] : builders) {
        builder->Reset();
    }
}

template<typename GenerateFn>
void generate_with_dbgen(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    GenerateFn generate_fn,
    size_t& total_rows) {

    const size_t batch_size = DBGEN_BATCH_SIZE;
    size_t rows_in_batch = 0;

    auto builders = create_builders_from_schema(schema);

    auto append_callback = [&](const void* row) {
        tpch::append_row_to_builders(opts.table, row, builders);
        rows_in_batch++;
        total_rows++;

        if (rows_in_batch >= batch_size) {
            auto batch = finish_batch(schema, builders, rows_in_batch);
            writer->write_batch(batch);
            reset_builders(builders);
            rows_in_batch = 0;

            if (opts.verbose && (total_rows % 100000 == 0)) {
                std::cout << "  Generated " << total_rows << " rows...\n";
            }
        }
    };

    // Call the appropriate generate_* function with callback
    generate_fn(dbgen, append_callback);

    // Flush remaining rows
    if (rows_in_batch > 0) {
        auto batch = finish_batch(schema, builders, rows_in_batch);
        writer->write_batch(batch);
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated: " << total_rows << "\n";
    }
}

// ============================================================================
// Phase 13.4: Zero-copy generation for lineitem
// ============================================================================

/**
 * Generate lineitem using zero-copy batch iterator
 *
 * Uses std::span and std::string_view for minimal memory copies.
 * Expected 60-80% reduction in memory bandwidth.
 */
void generate_lineitem_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;  // Match Phase 13.4 plan

    // Use batch iterator (zero-copy friendly)
    auto batch_iter = dbgen.generate_lineitem_batches(batch_size, opts.max_rows);

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        // Convert batch to Arrow using zero-copy span
        auto arrow_batch_result = tpch::ZeroCopyConverter::lineitem_to_recordbatch(
            dbgen_batch.span(),  // std::span view (no copy!)
            schema
        );

        if (!arrow_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + arrow_batch_result.status().ToString());
        }

        auto arrow_batch = arrow_batch_result.ValueOrDie();
        writer->write_batch(arrow_batch);

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (zero-copy): " << total_rows << "\n";
    }
}

void generate_orders_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;
    auto batch_iter = dbgen.generate_orders_batches(batch_size, opts.max_rows);

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto arrow_batch_result = tpch::ZeroCopyConverter::orders_to_recordbatch(
            dbgen_batch.span(), schema
        );

        if (!arrow_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + arrow_batch_result.status().ToString());
        }

        auto arrow_batch = arrow_batch_result.ValueOrDie();
        writer->write_batch(arrow_batch);

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (zero-copy): " << total_rows << "\n";
    }
}

void generate_customer_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;
    auto batch_iter = dbgen.generate_customer_batches(batch_size, opts.max_rows);

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto arrow_batch_result = tpch::ZeroCopyConverter::customer_to_recordbatch(
            dbgen_batch.span(), schema
        );

        if (!arrow_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + arrow_batch_result.status().ToString());
        }

        auto arrow_batch = arrow_batch_result.ValueOrDie();
        writer->write_batch(arrow_batch);

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (zero-copy): " << total_rows << "\n";
    }
}

void generate_part_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;
    auto batch_iter = dbgen.generate_part_batches(batch_size, opts.max_rows);

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto arrow_batch_result = tpch::ZeroCopyConverter::part_to_recordbatch(
            dbgen_batch.span(), schema
        );

        if (!arrow_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + arrow_batch_result.status().ToString());
        }

        auto arrow_batch = arrow_batch_result.ValueOrDie();
        writer->write_batch(arrow_batch);

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (zero-copy): " << total_rows << "\n";
    }
}

void generate_partsupp_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;
    auto batch_iter = dbgen.generate_partsupp_batches(batch_size, opts.max_rows);

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto arrow_batch_result = tpch::ZeroCopyConverter::partsupp_to_recordbatch(
            dbgen_batch.span(), schema
        );

        if (!arrow_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + arrow_batch_result.status().ToString());
        }

        auto arrow_batch = arrow_batch_result.ValueOrDie();
        writer->write_batch(arrow_batch);

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (zero-copy): " << total_rows << "\n";
    }
}

void generate_supplier_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;
    auto batch_iter = dbgen.generate_supplier_batches(batch_size, opts.max_rows);

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto arrow_batch_result = tpch::ZeroCopyConverter::supplier_to_recordbatch(
            dbgen_batch.span(), schema
        );

        if (!arrow_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + arrow_batch_result.status().ToString());
        }

        auto arrow_batch = arrow_batch_result.ValueOrDie();
        writer->write_batch(arrow_batch);

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (zero-copy): " << total_rows << "\n";
    }
}

void generate_nation_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;  // Nation table has exactly 25 rows
    auto batch_iter = dbgen.generate_nation_batches(batch_size, opts.max_rows);

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto arrow_batch_result = tpch::ZeroCopyConverter::nation_to_recordbatch(
            dbgen_batch.span(), schema
        );

        if (!arrow_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + arrow_batch_result.status().ToString());
        }

        auto arrow_batch = arrow_batch_result.ValueOrDie();
        writer->write_batch(arrow_batch);

        total_rows += dbgen_batch.size();
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (zero-copy): " << total_rows << "\n";
    }
}

void generate_region_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;  // Region table has exactly 5 rows
    auto batch_iter = dbgen.generate_region_batches(batch_size, opts.max_rows);

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto arrow_batch_result = tpch::ZeroCopyConverter::region_to_recordbatch(
            dbgen_batch.span(), schema
        );

        if (!arrow_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + arrow_batch_result.status().ToString());
        }

        auto arrow_batch = arrow_batch_result.ValueOrDie();
        writer->write_batch(arrow_batch);

        total_rows += dbgen_batch.size();
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (zero-copy): " << total_rows << "\n";
    }
}

// ============================================================================
// Phase 14.2.3: True Zero-Copy generation with Buffer::Wrap
// ============================================================================

/**
 * Generate lineitem using true zero-copy (Buffer::Wrap)
 *
 * Uses Buffer::Wrap to directly reference vector memory for numeric arrays.
 * Strings still use memcpy (non-contiguous in dbgen structs).
 *
 * Expected 10-20% speedup over Phase 14.1 for lineitem.
 */
void generate_lineitem_true_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;

    // Use batch iterator (zero-copy friendly)
    auto batch_iter = dbgen.generate_lineitem_batches(batch_size, opts.max_rows);

    // Enable streaming write mode for optimal memory usage with true zero-copy
    auto parquet_writer = dynamic_cast<tpch::ParquetWriter*>(writer.get());
    if (parquet_writer) {
        parquet_writer->enable_streaming_write(true);
    }

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        // Convert batch to Arrow using true zero-copy (Buffer::Wrap)
        auto managed_batch_result = tpch::ZeroCopyConverter::lineitem_to_recordbatch_wrapped(
            dbgen_batch.span(),  // std::span view (no copy!)
            schema
        );

        if (!managed_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + managed_batch_result.status().ToString());
        }

        auto managed_batch = managed_batch_result.ValueOrDie();
        if (parquet_writer) {
            parquet_writer->write_managed_batch(managed_batch);
        } else {
            // Fallback for non-Parquet writers
            writer->write_batch(managed_batch.batch);
        }

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (true zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (true zero-copy): " << total_rows << "\n";
    }
}

void generate_orders_true_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;
    auto batch_iter = dbgen.generate_orders_batches(batch_size, opts.max_rows);

    auto parquet_writer = dynamic_cast<tpch::ParquetWriter*>(writer.get());
    if (parquet_writer) {
        parquet_writer->enable_streaming_write(true);
    }

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto managed_batch_result = tpch::ZeroCopyConverter::orders_to_recordbatch_wrapped(
            dbgen_batch.span(), schema
        );

        if (!managed_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + managed_batch_result.status().ToString());
        }

        auto managed_batch = managed_batch_result.ValueOrDie();
        if (parquet_writer) {
            parquet_writer->write_managed_batch(managed_batch);
        } else {
            writer->write_batch(managed_batch.batch);
        }

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (true zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (true zero-copy): " << total_rows << "\n";
    }
}

void generate_customer_true_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;
    auto batch_iter = dbgen.generate_customer_batches(batch_size, opts.max_rows);

    auto parquet_writer = dynamic_cast<tpch::ParquetWriter*>(writer.get());
    if (parquet_writer) {
        parquet_writer->enable_streaming_write(true);
    }

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto managed_batch_result = tpch::ZeroCopyConverter::customer_to_recordbatch_wrapped(
            dbgen_batch.span(), schema
        );

        if (!managed_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + managed_batch_result.status().ToString());
        }

        auto managed_batch = managed_batch_result.ValueOrDie();
        if (parquet_writer) {
            parquet_writer->write_managed_batch(managed_batch);
        } else {
            writer->write_batch(managed_batch.batch);
        }

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (true zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (true zero-copy): " << total_rows << "\n";
    }
}

void generate_part_true_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;
    auto batch_iter = dbgen.generate_part_batches(batch_size, opts.max_rows);

    auto parquet_writer = dynamic_cast<tpch::ParquetWriter*>(writer.get());
    if (parquet_writer) {
        parquet_writer->enable_streaming_write(true);
    }

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto managed_batch_result = tpch::ZeroCopyConverter::part_to_recordbatch_wrapped(
            dbgen_batch.span(), schema
        );

        if (!managed_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + managed_batch_result.status().ToString());
        }

        auto managed_batch = managed_batch_result.ValueOrDie();
        if (parquet_writer) {
            parquet_writer->write_managed_batch(managed_batch);
        } else {
            writer->write_batch(managed_batch.batch);
        }

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (true zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (true zero-copy): " << total_rows << "\n";
    }
}

void generate_partsupp_true_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;
    auto batch_iter = dbgen.generate_partsupp_batches(batch_size, opts.max_rows);

    auto parquet_writer = dynamic_cast<tpch::ParquetWriter*>(writer.get());
    if (parquet_writer) {
        parquet_writer->enable_streaming_write(true);
    }

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto managed_batch_result = tpch::ZeroCopyConverter::partsupp_to_recordbatch_wrapped(
            dbgen_batch.span(), schema
        );

        if (!managed_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + managed_batch_result.status().ToString());
        }

        auto managed_batch = managed_batch_result.ValueOrDie();
        if (parquet_writer) {
            parquet_writer->write_managed_batch(managed_batch);
        } else {
            writer->write_batch(managed_batch.batch);
        }

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (true zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (true zero-copy): " << total_rows << "\n";
    }
}

void generate_supplier_true_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;
    auto batch_iter = dbgen.generate_supplier_batches(batch_size, opts.max_rows);

    auto parquet_writer = dynamic_cast<tpch::ParquetWriter*>(writer.get());
    if (parquet_writer) {
        parquet_writer->enable_streaming_write(true);
    }

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto managed_batch_result = tpch::ZeroCopyConverter::supplier_to_recordbatch_wrapped(
            dbgen_batch.span(), schema
        );

        if (!managed_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + managed_batch_result.status().ToString());
        }

        auto managed_batch = managed_batch_result.ValueOrDie();
        if (parquet_writer) {
            parquet_writer->write_managed_batch(managed_batch);
        } else {
            writer->write_batch(managed_batch.batch);
        }

        total_rows += dbgen_batch.size();

        if (opts.verbose && (total_rows % 100000 == 0)) {
            std::cout << "  Generated " << total_rows << " rows (true zero-copy)...\n";
        }
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (true zero-copy): " << total_rows << "\n";
    }
}

void generate_nation_true_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;  // Nation table has exactly 25 rows
    auto batch_iter = dbgen.generate_nation_batches(batch_size, opts.max_rows);

    auto parquet_writer = dynamic_cast<tpch::ParquetWriter*>(writer.get());
    if (parquet_writer) {
        parquet_writer->enable_streaming_write(true);
    }

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto managed_batch_result = tpch::ZeroCopyConverter::nation_to_recordbatch_wrapped(
            dbgen_batch.span(), schema
        );

        if (!managed_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + managed_batch_result.status().ToString());
        }

        auto managed_batch = managed_batch_result.ValueOrDie();
        if (parquet_writer) {
            parquet_writer->write_managed_batch(managed_batch);
        } else {
            writer->write_batch(managed_batch.batch);
        }

        total_rows += dbgen_batch.size();
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (true zero-copy): " << total_rows << "\n";
    }
}

void generate_region_true_zero_copy(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    size_t& total_rows) {

    const size_t batch_size = 10000;  // Region table has exactly 5 rows
    auto batch_iter = dbgen.generate_region_batches(batch_size, opts.max_rows);

    auto parquet_writer = dynamic_cast<tpch::ParquetWriter*>(writer.get());
    if (parquet_writer) {
        parquet_writer->enable_streaming_write(true);
    }

    while (batch_iter.has_next()) {
        auto dbgen_batch = batch_iter.next();

        auto managed_batch_result = tpch::ZeroCopyConverter::region_to_recordbatch_wrapped(
            dbgen_batch.span(), schema
        );

        if (!managed_batch_result.ok()) {
            throw std::runtime_error("Failed to convert batch: " + managed_batch_result.status().ToString());
        }

        auto managed_batch = managed_batch_result.ValueOrDie();
        if (parquet_writer) {
            parquet_writer->write_managed_batch(managed_batch);
        } else {
            writer->write_batch(managed_batch.batch);
        }

        total_rows += dbgen_batch.size();
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated (true zero-copy): " << total_rows << "\n";
    }
}

// ============================================================================
// Phase 12.6: Fork-after-init parallel generation (fixes Phase 12.3)
// ============================================================================

/**
 * Generate all tables in parallel using fork-after-init pattern
 *
 * Key insight: dbgen initialization does NOT corrupt table-generation seeds.
 * Seeds are partitioned by table, so we can initialize ONCE in the parent,
 * then fork child processes that inherit all initialization via COW.
 *
 * This eliminates the 8× initialization overhead that made Phase 12.3 broken.
 */
// Wire io_uring into a writer after IoUringPool::init() has been called.
// For Lance: delegates to the Rust runtime via enable_io_uring().
// For Parquet (and future formats): injects IoUringOutputStream.
// No-op if opts.io_uring is false or IoUringPool is unavailable.
static void wire_io_uring(const Options& opts, const std::string& path,
                          tpch::WriterInterface* writer) {
    if (!opts.io_uring || !tpch::IoUringPool::available())
        return;

#ifdef TPCH_ENABLE_LANCE
    if (auto* lw = dynamic_cast<tpch::LanceWriter*>(writer)) {
        lw->enable_io_uring(true);
        return;
    }
#endif
    if (auto* pw = dynamic_cast<tpch::ParquetWriter*>(writer)) {
        void* ring = tpch::IoUringPool::create_child_ring_struct();
        pw->set_output_stream(std::make_shared<tpch::IoUringOutputStream>(path, ring));
    }
    // Other formats (csv, orc, …) don't yet support stream injection — silently skip.
}

// Run one table in a child process.  Called after fork() — must not return to parent.
static void run_table_child(const Options& opts, const std::string& table) {
    try {
        std::string output_path = get_output_filename(opts.output_dir, opts.format, table);

        tpch::DBGenWrapper dbgen(opts.scale_factor, opts.verbose);
        dbgen.set_skip_init(true);  // distributions already loaded by parent (COW)

        std::shared_ptr<arrow::Schema> schema;
        if (table == "lineitem")  schema = tpch::DBGenWrapper::get_schema(tpch::TableType::LINEITEM,  opts.scale_factor);
        else if (table == "orders")   schema = tpch::DBGenWrapper::get_schema(tpch::TableType::ORDERS,   opts.scale_factor);
        else if (table == "customer") schema = tpch::DBGenWrapper::get_schema(tpch::TableType::CUSTOMER, opts.scale_factor);
        else if (table == "part")     schema = tpch::DBGenWrapper::get_schema(tpch::TableType::PART,     opts.scale_factor);
        else if (table == "partsupp") schema = tpch::DBGenWrapper::get_schema(tpch::TableType::PARTSUPP, opts.scale_factor);
        else if (table == "supplier") schema = tpch::DBGenWrapper::get_schema(tpch::TableType::SUPPLIER, opts.scale_factor);
        else if (table == "nation")   schema = tpch::DBGenWrapper::get_schema(tpch::TableType::NATION,   opts.scale_factor);
        else if (table == "region")   schema = tpch::DBGenWrapper::get_schema(tpch::TableType::REGION,   opts.scale_factor);
        else { fprintf(stderr, "tpch_benchmark: unknown table %s\n", table.c_str()); exit(1); }

        auto writer = create_writer(opts.format, output_path, opts.compression, opts.zero_copy);

#ifdef TPCH_ENABLE_LANCE
        if (auto* lw = dynamic_cast<tpch::LanceWriter*>(writer.get())) {
            if (opts.zero_copy) {
                bool use_async = (opts.zero_copy_mode == "async") || (opts.zero_copy_mode == "auto");
                lw->enable_streaming_write(!use_async);
            }
        }
#endif

        wire_io_uring(opts, output_path, writer.get());

        size_t total_rows = 0;
        Options child_opts = opts;
        child_opts.table = table;

        auto t0 = std::chrono::steady_clock::now();

        if (table == "lineitem") {
            if (opts.zero_copy) generate_lineitem_zero_copy(dbgen, child_opts, schema, writer, total_rows);
            else generate_with_dbgen(dbgen, child_opts, schema, writer,
                [&](auto& g, auto& cb) { g.generate_lineitem(cb, opts.max_rows); }, total_rows);
        } else if (table == "orders") {
            if (opts.zero_copy) generate_orders_zero_copy(dbgen, child_opts, schema, writer, total_rows);
            else generate_with_dbgen(dbgen, child_opts, schema, writer,
                [&](auto& g, auto& cb) { g.generate_orders(cb, opts.max_rows); }, total_rows);
        } else if (table == "customer") {
            if (opts.zero_copy) generate_customer_zero_copy(dbgen, child_opts, schema, writer, total_rows);
            else generate_with_dbgen(dbgen, child_opts, schema, writer,
                [&](auto& g, auto& cb) { g.generate_customer(cb, opts.max_rows); }, total_rows);
        } else if (table == "part") {
            if (opts.zero_copy) generate_part_zero_copy(dbgen, child_opts, schema, writer, total_rows);
            else generate_with_dbgen(dbgen, child_opts, schema, writer,
                [&](auto& g, auto& cb) { g.generate_part(cb, opts.max_rows); }, total_rows);
        } else if (table == "partsupp") {
            if (opts.zero_copy) generate_partsupp_zero_copy(dbgen, child_opts, schema, writer, total_rows);
            else generate_with_dbgen(dbgen, child_opts, schema, writer,
                [&](auto& g, auto& cb) { g.generate_partsupp(cb, opts.max_rows); }, total_rows);
        } else if (table == "supplier") {
            if (opts.zero_copy) generate_supplier_zero_copy(dbgen, child_opts, schema, writer, total_rows);
            else generate_with_dbgen(dbgen, child_opts, schema, writer,
                [&](auto& g, auto& cb) { g.generate_supplier(cb, opts.max_rows); }, total_rows);
        } else if (table == "nation") {
            if (opts.zero_copy) generate_nation_zero_copy(dbgen, child_opts, schema, writer, total_rows);
            else generate_with_dbgen(dbgen, child_opts, schema, writer,
                [&](auto& g, auto& cb) { g.generate_nation(cb); }, total_rows);
        } else if (table == "region") {
            if (opts.zero_copy) generate_region_zero_copy(dbgen, child_opts, schema, writer, total_rows);
            else generate_with_dbgen(dbgen, child_opts, schema, writer,
                [&](auto& g, auto& cb) { g.generate_region(cb); }, total_rows);
        }

        writer->close();

        double elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - t0).count();
        printf("tpch_benchmark: %-12s  SF=%ld  rows=%zu  elapsed=%.2fs  rate=%.0f rows/s\n",
               table.c_str(), opts.scale_factor, total_rows,
               elapsed, elapsed > 0 ? total_rows / elapsed : 0.0);
        printf("  output: %s\n", output_path.c_str());
        fflush(stdout);
        exit(0);
    } catch (const std::exception& e) {
        fprintf(stderr, "tpch_benchmark: [%s] failed: %s\n", table.c_str(), e.what());
        exit(1);
    }
}

// Fork-after-init parallel generation with rolling N-slot window.
int generate_all_tables_parallel(const Options& opts) {
    static const std::vector<std::string> tables = {
        "region", "nation", "supplier", "part",
        "partsupp", "customer", "orders", "lineitem"
    };
    const size_t ntables    = tables.size();
    const size_t slot_limit = (opts.parallel_tables > 0)
        ? static_cast<size_t>(opts.parallel_tables)
        : ntables;

    // Initialize dbgen ONCE in the parent — all children inherit via COW.
    fprintf(stderr, "tpch_benchmark: initializing dbgen (SF=%ld)...\n", opts.scale_factor);
    tpch::dbgen_init_global(opts.scale_factor, opts.verbose);

    // Initialize anchor io_uring ring before fork so children can
    // attach via IORING_SETUP_ATTACH_WQ and share one kernel worker pool.
    bool io_uring_ready = opts.io_uring && tpch::IoUringPool::init(opts.output_dir);

    auto t_wall = std::chrono::steady_clock::now();

    fprintf(stderr,
        "tpch_benchmark: parallel  SF=%ld  tables=%zu  slots=%zu  format=%s  io_uring=%s\n",
        opts.scale_factor, ntables, slot_limit, opts.format.c_str(),
        io_uring_ready ? "yes" : "no");

    std::vector<pid_t>  pids(ntables, -1);
    std::vector<size_t> slot_table(slot_limit, SIZE_MAX);
    size_t next   = 0;
    size_t active = 0;
    int    failed = 0;

    auto fork_next = [&](size_t slot) {
        if (next >= ntables) return;
        const std::string& tname = tables[next];

        pid_t pid = ::fork();
        if (pid < 0) { perror("fork"); ++failed; ++next; return; }
        if (pid == 0) {
            run_table_child(opts, tname);  // never returns
        }
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

        size_t freed_slot = SIZE_MAX;
        for (size_t s = 0; s < slot_limit; ++s) {
            if (slot_table[s] < ntables && pids[slot_table[s]] == done) {
                freed_slot = s;
                break;
            }
        }

        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            const char* tname = (freed_slot < slot_limit && slot_table[freed_slot] < ntables)
                ? tables[slot_table[freed_slot]].c_str()
                : "unknown";
            fprintf(stderr, "tpch_benchmark: [%s] child failed (pid=%d status=%d)\n",
                    tname, done, status);
            ++failed;
        }
        --active;

        if (freed_slot != SIZE_MAX)
            fork_next(freed_slot);
    }

    double wall = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t_wall).count();
    fprintf(stderr,
        "tpch_benchmark: parallel done  SF=%ld  %zu tables  wall=%.2fs  %s\n",
        opts.scale_factor, ntables, wall,
        failed ? "SOME TABLES FAILED" : "all ok");

    return failed ? 1 : 0;
}

}  // anonymous namespace

int main(int argc, char* argv[]) {
    try {
        auto opts = parse_args(argc, argv);

        if (opts.parallel) {
            return generate_all_tables_parallel(opts);
        }

        // Validate format
        if (opts.format != "csv" && opts.format != "parquet"
#ifdef TPCH_ENABLE_ORC
            && opts.format != "orc"
#endif
#ifdef TPCH_ENABLE_PAIMON
            && opts.format != "paimon"
#endif
#ifdef TPCH_ENABLE_ICEBERG
            && opts.format != "iceberg"
#endif
#ifdef TPCH_ENABLE_LANCE
            && opts.format != "lance"
#endif
        ) {
            std::cerr << "Error: Unknown format '" << opts.format << "'\n";
            return 1;
        }

        if (opts.verbose) {
            std::cout << "TPC-H Benchmark Driver\n";
            std::cout << "Data source: Official TPC-H dbgen\n";
            std::cout << "Scale factor: " << opts.scale_factor << "\n";
            std::cout << "Format: " << opts.format << "\n";
            std::cout << "Table: " << opts.table << "\n";
            std::cout << "Max rows: " << (opts.max_rows > 0 ? std::to_string(opts.max_rows) : std::string("all")) << "\n";
        }

        std::string output_path = get_output_filename(opts.output_dir, opts.format, opts.table);
        if (opts.verbose) {
            std::cout << "Output file: " << output_path << "\n";
        }

        // Create schema based on selected table
        std::shared_ptr<arrow::Schema> schema;
        if (opts.table == "lineitem") {
            schema = tpch::DBGenWrapper::get_schema(tpch::TableType::LINEITEM, opts.scale_factor);
        } else if (opts.table == "orders") {
            schema = tpch::DBGenWrapper::get_schema(tpch::TableType::ORDERS, opts.scale_factor);
        } else if (opts.table == "customer") {
            schema = tpch::DBGenWrapper::get_schema(tpch::TableType::CUSTOMER, opts.scale_factor);
        } else if (opts.table == "part") {
            schema = tpch::DBGenWrapper::get_schema(tpch::TableType::PART, opts.scale_factor);
        } else if (opts.table == "partsupp") {
            schema = tpch::DBGenWrapper::get_schema(tpch::TableType::PARTSUPP, opts.scale_factor);
        } else if (opts.table == "supplier") {
            schema = tpch::DBGenWrapper::get_schema(tpch::TableType::SUPPLIER, opts.scale_factor);
        } else if (opts.table == "nation") {
            schema = tpch::DBGenWrapper::get_schema(tpch::TableType::NATION, opts.scale_factor);
        } else if (opts.table == "region") {
            schema = tpch::DBGenWrapper::get_schema(tpch::TableType::REGION, opts.scale_factor);
        } else {
            std::cerr << "Error: Unknown table '" << opts.table << "'\n";
            return 1;
        }

        if (opts.verbose) {
            std::cout << "Schema: " << schema->ToString() << "\n";
        }

        // Create writer
        if (opts.io_uring)
            tpch::IoUringPool::init(opts.output_dir);

        auto writer = create_writer(opts.format, output_path, opts.compression, opts.zero_copy);
        wire_io_uring(opts, output_path, writer.get());

#ifdef TPCH_ENABLE_LANCE
        if (auto lance_writer = dynamic_cast<tpch::LanceWriter*>(writer.get())) {
            if (!opts.lance_stats_level.empty()) {
                setenv("LANCE_STATS_LEVEL", opts.lance_stats_level.c_str(), 1);
                if (opts.verbose) {
                    std::cout << "Lance stats level set to: " << opts.lance_stats_level << "\n";
                }
            }
            // Phase 3.1: Set cardinality sampling rate via environment variable
            if (opts.lance_cardinality_sample_rate < 1.0) {
                std::string rate_str = std::to_string(opts.lance_cardinality_sample_rate);
                setenv("LANCE_CARDINALITY_SAMPLE_RATE", rate_str.c_str(), 1);
                if (opts.verbose) {
                    std::cout << "Lance cardinality sample rate set to: " << opts.lance_cardinality_sample_rate << "\n";
                }
            }
            lance_writer->set_write_params(
                opts.lance_rows_per_file,
                opts.lance_rows_per_group,
                opts.lance_max_bytes_per_file,
                opts.lance_skip_auto_cleanup);
            if (opts.lance_stream_queue > 0) {
                lance_writer->set_stream_queue_depth(static_cast<size_t>(opts.lance_stream_queue));
            }

            if (opts.zero_copy) {
                bool use_async = (opts.zero_copy_mode == "async") ||
                                 (opts.zero_copy_mode == "auto");
                lance_writer->enable_streaming_write(!use_async);
                if (opts.verbose) {
                    std::cout << "Lance streaming write mode enabled (zero-copy, mode="
                              << opts.zero_copy_mode << ")\n";
                }
            }

        }
#endif

        // Start timing
        auto start_time = std::chrono::high_resolution_clock::now();

        size_t total_rows = 0;

        if (opts.verbose) {
            std::cout << "Starting data generation...\n";
        }

        // Generate data using official TPC-H dbgen
        tpch::DBGenWrapper dbgen(opts.scale_factor, opts.verbose);

        if (opts.table == "lineitem") {
            if (opts.zero_copy) {
                generate_lineitem_zero_copy(dbgen, opts, schema, writer, total_rows);
            } else {
                generate_with_dbgen(dbgen, opts, schema, writer,
                    [&](auto& g, auto& cb) { g.generate_lineitem(cb, opts.max_rows); }, total_rows);
            }
        } else if (opts.table == "orders") {
            if (opts.zero_copy) {
                generate_orders_zero_copy(dbgen, opts, schema, writer, total_rows);
            } else {
                generate_with_dbgen(dbgen, opts, schema, writer,
                    [&](auto& g, auto& cb) { g.generate_orders(cb, opts.max_rows); }, total_rows);
            }
        } else if (opts.table == "customer") {
            if (opts.zero_copy) {
                generate_customer_zero_copy(dbgen, opts, schema, writer, total_rows);
            } else {
                generate_with_dbgen(dbgen, opts, schema, writer,
                    [&](auto& g, auto& cb) { g.generate_customer(cb, opts.max_rows); }, total_rows);
            }
        } else if (opts.table == "part") {
            if (opts.zero_copy) {
                generate_part_zero_copy(dbgen, opts, schema, writer, total_rows);
            } else {
                generate_with_dbgen(dbgen, opts, schema, writer,
                    [&](auto& g, auto& cb) { g.generate_part(cb, opts.max_rows); }, total_rows);
            }
        } else if (opts.table == "partsupp") {
            if (opts.zero_copy) {
                generate_partsupp_zero_copy(dbgen, opts, schema, writer, total_rows);
            } else {
                generate_with_dbgen(dbgen, opts, schema, writer,
                    [&](auto& g, auto& cb) { g.generate_partsupp(cb, opts.max_rows); }, total_rows);
            }
        } else if (opts.table == "supplier") {
            if (opts.zero_copy) {
                generate_supplier_zero_copy(dbgen, opts, schema, writer, total_rows);
            } else {
                generate_with_dbgen(dbgen, opts, schema, writer,
                    [&](auto& g, auto& cb) { g.generate_supplier(cb, opts.max_rows); }, total_rows);
            }
        } else if (opts.table == "nation") {
            if (opts.zero_copy) {
                generate_nation_zero_copy(dbgen, opts, schema, writer, total_rows);
            } else {
                generate_with_dbgen(dbgen, opts, schema, writer,
                    [&](auto& g, auto& cb) { g.generate_nation(cb); }, total_rows);
            }
        } else if (opts.table == "region") {
            if (opts.zero_copy) {
                generate_region_zero_copy(dbgen, opts, schema, writer, total_rows);
            } else {
                generate_with_dbgen(dbgen, opts, schema, writer,
                    [&](auto& g, auto& cb) { g.generate_region(cb); }, total_rows);
            }
        } else {
            std::cerr << "Error: Unknown table '" << opts.table << "'\n";
            return 1;
        }

        // Close writer
        writer->close();

        // Calculate elapsed time
        auto end_time = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        // Get output file size
        long file_size = get_file_size(output_path);

        // Print summary
        std::cout << "\n=== TPC-H Data Generation Complete ===\n";
        std::cout << "Data source: Official TPC-H dbgen\n";
        std::cout << "Format: " << opts.format << "\n";
        std::cout << "Output file: " << output_path << "\n";
        std::cout << "Rows written: " << total_rows << "\n";
        if (file_size > 0) {
            std::cout << "File size: " << file_size << " bytes\n";
        }
        std::cout << "Time elapsed: " << std::fixed << std::setprecision(3)
                  << (static_cast<double>(elapsed.count()) / 1000.0) << " seconds\n";

        if (total_rows > 0 && elapsed.count() > 0) {
            double throughput = (static_cast<double>(total_rows) * 1000.0) / static_cast<double>(elapsed.count());
            std::cout << "Throughput: " << std::fixed << std::setprecision(0)
                      << throughput << " rows/sec\n";
        }

        if (file_size > 0 && elapsed.count() > 0) {
            double mb_per_sec = (static_cast<double>(file_size) / (1024.0 * 1024.0)) /
                                (static_cast<double>(elapsed.count()) / 1000.0);
            std::cout << "Write rate: " << std::fixed << std::setprecision(2)
                      << mb_per_sec << " MB/sec\n";
        }

#ifdef TPCH_ENABLE_PERF_COUNTERS
        // Print performance counters report if enabled
        tpch::PerformanceCounters::instance().print_report();
#endif

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
