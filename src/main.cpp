#include <iostream>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <getopt.h>
#include <iomanip>
#include <sys/stat.h>

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/builder.h>

#include "tpch/writer_interface.hpp"
#include "tpch/csv_writer.hpp"
#include "tpch/parquet_writer.hpp"
#ifdef TPCH_ENABLE_ORC
#include "tpch/orc_writer.hpp"
#endif

namespace {

struct Options {
    long scale_factor = 1;
    std::string format = "parquet";
    std::string output_dir = "/tmp";
    long max_rows = 1000;
    bool async_io = false;
    bool verbose = false;
};

void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  --scale-factor <SF>   TPC-H scale factor (default: 1)\n"
              << "  --format <format>     Output format: parquet, csv, orc (default: parquet)\n"
              << "  --output-dir <dir>    Output directory (default: /tmp)\n"
              << "  --max-rows <N>        Maximum rows to generate (default: 1000)\n"
#ifdef TPCH_ENABLE_ASYNC_IO
              << "  --async-io            Enable async I/O with io_uring\n"
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
#ifdef TPCH_ENABLE_ASYNC_IO
        {"async-io", no_argument, nullptr, 'a'},
#endif
        {"verbose", no_argument, nullptr, 'v'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0}
    };

    int c;
    while ((c = getopt_long(argc, argv, "s:f:o:m:avh", long_options, nullptr)) != -1) {
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
#ifdef TPCH_ENABLE_ASYNC_IO
            case 'a':
                opts.async_io = true;
                break;
#endif
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
    const std::string& format) {
    std::string filename = "sample_data." + format;
    if (!output_dir.empty() && output_dir.back() == '/') {
        return output_dir + filename;
    }
    return output_dir + "/" + filename;
}

long get_file_size(const std::string& filename) {
    struct stat st;
    if (stat(filename.c_str(), &st) != 0) {
        return -1;
    }
    return st.st_size;
}

std::unique_ptr<tpch::WriterInterface> create_writer(
    const std::string& format,
    const std::string& filepath) {
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
    else {
        throw std::invalid_argument("Unknown format: " + format);
    }
}

}  // anonymous namespace

int main(int argc, char* argv[]) {
    try {
        auto opts = parse_args(argc, argv);

        // Validate format
        if (opts.format != "csv" && opts.format != "parquet"
#ifdef TPCH_ENABLE_ORC
            && opts.format != "orc"
#endif
        ) {
            std::cerr << "Error: Unknown format '" << opts.format << "'\n";
            return 1;
        }

        if (opts.verbose) {
            std::cout << "TPC-H Benchmark Driver - Proof of Concept\n";
            std::cout << "Scale factor: " << opts.scale_factor << "\n";
            std::cout << "Format: " << opts.format << "\n";
            std::cout << "Max rows: " << opts.max_rows << "\n";
        }

        // Create output path
        std::string output_path = get_output_filename(opts.output_dir, opts.format);
        if (opts.verbose) {
            std::cout << "Output file: " << output_path << "\n";
        }

        // Create schema for sample lineitem table
        auto schema = arrow::schema({
            arrow::field("l_orderkey", arrow::int64()),
            arrow::field("l_partkey", arrow::int64()),
            arrow::field("l_suppkey", arrow::int64()),
            arrow::field("l_linenumber", arrow::int64()),
            arrow::field("l_quantity", arrow::float64()),
            arrow::field("l_extendedprice", arrow::float64()),
            arrow::field("l_discount", arrow::float64()),
            arrow::field("l_tax", arrow::float64()),
            arrow::field("l_returnflag", arrow::utf8()),
            arrow::field("l_linestatus", arrow::utf8()),
        });

        if (opts.verbose) {
            std::cout << "Schema: " << schema->ToString() << "\n";
        }

        // Create writer
        auto writer = create_writer(opts.format, output_path);

        // Start timing
        auto start_time = std::chrono::high_resolution_clock::now();

        // Generate synthetic data in batches
        const size_t batch_size = 10000;
        size_t total_rows = 0;
        size_t batch_count = 0;

        if (opts.verbose) {
            std::cout << "Starting data generation...\n";
        }

        // Create builders for each column
        auto orderkey_builder = std::make_shared<arrow::Int64Builder>();
        auto partkey_builder = std::make_shared<arrow::Int64Builder>();
        auto suppkey_builder = std::make_shared<arrow::Int64Builder>();
        auto linenumber_builder = std::make_shared<arrow::Int64Builder>();
        auto quantity_builder = std::make_shared<arrow::DoubleBuilder>();
        auto extprice_builder = std::make_shared<arrow::DoubleBuilder>();
        auto discount_builder = std::make_shared<arrow::DoubleBuilder>();
        auto tax_builder = std::make_shared<arrow::DoubleBuilder>();
        auto returnflag_builder = std::make_shared<arrow::StringBuilder>();
        auto linestatus_builder = std::make_shared<arrow::StringBuilder>();

        size_t rows_in_batch = 0;

        // Generate synthetic data
        for (long row_idx = 0; row_idx < opts.max_rows; ++row_idx) {
            // Add synthetic data to builders
            orderkey_builder->Append(row_idx + 1);
            partkey_builder->Append((row_idx % 200000) + 1);
            suppkey_builder->Append((row_idx % 10000) + 1);
            linenumber_builder->Append((row_idx % 7) + 1);
            quantity_builder->Append(10.0 + (row_idx % 50));
            extprice_builder->Append((row_idx % 100) * 100.0);
            discount_builder->Append((row_idx % 10) * 0.1);
            tax_builder->Append((row_idx % 8) * 0.01);
            returnflag_builder->Append(row_idx % 3 == 0 ? "R" : (row_idx % 2 == 0 ? "A" : "N"));
            linestatus_builder->Append(row_idx % 2 == 0 ? "O" : "F");

            rows_in_batch++;
            total_rows++;

            // Flush batch when it reaches batch_size or at the end
            if (rows_in_batch >= batch_size || row_idx == opts.max_rows - 1) {
                auto orderkey_array = orderkey_builder->Finish().ValueOrDie();
                auto partkey_array = partkey_builder->Finish().ValueOrDie();
                auto suppkey_array = suppkey_builder->Finish().ValueOrDie();
                auto linenumber_array = linenumber_builder->Finish().ValueOrDie();
                auto quantity_array = quantity_builder->Finish().ValueOrDie();
                auto extprice_array = extprice_builder->Finish().ValueOrDie();
                auto discount_array = discount_builder->Finish().ValueOrDie();
                auto tax_array = tax_builder->Finish().ValueOrDie();
                auto returnflag_array = returnflag_builder->Finish().ValueOrDie();
                auto linestatus_array = linestatus_builder->Finish().ValueOrDie();

                std::vector<std::shared_ptr<arrow::Array>> arrays = {
                    orderkey_array, partkey_array, suppkey_array, linenumber_array,
                    quantity_array, extprice_array, discount_array, tax_array,
                    returnflag_array, linestatus_array
                };

                auto batch = arrow::RecordBatch::Make(schema, rows_in_batch, arrays);
                writer->write_batch(batch);

                batch_count++;
                if (opts.verbose && batch_count % 10 == 0) {
                    std::cout << "  Batch " << batch_count << " (" << total_rows << " rows)\n";
                }

                // Reset builders for next batch
                rows_in_batch = 0;
                orderkey_builder->Reset();
                partkey_builder->Reset();
                suppkey_builder->Reset();
                linenumber_builder->Reset();
                quantity_builder->Reset();
                extprice_builder->Reset();
                discount_builder->Reset();
                tax_builder->Reset();
                returnflag_builder->Reset();
                linestatus_builder->Reset();
            }
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
        std::cout << "\n=== TPC-H Data Generation Complete (PoC) ===\n";
        std::cout << "Format: " << opts.format << "\n";
        std::cout << "Output file: " << output_path << "\n";
        std::cout << "Rows written: " << total_rows << "\n";
        std::cout << "Batches: " << batch_count << "\n";
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

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
