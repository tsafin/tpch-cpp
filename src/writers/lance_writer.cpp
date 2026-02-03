#include "tpch/lance_writer.hpp"

#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <fstream>
#include <sstream>
#include <ctime>
#include <chrono>
#include <iomanip>
#include <uuid/uuid.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

namespace fs = std::filesystem;

namespace tpch {

LanceWriter::LanceWriter(const std::string& dataset_path,
                         const std::string& dataset_name)
    : dataset_path_(dataset_path),
      dataset_name_(dataset_name) {
    // Ensure path ends with .lance
    const std::string suffix = ".lance";
    if (dataset_path_.length() < suffix.length() ||
        dataset_path_.substr(dataset_path_.length() - suffix.length()) != suffix) {
        dataset_path_ += ".lance";
    }
}

LanceWriter::~LanceWriter() {
    if (rust_writer_ != nullptr) {
        try {
            close();
        } catch (...) {
            // Suppress exceptions in destructor
        }
    }
}

std::string LanceWriter::generate_uuid() {
    uuid_t uuid;
    uuid_generate(uuid);
    char uuid_str[37];
    uuid_unparse(uuid, uuid_str);
    return std::string(uuid_str);
}

void LanceWriter::initialize_lance_dataset(
    const std::shared_ptr<arrow::RecordBatch>& first_batch) {
    if (schema_locked_) {
        return;
    }

    schema_ = first_batch->schema();
    schema_locked_ = true;

    // Create dataset directory structure
    try {
        fs::create_directories(dataset_path_);
        fs::create_directories(dataset_path_ + "/data");
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to create dataset directory: " +
                                std::string(e.what()));
    }

    // Initialize Rust FFI writer
    auto* raw_writer = lance_writer_create(dataset_path_.c_str(), nullptr);
    rust_writer_ = reinterpret_cast<void*>(raw_writer);

    if (rust_writer_ == nullptr) {
        throw std::runtime_error("Failed to create Lance writer via FFI");
    }

    std::cout << "Lance: Initialized dataset at " << dataset_path_ << "\n";
}

std::pair<void*, void*> LanceWriter::batch_to_ffi(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    // For now, we don't use Arrow C Data Interface
    // The actual data is written directly to Parquet below
    void* batch_ptr = batch.get();
    void* schema_ptr = batch->schema().get();
    return std::make_pair(batch_ptr, schema_ptr);
}

void LanceWriter::free_ffi_structures(void* /*array_ptr*/, void* /*schema_ptr*/) {
    // No dynamic allocation, nothing to free
}

void LanceWriter::write_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        throw std::runtime_error("Received null batch");
    }

    if (batch->num_rows() == 0) {
        return;
    }

    // Initialize on first batch
    if (!schema_locked_) {
        initialize_lance_dataset(batch);
    }

    // Validate schema consistency
    if (!batch->schema()->Equals(*schema_)) {
        throw std::runtime_error(
            "Batch schema does not match table schema. "
            "Expected: " + schema_->ToString() + ", "
            "Got: " + batch->schema()->ToString());
    }

    // Accumulate batches
    accumulated_batches_.push_back(batch);
    row_count_ += batch->num_rows();

    // Write to Parquet when accumulated rows reach threshold
    const int64_t BATCH_SIZE_THRESHOLD = 10000000;  // 10M rows
    if (row_count_ >= BATCH_SIZE_THRESHOLD || batch_count_ < 0) {
        flush_batches_to_parquet();
    }

    batch_count_++;
    if (batch_count_ % 100 == 0 || batch_count_ <= 3) {
        std::cout << "Lance: Received batch " << batch_count_ << ", "
                  << row_count_ << " rows total\n";
    }
}

void LanceWriter::flush_batches_to_parquet() {
    if (accumulated_batches_.empty()) {
        return;
    }

    // Combine all batches into a single table
    std::vector<std::shared_ptr<arrow::RecordBatch>> to_write = accumulated_batches_;
    accumulated_batches_.clear();

    try {
        // Create combined table
        auto table = arrow::Table::FromRecordBatches(to_write);
        if (!table.ok()) {
            throw std::runtime_error("Failed to create table from batches: " +
                                    table.status().ToString());
        }

        // Generate data file name
        std::ostringstream filename;
        filename << dataset_path_ << "/data/part-" << std::setfill('0')
                 << std::setw(5) << parquet_file_count_ << ".parquet";
        parquet_file_count_++;

        // Write to Parquet with compression
        auto properties = parquet::WriterProperties::Builder()
            .compression(parquet::Compression::SNAPPY)
            ->build();

        auto arrow_props = parquet::ArrowWriterProperties::Builder().build();

        // Create file output stream
        auto outfile_result = arrow::io::FileOutputStream::Open(filename.str());
        if (!outfile_result.ok()) {
            throw std::runtime_error("Failed to open file: " +
                                    outfile_result.status().ToString());
        }
        auto outfile = outfile_result.ValueOrDie();

        // Write table to file
        auto write_result = parquet::arrow::WriteTable(
            *table.ValueOrDie(),
            arrow::default_memory_pool(),
            outfile,
            1000,  // chunk size
            properties,
            arrow_props);

        if (!write_result.ok()) {
            throw std::runtime_error("Failed to write Parquet file: " +
                                    write_result.ToString());
        }

        if (!outfile->Close().ok()) {
            throw std::runtime_error("Failed to close Parquet file");
        }

        std::cout << "Lance: Wrote Parquet file " << filename.str() << "\n";
    } catch (const std::exception& e) {
        throw std::runtime_error("Error flushing batches to Parquet: " +
                                std::string(e.what()));
    }
}

void LanceWriter::close() {
    if (rust_writer_ == nullptr) {
        return;
    }

    try {
        // Flush any remaining batches
        flush_batches_to_parquet();

        // Write Lance metadata
        write_lance_metadata();

        // Close Rust writer
        auto* raw_writer = reinterpret_cast<::LanceWriter*>(rust_writer_);
        int result = lance_writer_close(raw_writer);
        if (result != 0) {
            throw std::runtime_error(
                "Lance writer close returned error code: " + std::to_string(result));
        }

        std::cout << "Lance dataset finalized: " << dataset_path_ << "\n"
                  << "  Total rows: " << row_count_ << "\n"
                  << "  Total batches: " << batch_count_ << "\n"
                  << "  Parquet files: " << parquet_file_count_ << "\n";

        // Clean up
        lance_writer_destroy(raw_writer);
        rust_writer_ = nullptr;
    } catch (const std::exception& e) {
        if (rust_writer_ != nullptr) {
            lance_writer_destroy(reinterpret_cast<::LanceWriter*>(rust_writer_));
            rust_writer_ = nullptr;
        }
        throw;
    }
}

void LanceWriter::write_lance_metadata() {
    // Get current timestamp
    auto now = std::chrono::system_clock::now();
    auto timestamp_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch())
            .count();

    // Generate schema UUID
    std::string schema_id = generate_uuid();

    // Write _metadata.json
    std::ostringstream metadata_json;
    metadata_json << "{\n"
                  << "  \"version\": 0,\n"
                  << "  \"manifests\": [\n"
                  << "    {\n"
                  << "      \"manifest_path\": \"_manifest.json\",\n"
                  << "      \"created_at\": " << timestamp_ms << ",\n"
                  << "      \"schema_id\": \"" << schema_id << "\"\n"
                  << "    }\n"
                  << "  ],\n"
                  << "  \"schema\": {\n"
                  << "    \"id\": \"" << schema_id << "\",\n"
                  << "    \"fields\": [\n";

    // Write schema fields
    for (int i = 0; i < schema_->num_fields(); i++) {
        if (i > 0) metadata_json << ",\n";
        auto field = schema_->field(i);
        metadata_json << "      {\n"
                      << "        \"id\": " << i << ",\n"
                      << "        \"name\": \"" << field->name() << "\",\n"
                      << "        \"type\": \"" << field->type()->ToString()
                      << "\"\n"
                      << "      }";
    }

    metadata_json << "\n    ]\n"
                  << "  }\n"
                  << "}\n";

    std::string metadata_path = dataset_path_ + "/_metadata.json";
    std::ofstream metadata_file(metadata_path);
    if (!metadata_file) {
        throw std::runtime_error("Failed to open " + metadata_path + " for writing");
    }
    metadata_file << metadata_json.str();
    metadata_file.close();

    // Write _manifest.json
    std::ostringstream manifest_json;
    manifest_json << "{\n"
                  << "  \"version\": 0,\n"
                  << "  \"created_at\": " << timestamp_ms << ",\n"
                  << "  \"files\": [\n";

    for (int i = 0; i < parquet_file_count_; i++) {
        if (i > 0) manifest_json << ",\n";
        std::ostringstream file_path;
        file_path << "data/part-" << std::setfill('0') << std::setw(5) << i
                  << ".parquet";
        manifest_json << "    {\n"
                      << "      \"path\": \"" << file_path.str() << "\",\n"
                      << "      \"row_count\": 0,\n"
                      << "      \"size_bytes\": 0\n"
                      << "    }";
    }

    manifest_json << "\n  ]\n"
                  << "}\n";

    std::string manifest_path = dataset_path_ + "/_manifest.json";
    std::ofstream manifest_file(manifest_path);
    if (!manifest_file) {
        throw std::runtime_error("Failed to open " + manifest_path + " for writing");
    }
    manifest_file << manifest_json.str();
    manifest_file.close();

    // Write _commits.json (commit log)
    std::ostringstream commits_json;
    commits_json << "{\n"
                 << "  \"version\": 0,\n"
                 << "  \"commits\": [\n"
                 << "    {\n"
                 << "      \"version\": 0,\n"
                 << "      \"timestamp\": " << timestamp_ms << ",\n"
                 << "      \"operation\": \"append\"\n"
                 << "    }\n"
                 << "  ]\n"
                 << "}\n";

    std::string commits_path = dataset_path_ + "/_commits.json";
    std::ofstream commits_file(commits_path);
    if (!commits_file) {
        throw std::runtime_error("Failed to open " + commits_path + " for writing");
    }
    commits_file << commits_json.str();
    commits_file.close();
}

}  // namespace tpch
