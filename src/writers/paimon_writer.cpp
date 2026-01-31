#include "tpch/paimon_writer.hpp"

#ifdef TPCH_ENABLE_PAIMON

#include <iostream>
#include <stdexcept>
#include <memory>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <iomanip>

namespace tpch {

PaimonWriter::PaimonWriter(const std::string& table_path, const std::string& table_name)
    : table_path_(table_path),
      table_name_(table_name) {

    // Create table directory if it doesn't exist
    try {
        std::filesystem::create_directories(table_path_);
    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to create Paimon table directory: ") + e.what()
        );
    }
}

PaimonWriter::~PaimonWriter() {
    if (!schema_locked_) {
        return;  // Never initialized
    }

    try {
        close();
    } catch (const std::exception& e) {
        std::cerr << "Warning closing Paimon writer: " << e.what() << std::endl;
    }
}

std::string PaimonWriter::arrow_type_to_paimon_type(
    const std::shared_ptr<arrow::DataType>& arrow_type) {

    switch (arrow_type->id()) {
        case arrow::Type::INT64:
            return "bigint";
        case arrow::Type::INT32:
            return "int";
        case arrow::Type::DOUBLE:
            return "double";
        case arrow::Type::FLOAT:
            return "float";
        case arrow::Type::STRING:
            return "string";
        case arrow::Type::DATE32:
            return "date";
        case arrow::Type::TIMESTAMP:
            return "timestamp";
        case arrow::Type::DECIMAL128:
            return "decimal";
        case arrow::Type::BOOL:
            return "boolean";
        default:
            throw std::runtime_error(
                std::string("Unsupported Arrow type for Paimon: ") + arrow_type->ToString()
            );
    }
}

void PaimonWriter::initialize_paimon_table(
    const std::shared_ptr<arrow::RecordBatch>& first_batch) {

    schema_ = first_batch->schema();

    try {
        // Create directory structure for Paimon table
        std::filesystem::create_directories(table_path_ + "/snapshot");
        std::filesystem::create_directories(table_path_ + "/manifest");
        std::filesystem::create_directories(table_path_ + "/data");

        schema_locked_ = true;

    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to initialize Paimon table: ") + e.what()
        );
    }
}

std::string PaimonWriter::write_data_file() {
    if (batches_.empty()) {
        return "";
    }

    try {
        // Generate data file path
        std::ostringstream filename;
        filename << "data_" << std::setfill('0') << std::setw(6) << file_count_ << ".parquet";
        std::string filepath = table_path_ + "/data/" + filename.str();

        // Create table from batches
        auto table_result = arrow::Table::FromRecordBatches(batches_);
        if (!table_result.ok()) {
            throw std::runtime_error("Failed to create Arrow table from batches: " + table_result.status().ToString());
        }
        auto table = table_result.ValueOrDie();

        // Write to Parquet file using parquet::arrow::WriteTable
        auto file_result = arrow::io::FileOutputStream::Open(filepath);
        if (!file_result.ok()) {
            throw std::runtime_error("Failed to open file for writing: " + file_result.status().ToString());
        }
        auto file = file_result.ValueOrDie();

        auto status = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), file);
        if (!status.ok()) {
            throw std::runtime_error("Failed to write Parquet file: " + status.ToString());
        }

        // Close file
        file->Close();

        // Track file and row count
        int64_t rows_written = 0;
        for (const auto& batch : batches_) {
            rows_written += batch->num_rows();
        }
        row_count_ += rows_written;
        written_files_.push_back(filename.str());
        file_count_++;

        // Clear batches for next file
        batches_.clear();

        return filepath;

    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to write Paimon data file: ") + e.what()
        );
    }
}

std::string PaimonWriter::create_manifest_metadata() {
    std::ostringstream json;
    json << "{\n"
         << "  \"version\": 1,\n"
         << "  \"manifestEntries\": [\n";

    for (size_t i = 0; i < written_files_.size(); ++i) {
        if (i > 0) json << ",\n";
        json << "    {\n"
             << "      \"dataFile\": \"" << written_files_[i] << "\",\n"
             << "      \"fileFormat\": \"parquet\",\n"
             << "      \"schemaId\": 0\n"
             << "    }";
    }

    json << "\n  ]\n}\n";
    return json.str();
}

std::string PaimonWriter::create_snapshot_metadata() {
    std::ostringstream json;
    json << "{\n"
         << "  \"version\": 1,\n"
         << "  \"snapshotId\": 1,\n"
         << "  \"schemaId\": 0,\n"
         << "  \"recordCount\": " << row_count_ << ",\n"
         << "  \"manifestList\": [\n";

    if (!written_files_.empty()) {
        json << "    \"manifest-1\"\n";
    }

    json << "  ]\n}\n";
    return json.str();
}

void PaimonWriter::write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || batch->num_rows() == 0) {
        return;
    }

    // Initialize on first batch
    if (!schema_locked_) {
        initialize_paimon_table(batch);
    }

    // Verify schema consistency
    if (batch->schema()->field_names() != schema_->field_names()) {
        throw std::runtime_error(
            "Schema mismatch: batch schema does not match first batch schema"
        );
    }

    try {
        // Accumulate batch for buffered writing
        batches_.push_back(batch);

        // Write when we have accumulated enough batches (e.g., ~10M rows per file)
        // This is a tuning parameter - adjust based on memory constraints
        int64_t accumulated_rows = 0;
        for (const auto& b : batches_) {
            accumulated_rows += b->num_rows();
        }

        if (accumulated_rows >= 10000000) {
            write_data_file();
        }

    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to write Paimon batch: ") + e.what()
        );
    }
}

void PaimonWriter::close() {
    if (!schema_locked_) {
        return;  // Nothing to close
    }

    try {
        // Flush remaining batches to data file
        if (!batches_.empty()) {
            write_data_file();
        }

        // Write manifest metadata
        if (!written_files_.empty()) {
            std::string manifest_json = create_manifest_metadata();
            std::ofstream manifest_file(table_path_ + "/manifest/manifest-1");
            manifest_file << manifest_json;
            manifest_file.close();
        }

        // Write snapshot metadata
        std::string snapshot_json = create_snapshot_metadata();
        std::ofstream snapshot_file(table_path_ + "/snapshot/snapshot-1");
        snapshot_file << snapshot_json;
        snapshot_file.close();

    } catch (const std::exception& e) {
        std::cerr << "Warning finalizing Paimon table: " << e.what() << std::endl;
    }
}

}  // namespace tpch

#endif  // TPCH_ENABLE_PAIMON
