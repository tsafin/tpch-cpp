#include "tpch/iceberg_writer.hpp"

#ifdef TPCH_ENABLE_ICEBERG

#include <iostream>
#include <stdexcept>
#include <memory>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <random>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

namespace tpch {

IcebergWriter::IcebergWriter(const std::string& table_path, const std::string& table_name)
    : table_path_(table_path),
      table_name_(table_name) {

    // Create table directory if it doesn't exist
    try {
        std::filesystem::create_directories(table_path_);
    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to create Iceberg table directory: ") + e.what()
        );
    }
}

std::string IcebergWriter::arrow_type_to_iceberg_type(
    const std::shared_ptr<arrow::DataType>& arrow_type) {

    switch (arrow_type->id()) {
        case arrow::Type::BOOL:
            return "boolean";
        case arrow::Type::INT32:
            return "int";
        case arrow::Type::INT64:
            return "long";
        case arrow::Type::FLOAT:
            return "float";
        case arrow::Type::DOUBLE:
            return "double";
        case arrow::Type::STRING:
            return "string";
        case arrow::Type::DATE32:
            return "date";
        case arrow::Type::TIMESTAMP:
            return "timestamp";
        case arrow::Type::DECIMAL128: {
            auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(arrow_type);
            return "decimal(" + std::to_string(decimal_type->precision()) + ","
                   + std::to_string(decimal_type->scale()) + ")";
        }
        default:
            throw std::runtime_error(
                std::string("Unsupported Arrow type for Iceberg: ") + arrow_type->ToString()
            );
    }
}

std::string IcebergWriter::generate_uuid() {
    // Simplified UUID generation (not cryptographically secure, but sufficient for table IDs)
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);

    std::ostringstream uuid;
    for (int i = 0; i < 8; ++i) {
        uuid << std::hex << dis(gen);
    }
    uuid << "-";
    for (int i = 0; i < 4; ++i) {
        uuid << std::hex << dis(gen);
    }
    uuid << "-";
    for (int i = 0; i < 4; ++i) {
        uuid << std::hex << dis(gen);
    }
    uuid << "-";
    for (int i = 0; i < 4; ++i) {
        uuid << std::hex << dis(gen);
    }
    uuid << "-";
    for (int i = 0; i < 12; ++i) {
        uuid << std::hex << dis(gen);
    }

    return uuid.str();
}

int64_t IcebergWriter::current_timestamp_ms() {
    using namespace std::chrono;
    auto now = system_clock::now();
    return duration_cast<milliseconds>(now.time_since_epoch()).count();
}

void IcebergWriter::initialize_iceberg_table(
    const std::shared_ptr<arrow::RecordBatch>& first_batch) {

    schema_ = first_batch->schema();

    try {
        // Create directory structure for Iceberg table
        std::filesystem::create_directories(table_path_ + "/metadata");
        std::filesystem::create_directories(table_path_ + "/data");

        schema_locked_ = true;

    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to initialize Iceberg table: ") + e.what()
        );
    }
}

std::string IcebergWriter::write_data_file() {
    if (batches_.empty()) {
        return "";
    }

    try {
        // Generate data file path: data_00000.parquet, data_00001.parquet, etc.
        std::ostringstream filename;
        filename << "data_" << std::setfill('0') << std::setw(5) << file_count_ << ".parquet";
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
            std::string("Failed to write Iceberg data file: ") + e.what()
        );
    }
}

std::string IcebergWriter::create_version_hint() {
    return "1\n";
}

std::string IcebergWriter::create_manifest_json() {
    std::ostringstream json;
    json << "{\n"
         << "  \"version\": 1,\n"
         << "  \"manifest-path\": \"metadata/manifest-1.json\",\n"
         << "  \"manifest-length\": 0,\n"
         << "  \"content\": \"data\",\n"
         << "  \"files\": [\n";

    for (size_t i = 0; i < written_files_.size(); ++i) {
        if (i > 0) json << ",\n";
        json << "    {\n"
             << "      \"status\": \"ADDED\",\n"
             << "      \"snapshot-id\": " << current_snapshot_id_ << ",\n"
             << "      \"data-file\": {\n"
             << "        \"file-path\": \"data/" << written_files_[i] << "\",\n"
             << "        \"file-format\": \"PARQUET\",\n"
             << "        \"spec-id\": 0,\n"
             << "        \"partition\": {},\n"
             << "        \"record-count\": 0,\n"
             << "        \"file-size-in-bytes\": 0,\n"
             << "        \"block-size-in-bytes\": 67108864,\n"
             << "        \"sort-order-id\": 0\n"
             << "      }\n"
             << "    }";
    }

    json << "\n  ]\n}\n";
    return json.str();
}

std::string IcebergWriter::create_manifest_list_json() {
    std::ostringstream json;
    json << "{\n"
         << "  \"version\": 1,\n"
         << "  \"snapshot-id\": " << current_snapshot_id_ << ",\n"
         << "  \"manifests\": [\n"
         << "    {\n"
         << "      \"manifest-path\": \"metadata/manifest-1.json\",\n"
         << "      \"manifest-length\": 0,\n"
         << "      \"partition-spec-id\": 0,\n"
         << "      \"content\": \"data\",\n"
         << "      \"sequence-number\": 0,\n"
         << "      \"min-sequence-number\": 0,\n"
         << "      \"added-snapshot-id\": " << current_snapshot_id_ << ",\n"
         << "      \"added-files-count\": " << written_files_.size() << ",\n"
         << "      \"existing-files-count\": 0,\n"
         << "      \"deleted-files-count\": 0\n"
         << "    }\n"
         << "  ]\n"
         << "}\n";
    return json.str();
}

std::string IcebergWriter::create_metadata_json() {
    std::ostringstream json;
    int64_t timestamp_ms = current_timestamp_ms();

    json << "{\n"
         << "  \"format-version\": 1,\n"
         << "  \"table-uuid\": \"" << generate_uuid() << "\",\n"
         << "  \"location\": \"" << table_path_ << "\",\n"
         << "  \"last-updated-ms\": " << timestamp_ms << ",\n"
         << "  \"last-column-id\": " << (schema_->num_fields() - 1) << ",\n"
         << "  \"schema\": {\n"
         << "    \"type\": \"struct\",\n"
         << "    \"schema-id\": 0,\n"
         << "    \"fields\": [\n";

    // Schema fields
    for (int i = 0; i < schema_->num_fields(); i++) {
        auto field = schema_->field(i);
        json << "      {\n"
             << "        \"id\": " << i << ",\n"
             << "        \"name\": \"" << field->name() << "\",\n"
             << "        \"required\": " << (!field->nullable() ? "true" : "false") << ",\n"
             << "        \"type\": \"" << arrow_type_to_iceberg_type(field->type()) << "\"\n"
             << "      }";
        if (i < schema_->num_fields() - 1) {
            json << ",";
        }
        json << "\n";
    }

    json << "    ]\n"
         << "  },\n"
         << "  \"current-snapshot-id\": " << current_snapshot_id_ << ",\n"
         << "  \"snapshots\": [\n"
         << "    {\n"
         << "      \"snapshot-id\": " << current_snapshot_id_ << ",\n"
         << "      \"timestamp-ms\": " << timestamp_ms << ",\n"
         << "      \"summary\": {\n"
         << "        \"operation\": \"append\",\n"
         << "        \"spark.app.id\": \"tpch-cpp\"\n"
         << "      },\n"
         << "      \"manifest-list\": \"metadata/snap-" << current_snapshot_id_ << ".manifest-list.json\"\n"
         << "    }\n"
         << "  ],\n"
         << "  \"snapshot-log\": [\n"
         << "    {\n"
         << "      \"snapshot-id\": " << current_snapshot_id_ << ",\n"
         << "      \"timestamp-ms\": " << timestamp_ms << "\n"
         << "    }\n"
         << "  ],\n"
         << "  \"metadata-log\": [],\n"
         << "  \"sort-orders\": []\n"
         << "}\n";

    return json.str();
}

void IcebergWriter::write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || batch->num_rows() == 0) {
        return;
    }

    // Initialize on first batch
    if (!schema_locked_) {
        initialize_iceberg_table(batch);
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
            std::string("Failed to write Iceberg batch: ") + e.what()
        );
    }
}

void IcebergWriter::close() {
    if (!schema_locked_) {
        return;  // Nothing to close
    }

    try {
        // Flush remaining batches to data file
        if (!batches_.empty()) {
            write_data_file();
        }

        // Write Iceberg metadata files in the correct order

        // 1. Write manifest metadata
        if (!written_files_.empty()) {
            std::string manifest_json = create_manifest_json();
            std::ofstream manifest_file(table_path_ + "/metadata/manifest-1.json");
            manifest_file << manifest_json;
            manifest_file.close();
        }

        // 2. Write manifest list
        std::string manifest_list_json = create_manifest_list_json();
        std::string manifest_list_path = table_path_ + "/metadata/snap-"
            + std::to_string(current_snapshot_id_) + ".manifest-list.json";
        std::ofstream manifest_list_file(manifest_list_path);
        manifest_list_file << manifest_list_json;
        manifest_list_file.close();

        // 3. Write main metadata file
        std::string metadata_json = create_metadata_json();
        std::string metadata_path = table_path_ + "/metadata/v1.metadata.json";
        std::ofstream metadata_file(metadata_path);
        metadata_file << metadata_json;
        metadata_file.close();

        // 4. Write version hint
        std::string version_hint_content = create_version_hint();
        std::string version_hint_path = table_path_ + "/metadata/version-hint.text";
        std::ofstream hint_file(version_hint_path);
        hint_file << version_hint_content;
        hint_file.close();

    } catch (const std::exception& e) {
        std::cerr << "Warning finalizing Iceberg table: " << e.what() << std::endl;
    }
}

}  // namespace tpch

#endif  // TPCH_ENABLE_ICEBERG
