#include "tpch/paimon_writer.hpp"

#ifdef TPCH_ENABLE_PAIMON

#include "tpch/avro_writer.hpp"

#include <iostream>
#include <stdexcept>
#include <memory>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>
#include <iomanip>
#include <random>
#include <chrono>
#include <cstring>
#include <limits>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <nlohmann/json.hpp>

namespace tpch {

using json = nlohmann::json;

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
    if (!schema_locked_ || closed_) {
        return;  // Never initialized or already closed
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

std::string PaimonWriter::generate_uuid() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);

    const char hex[] = "0123456789abcdef";
    std::string uuid;
    uuid.reserve(32);

    for (int i = 0; i < 32; ++i) {
        uuid += hex[dis(gen)];
    }

    return uuid;
}

int64_t PaimonWriter::current_timestamp_ms() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

const std::string& PaimonWriter::manifest_entry_schema() {
    static const std::string schema = R"({
  "type": "record",
  "name": "ManifestEntry",
  "fields": [
    {"name": "_KIND", "type": "int"},
    {"name": "_PARTITION", "type": "bytes"},
    {"name": "_BUCKET", "type": "int"},
    {"name": "_TOTAL_BUCKETS", "type": "int"},
    {
      "name": "_FILE",
      "type": {
        "type": "record",
        "name": "DataFileMetadata",
        "fields": [
          {"name": "fileName", "type": "string"},
          {"name": "fileSize", "type": "long"},
          {"name": "level", "type": "int"},
          {"name": "minKey", "type": ["null", "bytes"]},
          {"name": "maxKey", "type": ["null", "bytes"]},
          {"name": "minColumnStats", "type": ["null", {"type": "array", "items": "bytes"}]},
          {"name": "maxColumnStats", "type": ["null", {"type": "array", "items": "bytes"}]},
          {"name": "nullCounts", "type": ["null", {"type": "array", "items": "long"}]},
          {"name": "rowCount", "type": "long"},
          {"name": "sequenceNumber", "type": "long"},
          {"name": "fileSource", "type": "string"},
          {"name": "schemaId", "type": "long"}
        ]
      }
    }
  ]
})";
    return schema;
}

const std::string& PaimonWriter::manifest_list_entry_schema() {
    static const std::string schema = R"({
  "type": "record",
  "name": "ManifestListEntry",
  "fields": [
    {"name": "_FILE_NAME", "type": "string"},
    {"name": "_FILE_SIZE", "type": "long"},
    {"name": "_NUM_ADDED_FILES", "type": "long"},
    {"name": "_NUM_DELETED_FILES", "type": "long"},
    {
      "name": "_PARTITION_STATS",
      "type": ["null", {
        "type": "array",
        "items": {
          "type": "record",
          "name": "PartitionStats",
          "fields": [
            {"name": "min", "type": ["null", "bytes"]},
            {"name": "max", "type": ["null", "bytes"]}
          ]
        }
      }]
    },
    {"name": "_SCHEMA_ID", "type": "long"}
  ]
})";
    return schema;
}

void PaimonWriter::initialize_paimon_table(
    const std::shared_ptr<arrow::RecordBatch>& first_batch) {

    schema_ = first_batch->schema();

    try {
        // Create directory structure for Paimon table
        std::filesystem::create_directories(table_path_ + "/snapshot");
        std::filesystem::create_directories(table_path_ + "/manifest");
        std::filesystem::create_directories(table_path_ + "/bucket-0");
        std::filesystem::create_directories(table_path_ + "/schema");

        schema_locked_ = true;

        // Write initial metadata files
        write_options_file();
        write_schema_file();

    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to initialize Paimon table: ") + e.what()
        );
    }
}

void PaimonWriter::write_options_file() {
    try {
        std::ofstream options_file(table_path_ + "/OPTIONS");
        options_file << "table.type=APPEND_ONLY\n";
        options_file << "data-files.format=parquet\n";
        options_file << "bucket=-1\n";
        options_file.close();
    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to write OPTIONS file: ") + e.what()
        );
    }
}

void PaimonWriter::write_schema_file() {
    try {
        json schema_json;
        json fields_array = json::array();

        // Build fields array
        for (int i = 0; i < schema_->num_fields(); ++i) {
            auto field = schema_->field(i);
            json field_obj;
            field_obj["id"] = i;
            field_obj["name"] = field->name();
            field_obj["type"] = arrow_type_to_paimon_type(field->type());
            fields_array.push_back(field_obj);
        }

        schema_json["fields"] = fields_array;
        schema_json["primaryKeys"] = json::array();
        schema_json["partitionKeys"] = json::array();
        schema_json["options"] = json::object();

        std::ofstream schema_file(table_path_ + "/schema/schema-0");
        schema_file << schema_json.dump(2);  // Pretty-print with 2-space indent
        schema_file.close();

    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to write schema file: ") + e.what()
        );
    }
}

void PaimonWriter::write_data_file() {
    if (batches_.empty()) {
        return;
    }

    try {
        // Generate data file name: data-<UUID>-<seq>.parquet
        std::string uuid = generate_uuid();
        std::ostringstream filename;
        filename << "data-" << uuid << "-" << file_count_ << ".parquet";
        std::string filepath = table_path_ + "/bucket-0/" + filename.str();

        // Create table from batches
        auto table_result = arrow::Table::FromRecordBatches(batches_);
        if (!table_result.ok()) {
            throw std::runtime_error("Failed to create Arrow table from batches: " + table_result.status().ToString());
        }
        auto table = table_result.ValueOrDie();

        // Write to Parquet file
        auto file_result = arrow::io::FileOutputStream::Open(filepath);
        if (!file_result.ok()) {
            throw std::runtime_error("Failed to open file for writing: " + file_result.status().ToString());
        }
        auto file = file_result.ValueOrDie();

        auto status = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), file);
        if (!status.ok()) {
            throw std::runtime_error("Failed to write Parquet file: " + status.ToString());
        }

        file->Close();

        // Track file info
        int64_t rows_written = 0;
        for (const auto& batch : batches_) {
            rows_written += batch->num_rows();
        }
        row_count_ += rows_written;

        // Get file size
        int64_t file_size = std::filesystem::file_size(filepath);

        data_files_.push_back({filename.str(), file_size, rows_written});
        file_count_++;

        // Clear batches for next file
        batches_.clear();

    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to write Paimon data file: ") + e.what()
        );
    }
}

std::vector<uint8_t> PaimonWriter::encode_manifest_entry(const DataFileInfo& file) {
    std::vector<uint8_t> record;

    // _KIND: int 0 (ADD)
    avro_detail::write_zigzag_int(record, 0);

    // _PARTITION: bytes - empty BinaryRow (4-byte header: 0x04 0x00 0x00 0x00)
    const uint8_t empty_partition[] = {0x04, 0x00, 0x00, 0x00};
    avro_detail::write_avro_bytes(record, empty_partition, 4);

    // _BUCKET: int 0
    avro_detail::write_zigzag_int(record, 0);

    // _TOTAL_BUCKETS: int -1
    avro_detail::write_zigzag_int(record, -1);

    // _FILE (nested DataFileMetadata record, no length prefix)
    // fileName: string
    avro_detail::write_avro_string(record, file.name);

    // fileSize: long
    avro_detail::write_zigzag_long(record, file.size);

    // level: int 0
    avro_detail::write_zigzag_int(record, 0);

    // minKey: union null (index 0)
    avro_detail::write_union_null(record);

    // maxKey: union null
    avro_detail::write_union_null(record);

    // minColumnStats: union null
    avro_detail::write_union_null(record);

    // maxColumnStats: union null
    avro_detail::write_union_null(record);

    // nullCounts: union null
    avro_detail::write_union_null(record);

    // rowCount: long
    avro_detail::write_zigzag_long(record, file.rows);

    // sequenceNumber: long 0
    avro_detail::write_zigzag_long(record, 0);

    // fileSource: string "APPEND"
    avro_detail::write_avro_string(record, "APPEND");

    // schemaId: long 0
    avro_detail::write_zigzag_long(record, 0);

    return record;
}

std::vector<uint8_t> PaimonWriter::encode_manifest_list_entry(
    const std::string& manifest_name, int64_t manifest_size) {
    std::vector<uint8_t> record;

    // _FILE_NAME: string
    avro_detail::write_avro_string(record, manifest_name);

    // _FILE_SIZE: long
    avro_detail::write_zigzag_long(record, manifest_size);

    // _NUM_ADDED_FILES: long (count of data files)
    avro_detail::write_zigzag_long(record, static_cast<int64_t>(data_files_.size()));

    // _NUM_DELETED_FILES: long 0
    avro_detail::write_zigzag_long(record, 0);

    // _PARTITION_STATS: union null (no partitions)
    avro_detail::write_union_null(record);

    // _SCHEMA_ID: long 0
    avro_detail::write_zigzag_long(record, 0);

    return record;
}

std::string PaimonWriter::write_data_manifest() {
    try {
        // Create Avro file writer
        AvroFileWriter writer(manifest_entry_schema());

        // Encode and append one entry per data file
        for (const auto& file : data_files_) {
            auto entry = encode_manifest_entry(file);
            writer.append_record(entry);
        }

        // Generate manifest filename
        std::string uuid = generate_uuid();
        std::string manifest_name = "manifest-" + uuid + "-0";
        std::string manifest_path = table_path_ + "/manifest/" + manifest_name;

        // Write Avro container file
        writer.finish(manifest_path);

        return manifest_name;

    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to write data manifest: ") + e.what()
        );
    }
}

std::string PaimonWriter::write_manifest_list(
    const std::string& manifest_name, int64_t manifest_size) {
    try {
        // Create Avro file writer
        AvroFileWriter writer(manifest_list_entry_schema());

        // Encode and append one entry for the manifest
        auto entry = encode_manifest_list_entry(manifest_name, manifest_size);
        writer.append_record(entry);

        // Generate manifest list filename
        std::string uuid = generate_uuid();
        std::string manifest_list_name = "manifest-list-" + uuid + "-0";
        std::string manifest_list_path = table_path_ + "/manifest/" + manifest_list_name;

        // Write Avro container file
        writer.finish(manifest_list_path);

        return manifest_list_name;

    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to write manifest list: ") + e.what()
        );
    }
}

void PaimonWriter::write_snapshot(const std::string& delta_manifest_list_name) {
    try {
        json snapshot_json;

        snapshot_json["version"] = 3;
        snapshot_json["id"] = 1;
        snapshot_json["schemaId"] = 0;
        snapshot_json["baseManifestList"] = nullptr;
        snapshot_json["deltaManifestList"] = delta_manifest_list_name;
        snapshot_json["changelogManifestList"] = nullptr;
        snapshot_json["indexManifest"] = nullptr;
        snapshot_json["commitUser"] = generate_uuid();
        snapshot_json["commitIdentifier"] = std::numeric_limits<int64_t>::max();
        snapshot_json["commitKind"] = "APPEND";
        snapshot_json["timeMillis"] = current_timestamp_ms();
        snapshot_json["logOffsets"] = json::object();
        snapshot_json["totalRecordCount"] = row_count_;
        snapshot_json["deltaRecordCount"] = row_count_;
        snapshot_json["changelogRecordCount"] = 0;
        snapshot_json["watermark"] = std::numeric_limits<int64_t>::min();
        snapshot_json["statistics"] = nullptr;

        std::ofstream snapshot_file(table_path_ + "/snapshot/snapshot-1");
        snapshot_file << snapshot_json.dump(2);
        snapshot_file.close();

    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to write snapshot: ") + e.what()
        );
    }
}

void PaimonWriter::write_snapshot_hints() {
    try {
        // Write EARLIEST hint
        std::ofstream earliest(table_path_ + "/snapshot/EARLIEST");
        earliest << "1";
        earliest.close();

        // Write LATEST hint
        std::ofstream latest(table_path_ + "/snapshot/LATEST");
        latest << "1";
        latest.close();

    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to write snapshot hints: ") + e.what()
        );
    }
}

void PaimonWriter::write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || batch->num_rows() == 0) {
        return;
    }

    if (closed_) {
        throw std::runtime_error("Cannot write to closed Paimon writer");
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

        // Write when we have accumulated enough batches (~10M rows per file)
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
    if (!schema_locked_ || closed_) {
        return;  // Nothing to close or already closed
    }

    try {
        // Flush remaining batches to data file
        if (!batches_.empty()) {
            write_data_file();
        }

        // Write manifests and snapshot only if we have data files
        if (!data_files_.empty()) {
            // Write data manifest (contains all data file entries)
            std::string manifest_name = write_data_manifest();

            // Stat manifest file for size
            int64_t manifest_size = std::filesystem::file_size(
                table_path_ + "/manifest/" + manifest_name
            );

            // Write manifest list (references the manifest file)
            std::string manifest_list_name = write_manifest_list(manifest_name, manifest_size);

            // Write snapshot metadata
            write_snapshot(manifest_list_name);
        }

        // Write snapshot hint files
        write_snapshot_hints();

        closed_ = true;

    } catch (const std::exception& e) {
        std::cerr << "Warning finalizing Paimon table: " << e.what() << std::endl;
    }
}

}  // namespace tpch

#endif  // TPCH_ENABLE_PAIMON
