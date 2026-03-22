#include "tpch/iceberg_writer.hpp"

#ifdef TPCH_ENABLE_ICEBERG

#include "iceberg_avro.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

namespace tpch {

// ─────────────────────────────────────────────────────────────────────────────
// Iceberg v1 Avro schemas (stored verbatim in each manifest file's header).
//
// field-id attributes are part of the schema JSON and are read by Iceberg
// engines to perform column-id-based schema mapping.
// ─────────────────────────────────────────────────────────────────────────────

// Schema for entries inside the manifest list (snap-<id>.avro).
static const char* MANIFEST_LIST_SCHEMA = R"json(
{"type":"record","name":"manifest_file","fields":[
  {"name":"manifest_path","type":"string","field-id":500},
  {"name":"manifest_length","type":"long","field-id":501},
  {"name":"partition_spec_id","type":"int","field-id":502},
  {"name":"added_snapshot_id","type":["null","long"],"default":null,"field-id":503},
  {"name":"added_files_count","type":["null","int"],"default":null,"field-id":504},
  {"name":"existing_files_count","type":["null","int"],"default":null,"field-id":505},
  {"name":"deleted_files_count","type":["null","int"],"default":null,"field-id":506},
  {"name":"added_rows_count","type":["null","long"],"default":null,"field-id":512},
  {"name":"existing_rows_count","type":["null","long"],"default":null,"field-id":513},
  {"name":"deleted_rows_count","type":["null","long"],"default":null,"field-id":514}
]})json";

// Schema for entries inside each manifest file (manifest-1.avro).
// Column-level statistics are omitted (all optional, default null); readers
// fall back to full-table scans, which is acceptable for benchmark use.
static const char* MANIFEST_ENTRY_SCHEMA = R"json(
{"type":"record","name":"manifest_entry","fields":[
  {"name":"status","type":"int","field-id":0},
  {"name":"snapshot_id","type":["null","long"],"default":null,"field-id":1},
  {"name":"data_file","type":{
    "type":"record","name":"r2","fields":[
      {"name":"file_path","type":"string","field-id":100},
      {"name":"file_format","type":"string","field-id":101},
      {"name":"partition","type":{"type":"record","name":"r102","fields":[]},"field-id":102},
      {"name":"record_count","type":"long","field-id":103},
      {"name":"file_size_in_bytes","type":"long","field-id":104},
      {"name":"block_size_in_bytes","type":"long","field-id":105}
    ]
  },"field-id":2}
]})json";

// ─────────────────────────────────────────────────────────────────────────────
// Constructor
// ─────────────────────────────────────────────────────────────────────────────

IcebergWriter::IcebergWriter(const std::string& table_path,
                              const std::string& table_name)
    : table_path_(table_path), table_name_(table_name) {
    try {
        std::filesystem::create_directories(table_path_);
    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("IcebergWriter: cannot create table directory: ") + e.what());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Static helpers
// ─────────────────────────────────────────────────────────────────────────────

std::string IcebergWriter::arrow_type_to_iceberg_type(
    const std::shared_ptr<arrow::DataType>& arrow_type) {

    switch (arrow_type->id()) {
        case arrow::Type::BOOL:        return "boolean";
        case arrow::Type::INT32:       return "int";
        case arrow::Type::INT64:       return "long";
        case arrow::Type::FLOAT:       return "float";
        case arrow::Type::DOUBLE:      return "double";
        case arrow::Type::STRING:      return "string";
        case arrow::Type::DATE32:      return "date";
        case arrow::Type::TIMESTAMP:   return "timestamp";
        case arrow::Type::DICTIONARY:
            // dictionary<int8|int16, utf8> → expose as plain string to Iceberg
            return "string";
        case arrow::Type::DECIMAL128: {
            auto dt = std::static_pointer_cast<arrow::Decimal128Type>(arrow_type);
            return "decimal(" + std::to_string(dt->precision()) + ","
                              + std::to_string(dt->scale()) + ")";
        }
        default:
            throw std::runtime_error(
                std::string("IcebergWriter: unsupported Arrow type: ")
                + arrow_type->ToString());
    }
}

std::string IcebergWriter::generate_uuid() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<unsigned> dis(0, 15);

    std::ostringstream ss;
    auto hex4 = [&](int n) { for (int i = 0; i < n; ++i) ss << std::hex << dis(gen); };
    hex4(8); ss << '-'; hex4(4); ss << '-'; hex4(4); ss << '-'; hex4(4); ss << '-'; hex4(12);
    return ss.str();
}

int64_t IcebergWriter::current_timestamp_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// ─────────────────────────────────────────────────────────────────────────────
// Table initialisation (first batch)
// ─────────────────────────────────────────────────────────────────────────────

void IcebergWriter::initialize_iceberg_table(
    const std::shared_ptr<arrow::RecordBatch>& first_batch) {

    schema_ = first_batch->schema();

    try {
        std::filesystem::create_directories(table_path_ + "/metadata");
        std::filesystem::create_directories(table_path_ + "/data");
        schema_locked_ = true;
    } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("IcebergWriter: cannot initialise table: ") + e.what());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Data file writing (batches → Parquet)
// ─────────────────────────────────────────────────────────────────────────────

std::string IcebergWriter::write_data_file() {
    if (batches_.empty()) return "";

    std::ostringstream name_ss;
    name_ss << "data_" << std::setfill('0') << std::setw(5) << file_count_ << ".parquet";
    std::string filename = name_ss.str();
    std::string filepath = table_path_ + "/data/" + filename;

    // Count rows before clearing batches_
    int64_t rows = 0;
    for (const auto& b : batches_) rows += b->num_rows();

    // Combine batches into a table and write Parquet
    auto table_result = arrow::Table::FromRecordBatches(batches_);
    if (!table_result.ok())
        throw std::runtime_error("IcebergWriter: cannot create Arrow table: "
                                 + table_result.status().ToString());
    auto table = table_result.ValueOrDie();

    auto file_result = arrow::io::FileOutputStream::Open(filepath);
    if (!file_result.ok())
        throw std::runtime_error("IcebergWriter: cannot open " + filepath + ": "
                                 + file_result.status().ToString());
    auto file = file_result.ValueOrDie();

    auto st = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), file);
    if (!st.ok())
        throw std::runtime_error("IcebergWriter: Parquet write failed: " + st.ToString());

    auto close_st = file->Close();
    if (!close_st.ok())
        throw std::runtime_error("IcebergWriter: file close failed: " + close_st.ToString());

    // Record per-file metadata (need actual size after flush)
    int64_t fsize = static_cast<int64_t>(std::filesystem::file_size(filepath));

    row_count_ += rows;
    written_files_.push_back({filename, rows, fsize});
    ++file_count_;
    batches_.clear();

    return filepath;
}

// ─────────────────────────────────────────────────────────────────────────────
// Avro manifest file  (manifest-1.avro)
// ─────────────────────────────────────────────────────────────────────────────

std::pair<std::string, int64_t> IcebergWriter::write_manifest_avro() const {
    std::string manifest_path = table_path_ + "/metadata/manifest-1.avro";

    avro::FileWriter fw(MANIFEST_ENTRY_SCHEMA);

    for (const auto& info : written_files_) {
        std::vector<uint8_t> rec;

        avro::enc_int(1, rec);                               // status = ADDED (1)
        avro::enc_union_long(current_snapshot_id_, rec);     // snapshot_id: non-null

        // data_file record:
        std::string abs_path = table_path_ + "/data/" + info.filename;
        avro::enc_string(abs_path, rec);                     // file_path
        avro::enc_string("PARQUET", rec);                    // file_format (string)
        // partition record (r102) is empty — zero bytes
        avro::enc_long(info.record_count, rec);              // record_count
        avro::enc_long(info.file_size,    rec);              // file_size_in_bytes
        avro::enc_long(67108864LL,        rec);              // block_size_in_bytes (64 MiB, v1 legacy)

        fw.add_record(std::move(rec));
    }

    fw.write_to_file(manifest_path);

    int64_t sz = static_cast<int64_t>(std::filesystem::file_size(manifest_path));
    return {manifest_path, sz};
}

// ─────────────────────────────────────────────────────────────────────────────
// Avro manifest list  (snap-<id>.avro)
// ─────────────────────────────────────────────────────────────────────────────

std::string IcebergWriter::write_manifest_list_avro(const std::string& manifest_path,
                                                      int64_t manifest_size) const {
    std::string ml_path = table_path_ + "/metadata/snap-"
                         + std::to_string(current_snapshot_id_) + ".avro";

    int64_t total_rows = 0;
    for (const auto& info : written_files_) total_rows += info.record_count;

    avro::FileWriter fw(MANIFEST_LIST_SCHEMA);
    std::vector<uint8_t> rec;

    avro::enc_string(manifest_path, rec);                         // manifest_path
    avro::enc_long(manifest_size, rec);                           // manifest_length
    avro::enc_int(0, rec);                                        // partition_spec_id = 0
    avro::enc_union_long(current_snapshot_id_, rec);              // added_snapshot_id
    avro::enc_union_int(static_cast<int32_t>(written_files_.size()), rec);  // added_files_count
    avro::enc_null_union(rec);                                    // existing_files_count = null
    avro::enc_null_union(rec);                                    // deleted_files_count = null
    avro::enc_union_long(total_rows, rec);                        // added_rows_count
    avro::enc_null_union(rec);                                    // existing_rows_count = null
    avro::enc_null_union(rec);                                    // deleted_rows_count = null

    fw.add_record(std::move(rec));
    fw.write_to_file(ml_path);

    return ml_path;
}

// ─────────────────────────────────────────────────────────────────────────────
// Table metadata JSON  (v1.metadata.json)
// ─────────────────────────────────────────────────────────────────────────────

void IcebergWriter::write_metadata_json(const std::string& manifest_list_path) const {
    int64_t ts = current_timestamp_ms();
    std::ostringstream j;

    j << "{\n"
      << "  \"format-version\": 1,\n"
      << "  \"table-uuid\": \"" << generate_uuid() << "\",\n"
      << "  \"location\": \"" << table_path_ << "\",\n"
      << "  \"last-updated-ms\": " << ts << ",\n"
         // Field IDs are 1-based: highest assigned ID == num_fields().
      << "  \"last-column-id\": " << schema_->num_fields() << ",\n"
      << "  \"schema\": {\n"
      << "    \"type\": \"struct\",\n"
      << "    \"schema-id\": 0,\n"
      << "    \"fields\": [\n";

    for (int i = 0; i < schema_->num_fields(); ++i) {
        const auto& f = schema_->field(i);
        j << "      {\n"
          << "        \"id\": " << (i + 1) << ",\n"   // 1-based field IDs
          << "        \"name\": \"" << f->name() << "\",\n"
          << "        \"required\": " << (!f->nullable() ? "true" : "false") << ",\n"
          << "        \"type\": \"" << arrow_type_to_iceberg_type(f->type()) << "\"\n"
          << "      }";
        if (i < schema_->num_fields() - 1) j << ",";
        j << "\n";
    }

    j << "    ]\n"
      << "  },\n"
      // Partition spec: one spec with no fields (unpartitioned table)
      << "  \"partition-specs\": [{\"spec-id\": 0, \"fields\": []}],\n"
      << "  \"current-spec-id\": 0,\n"
      // Sort order: unsorted (order-id 0)
      << "  \"sort-orders\": [{\"order-id\": 0, \"fields\": []}],\n"
      << "  \"default-sort-order-id\": 0,\n"
      << "  \"properties\": {},\n"
      << "  \"current-snapshot-id\": " << current_snapshot_id_ << ",\n"
      << "  \"snapshots\": [\n"
      << "    {\n"
      << "      \"snapshot-id\": " << current_snapshot_id_ << ",\n"
      << "      \"timestamp-ms\": " << ts << ",\n"
      << "      \"summary\": {\"operation\": \"append\"},\n"
      << "      \"manifest-list\": \"" << manifest_list_path << "\"\n"
      << "    }\n"
      << "  ],\n"
      << "  \"snapshot-log\": [\n"
      << "    {\"snapshot-id\": " << current_snapshot_id_
                                   << ", \"timestamp-ms\": " << ts << "}\n"
      << "  ],\n"
      << "  \"metadata-log\": []\n"
      << "}\n";

    std::ofstream f(table_path_ + "/metadata/v1.metadata.json");
    if (!f.is_open())
        throw std::runtime_error("IcebergWriter: cannot write v1.metadata.json");
    f << j.str();
}

// ─────────────────────────────────────────────────────────────────────────────
// Version hint
// ─────────────────────────────────────────────────────────────────────────────

void IcebergWriter::write_version_hint() const {
    std::ofstream f(table_path_ + "/metadata/version-hint.text");
    if (!f.is_open())
        throw std::runtime_error("IcebergWriter: cannot write version-hint.text");
    f << "1\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// Public WriterInterface implementation
// ─────────────────────────────────────────────────────────────────────────────

void IcebergWriter::write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || batch->num_rows() == 0) return;

    if (!schema_locked_) initialize_iceberg_table(batch);

    if (batch->schema()->field_names() != schema_->field_names())
        throw std::runtime_error(
            "IcebergWriter: schema mismatch on subsequent batch");

    batches_.push_back(batch);

    int64_t accumulated = 0;
    for (const auto& b : batches_) accumulated += b->num_rows();
    if (accumulated >= 10'000'000) write_data_file();
}

void IcebergWriter::close() {
    if (!schema_locked_) return;  // nothing written

    try {
        // 1. Flush any remaining buffered batches to a Parquet file.
        if (!batches_.empty()) write_data_file();

        if (written_files_.empty()) return;  // no data → skip metadata

        // 2. Write manifest-1.avro (per-file entries).
        auto [manifest_path, manifest_size] = write_manifest_avro();

        // 3. Write snap-<id>.avro (manifest list).
        std::string ml_path = write_manifest_list_avro(manifest_path, manifest_size);

        // 4. Write v1.metadata.json (references manifest list).
        write_metadata_json(ml_path);

        // 5. Write version-hint.text.
        write_version_hint();

    } catch (const std::exception& e) {
        std::cerr << "IcebergWriter::close error: " << e.what() << "\n";
        throw;
    }
}

}  // namespace tpch

#endif  // TPCH_ENABLE_ICEBERG
