#ifndef TPCH_PAIMON_WRITER_HPP
#define TPCH_PAIMON_WRITER_HPP

#include <memory>
#include <string>
#include <arrow/record_batch.h>

#include "writer_interface.hpp"

namespace tpch {

/**
 * Apache Paimon table writer (spec-compliant, Flink/Spark compatible).
 *
 * IMPORTANT: Paimon is a TABLE FORMAT (lakehouse), not just a file format.
 * This writer creates a production-ready table with:
 *   - Table directory structure (OPTIONS, schema/, snapshot/, manifest/, bucket-0/)
 *   - Snapshot metadata files (snapshot-1, EARLIEST, LATEST hints)
 *   - Manifest files in Avro binary format
 *   - Data files in Parquet format (bucket-0/)
 *
 * Output is a directory containing a complete Paimon table that can be
 * read by Apache Paimon (Java), Apache Flink, or Apache Spark directly.
 *
 * Directory structure:
 *   table_path/
 *     OPTIONS                      - Table configuration (plain text)
 *     schema/
 *       schema-0                   - Schema file (JSON)
 *     snapshot/
 *       EARLIEST                   - Hint file with earliest snapshot ID
 *       LATEST                     - Hint file with latest snapshot ID
 *       snapshot-1                 - Snapshot metadata (JSON, v3)
 *     manifest/
 *       manifest-<UUID>-0          - Manifest entries (Avro binary)
 *       manifest-list-<UUID>-0     - Manifest list (Avro binary)
 *     bucket-0/
 *       data-<UUID>-0.parquet      - Parquet data files
 */
class PaimonWriter : public WriterInterface {
public:
    /**
     * Create Paimon table writer.
     *
     * @param table_path Directory path for the Paimon table (NOT a file!)
     * @param table_name Logical table name (used in metadata, default: "tpch_table")
     *
     * The directory will be created if it doesn't exist.
     */
    explicit PaimonWriter(const std::string& table_path,
                         const std::string& table_name = "tpch_table");

    ~PaimonWriter() override;

    /**
     * Write a batch of records to the Paimon table.
     *
     * On first call, locks schema and initializes Paimon write context.
     * Subsequent batches must match the locked schema exactly.
     *
     * @param batch Arrow RecordBatch to write
     * @throws std::runtime_error if batch schema doesn't match or write fails
     */
    void write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    /**
     * Finalize the Paimon table by committing snapshot.
     *
     * Creates snapshot metadata and manifest files to make written data visible.
     * Must be called before reading the table with other tools.
     */
    void close() override;

private:
    struct DataFileInfo {
        std::string name;
        int64_t size;
        int64_t rows;
    };

    std::string table_path_;
    std::string table_name_;
    std::shared_ptr<arrow::Schema> schema_;
    bool schema_locked_ = false;
    bool closed_ = false;
    int64_t row_count_ = 0;
    int32_t file_count_ = 0;

    // Store batches for buffered writing to Parquet
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    std::vector<DataFileInfo> data_files_;

    /**
     * Initialize Paimon write context on first batch.
     * Locks schema and creates necessary directories and metadata structures.
     */
    void initialize_paimon_table(const std::shared_ptr<arrow::RecordBatch>& first_batch);

    /**
     * Write accumulated batches to Parquet data file.
     * Creates a data file in bucket-0/ and tracks metadata.
     */
    void write_data_file();

    /**
     * Write OPTIONS file.
     */
    void write_options_file();

    /**
     * Write schema/schema-0 file.
     */
    void write_schema_file();

    /**
     * Write manifest (Avro binary) containing all data file entries.
     * @return Manifest filename (not full path)
     */
    std::string write_data_manifest();

    /**
     * Write manifest list (Avro binary) referencing the manifest file.
     * @param manifest_name Manifest filename to reference
     * @param manifest_size Size of manifest file in bytes
     * @return Manifest list filename (not full path)
     */
    std::string write_manifest_list(const std::string& manifest_name, int64_t manifest_size);

    /**
     * Write snapshot metadata file (snapshot-1).
     * @param delta_manifest_list_name Manifest list filename to reference
     */
    void write_snapshot(const std::string& delta_manifest_list_name);

    /**
     * Write snapshot hint files (EARLIEST, LATEST).
     */
    void write_snapshot_hints();

    /**
     * Encode a ManifestEntry record in Avro format.
     * @param file Data file metadata to encode
     * @return Encoded Avro record bytes
     */
    std::vector<uint8_t> encode_manifest_entry(const DataFileInfo& file);

    /**
     * Encode a ManifestListEntry record in Avro format.
     * @param manifest_name Manifest filename to reference
     * @param manifest_size Size of manifest file
     * @return Encoded Avro record bytes
     */
    std::vector<uint8_t> encode_manifest_list_entry(const std::string& manifest_name, int64_t manifest_size);

    /**
     * Get the Avro schema for ManifestEntry records (JSON string).
     */
    static const std::string& manifest_entry_schema();

    /**
     * Get the Avro schema for ManifestListEntry records (JSON string).
     */
    static const std::string& manifest_list_entry_schema();

    /**
     * Generate a random UUID (used in file names).
     * @return UUID string without hyphens (32 hex chars)
     */
    static std::string generate_uuid();

    /**
     * Get current timestamp in milliseconds since epoch.
     */
    static int64_t current_timestamp_ms();

    /**
     * Convert Arrow DataType to Paimon type string.
     * @param arrow_type Arrow data type
     * @return Paimon type string (e.g., "bigint", "double", "string")
     * @throws std::runtime_error if type is unsupported
     */
    static std::string arrow_type_to_paimon_type(const std::shared_ptr<arrow::DataType>& arrow_type);
};

}  // namespace tpch

#endif  // TPCH_PAIMON_WRITER_HPP
