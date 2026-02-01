#ifndef TPCH_ICEBERG_WRITER_HPP
#define TPCH_ICEBERG_WRITER_HPP

#include <memory>
#include <string>
#include <arrow/record_batch.h>

#include "writer_interface.hpp"

namespace tpch {

/**
 * Apache Iceberg table writer for lakehouse table format support.
 *
 * IMPORTANT: Iceberg is a TABLE FORMAT (lakehouse), not just a file format.
 * This writer creates:
 *   - Table directory structure
 *   - Snapshot metadata files
 *   - Manifest metadata files
 *   - Data files (Parquet backing format)
 *
 * Output is a directory containing a complete Iceberg table that can be
 * read by Apache Iceberg (all engines), Spark, Trino, Flink, DuckDB, etc.
 *
 * Directory structure:
 *   table_path/
 *     metadata/
 *       v1.metadata.json         - Main table metadata
 *       snap-<id>.manifest-list.json - Manifest list
 *       manifest-*.json          - Manifest files
 *       version-hint.text        - Pointer to current metadata version
 *     data/                      - Parquet data files
 *       data_00000.parquet
 *       data_00001.parquet
 *       ...
 *
 * Phase 2.1 (Simplified Iceberg):
 *   - Unpartitioned tables only (flat data directory)
 *   - Single schema version (no evolution)
 *   - Append-only snapshots
 *   - Parquet backing format
 *   - JSON metadata (Avro deferred to Phase 2.2)
 *   - Compatible with Iceberg readers (Spark, Trino, DuckDB)
 */
class IcebergWriter : public WriterInterface {
public:
    /**
     * Create Iceberg table writer.
     *
     * @param table_path Directory path for the Iceberg table (NOT a file!)
     * @param table_name Logical table name (used in metadata, default: "tpch_table")
     *
     * The directory will be created if it doesn't exist.
     */
    explicit IcebergWriter(const std::string& table_path,
                          const std::string& table_name = "tpch_table");

    ~IcebergWriter() override = default;

    /**
     * Write a batch of records to the Iceberg table.
     *
     * On first call, locks schema and initializes Iceberg write context.
     * Subsequent batches must match the locked schema exactly.
     *
     * @param batch Arrow RecordBatch to write
     * @throws std::runtime_error if batch schema doesn't match or write fails
     */
    void write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    /**
     * Finalize the Iceberg table by creating metadata files.
     *
     * Creates v1.metadata.json, manifest list, and manifest files to make
     * written data visible according to Iceberg format specification.
     * Must be called before reading the table with other tools.
     */
    void close() override;

private:
    std::string table_path_;
    std::string table_name_;
    std::shared_ptr<arrow::Schema> schema_;
    bool schema_locked_ = false;
    int64_t row_count_ = 0;
    int32_t file_count_ = 0;
    int64_t current_snapshot_id_ = 1;

    // Store batches for buffered writing to Parquet
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    std::vector<std::string> written_files_;

    /**
     * Initialize Iceberg write context on first batch.
     * Locks schema and creates necessary directories and metadata structures.
     */
    void initialize_iceberg_table(const std::shared_ptr<arrow::RecordBatch>& first_batch);

    /**
     * Write accumulated batches to Parquet data file.
     * Creates a data file and tracks it for manifest.
     * @return Path to written file
     */
    std::string write_data_file();

    /**
     * Create Iceberg v1 metadata JSON file.
     * Contains schema, snapshots, snapshot log according to Iceberg spec v1.
     * @return JSON string representing metadata
     */
    std::string create_metadata_json();

    /**
     * Create Iceberg manifest list JSON file.
     * Lists the manifest files for the current snapshot.
     * @return JSON string representing manifest list
     */
    std::string create_manifest_list_json();

    /**
     * Create Iceberg manifest JSON file.
     * Lists data files and their metadata for the snapshot.
     * @return JSON string representing manifest
     */
    std::string create_manifest_json();

    /**
     * Create Iceberg version hint file.
     * Points to the current metadata version (simple text file).
     * @return Content of version-hint.text file
     */
    std::string create_version_hint();

    /**
     * Convert Arrow DataType to Iceberg type string.
     * @param arrow_type Arrow data type
     * @return Iceberg type string (e.g., "long", "double", "string")
     * @throws std::runtime_error if type is unsupported
     */
    static std::string arrow_type_to_iceberg_type(
        const std::shared_ptr<arrow::DataType>& arrow_type);

    /**
     * Generate a UUID for the table (simplified implementation).
     * @return UUID string
     */
    static std::string generate_uuid();

    /**
     * Get current timestamp in milliseconds since epoch.
     * @return Timestamp as int64_t
     */
    static int64_t current_timestamp_ms();
};

}  // namespace tpch

#endif  // TPCH_ICEBERG_WRITER_HPP
