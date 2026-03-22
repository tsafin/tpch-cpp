#ifndef TPCH_ICEBERG_WRITER_HPP
#define TPCH_ICEBERG_WRITER_HPP

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <arrow/record_batch.h>

#include "writer_interface.hpp"

namespace tpch {

/**
 * Apache Iceberg table writer for lakehouse table format support.
 *
 * IMPORTANT: Iceberg is a TABLE FORMAT (lakehouse), not just a file format.
 * This writer creates a complete Iceberg v1 table directory that is readable
 * by Apache Spark, Trino, Flink, DuckDB, and any other Iceberg-compliant
 * engine.
 *
 * Directory structure:
 *   table_path/
 *     metadata/
 *       v1.metadata.json         - Main table metadata (JSON, per spec)
 *       snap-<id>.avro           - Manifest list (Avro binary, per spec)
 *       manifest-1.avro          - Manifest file  (Avro binary, per spec)
 *       version-hint.text        - Pointer to current metadata version
 *     data/
 *       data_00000.parquet
 *       data_00001.parquet
 *       ...
 *
 * Iceberg v1 compliance:
 *   - Manifest list and manifest files are written as Avro binary
 *     (not JSON) as required by the specification.
 *   - Column field IDs are 1-based (field 1 … N).
 *   - Per-file record counts and file sizes are populated accurately.
 *   - Unpartitioned tables only (single partition spec with no fields).
 *   - Single schema version (no schema evolution).
 *   - Append-only snapshots.
 *   - Parquet backing format.
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
     * On first call, locks schema and initialises Iceberg write context.
     * Subsequent batches must match the locked schema exactly.
     */
    void write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    /**
     * Finalise the Iceberg table.
     *
     * Flushes any buffered data to Parquet, then writes in order:
     *   manifest-1.avro  → snap-<id>.avro  → v1.metadata.json  → version-hint.text
     *
     * Must be called before reading the table with other tools.
     */
    void close() override;

private:
    // Metadata tracked for each written data file.
    struct DataFileInfo {
        std::string filename;      // basename only, e.g. "data_00000.parquet"
        int64_t     record_count;
        int64_t     file_size;     // bytes
    };

    std::string table_path_;
    std::string table_name_;
    std::shared_ptr<arrow::Schema> schema_;
    bool    schema_locked_        = false;
    int64_t row_count_            = 0;
    int32_t file_count_           = 0;
    int64_t current_snapshot_id_  = 1;

    // RecordBatches accumulated until the per-file row threshold is met.
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    // One entry per Parquet file written to data/.
    std::vector<DataFileInfo> written_files_;

    // Initialise subdirectories and lock schema on the first batch.
    void initialize_iceberg_table(const std::shared_ptr<arrow::RecordBatch>& first_batch);

    // Flush accumulated batches → one Parquet data file; update written_files_.
    std::string write_data_file();

    // Write manifest-1.avro; return {absolute_path, file_size_bytes}.
    std::pair<std::string, int64_t> write_manifest_avro() const;

    // Write snap-<id>.avro (manifest list); return absolute path.
    std::string write_manifest_list_avro(const std::string& manifest_path,
                                         int64_t manifest_size) const;

    // Write v1.metadata.json referencing the given manifest-list path.
    void write_metadata_json(const std::string& manifest_list_path) const;

    // Write version-hint.text ("1\n").
    void write_version_hint() const;

    // Convert an Arrow DataType to its Iceberg type string.
    static std::string arrow_type_to_iceberg_type(
        const std::shared_ptr<arrow::DataType>& arrow_type);

    // Generate a random UUID (sufficient for table IDs, not crypto-secure).
    static std::string generate_uuid();

    // Return current wall-clock time in milliseconds since Unix epoch.
    static int64_t current_timestamp_ms();
};

}  // namespace tpch

#endif  // TPCH_ICEBERG_WRITER_HPP
