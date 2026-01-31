#ifndef TPCH_PAIMON_WRITER_HPP
#define TPCH_PAIMON_WRITER_HPP

#include <memory>
#include <string>
#include <arrow/record_batch.h>

#include "writer_interface.hpp"

namespace tpch {

/**
 * Apache Paimon table writer using paimon-cpp library.
 *
 * IMPORTANT: Paimon is a TABLE FORMAT (lakehouse), not just a file format.
 * This writer creates:
 *   - Table directory structure
 *   - Snapshot metadata files
 *   - Manifest files
 *   - Data files (Parquet backend by default)
 *
 * Output is a directory containing a complete Paimon table that can be
 * read by Apache Paimon (Java), Flink, or Spark.
 *
 * Directory structure:
 *   table_path/
 *     snapshot/           - Snapshot metadata files
 *     manifest/           - Manifest files
 *     data/               - Parquet data files
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
    std::string table_path_;
    std::string table_name_;
    std::shared_ptr<arrow::Schema> schema_;
    bool schema_locked_ = false;

    // Opaque pointers to paimon-cpp implementations
    // Cast to actual types in implementation file to avoid header pollution
    void* paimon_write_context_;      // WriteContext*
    void* paimon_record_batch_builder_;  // RecordBatchBuilder*
    void* paimon_commit_context_;     // CommitContext*

    /**
     * Initialize Paimon write context on first batch.
     * Locks schema and creates necessary directories and metadata structures.
     */
    void initialize_paimon_table(const std::shared_ptr<arrow::RecordBatch>& first_batch);

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
