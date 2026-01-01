#ifndef TPCH_PARQUET_WRITER_HPP
#define TPCH_PARQUET_WRITER_HPP

#include <memory>
#include <string>
#include <arrow/record_batch.h>

#include "writer_interface.hpp"

namespace tpch {

/**
 * Parquet writer implementation using Apache Parquet C++ library.
 * Writes Arrow RecordBatch data to Parquet files with compression and schema support.
 */
class ParquetWriter : public WriterInterface {
public:
    /**
     * Create a Parquet writer for the specified filepath.
     * The file will be created or overwritten.
     *
     * @param filepath Path to the output Parquet file
     * @throws std::runtime_error if Parquet initialization fails
     */
    explicit ParquetWriter(const std::string& filepath);

    ~ParquetWriter() override;

    /**
     * Write a batch of rows to the Parquet file.
     * The schema is inferred from the first batch and locked for subsequent batches.
     *
     * @param batch Arrow RecordBatch to write
     * @throws std::runtime_error if write fails or schema is inconsistent
     */
    void write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    /**
     * Finalize and close the output file.
     */
    void close() override;

private:
    std::string filepath_;
    std::shared_ptr<arrow::RecordBatch> first_batch_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    bool closed_ = false;
};

}  // namespace tpch

#endif  // TPCH_PARQUET_WRITER_HPP
