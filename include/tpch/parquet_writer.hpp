#ifndef TPCH_PARQUET_WRITER_HPP
#define TPCH_PARQUET_WRITER_HPP

#include <memory>
#include <string>
#include <arrow/record_batch.h>
#include <arrow/memory_pool.h>

#include "writer_interface.hpp"

namespace tpch {

// Forward declaration
class AsyncIOContext;

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
     * @param memory_pool Custom Arrow memory pool (nullptr = default pool)
     * @param estimated_rows Estimated number of rows for pre-allocation (0 = no pre-alloc)
     * @throws std::runtime_error if Parquet initialization fails
     */
    explicit ParquetWriter(
        const std::string& filepath,
        arrow::MemoryPool* memory_pool = nullptr,
        int64_t estimated_rows = 0);

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
     * If an async context is set, uses asynchronous I/O for writing.
     */
    void close() override;

    /**
     * Set an async I/O context for asynchronous writes.
     * If set, the Parquet file will be written asynchronously.
     *
     * @param async_context AsyncIOContext to use for async writes
     */
    void set_async_context(std::shared_ptr<AsyncIOContext> async_context);

private:
    std::string filepath_;
    std::shared_ptr<arrow::RecordBatch> first_batch_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    std::shared_ptr<AsyncIOContext> async_context_;
    std::shared_ptr<arrow::Buffer> async_buffer_;  // Keep buffer alive during async I/O
    int async_fd_ = -1;  // Track file descriptor for cleanup
    bool closed_ = false;

    // Memory pool optimization (Phase 13.3)
    arrow::MemoryPool* memory_pool_ = nullptr;
    int64_t estimated_rows_ = 0;
};

}  // namespace tpch

#endif  // TPCH_PARQUET_WRITER_HPP
