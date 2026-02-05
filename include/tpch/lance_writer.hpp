#ifndef TPCH_LANCE_WRITER_HPP
#define TPCH_LANCE_WRITER_HPP

#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <arrow/record_batch.h>

#include "writer_interface.hpp"
#include "lance_ffi.h"

namespace tpch {

/**
 * Lance columnar format writer using Rust FFI bridge.
 *
 * IMPORTANT: Lance is a modern columnar format optimized for ML/AI workloads.
 * This writer creates a Lance dataset that can be read by:
 *   - Python: pylance library
 *   - Spark: Spark-Lance connector
 *   - DuckDB: Lance extension
 *   - Flink: Flink-Lance connector
 *
 * Output is a directory containing a complete Lance dataset.
 *
 * Phase 1 Implementation (Streaming via Rust FFI):
 *   - Batches are streamed directly to Rust writer (no C++ accumulation)
 *   - Uses Arrow C Data Interface for zero-copy batch passing
 *   - Rust writer handles Lance format writing and metadata creation
 *   - Proper lifecycle management of opaque Rust pointers
 *
 * Implementation Details:
 *   - Uses Arrow C Data Interface structs (ArrowArray, ArrowSchema) for FFI
 *   - Communicates with Rust FFI library (liblance_ffi.a)
 *   - Converts Arrow RecordBatch to C Data Interface format
 *   - Streams each batch to Rust for immediate writing (no accumulation)
 */
class LanceWriter : public WriterInterface {
public:
    /**
     * Create Lance dataset writer.
     *
     * @param dataset_path Directory path for the Lance dataset (should end with .lance)
     * @param dataset_name Logical dataset name (used in metadata, default: "tpch_dataset")
     *
     * The directory will be created if it doesn't exist.
     */
    explicit LanceWriter(const std::string& dataset_path,
                        const std::string& dataset_name = "tpch_dataset");

    ~LanceWriter() override;

    /**
     * Write a batch of records to the Lance dataset.
     *
     * On first call, initializes Lance writer and locks schema.
     * Subsequent batches must match the locked schema exactly.
     * Batches are streamed directly to Rust writer (no C++ accumulation).
     *
     * @param batch Arrow RecordBatch to write
     * @throws std::runtime_error if batch schema doesn't match or write fails
     */
    void write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    /**
     * Finalize the Lance dataset.
     *
     * Flushes remaining batches, creates metadata files, and closes the writer.
     * Must be called before reading the dataset with other tools.
     */
    void close() override;

private:
    std::string dataset_path_;
    std::string dataset_name_;
    std::shared_ptr<arrow::Schema> schema_;
    bool schema_locked_ = false;
    int64_t row_count_ = 0;
    int32_t batch_count_ = 0;

    // Opaque pointer to Rust LanceWriter (use void* to avoid type conflicts)
    void* rust_writer_ = nullptr;

    /**
     * Initialize Lance writer on first batch.
     * Creates directory structure and initializes Rust writer.
     */
    void initialize_lance_dataset(const std::shared_ptr<arrow::RecordBatch>& first_batch);

    /**
     * Convert Arrow RecordBatch to Arrow C Data Interface format.
     *
     * @param batch RecordBatch to convert
     * @return Pair of (ArrowArray pointer, ArrowSchema pointer) allocated with new
     */
    std::pair<void*, void*> batch_to_ffi(const std::shared_ptr<arrow::RecordBatch>& batch);

    /**
     * Free Arrow C Data Interface structures and call their release callbacks.
     *
     * @param array_ptr Pointer to ArrowArray FFI structure
     * @param schema_ptr Pointer to ArrowSchema FFI structure
     */
    void free_ffi_structures(void* array_ptr, void* schema_ptr);
};

}  // namespace tpch

#endif  // TPCH_LANCE_WRITER_HPP
