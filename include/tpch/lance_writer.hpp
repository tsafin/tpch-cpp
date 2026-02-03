#ifndef TPCH_LANCE_WRITER_HPP
#define TPCH_LANCE_WRITER_HPP

#include <memory>
#include <string>
#include <vector>
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
 * Current implementation (stub/placeholder):
 *   - Creates dataset_path directory with data/ subdirectory
 *   - Communicates with Rust FFI library (liblance_ffi.a) via opaque pointers
 *   - Proper lifecycle management of opaque Rust pointers
 *   - Full Lance data writing and metadata file creation implemented in future phases
 *
 * Implementation Details:
 *   - Uses Arrow C Data Interface (opaque pointers) for passing data to Rust layer
 *   - Communicates with Rust FFI library (liblance_ffi.a)
 *   - Proper lifecycle management of opaque Rust pointers
 *   - Currently stores writer state without writing to Lance format
 */
class LanceWriter : public WriterInterface {
public:
    /**
     * Create Lance dataset writer.
     *
     * @param dataset_path Directory path for the Lance dataset (NOT a file!)
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
     *
     * Uses Arrow C Data Interface for zero-copy data transfer to Rust layer.
     *
     * @param batch Arrow RecordBatch to write
     * @throws std::runtime_error if batch schema doesn't match or write fails
     */
    void write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    /**
     * Finalize the Lance dataset.
     *
     * Closes the Rust writer and creates dataset metadata files.
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
     * Creates C-compatible FFI structures for passing to Rust layer.
     *
     * @param batch RecordBatch to convert
     * @return Pair of (ArrowArray pointer, ArrowSchema pointer) for FFI
     *
     * Note: These pointers must be freed after use.
     */
    std::pair<void*, void*> batch_to_ffi(const std::shared_ptr<arrow::RecordBatch>& batch);

    /**
     * Free Arrow C Data Interface structures created by batch_to_ffi().
     *
     * @param array_ptr Pointer to ArrowArray FFI structure
     * @param schema_ptr Pointer to ArrowSchema FFI structure
     */
    void free_ffi_structures(void* array_ptr, void* schema_ptr);
};

}  // namespace tpch

#endif  // TPCH_LANCE_WRITER_HPP
