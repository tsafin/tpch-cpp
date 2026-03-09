#ifndef TPCH_LANCE_WRITER_HPP
#define TPCH_LANCE_WRITER_HPP

#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <chrono>
#include <arrow/record_batch.h>

#include "writer_interface.hpp"
#include "lance_ffi.h"

namespace tpch {

struct StreamState;
class StreamRecordBatchReader;

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

    /**
     * Enable streaming write mode (Phase 2).
     *
     * If enabled, uses a background task and channels in Rust to write data concurrently
     * without blocking the main C++ thread. Backpressure is applied if the channel fills up.
     *
     * @param enabled True to enable streaming mode
     */
    void enable_streaming_write(bool enabled) { streaming_enabled_ = enabled; }

    /**
     * Configure Lance write parameters (optional).
     *
     * Pass 0 for any numeric value to keep Lance defaults.
     */
    void set_write_params(int64_t max_rows_per_file,
                          int64_t max_rows_per_group,
                          int64_t max_bytes_per_file,
                          bool skip_auto_cleanup);

    /**
     * Configure the in-memory stream queue depth (backpressure).
     */
    void set_stream_queue_depth(size_t depth) { stream_queue_depth_ = depth; }

    /**
     * Configure Tokio runtime settings used by Rust streaming writer.
     *
     * @param max_blocking_threads Cap for Tokio blocking thread pool (0 keeps default)
     */
    void set_runtime_config(int max_blocking_threads) {
        if (max_blocking_threads > 0) {
            stream_max_blocking_threads_ = max_blocking_threads;
        }
    }

    /**
     * Enable/disable Rust-side memory profiling logs for streaming mode.
     *
     * @param enabled Emit stage and per-batch RSS logs from Lance FFI
     * @param report_every_batches Emit per-batch log every N batches
     */
    void set_profile_config(bool enabled, size_t report_every_batches) {
        stream_mem_profile_enabled_ = enabled;
        if (report_every_batches > 0) {
            stream_mem_profile_every_batches_ = report_every_batches;
        }
    }

    /**
     * Configure Rust-side scatter/gather chunked stream handoff.
     *
     * @param batches_per_chunk 1 disables, >1 enables chunking
     * @param queue_chunks Bounded queue size in chunks
     */
    void set_scatter_gather_config(size_t batches_per_chunk, size_t queue_chunks) {
        if (batches_per_chunk > 0) {
            stream_scatter_gather_batches_ = batches_per_chunk;
        }
        if (queue_chunks > 0) {
            stream_scatter_gather_queue_chunks_ = queue_chunks;
        }
    }

    /**
     * Enable io_uring write path (Linux only, requires io-uring feature compiled in).
     * Must be called before the first batch is written.
     */
#ifdef TPCH_LANCE_IO_URING
    void enable_io_uring(bool enabled) { use_io_uring_ = enabled; }
#endif

private:
    std::string dataset_path_;
    std::string dataset_name_;
    std::shared_ptr<arrow::Schema> schema_;
    bool schema_locked_ = false;
    bool streaming_enabled_ = false;
    bool streaming_started_ = false;
    int64_t row_count_ = 0;
    int32_t batch_count_ = 0;
    int64_t total_byte_count_ = 0;
    int64_t last_report_row_count_ = 0;
    int32_t last_report_batch_count_ = 0;
    int64_t last_report_byte_count_ = 0;
    std::chrono::steady_clock::time_point start_time_;
    std::chrono::steady_clock::time_point last_report_time_;

    // Opaque pointer to Rust LanceWriter (use void* to avoid type conflicts)
    void* rust_writer_ = nullptr;

    // Lance write parameters (optional overrides)
    int64_t max_rows_per_file_ = 0;
    int64_t max_rows_per_group_ = 0;
    int64_t max_bytes_per_file_ = 0;
    bool skip_auto_cleanup_ = false;
#ifdef TPCH_LANCE_IO_URING
    bool use_io_uring_ = false;
#endif

    size_t stream_queue_depth_ = 16;
    int stream_max_blocking_threads_ = 8;
    bool stream_mem_profile_enabled_ = false;
    size_t stream_mem_profile_every_batches_ = 100;
    size_t stream_scatter_gather_batches_ = 1;
    size_t stream_scatter_gather_queue_chunks_ = 4;
    std::shared_ptr<StreamState> stream_state_;
    std::shared_ptr<StreamRecordBatchReader> stream_reader_;

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
     *
     * NOTE: Ownership of returned pointers is transferred to Rust FFI layer via
     * lance_writer_write_batch(). Rust calls FFI_ArrowSchema::from_raw() which takes
     * ownership and is responsible for calling release callbacks via Drop trait.
     * C++ must NOT call release() or delete these pointers.
     */
    std::pair<void*, void*> batch_to_ffi(const std::shared_ptr<arrow::RecordBatch>& batch);
};

}  // namespace tpch

#endif  // TPCH_LANCE_WRITER_HPP
