#pragma once

#include <memory>
#include <vector>
#include <cstdint>
#include <string_view>

namespace tpch {

/**
 * Manages lifetime of temporary data buffers for true zero-copy Arrow conversion.
 *
 * When using Buffer::Wrap(), Arrow buffers directly reference vector memory
 * instead of copying. This manager ensures vectors stay alive until Parquet
 * encoding completes.
 *
 * **Design Pattern:**
 * - Temporary vectors are created with shared_ptr via create_*_buffer()
 * - These shared_ptrs are captured in Arrow Buffer deallocators
 * - When Arrow buffer is freed, deallocator runs and shared_ptr refcount drops
 * - When vector has no other references, it's automatically freed
 * - Safe for both streaming and batch accumulation modes
 */
struct BufferLifetimeManager {
    // Numeric data vectors (for Buffer::Wrap)
    std::vector<std::shared_ptr<std::vector<int64_t>>> int64_buffers;
    std::vector<std::shared_ptr<std::vector<double>>> double_buffers;

    // String view vectors (already zero-copy, but need lifetime extension)
    std::vector<std::shared_ptr<std::vector<std::string_view>>> string_view_buffers;

    /**
     * Create a managed int64 vector
     * Returns shared_ptr that will be stored to extend lifetime
     *
     * @param reserve_size Optional hint for vector capacity
     * @return Managed vector ready for use in data extraction
     */
    std::shared_ptr<std::vector<int64_t>> create_int64_buffer(size_t reserve_size = 0) {
        auto buffer = std::make_shared<std::vector<int64_t>>();
        if (reserve_size > 0) {
            buffer->reserve(reserve_size);
        }
        int64_buffers.push_back(buffer);
        return buffer;
    }

    /**
     * Create a managed double vector
     *
     * @param reserve_size Optional hint for vector capacity
     * @return Managed vector ready for use in data extraction
     */
    std::shared_ptr<std::vector<double>> create_double_buffer(size_t reserve_size = 0) {
        auto buffer = std::make_shared<std::vector<double>>();
        if (reserve_size > 0) {
            buffer->reserve(reserve_size);
        }
        double_buffers.push_back(buffer);
        return buffer;
    }

    /**
     * Create a managed string_view vector
     *
     * @param reserve_size Optional hint for vector capacity
     * @return Managed vector ready for use in data extraction
     */
    std::shared_ptr<std::vector<std::string_view>> create_string_view_buffer(size_t reserve_size = 0) {
        auto buffer = std::make_shared<std::vector<std::string_view>>();
        if (reserve_size > 0) {
            buffer->reserve(reserve_size);
        }
        string_view_buffers.push_back(buffer);
        return buffer;
    }

    /**
     * Get total memory footprint of managed buffers
     * Useful for monitoring memory usage in batch accumulation mode
     *
     * @return Total bytes allocated by all managed vectors
     */
    size_t memory_usage() const {
        size_t total = 0;
        for (const auto& buf : int64_buffers) {
            total += buf->capacity() * sizeof(int64_t);
        }
        for (const auto& buf : double_buffers) {
            total += buf->capacity() * sizeof(double);
        }
        for (const auto& buf : string_view_buffers) {
            total += buf->capacity() * sizeof(std::string_view);
        }
        return total;
    }

    /**
     * Get count of managed buffers
     * Debug metric to verify cleanup
     */
    size_t buffer_count() const {
        return int64_buffers.size() + double_buffers.size() + string_view_buffers.size();
    }
};

/**
 * Combined RecordBatch + lifetime manager
 * Ensures source buffers stay alive until RecordBatch is encoded
 *
 * **Lifetime Guarantees:**
 * - When ManagedRecordBatch is destroyed, batch goes out of scope
 * - Arrow buffers in the batch are freed
 * - Their deallocators run, releasing the captured shared_ptrs
 * - Vectors are freed when no longer referenced
 *
 * **Safe for:**
 * - Streaming mode: ManagedRecordBatch destroyed after write completes
 * - Batch mode: ManagedRecordBatch stored in vector, destroyed in close()
 * - Both modes ensure vectors live at least as long as Parquet encoder needs them
 */
struct ManagedRecordBatch {
    std::shared_ptr<arrow::RecordBatch> batch;
    std::shared_ptr<BufferLifetimeManager> lifetime_mgr;

    /**
     * Create a ManagedRecordBatch
     *
     * @param b Arrow RecordBatch with wrapped buffers
     * @param mgr BufferLifetimeManager holding source vectors
     */
    ManagedRecordBatch(
        std::shared_ptr<arrow::RecordBatch> b,
        std::shared_ptr<BufferLifetimeManager> mgr)
        : batch(std::move(b)), lifetime_mgr(std::move(mgr)) {}

    /**
     * Create empty ManagedRecordBatch (for empty batches)
     */
    ManagedRecordBatch() : batch(nullptr), lifetime_mgr(nullptr) {}
};

} // namespace tpch
