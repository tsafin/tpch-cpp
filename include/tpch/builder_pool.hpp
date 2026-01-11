#ifndef TPCH_BUILDER_POOL_HPP
#define TPCH_BUILDER_POOL_HPP

#include <arrow/api.h>
#include <arrow/record_batch.h>
#include <memory>
#include <vector>
#include <mutex>

namespace tpch {

/**
 * Object pool for Arrow RecordBatchBuilder instances.
 *
 * Creating and destroying Arrow builders has overhead (allocations, vtable setup, etc.).
 * This pool maintains a set of pre-created builders that can be reused across batches.
 *
 * Benefits:
 * - Eliminates repeated builder construction/destruction overhead
 * - Builders can pre-allocate capacity, avoiding incremental growth
 * - Thread-safe acquire/release for parallel generation
 *
 * Usage:
 *   auto pool = BuilderPool::create(schema, 4);  // Pool of 4 builders
 *   auto builder = pool->acquire();
 *   // ... use builder ...
 *   pool->release(std::move(builder));
 */
class BuilderPool {
public:
    /**
     * Create a builder pool for the given schema.
     *
     * @param schema Arrow schema for the builders
     * @param pool_size Number of builders to maintain in the pool
     * @param initial_capacity Initial capacity for each builder (rows)
     * @param memory_pool Arrow memory pool to use (default pool if nullptr)
     */
    static std::shared_ptr<BuilderPool> create(
        const std::shared_ptr<arrow::Schema>& schema,
        size_t pool_size = 4,
        int64_t initial_capacity = 100000,
        arrow::MemoryPool* memory_pool = nullptr);

    /**
     * Acquire a builder from the pool.
     * If the pool is empty, creates a new builder.
     *
     * @return RecordBatchBuilder ready to use
     */
    std::unique_ptr<arrow::RecordBatchBuilder> acquire();

    /**
     * Release a builder back to the pool.
     * The builder is reset (data cleared) before being returned to the pool.
     *
     * @param builder Builder to release (should be empty after Flush())
     */
    void release(std::unique_ptr<arrow::RecordBatchBuilder> builder);

    /**
     * Get statistics about pool usage.
     */
    struct Stats {
        size_t pool_size;
        size_t available;
        size_t total_acquires;
        size_t total_releases;
        size_t heap_allocations;  // Builders created when pool was empty
    };

    Stats get_stats() const;

private:
    BuilderPool(const std::shared_ptr<arrow::Schema>& schema,
               size_t pool_size,
               int64_t initial_capacity,
               arrow::MemoryPool* memory_pool);

    std::unique_ptr<arrow::RecordBatchBuilder> create_builder();

    std::shared_ptr<arrow::Schema> schema_;
    int64_t initial_capacity_;
    arrow::MemoryPool* memory_pool_;

    mutable std::mutex mutex_;
    std::vector<std::unique_ptr<arrow::RecordBatchBuilder>> available_;

    // Statistics (protected by mutex_)
    size_t total_acquires_ = 0;
    size_t total_releases_ = 0;
    size_t heap_allocations_ = 0;
};

}  // namespace tpch

#endif  // TPCH_BUILDER_POOL_HPP
