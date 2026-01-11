#include "tpch/builder_pool.hpp"
#include <stdexcept>

namespace tpch {

std::shared_ptr<BuilderPool> BuilderPool::create(
    const std::shared_ptr<arrow::Schema>& schema,
    size_t pool_size,
    int64_t initial_capacity,
    arrow::MemoryPool* memory_pool) {

    if (!memory_pool) {
        memory_pool = arrow::default_memory_pool();
    }

    // Use shared_ptr with custom deleter to allow private constructor
    return std::shared_ptr<BuilderPool>(
        new BuilderPool(schema, pool_size, initial_capacity, memory_pool));
}

BuilderPool::BuilderPool(
    const std::shared_ptr<arrow::Schema>& schema,
    size_t pool_size,
    int64_t initial_capacity,
    arrow::MemoryPool* memory_pool)
    : schema_(schema)
    , initial_capacity_(initial_capacity)
    , memory_pool_(memory_pool) {

    // Pre-create pool_size builders
    available_.reserve(pool_size);
    for (size_t i = 0; i < pool_size; ++i) {
        available_.push_back(create_builder());
    }
}

std::unique_ptr<arrow::RecordBatchBuilder> BuilderPool::create_builder() {
    auto result = arrow::RecordBatchBuilder::Make(
        schema_,
        memory_pool_,
        initial_capacity_);

    if (!result.ok()) {
        throw std::runtime_error(
            "Failed to create RecordBatchBuilder: " + result.status().message());
    }

    return std::move(result).ValueOrDie();
}

std::unique_ptr<arrow::RecordBatchBuilder> BuilderPool::acquire() {
    std::lock_guard<std::mutex> lock(mutex_);

    total_acquires_++;

    if (available_.empty()) {
        // Pool exhausted, create a new builder
        heap_allocations_++;
        return create_builder();
    }

    // Return a builder from the pool
    auto builder = std::move(available_.back());
    available_.pop_back();
    return builder;
}

void BuilderPool::release(std::unique_ptr<arrow::RecordBatchBuilder> builder) {
    if (!builder) {
        return;  // Ignore null builders
    }

    std::lock_guard<std::mutex> lock(mutex_);

    total_releases_++;

    // Reset the builder (clear data but keep allocated buffers)
    // Note: RecordBatchBuilder doesn't have a Reset() method, so we need to
    // flush it to clear the data. This will keep the capacity.
    auto flush_result = builder->Flush();
    if (!flush_result.ok()) {
        // If flush fails, discard the builder (don't return to pool)
        return;
    }

    // Return to pool
    available_.push_back(std::move(builder));
}

BuilderPool::Stats BuilderPool::get_stats() const {
    std::lock_guard<std::mutex> lock(mutex_);

    return Stats{
        available_.capacity(),
        available_.size(),
        total_acquires_,
        total_releases_,
        heap_allocations_
    };
}

}  // namespace tpch
