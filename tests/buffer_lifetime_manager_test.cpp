#include <gtest/gtest.h>
#include <arrow/api.h>
#include <memory>
#include <vector>

#include "tpch/buffer_lifetime_manager.hpp"
#include "tpch/zero_copy_converter.hpp"

namespace tpch {

// ============================================================================
// BufferLifetimeManager Tests
// ============================================================================

class BufferLifetimeManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        manager = std::make_shared<BufferLifetimeManager>();
    }

    std::shared_ptr<BufferLifetimeManager> manager;
};

TEST_F(BufferLifetimeManagerTest, CreateInt64Buffer) {
    auto vec = manager->create_int64_buffer(100);
    EXPECT_NE(vec, nullptr);
    EXPECT_EQ(vec->capacity(), 100);
    EXPECT_EQ(vec->size(), 0);
}

TEST_F(BufferLifetimeManagerTest, CreateDoubleBuffer) {
    auto vec = manager->create_double_buffer(50);
    EXPECT_NE(vec, nullptr);
    EXPECT_EQ(vec->capacity(), 50);
    EXPECT_EQ(vec->size(), 0);
}

TEST_F(BufferLifetimeManagerTest, CreateStringViewBuffer) {
    auto vec = manager->create_string_view_buffer(75);
    EXPECT_NE(vec, nullptr);
    EXPECT_EQ(vec->capacity(), 75);
    EXPECT_EQ(vec->size(), 0);
}

TEST_F(BufferLifetimeManagerTest, MultipleBuffers) {
    auto int64_vec1 = manager->create_int64_buffer(100);
    auto int64_vec2 = manager->create_int64_buffer(200);
    auto double_vec = manager->create_double_buffer(150);
    auto str_vec = manager->create_string_view_buffer(50);

    EXPECT_EQ(manager->buffer_count(), 4);
}

TEST_F(BufferLifetimeManagerTest, BufferIsPersistent) {
    // Create and fill a buffer
    auto vec = manager->create_int64_buffer(10);
    vec->push_back(42);
    vec->push_back(100);
    vec->push_back(999);

    EXPECT_EQ(vec->size(), 3);
    EXPECT_EQ((*vec)[0], 42);
    EXPECT_EQ((*vec)[1], 100);
    EXPECT_EQ((*vec)[2], 999);
}

TEST_F(BufferLifetimeManagerTest, MemoryUsage) {
    // Empty manager
    EXPECT_EQ(manager->memory_usage(), 0);

    // Create some buffers
    auto int64_vec = manager->create_int64_buffer(100);
    auto double_vec = manager->create_double_buffer(50);

    size_t expected = (100 * sizeof(int64_t)) + (50 * sizeof(double));
    EXPECT_EQ(manager->memory_usage(), expected);
}

TEST_F(BufferLifetimeManagerTest, ManagedRecordBatchCreation) {
    // Create a simple schema
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::float64())
    });

    // Create empty arrays
    auto id_array = std::make_shared<arrow::Int64Array>(0, nullptr);
    auto value_array = std::make_shared<arrow::DoubleArray>(0, nullptr);

    std::vector<std::shared_ptr<arrow::Array>> arrays = {id_array, value_array};
    auto batch = arrow::RecordBatch::Make(schema, 0, arrays);

    // Create managed batch
    ManagedRecordBatch managed(batch, manager);

    EXPECT_NE(managed.batch, nullptr);
    EXPECT_NE(managed.lifetime_mgr, nullptr);
    EXPECT_EQ(managed.batch->num_rows(), 0);
}

TEST_F(BufferLifetimeManagerTest, ManagedRecordBatchEmpty) {
    // Create empty managed batch (edge case)
    ManagedRecordBatch managed;
    EXPECT_EQ(managed.batch, nullptr);
    EXPECT_EQ(managed.lifetime_mgr, nullptr);
}

// ============================================================================
// Wrapped Array Builder Tests
// ============================================================================

class WrappedArrayBuilderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Note: These are integration tests that call private methods
        // through public converters where possible
    }
};

TEST_F(WrappedArrayBuilderTest, Int64WrappedBufferEqualsContent) {
    // Create a vector of int64 values
    auto vec = std::make_shared<std::vector<int64_t>>();
    vec->push_back(10);
    vec->push_back(20);
    vec->push_back(30);
    vec->push_back(40);
    vec->push_back(50);

    // Create wrapped buffer using Arrow's Buffer::Wrap directly
    // Note: vec must stay alive while buffer is in use
    const int64_t count = vec->size();
    const int64_t size_bytes = count * sizeof(int64_t);

    auto buffer = arrow::Buffer::Wrap(
        reinterpret_cast<const uint8_t*>(vec->data()),
        size_bytes
    );

    // Verify buffer contents
    EXPECT_EQ(buffer->size(), size_bytes);
    const int64_t* data_ptr = reinterpret_cast<const int64_t*>(buffer->data());
    EXPECT_EQ(data_ptr[0], 10);
    EXPECT_EQ(data_ptr[1], 20);
    EXPECT_EQ(data_ptr[2], 30);
    EXPECT_EQ(data_ptr[3], 40);
    EXPECT_EQ(data_ptr[4], 50);
}

TEST_F(WrappedArrayBuilderTest, DoubleWrappedBufferEqualsContent) {
    // Create a vector of double values
    auto vec = std::make_shared<std::vector<double>>();
    vec->push_back(1.5);
    vec->push_back(2.7);
    vec->push_back(3.14);
    vec->push_back(4.0);

    // Create wrapped buffer
    // Note: vec must stay alive while buffer is in use
    const int64_t count = vec->size();
    const int64_t size_bytes = count * sizeof(double);

    auto buffer = arrow::Buffer::Wrap(
        reinterpret_cast<const uint8_t*>(vec->data()),
        size_bytes
    );

    // Verify buffer contents
    EXPECT_EQ(buffer->size(), size_bytes);
    const double* data_ptr = reinterpret_cast<const double*>(buffer->data());
    EXPECT_DOUBLE_EQ(data_ptr[0], 1.5);
    EXPECT_DOUBLE_EQ(data_ptr[1], 2.7);
    EXPECT_DOUBLE_EQ(data_ptr[2], 3.14);
    EXPECT_DOUBLE_EQ(data_ptr[3], 4.0);
}

TEST_F(WrappedArrayBuilderTest, EmptyBufferHandling) {
    // Create empty vector
    auto vec = std::make_shared<std::vector<int64_t>>();

    const int64_t count = vec->size();
    const int64_t size_bytes = count * sizeof(int64_t);

    // Should handle empty vector gracefully
    auto buffer = arrow::Buffer::Wrap(
        reinterpret_cast<const uint8_t*>(vec->data()),
        size_bytes
    );

    EXPECT_EQ(buffer->size(), 0);
}

TEST_F(WrappedArrayBuilderTest, VectorLifetimeManagement) {
    // Test that BufferLifetimeManager keeps vectors alive
    // when passed to wrapped array builders
    auto manager = std::make_shared<BufferLifetimeManager>();
    std::weak_ptr<std::vector<int64_t>> weak_vec;

    {
        auto vec = manager->create_int64_buffer(10);
        vec->push_back(42);
        vec->push_back(100);

        weak_vec = vec;

        // Vector is still alive within manager
        EXPECT_FALSE(weak_vec.expired());

        const int64_t count = vec->size();
        const int64_t size_bytes = count * sizeof(int64_t);

        auto buffer = arrow::Buffer::Wrap(
            reinterpret_cast<const uint8_t*>(vec->data()),
            size_bytes
        );

        // Vector should still be alive here (manager keeps it alive)
        EXPECT_FALSE(weak_vec.expired());
    }

    // After local scope, the vector is still alive because manager holds it
    EXPECT_FALSE(weak_vec.expired());

    // Clear the manager to release the vector
    manager.reset();

    // Now the vector should be freed
    EXPECT_TRUE(weak_vec.expired());
}

// ============================================================================
// Integration Tests
// ============================================================================

class BufferLifetimeIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        manager = std::make_shared<BufferLifetimeManager>();
    }

    std::shared_ptr<BufferLifetimeManager> manager;
};

TEST_F(BufferLifetimeIntegrationTest, ManagerTracksAllBufferTypes) {
    // Create buffers of all types
    auto int64_1 = manager->create_int64_buffer(10);
    auto int64_2 = manager->create_int64_buffer(20);
    auto double_1 = manager->create_double_buffer(15);
    auto double_2 = manager->create_double_buffer(25);
    auto string_1 = manager->create_string_view_buffer(30);

    // Fill them with data
    int64_1->push_back(100);
    double_1->push_back(3.14);
    string_1->emplace_back("hello", 5);

    // Verify manager tracks all
    EXPECT_EQ(manager->buffer_count(), 5);

    // Verify memory calculation
    size_t expected = (10 * sizeof(int64_t)) +
                      (20 * sizeof(int64_t)) +
                      (15 * sizeof(double)) +
                      (25 * sizeof(double)) +
                      (30 * sizeof(std::string_view));
    EXPECT_EQ(manager->memory_usage(), expected);
}

TEST_F(BufferLifetimeIntegrationTest, LargeBufferHandling) {
    // Create a large buffer
    const size_t large_count = 1000000;
    auto large_vec = manager->create_int64_buffer(large_count);

    // Populate it
    for (size_t i = 0; i < 1000; ++i) {
        large_vec->push_back(i * 2);
    }

    EXPECT_EQ(large_vec->size(), 1000);
    EXPECT_EQ(manager->buffer_count(), 1);

    // Memory usage should be substantial
    size_t expected = large_count * sizeof(int64_t);
    EXPECT_EQ(manager->memory_usage(), expected);
}

} // namespace tpch
