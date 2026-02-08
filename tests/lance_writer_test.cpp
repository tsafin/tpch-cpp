#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <filesystem>
#include <memory>

#include "tpch/lance_writer.hpp"
#include <arrow/api.h>
#include <arrow/record_batch.h>

namespace tpch {

namespace fs = std::filesystem;

// =====================================================================
// LanceWriterIntegrationTest: End-to-end Lance dataset creation
// =====================================================================

class LanceWriterIntegrationTest : public ::testing::Test {
protected:
    std::string temp_dir;

    void SetUp() override {
        temp_dir = (fs::temp_directory_path() / "lance_writer_test").string();
        fs::remove_all(temp_dir);
        fs::create_directories(temp_dir);
    }

    void TearDown() override {
        fs::remove_all(temp_dir);
    }

    std::shared_ptr<arrow::RecordBatch> create_simple_batch(int num_rows) {
        auto schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8())
        });

        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;

        for (int i = 0; i < num_rows; ++i) {
            id_builder.Append(static_cast<int64_t>(1000 + i));
            name_builder.Append("record_" + std::to_string(i));
        }

        std::shared_ptr<arrow::Array> id_array;
        std::shared_ptr<arrow::Array> name_array;
        id_builder.Finish(&id_array);
        name_builder.Finish(&name_array);

        return arrow::RecordBatch::Make(schema, num_rows, {id_array, name_array});
    }

    std::shared_ptr<arrow::RecordBatch> create_int_only_batch(int num_rows) {
        auto schema = arrow::schema({arrow::field("value", arrow::int64())});

        arrow::Int64Builder builder;
        for (int i = 0; i < num_rows; ++i) {
            builder.Append(static_cast<int64_t>(i * 100));
        }

        std::shared_ptr<arrow::Array> array;
        builder.Finish(&array);
        return arrow::RecordBatch::Make(schema, num_rows, {array});
    }

    std::shared_ptr<arrow::RecordBatch> create_nullable_batch(int num_rows) {
        auto schema = arrow::schema({
            arrow::field("id", arrow::int64(), true),
            arrow::field("name", arrow::utf8(), true)
        });

        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;

        for (int i = 0; i < num_rows; ++i) {
            if (i % 3 == 0) {
                id_builder.AppendNull();
            } else {
                id_builder.Append(static_cast<int64_t>(1000 + i));
            }

            if (i % 2 == 0) {
                name_builder.AppendNull();
            } else {
                name_builder.Append("name_" + std::to_string(i));
            }
        }

        std::shared_ptr<arrow::Array> id_array;
        std::shared_ptr<arrow::Array> name_array;
        id_builder.Finish(&id_array);
        name_builder.Finish(&name_array);

        return arrow::RecordBatch::Make(schema, num_rows, {id_array, name_array});
    }
};

TEST_F(LanceWriterIntegrationTest, SingleBatchWrite) {
    std::string dataset_path = temp_dir + "/test.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    auto batch = create_simple_batch(50);
    writer.write_batch(batch);
    writer.close();

    EXPECT_TRUE(fs::exists(dataset_path));
    EXPECT_TRUE(fs::is_directory(dataset_path));
}

TEST_F(LanceWriterIntegrationTest, MultipleBatches) {
    std::string dataset_path = temp_dir + "/multi.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    auto batch1 = create_simple_batch(25);
    auto batch2 = create_simple_batch(25);

    writer.write_batch(batch1);
    writer.write_batch(batch2);
    writer.close();

    EXPECT_TRUE(fs::exists(dataset_path));
}

TEST_F(LanceWriterIntegrationTest, EmptyBatchIgnored) {
    std::string dataset_path = temp_dir + "/empty.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    auto batch = create_simple_batch(10);
    writer.write_batch(batch);

    // Empty batch should be ignored without throwing
    auto schema = batch->schema();
    auto empty_batch = arrow::RecordBatch::Make(schema, 0,
        std::vector<std::shared_ptr<arrow::Array>>(schema->num_fields()));

    EXPECT_NO_THROW(writer.write_batch(empty_batch));
    writer.close();

    EXPECT_TRUE(fs::exists(dataset_path));
}

TEST_F(LanceWriterIntegrationTest, NullBatchThrows) {
    std::string dataset_path = temp_dir + "/null.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    std::shared_ptr<arrow::RecordBatch> null_batch = nullptr;
    EXPECT_THROW(writer.write_batch(null_batch), std::runtime_error);
}

TEST_F(LanceWriterIntegrationTest, SchemaMismatchThrows) {
    std::string dataset_path = temp_dir + "/mismatch.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    // Write first batch to lock schema
    auto batch1 = create_simple_batch(10);
    writer.write_batch(batch1);

    // Try different schema
    auto batch2 = create_int_only_batch(10);
    EXPECT_THROW(writer.write_batch(batch2), std::runtime_error);
}

TEST_F(LanceWriterIntegrationTest, NullableFieldsSupported) {
    std::string dataset_path = temp_dir + "/nullable.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    auto batch = create_nullable_batch(20);
    EXPECT_NO_THROW({
        writer.write_batch(batch);
        writer.close();
    });

    EXPECT_TRUE(fs::exists(dataset_path));
}

TEST_F(LanceWriterIntegrationTest, DestructorHandlesClose) {
    std::string dataset_path = temp_dir + "/dtor.lance";
    {
        LanceWriter writer(dataset_path, "test_dataset");
        auto batch = create_simple_batch(10);
        writer.write_batch(batch);
        // Destructor should close without throwing
    }

    EXPECT_TRUE(fs::exists(dataset_path));
}

// =====================================================================
// LanceWriterDataTypeTest: Verify support for various Arrow data types
// =====================================================================

class LanceWriterDataTypeTest : public ::testing::Test {
protected:
    std::string temp_dir;

    void SetUp() override {
        temp_dir = (fs::temp_directory_path() / "lance_datatype_test").string();
        fs::remove_all(temp_dir);
        fs::create_directories(temp_dir);
    }

    void TearDown() override {
        fs::remove_all(temp_dir);
    }
};

TEST_F(LanceWriterDataTypeTest, Int32) {
    std::string dataset_path = temp_dir + "/int32.lance";
    LanceWriter writer(dataset_path);

    auto schema = arrow::schema({arrow::field("value", arrow::int32())});
    arrow::Int32Builder builder;
    for (int i = 0; i < 10; ++i) {
        builder.Append(i * 100);
    }
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    auto batch = arrow::RecordBatch::Make(schema, 10, {array});

    EXPECT_NO_THROW({
        writer.write_batch(batch);
        writer.close();
    });
}

TEST_F(LanceWriterDataTypeTest, Int64) {
    std::string dataset_path = temp_dir + "/int64.lance";
    LanceWriter writer(dataset_path);

    auto schema = arrow::schema({arrow::field("value", arrow::int64())});
    arrow::Int64Builder builder;
    for (int i = 0; i < 10; ++i) {
        builder.Append(static_cast<int64_t>(i) * 1000000000LL);
    }
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    auto batch = arrow::RecordBatch::Make(schema, 10, {array});

    EXPECT_NO_THROW({
        writer.write_batch(batch);
        writer.close();
    });
}

TEST_F(LanceWriterDataTypeTest, Float32) {
    std::string dataset_path = temp_dir + "/float32.lance";
    LanceWriter writer(dataset_path);

    auto schema = arrow::schema({arrow::field("value", arrow::float32())});
    arrow::FloatBuilder builder;
    for (int i = 0; i < 10; ++i) {
        builder.Append(static_cast<float>(i) * 3.14f);
    }
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    auto batch = arrow::RecordBatch::Make(schema, 10, {array});

    EXPECT_NO_THROW({
        writer.write_batch(batch);
        writer.close();
    });
}

TEST_F(LanceWriterDataTypeTest, Float64) {
    std::string dataset_path = temp_dir + "/float64.lance";
    LanceWriter writer(dataset_path);

    auto schema = arrow::schema({arrow::field("value", arrow::float64())});
    arrow::DoubleBuilder builder;
    for (int i = 0; i < 10; ++i) {
        builder.Append(static_cast<double>(i) * 3.14159);
    }
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    auto batch = arrow::RecordBatch::Make(schema, 10, {array});

    EXPECT_NO_THROW({
        writer.write_batch(batch);
        writer.close();
    });
}

TEST_F(LanceWriterDataTypeTest, Utf8String) {
    std::string dataset_path = temp_dir + "/utf8.lance";
    LanceWriter writer(dataset_path);

    auto schema = arrow::schema({arrow::field("text", arrow::utf8())});
    arrow::StringBuilder builder;
    for (int i = 0; i < 10; ++i) {
        builder.Append("string_" + std::to_string(i));
    }
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    auto batch = arrow::RecordBatch::Make(schema, 10, {array});

    EXPECT_NO_THROW({
        writer.write_batch(batch);
        writer.close();
    });
}

TEST_F(LanceWriterDataTypeTest, Date32) {
    std::string dataset_path = temp_dir + "/date32.lance";
    LanceWriter writer(dataset_path);

    auto schema = arrow::schema({arrow::field("date", arrow::date32())});
    arrow::Date32Builder builder;
    for (int i = 0; i < 10; ++i) {
        builder.Append(18000 + i);
    }
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    auto batch = arrow::RecordBatch::Make(schema, 10, {array});

    EXPECT_NO_THROW({
        writer.write_batch(batch);
        writer.close();
    });
}

TEST_F(LanceWriterDataTypeTest, Boolean) {
    std::string dataset_path = temp_dir + "/bool.lance";
    LanceWriter writer(dataset_path);

    auto schema = arrow::schema({arrow::field("flag", arrow::boolean())});
    arrow::BooleanBuilder builder;
    for (int i = 0; i < 10; ++i) {
        builder.Append(i % 2 == 0);
    }
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    auto batch = arrow::RecordBatch::Make(schema, 10, {array});

    EXPECT_NO_THROW({
        writer.write_batch(batch);
        writer.close();
    });
}

}  // namespace tpch
