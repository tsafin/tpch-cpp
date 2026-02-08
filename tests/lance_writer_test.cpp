#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <fstream>
#include <filesystem>
#include <memory>

#include "tpch/lance_writer.hpp"
#include <arrow/api.h>
#include <arrow/record_batch.h>

namespace tpch {
namespace test {

namespace fs = std::filesystem;

// =====================================================================
// LanceWriterBasicTest: Unit tests for Lance writer initialization
// =====================================================================

class LanceWriterBasicTest : public ::testing::Test {
protected:
    std::string temp_dir;

    void SetUp() override {
        temp_dir = (fs::temp_directory_path() / "lance_writer_test").string();
        fs::remove_all(temp_dir);  // Clean before test
        fs::create_directories(temp_dir);
    }

    void TearDown() override {
        fs::remove_all(temp_dir);  // Clean after test
    }

    std::shared_ptr<arrow::RecordBatch> create_int_batch(int num_rows) {
        // Create a simple schema: int64 id
        auto field_id = arrow::field("id", arrow::int64());
        auto schema = arrow::schema({field_id});

        arrow::Int64Builder id_builder;
        for (int i = 0; i < num_rows; ++i) {
            id_builder.Append(static_cast<int64_t>(1000 + i));
        }

        std::shared_ptr<arrow::Array> id_array;
        id_builder.Finish(&id_array);

        auto batch = arrow::RecordBatch::Make(schema, num_rows, {id_array});
        return batch;
    }

    std::shared_ptr<arrow::RecordBatch> create_string_batch(int num_rows) {
        // Create schema with string field
        auto field_name = arrow::field("name", arrow::utf8());
        auto schema = arrow::schema({field_name});

        arrow::StringBuilder name_builder;
        for (int i = 0; i < num_rows; ++i) {
            name_builder.Append("name_" + std::to_string(i));
        }

        std::shared_ptr<arrow::Array> name_array;
        name_builder.Finish(&name_array);

        auto batch = arrow::RecordBatch::Make(schema, num_rows, {name_array});
        return batch;
    }

    std::shared_ptr<arrow::RecordBatch> create_mixed_batch(int num_rows) {
        // Create schema with multiple types: int64, float64, string
        auto field_id = arrow::field("id", arrow::int64());
        auto field_value = arrow::field("value", arrow::float64());
        auto field_name = arrow::field("name", arrow::utf8());
        auto schema = arrow::schema({field_id, field_value, field_name});

        arrow::Int64Builder id_builder;
        arrow::DoubleBuilder value_builder;
        arrow::StringBuilder name_builder;

        for (int i = 0; i < num_rows; ++i) {
            id_builder.Append(static_cast<int64_t>(1000 + i));
            value_builder.Append(static_cast<double>(i) * 1.5);
            name_builder.Append("item_" + std::to_string(i));
        }

        std::shared_ptr<arrow::Array> id_array;
        std::shared_ptr<arrow::Array> value_array;
        std::shared_ptr<arrow::Array> name_array;
        id_builder.Finish(&id_array);
        value_builder.Finish(&value_array);
        name_builder.Finish(&name_array);

        auto batch = arrow::RecordBatch::Make(schema, num_rows, {id_array, value_array, name_array});
        return batch;
    }

    std::shared_ptr<arrow::RecordBatch> create_batch_with_nulls(int num_rows) {
        // Create schema with nullable fields
        auto field_id = arrow::field("id", arrow::int64(), true);
        auto field_name = arrow::field("name", arrow::utf8(), true);
        auto schema = arrow::schema({field_id, field_name});

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

        auto batch = arrow::RecordBatch::Make(schema, num_rows, {id_array, name_array});
        return batch;
    }
};

TEST_F(LanceWriterBasicTest, ConstructorCreatesPath) {
    std::string dataset_path = temp_dir + "/test.lance";
    LanceWriter writer(dataset_path, "test_dataset");
    EXPECT_EQ(writer.dataset_path_, dataset_path);
}

TEST_F(LanceWriterBasicTest, PathAppendsSuffixIfMissing) {
    std::string dataset_path = temp_dir + "/test";
    LanceWriter writer(dataset_path, "test_dataset");
    // Constructor should append .lance
    EXPECT_EQ(writer.dataset_path_, dataset_path + ".lance");
}

TEST_F(LanceWriterBasicTest, PathPreservesExistingSuffix) {
    std::string dataset_path = temp_dir + "/test.lance";
    LanceWriter writer(dataset_path, "test_dataset");
    EXPECT_EQ(writer.dataset_path_, dataset_path);
}

// =====================================================================
// LanceWriterIntegrationTest: End-to-end Lance dataset creation
// =====================================================================

class LanceWriterIntegrationTest : public ::testing::Test {
protected:
    std::string temp_dir;

    void SetUp() override {
        temp_dir = (fs::temp_directory_path() / "lance_integration_test").string();
        fs::remove_all(temp_dir);
        fs::create_directories(temp_dir);
    }

    void TearDown() override {
        fs::remove_all(temp_dir);
    }

    std::shared_ptr<arrow::RecordBatch> create_test_batch(int num_rows) {
        auto field_id = arrow::field("id", arrow::int64());
        auto field_name = arrow::field("name", arrow::utf8());
        auto schema = arrow::schema({field_id, field_name});

        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;

        for (int i = 0; i < num_rows; ++i) {
            id_builder.Append(static_cast<int64_t>(1000 + i));
            name_builder.Append("name_" + std::to_string(i));
        }

        std::shared_ptr<arrow::Array> id_array;
        std::shared_ptr<arrow::Array> name_array;
        id_builder.Finish(&id_array);
        name_builder.Finish(&name_array);

        auto batch = arrow::RecordBatch::Make(schema, num_rows, {id_array, name_array});
        return batch;
    }

    std::shared_ptr<arrow::RecordBatch> create_int_batch(int num_rows) {
        auto field_id = arrow::field("id", arrow::int64());
        auto schema = arrow::schema({field_id});

        arrow::Int64Builder id_builder;
        for (int i = 0; i < num_rows; ++i) {
            id_builder.Append(static_cast<int64_t>(1000 + i));
        }

        std::shared_ptr<arrow::Array> id_array;
        id_builder.Finish(&id_array);

        auto batch = arrow::RecordBatch::Make(schema, num_rows, {id_array});
        return batch;
    }
};

TEST_F(LanceWriterIntegrationTest, DirectoryStructure) {
    std::string dataset_path = temp_dir + "/test.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    auto batch = create_test_batch(10);
    writer.write_batch(batch);
    writer.close();

    // Verify directory structure
    EXPECT_TRUE(fs::exists(dataset_path));
    EXPECT_TRUE(fs::is_directory(dataset_path));
}

TEST_F(LanceWriterIntegrationTest, SingleBatchWrite) {
    std::string dataset_path = temp_dir + "/test.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    auto batch = create_test_batch(100);
    writer.write_batch(batch);
    writer.close();

    // Verify files were created
    EXPECT_TRUE(fs::exists(dataset_path));

    // Check for Lance data files
    bool found_data_files = false;
    if (fs::exists(dataset_path)) {
        for (const auto& entry : fs::recursive_directory_iterator(dataset_path)) {
            if (entry.path().extension() == ".lance") {
                found_data_files = true;
                break;
            }
        }
    }
    EXPECT_TRUE(found_data_files);
}

TEST_F(LanceWriterIntegrationTest, MultipleBatchesAccumulate) {
    std::string dataset_path = temp_dir + "/test.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    // Write multiple batches
    auto batch1 = create_test_batch(50);
    auto batch2 = create_test_batch(50);
    auto batch3 = create_test_batch(25);

    writer.write_batch(batch1);
    writer.write_batch(batch2);
    writer.write_batch(batch3);
    writer.close();

    // Verify dataset was created
    EXPECT_TRUE(fs::exists(dataset_path));
    EXPECT_TRUE(fs::is_directory(dataset_path));
}

TEST_F(LanceWriterIntegrationTest, EmptyBatchIgnored) {
    std::string dataset_path = temp_dir + "/test.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    auto batch = create_test_batch(10);
    writer.write_batch(batch);

    // Create an empty batch
    auto empty_batch = arrow::RecordBatch::Make(
        batch->schema(), 0, std::vector<std::shared_ptr<arrow::Array>>{}
    );
    EXPECT_NO_THROW(writer.write_batch(empty_batch));

    writer.close();
    EXPECT_TRUE(fs::exists(dataset_path));
}

TEST_F(LanceWriterIntegrationTest, NullBatch) {
    std::string dataset_path = temp_dir + "/test.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    EXPECT_THROW({
        std::shared_ptr<arrow::RecordBatch> null_batch = nullptr;
        writer.write_batch(null_batch);
    }, std::runtime_error);
}

TEST_F(LanceWriterIntegrationTest, SchemaLockedAfterFirstBatch) {
    std::string dataset_path = temp_dir + "/test.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    auto batch1 = create_test_batch(10);
    writer.write_batch(batch1);

    // Schema should be locked
    EXPECT_TRUE(writer.schema_locked_);

    // Second batch with same schema should succeed
    auto batch2 = create_test_batch(10);
    EXPECT_NO_THROW(writer.write_batch(batch2));

    writer.close();
}

TEST_F(LanceWriterIntegrationTest, SchemaMismatchDetected) {
    std::string dataset_path = temp_dir + "/test.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    // Write first batch to lock schema
    auto batch1 = create_test_batch(10);
    writer.write_batch(batch1);

    // Try to write batch with different schema
    auto batch2 = create_int_batch(10);
    EXPECT_THROW({
        writer.write_batch(batch2);
    }, std::runtime_error);
}

TEST_F(LanceWriterIntegrationTest, NullableFieldsHandled) {
    std::string dataset_path = temp_dir + "/test.lance";
    LanceWriter writer(dataset_path, "test_dataset");

    // Create batch with nullable fields
    auto field_id = arrow::field("id", arrow::int64(), true);
    auto field_name = arrow::field("name", arrow::utf8(), true);
    auto schema = arrow::schema({field_id, field_name});

    arrow::Int64Builder id_builder;
    arrow::StringBuilder name_builder;

    for (int i = 0; i < 10; ++i) {
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

    auto batch = arrow::RecordBatch::Make(schema, 10, {id_array, name_array});

    // Should not throw
    EXPECT_NO_THROW({
        writer.write_batch(batch);
        writer.close();
    });

    EXPECT_TRUE(fs::exists(dataset_path));
}

TEST_F(LanceWriterIntegrationTest, DestructorClosesGracefully) {
    {
        std::string dataset_path = temp_dir + "/test.lance";
        LanceWriter writer(dataset_path, "test_dataset");

        auto batch = create_test_batch(10);
        writer.write_batch(batch);

        // Destructor should close writer without exceptions
    }

    // If we get here without crash, test passes
    EXPECT_TRUE(fs::exists(temp_dir + "/test.lance"));
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

TEST_F(LanceWriterDataTypeTest, Int32Support) {
    std::string dataset_path = temp_dir + "/test_int32.lance";
    LanceWriter writer(dataset_path);

    auto field = arrow::field("value", arrow::int32());
    auto schema = arrow::schema({field});

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

TEST_F(LanceWriterDataTypeTest, Int64Support) {
    std::string dataset_path = temp_dir + "/test_int64.lance";
    LanceWriter writer(dataset_path);

    auto field = arrow::field("value", arrow::int64());
    auto schema = arrow::schema({field});

    arrow::Int64Builder builder;
    for (int i = 0; i < 10; ++i) {
        builder.Append(static_cast<int64_t>(i) * 1000000000);
    }
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);

    auto batch = arrow::RecordBatch::Make(schema, 10, {array});

    EXPECT_NO_THROW({
        writer.write_batch(batch);
        writer.close();
    });
}

TEST_F(LanceWriterDataTypeTest, Float32Support) {
    std::string dataset_path = temp_dir + "/test_float32.lance";
    LanceWriter writer(dataset_path);

    auto field = arrow::field("value", arrow::float32());
    auto schema = arrow::schema({field});

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

TEST_F(LanceWriterDataTypeTest, Float64Support) {
    std::string dataset_path = temp_dir + "/test_float64.lance";
    LanceWriter writer(dataset_path);

    auto field = arrow::field("value", arrow::float64());
    auto schema = arrow::schema({field});

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

TEST_F(LanceWriterDataTypeTest, Utf8Support) {
    std::string dataset_path = temp_dir + "/test_utf8.lance";
    LanceWriter writer(dataset_path);

    auto field = arrow::field("value", arrow::utf8());
    auto schema = arrow::schema({field});

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

TEST_F(LanceWriterDataTypeTest, Date32Support) {
    std::string dataset_path = temp_dir + "/test_date32.lance";
    LanceWriter writer(dataset_path);

    auto field = arrow::field("date", arrow::date32());
    auto schema = arrow::schema({field});

    arrow::Date32Builder builder;
    for (int i = 0; i < 10; ++i) {
        builder.Append(18000 + i);  // Days since epoch
    }
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);

    auto batch = arrow::RecordBatch::Make(schema, 10, {array});

    EXPECT_NO_THROW({
        writer.write_batch(batch);
        writer.close();
    });
}

TEST_F(LanceWriterDataTypeTest, BooleanSupport) {
    std::string dataset_path = temp_dir + "/test_bool.lance";
    LanceWriter writer(dataset_path);

    auto field = arrow::field("flag", arrow::boolean());
    auto schema = arrow::schema({field});

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

}  // namespace test
}  // namespace tpch
