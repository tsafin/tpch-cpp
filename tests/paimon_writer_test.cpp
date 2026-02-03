#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <fstream>
#include <filesystem>
#include <cstring>
#include <limits>

#include "tpch/avro_writer.hpp"
#include "tpch/paimon_writer.hpp"
#include <arrow/api.h>

namespace tpch {
namespace test {

namespace fs = std::filesystem;

// =====================================================================
// AvroEncodingTest: Unit tests for Avro encoding primitives
// =====================================================================

class AvroEncodingTest : public ::testing::Test {
protected:
    std::vector<uint8_t> buf;

    void SetUp() override {
        buf.clear();
    }
};

TEST_F(AvroEncodingTest, ZigzagLongZero) {
    avro_detail::write_zigzag_long(buf, 0);
    ASSERT_EQ(buf.size(), 1);
    EXPECT_EQ(buf[0], 0x00);
}

TEST_F(AvroEncodingTest, ZigzagLongOne) {
    avro_detail::write_zigzag_long(buf, 1);
    ASSERT_EQ(buf.size(), 1);
    EXPECT_EQ(buf[0], 0x02);
}

TEST_F(AvroEncodingTest, ZigzagLongMinusOne) {
    avro_detail::write_zigzag_long(buf, -1);
    ASSERT_EQ(buf.size(), 1);
    EXPECT_EQ(buf[0], 0x01);
}

TEST_F(AvroEncodingTest, ZigzagLongLargePositive) {
    // 300: zigzag = (300 << 1) ^ 0 = 600 = 0x258
    // varint(600): 600 & 0x7F = 88 (0x58), with continuation bit = 0xD8
    //              600 >> 7 = 4
    // Encoded: 0xD8, 0x04
    avro_detail::write_zigzag_long(buf, 300);
    ASSERT_EQ(buf.size(), 2);
    EXPECT_EQ(buf[0], 0xD8);
    EXPECT_EQ(buf[1], 0x04);
}

TEST_F(AvroEncodingTest, ZigzagLongMax) {
    // INT64_MAX: very large zigzag value, should encode to 10 bytes
    avro_detail::write_zigzag_long(buf, std::numeric_limits<int64_t>::max());
    EXPECT_EQ(buf.size(), 10);
}

TEST_F(AvroEncodingTest, ZigzagIntZero) {
    avro_detail::write_zigzag_int(buf, 0);
    ASSERT_EQ(buf.size(), 1);
    EXPECT_EQ(buf[0], 0x00);
}

TEST_F(AvroEncodingTest, ZigzagIntOne) {
    avro_detail::write_zigzag_int(buf, 1);
    ASSERT_EQ(buf.size(), 1);
    EXPECT_EQ(buf[0], 0x02);
}

TEST_F(AvroEncodingTest, AvroStringHello) {
    avro_detail::write_avro_string(buf, "hello");
    // Length of "hello" is 5, zigzag(5) = 10 = 0x0A
    // Then 5 bytes: h, e, l, l, o
    ASSERT_GE(buf.size(), 6);
    EXPECT_EQ(buf[0], 0x0A);
    EXPECT_EQ(buf[1], 'h');
    EXPECT_EQ(buf[2], 'e');
    EXPECT_EQ(buf[3], 'l');
    EXPECT_EQ(buf[4], 'l');
    EXPECT_EQ(buf[5], 'o');
}

TEST_F(AvroEncodingTest, AvroStringEmpty) {
    avro_detail::write_avro_string(buf, "");
    ASSERT_EQ(buf.size(), 1);
    EXPECT_EQ(buf[0], 0x00);  // Length 0, zigzag(0) = 0
}

TEST_F(AvroEncodingTest, AvroBytes) {
    const uint8_t data[] = {0xAA, 0xBB, 0xCC, 0xDD};
    avro_detail::write_avro_bytes(buf, data, 4);
    // Length 4, zigzag(4) = 8 = 0x08
    ASSERT_EQ(buf.size(), 5);
    EXPECT_EQ(buf[0], 0x08);
    EXPECT_EQ(buf[1], 0xAA);
    EXPECT_EQ(buf[2], 0xBB);
    EXPECT_EQ(buf[3], 0xCC);
    EXPECT_EQ(buf[4], 0xDD);
}

TEST_F(AvroEncodingTest, UnionNull) {
    avro_detail::write_union_null(buf);
    ASSERT_EQ(buf.size(), 1);
    EXPECT_EQ(buf[0], 0x00);
}

TEST_F(AvroEncodingTest, UnionIndex) {
    avro_detail::write_union_index(buf, 1);
    // zigzag(1) = 2
    ASSERT_EQ(buf.size(), 1);
    EXPECT_EQ(buf[0], 0x02);
}

// =====================================================================
// AvroFileWriterTest: Tests for Avro file container structure
// =====================================================================

class AvroFileWriterTest : public ::testing::Test {
protected:
    std::string temp_dir;

    void SetUp() override {
        temp_dir = fs::temp_directory_path() / "paimon_avro_test";
        fs::create_directories(temp_dir);
    }

    void TearDown() override {
        fs::remove_all(temp_dir);
    }

    std::vector<uint8_t> read_file(const std::string& path) {
        std::ifstream file(path, std::ios::binary);
        return std::vector<uint8_t>(
            (std::istreambuf_iterator<char>(file)),
            std::istreambuf_iterator<char>()
        );
    }
};

TEST_F(AvroFileWriterTest, MagicBytesPresent) {
    std::string schema = R"({"type":"record","name":"Test","fields":[]})";
    AvroFileWriter writer(schema);
    std::string output_path = temp_dir + "/test.avro";
    writer.finish(output_path);

    auto content = read_file(output_path);
    ASSERT_GE(content.size(), 4);
    EXPECT_EQ(content[0], 'O');
    EXPECT_EQ(content[1], 'b');
    EXPECT_EQ(content[2], 'j');
    EXPECT_EQ(content[3], 0x01);
}

TEST_F(AvroFileWriterTest, MetadataContainsSchema) {
    std::string schema = R"({"type":"record","name":"Test","fields":[]})";
    AvroFileWriter writer(schema);
    std::string output_path = temp_dir + "/test.avro";
    writer.finish(output_path);

    auto content = read_file(output_path);
    // Convert to string for searching
    std::string content_str(content.begin(), content.end());
    // The schema string should appear somewhere in the file
    EXPECT_NE(content_str.find("avro.schema"), std::string::npos);
}

TEST_F(AvroFileWriterTest, ZeroRecords) {
    std::string schema = R"({"type":"record","name":"Test","fields":[]})";
    AvroFileWriter writer(schema);
    EXPECT_EQ(writer.record_count(), 0);

    std::string output_path = temp_dir + "/zero.avro";
    writer.finish(output_path);

    auto content = read_file(output_path);
    EXPECT_GE(content.size(), 20);  // At least header + metadata
}

TEST_F(AvroFileWriterTest, SingleRecord) {
    std::string schema = R"({"type":"record","name":"Test","fields":[]})";
    AvroFileWriter writer(schema);

    std::vector<uint8_t> record = {0x01, 0x02, 0x03};
    writer.append_record(record);
    EXPECT_EQ(writer.record_count(), 1);

    std::string output_path = temp_dir + "/single.avro";
    writer.finish(output_path);

    auto content = read_file(output_path);
    EXPECT_GE(content.size(), 20);  // Header + metadata + record + sync
}

TEST_F(AvroFileWriterTest, MultipleRecords) {
    std::string schema = R"({"type":"record","name":"Test","fields":[]})";
    AvroFileWriter writer(schema);

    for (int i = 0; i < 5; ++i) {
        std::vector<uint8_t> record(i + 1, static_cast<uint8_t>(i));
        writer.append_record(record);
    }
    EXPECT_EQ(writer.record_count(), 5);

    std::string output_path = temp_dir + "/multi.avro";
    writer.finish(output_path);

    auto content = read_file(output_path);
    EXPECT_GE(content.size(), 50);  // Should be larger for multiple records
}

// =====================================================================
// PaimonWriterIntegrationTest: End-to-end Paimon table creation
// =====================================================================

class PaimonWriterIntegrationTest : public ::testing::Test {
protected:
    std::string temp_table_dir;

    void SetUp() override {
        temp_table_dir = fs::temp_directory_path() / "paimon_integration_test";
        fs::remove_all(temp_table_dir);  // Clean before test
    }

    void TearDown() override {
        fs::remove_all(temp_table_dir);  // Clean after test
    }

    std::shared_ptr<arrow::RecordBatch> create_test_batch(int num_rows) {
        // Create a simple schema: int64 id + string name
        auto field_id = arrow::field("id", arrow::int64());
        auto field_name = arrow::field("name", arrow::utf8());
        auto schema = arrow::schema({field_id, field_name});

        // Create arrays
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
};

TEST_F(PaimonWriterIntegrationTest, TableDirectoryStructure) {
    PaimonWriter writer(temp_table_dir, "test_table");
    auto batch = create_test_batch(10);
    writer.write_batch(batch);
    writer.close();

    // Verify directory structure
    EXPECT_TRUE(fs::is_directory(temp_table_dir + "/snapshot"));
    EXPECT_TRUE(fs::is_directory(temp_table_dir + "/manifest"));
    EXPECT_TRUE(fs::is_directory(temp_table_dir + "/bucket-0"));
    EXPECT_TRUE(fs::is_directory(temp_table_dir + "/schema"));
}

TEST_F(PaimonWriterIntegrationTest, OptionsFileExists) {
    PaimonWriter writer(temp_table_dir, "test_table");
    auto batch = create_test_batch(10);
    writer.write_batch(batch);
    writer.close();

    std::string options_path = temp_table_dir + "/OPTIONS";
    EXPECT_TRUE(fs::exists(options_path));

    std::ifstream options_file(options_path);
    std::string content((std::istreambuf_iterator<char>(options_file)),
                        std::istreambuf_iterator<char>());
    EXPECT_NE(content.find("table.type=APPEND_ONLY"), std::string::npos);
    EXPECT_NE(content.find("data-files.format=parquet"), std::string::npos);
}

TEST_F(PaimonWriterIntegrationTest, SchemaFileExists) {
    PaimonWriter writer(temp_table_dir, "test_table");
    auto batch = create_test_batch(10);
    writer.write_batch(batch);
    writer.close();

    std::string schema_path = temp_table_dir + "/schema/schema-0";
    EXPECT_TRUE(fs::exists(schema_path));

    std::ifstream schema_file(schema_path);
    std::string content((std::istreambuf_iterator<char>(schema_file)),
                        std::istreambuf_iterator<char>());
    // Should be valid JSON with field definitions
    EXPECT_NE(content.find("\"fields\""), std::string::npos);
    EXPECT_NE(content.find("\"id\""), std::string::npos);
    EXPECT_NE(content.find("\"name\""), std::string::npos);
}

TEST_F(PaimonWriterIntegrationTest, SnapshotHints) {
    PaimonWriter writer(temp_table_dir, "test_table");
    auto batch = create_test_batch(10);
    writer.write_batch(batch);
    writer.close();

    std::string earliest_path = temp_table_dir + "/snapshot/EARLIEST";
    std::string latest_path = temp_table_dir + "/snapshot/LATEST";

    EXPECT_TRUE(fs::exists(earliest_path));
    EXPECT_TRUE(fs::exists(latest_path));

    std::ifstream earliest_file(earliest_path);
    std::string earliest_content((std::istreambuf_iterator<char>(earliest_file)),
                                 std::istreambuf_iterator<char>());
    EXPECT_EQ(earliest_content, "1");

    std::ifstream latest_file(latest_path);
    std::string latest_content((std::istreambuf_iterator<char>(latest_file)),
                               std::istreambuf_iterator<char>());
    EXPECT_EQ(latest_content, "1");
}

TEST_F(PaimonWriterIntegrationTest, SnapshotMetadata) {
    PaimonWriter writer(temp_table_dir, "test_table");
    auto batch = create_test_batch(10);
    writer.write_batch(batch);
    writer.close();

    std::string snapshot_path = temp_table_dir + "/snapshot/snapshot-1";
    EXPECT_TRUE(fs::exists(snapshot_path));

    std::ifstream snapshot_file(snapshot_path);
    std::string content((std::istreambuf_iterator<char>(snapshot_file)),
                        std::istreambuf_iterator<char>());

    // Verify all 17 required fields are present
    EXPECT_NE(content.find("\"version\": 3"), std::string::npos);
    EXPECT_NE(content.find("\"id\": 1"), std::string::npos);
    EXPECT_NE(content.find("\"schemaId\""), std::string::npos);
    EXPECT_NE(content.find("\"commitUser\""), std::string::npos);
    EXPECT_NE(content.find("\"commitIdentifier\""), std::string::npos);
    EXPECT_NE(content.find("\"commitKind\": \"APPEND\""), std::string::npos);
    EXPECT_NE(content.find("\"timeMillis\""), std::string::npos);
    EXPECT_NE(content.find("\"totalRecordCount\": 10"), std::string::npos);
    EXPECT_NE(content.find("\"deltaRecordCount\": 10"), std::string::npos);
    EXPECT_NE(content.find("\"changelogRecordCount\": 0"), std::string::npos);
    EXPECT_NE(content.find("\"watermark\""), std::string::npos);
}

TEST_F(PaimonWriterIntegrationTest, DataFilesInBucket) {
    PaimonWriter writer(temp_table_dir, "test_table");
    auto batch = create_test_batch(10);
    writer.write_batch(batch);
    writer.close();

    // Check that at least one data file exists
    std::string bucket_dir = temp_table_dir + "/bucket-0";
    EXPECT_TRUE(fs::is_directory(bucket_dir));

    int parquet_count = 0;
    for (const auto& entry : fs::directory_iterator(bucket_dir)) {
        if (entry.path().extension() == ".parquet") {
            parquet_count++;
        }
    }
    EXPECT_GT(parquet_count, 0);
}

TEST_F(PaimonWriterIntegrationTest, ManifestFilesExist) {
    PaimonWriter writer(temp_table_dir, "test_table");
    auto batch = create_test_batch(10);
    writer.write_batch(batch);
    writer.close();

    std::string manifest_dir = temp_table_dir + "/manifest";
    EXPECT_TRUE(fs::is_directory(manifest_dir));

    // Count manifest files
    int manifest_count = 0;
    int manifest_list_count = 0;
    for (const auto& entry : fs::directory_iterator(manifest_dir)) {
        std::string filename = entry.path().filename().string();
        if (filename.find("manifest-") == 0 && filename.find("manifest-list") == std::string::npos) {
            manifest_count++;
        }
        if (filename.find("manifest-list-") == 0) {
            manifest_list_count++;
        }
    }

    EXPECT_EQ(manifest_count, 1);
    EXPECT_EQ(manifest_list_count, 1);
}

TEST_F(PaimonWriterIntegrationTest, ManifestFilesAreAvro) {
    PaimonWriter writer(temp_table_dir, "test_table");
    auto batch = create_test_batch(10);
    writer.write_batch(batch);
    writer.close();

    // Read first manifest file and check for Avro magic bytes
    std::string manifest_dir = temp_table_dir + "/manifest";
    for (const auto& entry : fs::directory_iterator(manifest_dir)) {
        std::string filename = entry.path().filename().string();
        if (filename.find("manifest-") == 0 && filename.find("manifest-list") == std::string::npos) {
            std::ifstream file(entry.path(), std::ios::binary);
            char magic[4];
            file.read(magic, 4);
            EXPECT_EQ(magic[0], 'O');
            EXPECT_EQ(magic[1], 'b');
            EXPECT_EQ(magic[2], 'j');
            EXPECT_EQ(magic[3], 0x01);
            break;
        }
    }
}

TEST_F(PaimonWriterIntegrationTest, MultipleBatches) {
    PaimonWriter writer(temp_table_dir, "test_table");

    // Write multiple batches
    auto batch1 = create_test_batch(5);
    auto batch2 = create_test_batch(5);
    writer.write_batch(batch1);
    writer.write_batch(batch2);
    writer.close();

    // Verify total record count in snapshot
    std::string snapshot_path = temp_table_dir + "/snapshot/snapshot-1";
    std::ifstream snapshot_file(snapshot_path);
    std::string content((std::istreambuf_iterator<char>(snapshot_file)),
                        std::istreambuf_iterator<char>());
    EXPECT_NE(content.find("\"totalRecordCount\": 10"), std::string::npos);
}

}  // namespace test
}  // namespace tpch
