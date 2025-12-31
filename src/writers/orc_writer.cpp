#include <iostream>
#include <sstream>
#include <memory>
#include <stdexcept>

#include <arrow/api.h>
#include <orc/OrcFile.hh>

#include "tpch/orc_writer.hpp"

namespace tpch {

namespace {

/**
 * Convert Arrow type to ORC type string.
 * Returns the ORC type definition string suitable for ORC schema creation.
 */
std::string arrow_type_to_orc_type_name(const std::shared_ptr<arrow::DataType>& type) {
    if (type->id() == arrow::Type::INT64) {
        return "bigint";
    } else if (type->id() == arrow::Type::INT32) {
        return "int";
    } else if (type->id() == arrow::Type::FLOAT) {
        return "float";
    } else if (type->id() == arrow::Type::DOUBLE) {
        return "double";
    } else if (type->id() == arrow::Type::STRING) {
        return "string";
    } else {
        throw std::runtime_error("Unsupported Arrow type for ORC conversion");
    }
}

/**
 * Build ORC schema string from Arrow schema.
 * Returns a string like: "struct<col1:bigint,col2:double,col3:string>"
 */
std::string build_orc_schema_string(const std::shared_ptr<arrow::Schema>& schema) {
    std::ostringstream oss;
    oss << "struct<";

    for (int i = 0; i < schema->num_fields(); ++i) {
        if (i > 0) oss << ",";

        auto field = schema->field(i);
        oss << field->name() << ":" << arrow_type_to_orc_type_name(field->type());
    }

    oss << ">";
    return oss.str();
}

/**
 * Copy Arrow array to ORC ColumnVectorBatch column.
 * Handles type conversions and null values.
 */
void copy_array_to_orc_column(
    orc::ColumnVectorBatch* col_batch,
    const std::shared_ptr<arrow::Array>& array) {

    size_t size = static_cast<size_t>(array->length());
    col_batch->numElements = size;

    if (array->type()->id() == arrow::Type::INT64) {
        auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);
        auto* long_col = dynamic_cast<orc::LongVectorBatch*>(col_batch);
        if (!long_col) {
            throw std::runtime_error("Failed to cast ORC column to LongVectorBatch");
        }
        for (size_t i = 0; i < size; ++i) {
            if (int_array->IsNull(static_cast<int64_t>(i))) {
                long_col->notNull[i] = 0;
            } else {
                long_col->notNull[i] = 1;
                long_col->data[i] = int_array->Value(static_cast<int64_t>(i));
            }
        }
    } else if (array->type()->id() == arrow::Type::INT32) {
        auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
        auto* long_col = dynamic_cast<orc::IntVectorBatch*>(col_batch);
        if (!long_col) {
            throw std::runtime_error("Failed to cast ORC column to IntVectorBatch");
        }
        for (size_t i = 0; i < size; ++i) {
            if (int_array->IsNull(static_cast<int64_t>(i))) {
                long_col->notNull[i] = 0;
            } else {
                long_col->notNull[i] = 1;
                long_col->data[i] = int_array->Value(static_cast<int64_t>(i));
            }
        }
    } else if (array->type()->id() == arrow::Type::DOUBLE) {
        auto double_array = std::static_pointer_cast<arrow::DoubleArray>(array);
        auto* double_col = dynamic_cast<orc::DoubleVectorBatch*>(col_batch);
        if (!double_col) {
            throw std::runtime_error("Failed to cast ORC column to DoubleVectorBatch");
        }
        for (size_t i = 0; i < size; ++i) {
            if (double_array->IsNull(static_cast<int64_t>(i))) {
                double_col->notNull[i] = 0;
            } else {
                double_col->notNull[i] = 1;
                double_col->data[i] = double_array->Value(static_cast<int64_t>(i));
            }
        }
    } else if (array->type()->id() == arrow::Type::FLOAT) {
        auto float_array = std::static_pointer_cast<arrow::FloatArray>(array);
        auto* double_col = dynamic_cast<orc::DoubleVectorBatch*>(col_batch);
        if (!double_col) {
            throw std::runtime_error("Failed to cast ORC column to DoubleVectorBatch");
        }
        for (size_t i = 0; i < size; ++i) {
            if (float_array->IsNull(static_cast<int64_t>(i))) {
                double_col->notNull[i] = 0;
            } else {
                double_col->notNull[i] = 1;
                double_col->data[i] = float_array->Value(static_cast<int64_t>(i));
            }
        }
    } else if (array->type()->id() == arrow::Type::STRING) {
        auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
        auto* string_col = dynamic_cast<orc::StringVectorBatch*>(col_batch);
        if (!string_col) {
            throw std::runtime_error("Failed to cast ORC column to StringVectorBatch");
        }
        for (size_t i = 0; i < size; ++i) {
            if (string_array->IsNull(static_cast<int64_t>(i))) {
                string_col->notNull[i] = 0;
            } else {
                string_col->notNull[i] = 1;
                auto str = string_array->GetString(static_cast<int64_t>(i));
                string_col->data[i] = const_cast<char*>(str.data());
                string_col->length[i] = static_cast<int64_t>(str.length());
            }
        }
    } else {
        throw std::runtime_error("Unsupported Arrow type for ORC column copy");
    }
}

}  // anonymous namespace

ORCWriter::ORCWriter(const std::string& filepath)
    : filepath_(filepath), orc_writer_(nullptr) {
    // Constructor doesn't create writer yet - we wait for first batch to get schema
}

ORCWriter::~ORCWriter() {
    try {
        close();
    } catch (...) {
        // Suppress exceptions in destructor
    }
}

void ORCWriter::write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || batch->num_rows() == 0) {
        return;
    }

    // Lock schema on first batch
    if (!schema_locked_) {
        first_batch_ = batch;
        schema_locked_ = true;

        auto schema = batch->schema();
        std::string orc_schema_str = build_orc_schema_string(schema);

        try {
            // Create ORC type from schema string
            auto orc_type = orc::Type::buildTypeFromString(orc_schema_str);

            // Create output file stream using ORC factory function
            auto out_stream = orc::writeLocalFile(filepath_);

            // Create writer options
            orc::WriterOptions writer_options;
            writer_options.setStripeSize(64 * 1024 * 1024);  // 64MB stripes
            writer_options.setRowIndexStride(10000);

            // Create ORC writer using factory function
            auto writer = orc::createWriter(*orc_type, out_stream.get(), writer_options);
            orc_writer_ = writer.release();

        } catch (const std::exception& e) {
            schema_locked_ = false;
            throw std::runtime_error(std::string("Failed to create ORC writer: ") + e.what());
        }
    }

    // Verify schema matches
    if (batch->schema()->field_names() != first_batch_->schema()->field_names()) {
        throw std::runtime_error("Schema mismatch: batch schema does not match first batch schema");
    }

    // Write batch to ORC
    try {
        auto* writer = reinterpret_cast<orc::Writer*>(orc_writer_);
        auto schema = batch->schema();
        int num_cols = batch->num_columns();
        int64_t num_rows = batch->num_rows();

        // Create root ColumnVectorBatch (ORC manages memory via MemoryPool)
        auto root_batch = writer->createRowBatch(static_cast<uint64_t>(num_rows));

        // root_batch should be a StructVectorBatch for the row data
        auto* struct_batch = dynamic_cast<orc::StructVectorBatch*>(root_batch.get());
        if (!struct_batch) {
            throw std::runtime_error("Root batch is not a StructVectorBatch");
        }

        // Copy data from Arrow columns to ORC columns
        for (int col_idx = 0; col_idx < num_cols; ++col_idx) {
            auto col_array = batch->column(col_idx);
            auto* orc_col = struct_batch->fields[col_idx];
            copy_array_to_orc_column(orc_col, col_array);
        }

        // Write the batch
        writer->add(*root_batch);

    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to write ORC batch: ") + e.what());
    }
}

void ORCWriter::close() {
    if (orc_writer_) {
        try {
            auto* writer = reinterpret_cast<orc::Writer*>(orc_writer_);
            writer->close();
            delete writer;
            orc_writer_ = nullptr;
        } catch (const std::exception& e) {
            std::cerr << "Error closing ORC writer: " << e.what() << std::endl;
        }
    }
}

}  // namespace tpch
