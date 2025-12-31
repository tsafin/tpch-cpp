#include "tpch/csv_writer.hpp"

#include <iostream>
#include <sstream>

#include <arrow/api.h>
#include <arrow/csv/api.h>

namespace tpch {

CSVWriter::CSVWriter(const std::string& filepath)
    : filepath_(filepath), output_(filepath) {
  if (!output_.is_open()) {
    throw std::runtime_error("Failed to open CSV output file: " + filepath);
  }
}

CSVWriter::~CSVWriter() { close(); }

void CSVWriter::write_header(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto schema = batch->schema();
  for (int i = 0; i < schema->num_fields(); ++i) {
    if (i > 0) output_ << ",";
    output_ << schema->field(i)->name();
  }
  output_ << "\n";
}

std::string CSVWriter::escape_csv_value(const std::string& value) {
  // Check if value needs quoting (contains comma, quote, or newline)
  bool needs_quoting = false;
  for (char c : value) {
    if (c == ',' || c == '"' || c == '\n' || c == '\r') {
      needs_quoting = true;
      break;
    }
  }

  if (!needs_quoting) {
    return value;
  }

  // Quote the value and escape internal quotes
  std::string escaped;
  escaped += '"';
  for (char c : value) {
    if (c == '"') {
      escaped += "\"\"";  // Escape quotes by doubling
    } else {
      escaped += c;
    }
  }
  escaped += '"';
  return escaped;
}

void CSVWriter::write_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  if (!output_.is_open()) {
    throw std::runtime_error(
        "CSV output file is not open");
  }

  // Write header on first batch
  if (!header_written_) {
    write_header(batch);
    header_written_ = true;
  }

  auto schema = batch->schema();
  int64_t num_rows = batch->num_rows();

  // Write each row
  for (int64_t row = 0; row < num_rows; ++row) {
    for (int col = 0; col < batch->num_columns(); ++col) {
      if (col > 0) output_ << ",";

      auto array = batch->column(col);
      if (array->IsNull(row)) {
        // Write empty string for null values
      } else {
        auto field_type = schema->field(col)->type();

        if (field_type->id() == arrow::Type::INT64) {
          auto int_array =
              std::dynamic_pointer_cast<arrow::Int64Array>(array);
          output_ << int_array->Value(row);
        } else if (field_type->id() == arrow::Type::DOUBLE) {
          auto double_array =
              std::dynamic_pointer_cast<arrow::DoubleArray>(array);
          output_ << double_array->Value(row);
        } else if (field_type->id() == arrow::Type::STRING) {
          auto string_array =
              std::dynamic_pointer_cast<arrow::StringArray>(array);
          auto str = string_array->GetString(row);
          output_ << escape_csv_value(str);
        } else if (field_type->id() == arrow::Type::INT32) {
          auto int_array =
              std::dynamic_pointer_cast<arrow::Int32Array>(array);
          output_ << int_array->Value(row);
        } else if (field_type->id() == arrow::Type::FLOAT) {
          auto float_array =
              std::dynamic_pointer_cast<arrow::FloatArray>(array);
          output_ << float_array->Value(row);
        } else {
          // Fallback: convert to string using Arrow's ToString
          output_ << array->ToString();
        }
      }
    }
    output_ << "\n";
  }
}

void CSVWriter::close() {
  if (output_.is_open()) {
    output_.flush();
    output_.close();
  }
}

}  // namespace tpch
