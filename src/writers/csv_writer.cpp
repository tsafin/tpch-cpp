#include "tpch/csv_writer.hpp"
#include "tpch/async_io.hpp"

#include <iostream>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>

#include <arrow/api.h>
#include <arrow/csv/api.h>

namespace tpch {

CSVWriter::CSVWriter(const std::string& filepath)
    : filepath_(filepath), output_(filepath) {
  if (!output_.is_open()) {
    throw std::runtime_error("Failed to open CSV output file: " + filepath);
  }

  // Get file descriptor for async I/O operations
  file_descriptor_ = ::open(filepath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (file_descriptor_ < 0) {
    throw std::runtime_error("Failed to open file descriptor for: " + filepath);
  }

  // Pre-allocate buffer for async writes
  write_buffer_.reserve(BUFFER_SIZE);
}

CSVWriter::~CSVWriter() {
  try {
    close();
  } catch (...) {
    // Suppress exceptions in destructor
  }
  if (file_descriptor_ >= 0) {
    ::close(file_descriptor_);
  }
}

void CSVWriter::set_async_context(std::shared_ptr<AsyncIOContext> context) {
  async_context_ = context;
}

void CSVWriter::write_data(const void* data, size_t size) {
  if (!async_context_) {
    // Synchronous write to ofstream
    output_.write(static_cast<const char*>(data), size);
    return;
  }

  // Async writes: buffer data and submit when full
  const uint8_t* byte_data = static_cast<const uint8_t*>(data);
  size_t written = 0;

  while (written < size) {
    size_t available = BUFFER_SIZE - write_buffer_.size();
    size_t to_write = std::min(available, size - written);

    // Add data to buffer
    write_buffer_.insert(write_buffer_.end(),
                        byte_data + written,
                        byte_data + written + to_write);
    written += to_write;

    // Submit if buffer is full
    if (write_buffer_.size() >= BUFFER_SIZE) {
      flush_buffer();
    }
  }
}

void CSVWriter::flush_buffer() {
  if (write_buffer_.empty()) {
    return;
  }

  if (!async_context_) {
    // Synchronous flush
    output_.write(reinterpret_cast<const char*>(write_buffer_.data()),
                 write_buffer_.size());
    write_buffer_.clear();
    return;
  }

  // Async flush: submit write to io_uring
  try {
    async_context_->submit_write(file_descriptor_,
                                write_buffer_.data(),
                                write_buffer_.size(),
                                0);  // offset is managed by O_APPEND or tracked
    write_buffer_.clear();
  } catch (const std::exception& e) {
    throw std::runtime_error("Async write failed: " + std::string(e.what()));
  }
}

void CSVWriter::write_header(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto schema = batch->schema();
  std::ostringstream oss;

  for (int i = 0; i < schema->num_fields(); ++i) {
    if (i > 0) oss << ",";
    oss << schema->field(i)->name();
  }
  oss << "\n";

  std::string header = oss.str();
  write_data(header.data(), header.size());
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
  if (file_descriptor_ < 0) {
    throw std::runtime_error("CSV output file is not open");
  }

  // Write header on first batch
  if (!header_written_) {
    write_header(batch);
    header_written_ = true;
  }

  auto schema = batch->schema();
  int64_t num_rows = batch->num_rows();
  std::ostringstream row_buffer;

  // Write each row
  for (int64_t row = 0; row < num_rows; ++row) {
    for (int col = 0; col < batch->num_columns(); ++col) {
      if (col > 0) row_buffer << ",";

      auto array = batch->column(col);
      if (array->IsNull(row)) {
        // Write empty string for null values
      } else {
        auto field_type = schema->field(col)->type();

        if (field_type->id() == arrow::Type::INT64) {
          auto int_array =
              std::dynamic_pointer_cast<arrow::Int64Array>(array);
          row_buffer << int_array->Value(row);
        } else if (field_type->id() == arrow::Type::DOUBLE) {
          auto double_array =
              std::dynamic_pointer_cast<arrow::DoubleArray>(array);
          row_buffer << double_array->Value(row);
        } else if (field_type->id() == arrow::Type::STRING) {
          auto string_array =
              std::dynamic_pointer_cast<arrow::StringArray>(array);
          auto str = string_array->GetString(row);
          row_buffer << escape_csv_value(str);
        } else if (field_type->id() == arrow::Type::INT32) {
          auto int_array =
              std::dynamic_pointer_cast<arrow::Int32Array>(array);
          row_buffer << int_array->Value(row);
        } else if (field_type->id() == arrow::Type::FLOAT) {
          auto float_array =
              std::dynamic_pointer_cast<arrow::FloatArray>(array);
          row_buffer << float_array->Value(row);
        } else {
          // Fallback: convert to string using Arrow's ToString
          row_buffer << array->ToString();
        }
      }
    }
    row_buffer << "\n";
  }

  std::string row_str = row_buffer.str();
  write_data(row_str.data(), row_str.size());
}

void CSVWriter::close() {
  // Flush any remaining buffered data
  flush_buffer();

  // Wait for all async operations to complete
  if (async_context_) {
    async_context_->flush();
  }

  if (output_.is_open()) {
    output_.flush();
    output_.close();
  }
}

}  // namespace tpch
