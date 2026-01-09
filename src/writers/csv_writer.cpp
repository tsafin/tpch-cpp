#include "tpch/csv_writer.hpp"
#include "tpch/async_io.hpp"

#include <iostream>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

#include <arrow/api.h>
#include <arrow/csv/api.h>

namespace tpch {

CSVWriter::CSVWriter(const std::string& filepath)
    : filepath_(filepath) {
  // Use ONLY raw fd for ALL writes (async and sync)
  file_descriptor_ = ::open(filepath.c_str(),
                           O_WRONLY | O_CREAT | O_TRUNC,
                           0644);
  if (file_descriptor_ < 0) {
    throw std::runtime_error("Failed to open: " + filepath);
  }

  // Pre-allocate buffer pool
  for (auto& buf : buffer_pool_) {
    buf.reserve(BUFFER_SIZE);
  }
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

size_t CSVWriter::acquire_buffer() {
  // Find free buffer, or wait for completion if all in-flight
  for (size_t i = 0; i < NUM_BUFFERS; ++i) {
    if (!buffer_in_flight_[i]) {
      buffer_in_flight_[i] = true;
      return i;
    }
  }
  // All buffers in-flight - wait for at least one completion
  wait_for_completion();
  return acquire_buffer();  // Retry
}

void CSVWriter::wait_for_completion() {
  if (!async_context_) return;
  async_context_->wait_completions(1);
  // Process completions - need user_data to know which buffer (see Phase 11.2)
  // For now, we rely on external completion handling via callbacks
}

void CSVWriter::release_buffer(size_t idx) {
  if (idx < NUM_BUFFERS) {
    buffer_in_flight_[idx] = false;
    buffer_pool_[idx].clear();
  }
}

void CSVWriter::write_data(const void* data, size_t size) {
  const uint8_t* byte_data = static_cast<const uint8_t*>(data);
  size_t written = 0;

  while (written < size) {
    auto& buffer = buffer_pool_[current_buffer_idx_];
    size_t available = BUFFER_SIZE - buffer_fill_size_;
    size_t to_write = std::min(available, size - written);

    // Add data to current buffer
    buffer.insert(buffer.end(),
                 byte_data + written,
                 byte_data + written + to_write);
    buffer_fill_size_ += to_write;
    written += to_write;

    // Submit if buffer is full
    if (buffer_fill_size_ >= BUFFER_SIZE) {
      flush_buffer();
    }
  }
}

void CSVWriter::flush_buffer() {
  if (buffer_fill_size_ == 0) return;

  size_t buf_idx = current_buffer_idx_;
  auto& buffer = buffer_pool_[buf_idx];

  if (!async_context_) {
    // Use pwrite for sync (NOT ofstream)
    ssize_t written = ::pwrite(file_descriptor_,
                              buffer.data(),
                              buffer_fill_size_,
                              current_offset_);
    if (written < 0) {
      throw std::runtime_error("Write failed: " + std::string(strerror(errno)));
    }
    current_offset_ += written;
    buffer.clear();
    buffer_fill_size_ = 0;
    return;
  }

  // Submit async write - buffer stays valid until completion
  async_context_->submit_write(file_descriptor_,
                              buffer.data(),
                              buffer_fill_size_,
                              current_offset_);
  current_offset_ += buffer_fill_size_;

  // Get next buffer (don't clear current - it's in-flight!)
  current_buffer_idx_ = acquire_buffer();
  buffer_fill_size_ = 0;
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
  // Flush current buffer if has data
  if (buffer_fill_size_ > 0) {
    flush_buffer();
  }

  // Wait for ALL in-flight operations
  if (async_context_) {
    async_context_->flush();
  }

  // Sync file data to disk
  if (file_descriptor_ >= 0) {
    ::fsync(file_descriptor_);
    ::close(file_descriptor_);
    file_descriptor_ = -1;
  }
}

}  // namespace tpch
