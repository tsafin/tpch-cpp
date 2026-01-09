#include "tpch/parquet_writer.hpp"
#include "tpch/async_io.hpp"

#include <iostream>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

namespace tpch {

ParquetWriter::ParquetWriter(const std::string& filepath)
    : filepath_(filepath), async_context_(nullptr), async_buffer_(nullptr), async_fd_(-1), closed_(false) {}

ParquetWriter::~ParquetWriter() {
    if (!closed_) {
        try {
            close();
        } catch (...) {
            // Suppress exceptions in destructor
        }
    }
    // Emergency cleanup of fd in case close() failed
    if (async_fd_ >= 0) {
        ::close(async_fd_);
        async_fd_ = -1;
    }
    // Clear buffer reference
    async_buffer_.reset();
}

void ParquetWriter::write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (closed_) {
        throw std::runtime_error("Cannot write to a closed Parquet writer");
    }

    if (!batch) {
        throw std::runtime_error("Cannot write null batch");
    }

    if (batch->num_rows() == 0) {
        return;  // Skip empty batches
    }

    // Store the first batch to infer schema
    if (!first_batch_) {
        first_batch_ = batch;
    }

    // Accumulate batches for later writing
    batches_.push_back(batch);
}

void ParquetWriter::set_async_context(std::shared_ptr<AsyncIOContext> async_context) {
    async_context_ = async_context;
}

void ParquetWriter::close() {
    if (closed_) {
        return;  // Already closed
    }

    if (!first_batch_) {
        // No data written, create empty file
        closed_ = true;
        return;
    }

    try {
        if (async_context_) {
            // Option B: Write Parquet to memory buffer, then async write to disk

            // Create buffer output stream
            auto buffer_stream_result = arrow::io::BufferOutputStream::Create();
            if (!buffer_stream_result.ok()) {
                throw std::runtime_error("Failed to create buffer stream: " + buffer_stream_result.status().message());
            }
            auto buffer_stream = buffer_stream_result.ValueOrDie();

            // Create table from batches
            auto table = arrow::Table::FromRecordBatches(batches_);
            if (!table.ok()) {
                throw std::runtime_error("Failed to create Arrow table: " + table.status().message());
            }

            // Write table to Parquet (in memory)
            auto status = parquet::arrow::WriteTable(
                **table,
                arrow::default_memory_pool(),
                buffer_stream,
                1024 * 1024  // 1MB row group size
            );

            if (!status.ok()) {
                throw std::runtime_error("Failed to write Parquet to buffer: " + status.message());
            }

            // Get the complete buffer
            auto buffer_result = buffer_stream->Finish();
            if (!buffer_result.ok()) {
                throw std::runtime_error("Failed to finish buffer: " + buffer_result.status().message());
            }
            auto buffer = buffer_result.ValueOrDie();

            // Store buffer to keep it alive during async I/O
            async_buffer_ = buffer;

            // Open file for async writing
            int fd = ::open(filepath_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd < 0) {
                throw std::runtime_error("Failed to open file for writing: " + std::string(strerror(errno)));
            }

            // Store fd for cleanup
            async_fd_ = fd;

            // Submit async write of the complete buffer
            async_context_->queue_write(fd, buffer->data(), buffer->size(), 0, 0);
            async_context_->submit_queued();

            // Wait for all async operations to complete before returning
            async_context_->flush();

            // Now safe to close the file descriptor
            if (async_fd_ >= 0) {
                ::close(async_fd_);
                async_fd_ = -1;
            }

            // Clear buffer reference
            async_buffer_.reset();
        } else {
            // Synchronous write (original behavior)
            auto outfile_result = arrow::io::FileOutputStream::Open(filepath_);
            if (!outfile_result.ok()) {
                throw std::runtime_error("Failed to open file: " + outfile_result.status().message());
            }
            auto outfile = outfile_result.ValueOrDie();

            // Create table from batches
            auto table = arrow::Table::FromRecordBatches(batches_);
            if (!table.ok()) {
                throw std::runtime_error("Failed to create Arrow table: " + table.status().message());
            }

            // Write table to Parquet
            auto status = parquet::arrow::WriteTable(
                **table,
                arrow::default_memory_pool(),
                outfile,
                1024 * 1024  // 1MB row group size
            );

            if (!status.ok()) {
                throw std::runtime_error("Failed to write Parquet: " + status.message());
            }
        }

        closed_ = true;
    } catch (const std::exception& e) {
        closed_ = true;
        throw;
    }
}

}  // namespace tpch
