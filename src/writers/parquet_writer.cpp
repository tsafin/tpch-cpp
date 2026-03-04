#include "tpch/parquet_writer.hpp"
#include "tpch/async_io.hpp"
#include "tpch/performance_counters.hpp"

#include <iostream>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <filesystem>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#ifdef TPCH_ENABLE_ASYNC_IO

namespace tpch {

/**
 * Arrow OutputStream backed by io_uring for async disk writes.
 *
 * Matches the Rust io_uring_store design:
 *   - 512 KB chunks (same as Rust CHUNK_SIZE)
 *   - 8-buffer circular ring: up to 7 SQEs in-flight while filling the 8th
 *   - submit_and_wait(1) in the drain path: single syscall instead of two
 *   - sysfs-calibrated queue depth (nr_requests/2, clamped [8,128])
 *   - IORING_SETUP_ATTACH_WQ shared worker pool (via AsyncIOContext ctor)
 */
class IoUringOutputStream : public arrow::io::OutputStream {
    // 512 KB per SQE — matches Rust CHUNK_SIZE
    static constexpr size_t kChunkSize = 512 * 1024;
    // Circular pool of staging buffers.
    // At steady state kNumBufs-1 = 7 SQEs are in-flight while we fill the 8th.
    static constexpr int kNumBufs = 8;

    std::string filepath_;
    int fd_ = -1;
    int64_t file_offset_ = 0;
    bool closed_ = false;

    std::array<std::vector<uint8_t>, kNumBufs> pool_;
    int write_idx_ = 0;  // slot currently being filled
    int read_idx_  = 0;  // oldest submitted slot (FIFO order)

    std::unique_ptr<AsyncIOContext> context_;

public:
    explicit IoUringOutputStream(const std::string& filepath)
        : filepath_(filepath) {
        fd_ = ::open(filepath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) {
            throw std::runtime_error("IoUringOutputStream: open failed: " + std::string(strerror(errno)));
        }
        // Calibrate queue depth from the target block device via sysfs.
        uint32_t qd = AsyncIOContext::calibrate_queue_depth(filepath.c_str());
        context_ = std::make_unique<AsyncIOContext>(qd);
        for (auto& b : pool_) b.reserve(kChunkSize);
    }

    ~IoUringOutputStream() override {
        if (!closed_) {
            try { Close(); } catch (...) {}
        }
    }

    arrow::Status Write(const void* data, int64_t nbytes) override {
        const uint8_t* ptr = static_cast<const uint8_t*>(data);
        while (nbytes > 0) {
            auto& cur = pool_[write_idx_];
            size_t space   = kChunkSize - cur.size();
            size_t to_copy = std::min(static_cast<size_t>(nbytes), space);
            cur.insert(cur.end(), ptr, ptr + to_copy);
            ptr    += to_copy;
            nbytes -= static_cast<int64_t>(to_copy);
            if (cur.size() >= kChunkSize) {
                ARROW_RETURN_NOT_OK(submit_head());
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Flush() override {
        // Submit any partial buffer.
        if (!pool_[write_idx_].empty()) {
            ARROW_RETURN_NOT_OK(submit_head());
        }
        // Drain all remaining in-flight SQEs.
        if (context_->queued_count() > 0) {
            context_->submit_queued();
        }
        while (context_->pending_count() > 0) {
            context_->wait_completions(1);
            pool_[read_idx_].clear();
            read_idx_ = (read_idx_ + 1) % kNumBufs;
        }
        return arrow::Status::OK();
    }

    arrow::Status Close() override {
        if (closed_) return arrow::Status::OK();
        ARROW_RETURN_NOT_OK(Flush());
        ::close(fd_);
        fd_ = -1;
        closed_ = true;
        return arrow::Status::OK();
    }

    bool closed() const override { return closed_; }

    arrow::Result<int64_t> Tell() const override {
        return file_offset_ + static_cast<int64_t>(pool_[write_idx_].size());
    }

private:
    // Submit pool_[write_idx_] to io_uring, then advance write_idx_.
    //
    // Invariant: (write_idx_ - read_idx_ + kNumBufs) % kNumBufs == pending_count()
    // When the pool is full (pending == kNumBufs-1), drain the oldest slot first
    // using submit_and_wait(1) — one syscall for both submit and wait.
    arrow::Status submit_head() {
        auto& cur = pool_[write_idx_];

        context_->queue_write(fd_, cur.data(), cur.size(), file_offset_, 0);
        file_offset_ += static_cast<int64_t>(cur.size());

        if (context_->pending_count() + 1 >= kNumBufs) {
            // Pool full: submit new SQE + wait for oldest completion in 1 syscall.
            context_->submit_and_wait(1);
            pool_[read_idx_].clear();
            read_idx_ = (read_idx_ + 1) % kNumBufs;
        } else {
            context_->submit_queued();
        }

        write_idx_ = (write_idx_ + 1) % kNumBufs;
        return arrow::Status::OK();
    }
};

}  // namespace tpch

#endif  // TPCH_ENABLE_ASYNC_IO

namespace tpch {

ParquetWriter::ParquetWriter(
    const std::string& filepath,
    arrow::MemoryPool* memory_pool,
    int64_t estimated_rows)
    : filepath_(filepath)
    , async_context_(nullptr)
    , async_buffer_(nullptr)
    , async_fd_(-1)
    , closed_(false)
    , memory_pool_(memory_pool ? memory_pool : arrow::default_memory_pool())
    , estimated_rows_(estimated_rows) {

    // Create parent directory if it doesn't exist
    std::filesystem::path file_path(filepath);
    std::filesystem::path parent_dir = file_path.parent_path();

    if (!parent_dir.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(parent_dir, ec);
        if (ec) {
            throw std::runtime_error("Failed to create parent directory: " + parent_dir.string() + " (" + ec.message() + ")");
        }
    }

    // Pre-allocate batches vector if we have an estimate
    if (estimated_rows_ > 0) {
        // Reserve space for batches (assume 10k rows per batch)
        size_t estimated_batches = (estimated_rows_ + 9999) / 10000;
        batches_.reserve(estimated_batches);
    }
}

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

    if (streaming_mode_) {
        // Streaming mode: Write batch immediately (NO ACCUMULATION!)
        // Lazy initialization of FileWriter on first batch
        if (!parquet_file_writer_) {
            init_file_writer();
        }

        // Write batch immediately
        auto status = parquet_file_writer_->WriteRecordBatch(*batch);
        if (!status.ok()) {
            throw std::runtime_error("Failed to write RecordBatch: " + status.message());
        }

        // Batch is discarded after write (no memory accumulation)
    } else {
        // Batch accumulation mode (original behavior)
        batches_.push_back(batch);
    }
}

void ParquetWriter::set_async_context(std::shared_ptr<AsyncIOContext> async_context) {
    async_context_ = async_context;
}

void ParquetWriter::enable_streaming_write(bool use_threads) {
    if (!batches_.empty()) {
        throw std::runtime_error("Cannot enable streaming mode after batches have been written");
    }
    streaming_mode_ = true;
    use_threads_ = use_threads;
}

void ParquetWriter::enable_io_uring(bool enable) {
    use_io_uring_ = enable;
}

void ParquetWriter::write_managed_batch(const ManagedRecordBatch& managed_batch) {
    if (closed_) {
        throw std::runtime_error("Cannot write to a closed Parquet writer");
    }

    if (!managed_batch.batch) {
        throw std::runtime_error("Cannot write null batch");
    }

    if (managed_batch.batch->num_rows() == 0) {
        return;  // Skip empty batches
    }

    // Store the first batch to infer schema
    if (!first_batch_) {
        first_batch_ = managed_batch.batch;
    }

    if (streaming_mode_) {
        // Streaming mode: Write batch immediately
        // Lifetime manager is freed after this function returns (optimal memory usage)
        if (!parquet_file_writer_) {
            init_file_writer();
        }

        // Write batch immediately
        auto status = parquet_file_writer_->WriteRecordBatch(*managed_batch.batch);
        if (!status.ok()) {
            throw std::runtime_error("Failed to write RecordBatch: " + status.message());
        }

        // After this function returns, managed_batch goes out of scope
        // → lifetime_mgr refcount drops to 0
        // → vectors freed automatically
        // This is safe because Parquet encoding already completed!
    } else {
        // Batch accumulation mode: Store managed batch
        // This keeps the lifetime manager alive until close() is called
        managed_batches_.push_back(managed_batch);
    }
}

void ParquetWriter::init_file_writer() {
    if (parquet_file_writer_) {
        return;  // Already initialized
    }

    if (!first_batch_) {
        throw std::runtime_error("Cannot initialize Parquet writer without schema (no batches written yet)");
    }

    // Configure Parquet writer properties
    auto writer_props = parquet::WriterProperties::Builder()
        .compression(parquet::Compression::SNAPPY)
        ->build();

    auto arrow_props = parquet::ArrowWriterProperties::Builder()
        .set_use_threads(use_threads_)
        ->build();

    // Create output stream
    std::shared_ptr<arrow::io::OutputStream> outfile;
#ifdef TPCH_ENABLE_ASYNC_IO
    if (use_io_uring_) {
        outfile = std::make_shared<IoUringOutputStream>(filepath_);
    } else {
#endif
        auto outfile_result = arrow::io::FileOutputStream::Open(filepath_);
        if (!outfile_result.ok()) {
            throw std::runtime_error("Failed to open file: " + outfile_result.status().message());
        }
        outfile = outfile_result.ValueOrDie();
#ifdef TPCH_ENABLE_ASYNC_IO
    }
#endif

    // Create FileWriter for streaming RecordBatches
    auto writer_result = parquet::arrow::FileWriter::Open(
        *first_batch_->schema(),
        memory_pool_,
        outfile,
        writer_props,
        arrow_props
    );

    if (!writer_result.ok()) {
        throw std::runtime_error("Failed to create Parquet FileWriter: " + writer_result.status().message());
    }

    parquet_file_writer_ = std::move(writer_result.ValueOrDie());
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
        if (streaming_mode_) {
            // Streaming mode: All batches already written, just close the writer
            if (parquet_file_writer_) {
                TPCH_SCOPED_TIMER("parquet_close_streaming");
                auto status = parquet_file_writer_->Close();
                if (!status.ok()) {
                    throw std::runtime_error("Failed to close Parquet writer: " + status.message());
                }
                parquet_file_writer_.reset();
            }
        } else if (async_context_) {
            // Async batch mode: Write all batches to memory buffer, then async write to disk

            // Create buffer output stream
            auto buffer_stream_result = arrow::io::BufferOutputStream::Create();
            if (!buffer_stream_result.ok()) {
                throw std::runtime_error("Failed to create buffer stream: " + buffer_stream_result.status().message());
            }
            auto buffer_stream = buffer_stream_result.ValueOrDie();

            // Phase 14.2: Use FileWriter + WriteRecordBatch instead of Table + WriteTable
            // This eliminates the Table construction copy!
            {
                TPCH_SCOPED_TIMER("parquet_encode_batches");

                // Configure Parquet writer properties
                auto writer_props = parquet::WriterProperties::Builder()
                    .compression(parquet::Compression::SNAPPY)
                    ->build();

                auto arrow_props = parquet::ArrowWriterProperties::Builder()
                    .set_use_threads(use_threads_)
                    ->build();

                // Create FileWriter
                auto writer_result = parquet::arrow::FileWriter::Open(
                    *first_batch_->schema(),
                    memory_pool_,
                    buffer_stream,
                    writer_props,
                    arrow_props
                );

                if (!writer_result.ok()) {
                    throw std::runtime_error("Failed to create Parquet FileWriter: " + writer_result.status().message());
                }
                auto writer = std::move(writer_result.ValueOrDie());

                // Write each RecordBatch directly (NO TABLE CONSTRUCTION!)
                for (const auto& batch : batches_) {
                    auto status = writer->WriteRecordBatch(*batch);
                    if (!status.ok()) {
                        throw std::runtime_error("Failed to write RecordBatch: " + status.message());
                    }
                }

                // Also write managed batches (Phase 14.2.3)
                for (const auto& managed_batch : managed_batches_) {
                    auto status = writer->WriteRecordBatch(*managed_batch.batch);
                    if (!status.ok()) {
                        throw std::runtime_error("Failed to write ManagedRecordBatch: " + status.message());
                    }
                }

                // Close writer
                auto close_status = writer->Close();
                if (!close_status.ok()) {
                    throw std::runtime_error("Failed to close Parquet writer: " + close_status.message());
                }
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
            {
                TPCH_SCOPED_TIMER("parquet_async_write");
                async_context_->queue_write(fd, buffer->data(), buffer->size(), 0, 0);
                async_context_->submit_queued();

                // Wait for all async operations to complete before returning
                async_context_->flush();
            }

            // Now safe to close the file descriptor
            if (async_fd_ >= 0) {
                ::close(async_fd_);
                async_fd_ = -1;
            }

            // Clear buffer reference
            async_buffer_.reset();
        } else {
            // Synchronous batch mode: Write all batches directly
            // Phase 14.2: Use FileWriter + WriteRecordBatch instead of Table + WriteTable
            // This eliminates the Table construction copy!

            TPCH_SCOPED_TIMER("parquet_encode_sync");

            // Configure Parquet writer properties
            auto writer_props = parquet::WriterProperties::Builder()
                .compression(parquet::Compression::SNAPPY)
                ->build();

            auto arrow_props = parquet::ArrowWriterProperties::Builder()
                .set_use_threads(use_threads_)
                ->build();

            // Create output stream
            auto outfile_result = arrow::io::FileOutputStream::Open(filepath_);
            if (!outfile_result.ok()) {
                throw std::runtime_error("Failed to open file: " + outfile_result.status().message());
            }
            auto outfile = outfile_result.ValueOrDie();

            // Create FileWriter
            auto writer_result = parquet::arrow::FileWriter::Open(
                *first_batch_->schema(),
                memory_pool_,
                outfile,
                writer_props,
                arrow_props
            );

            if (!writer_result.ok()) {
                throw std::runtime_error("Failed to create Parquet FileWriter: " + writer_result.status().message());
            }
            auto writer = std::move(writer_result.ValueOrDie());

            // Write each RecordBatch directly (NO TABLE CONSTRUCTION!)
            for (const auto& batch : batches_) {
                auto status = writer->WriteRecordBatch(*batch);
                if (!status.ok()) {
                    throw std::runtime_error("Failed to write RecordBatch: " + status.message());
                }
            }

            // Also write managed batches (Phase 14.2.3)
            for (const auto& managed_batch : managed_batches_) {
                auto status = writer->WriteRecordBatch(*managed_batch.batch);
                if (!status.ok()) {
                    throw std::runtime_error("Failed to write ManagedRecordBatch: " + status.message());
                }
            }

            // Close writer
            auto close_status = writer->Close();
            if (!close_status.ok()) {
                throw std::runtime_error("Failed to close Parquet writer: " + close_status.message());
            }
        }

        closed_ = true;
        batches_.clear();  // Free memory
        managed_batches_.clear();  // Free managed batches and lifetime managers (Phase 14.2.3)
    } catch (const std::exception& e) {
        closed_ = true;
        throw;
    }
}

}  // namespace tpch
