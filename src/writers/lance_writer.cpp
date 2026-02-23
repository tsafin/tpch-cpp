#include "tpch/lance_writer.hpp"

#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <arrow/type.h>
#include <arrow/record_batch.h>
#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/c/abi.h>

namespace fs = std::filesystem;

namespace tpch {

struct StreamState {
    explicit StreamState(size_t max_queue_batches)
        : max_queue_batches_(max_queue_batches) {}

    void push(const std::shared_ptr<arrow::RecordBatch>& batch, int64_t batch_bytes) {
        std::unique_lock<std::mutex> lock(mu_);
        if (!closed_ && status_.ok() && queue_.size() >= max_queue_batches_) {
            auto wait_start = std::chrono::steady_clock::now();
            not_full_cv_.wait(lock, [&] {
                return closed_ || queue_.size() < max_queue_batches_ || !status_.ok();
            });
            auto wait_end = std::chrono::steady_clock::now();
            auto waited = std::chrono::duration_cast<std::chrono::nanoseconds>(wait_end - wait_start).count();
            stall_ns_.fetch_add(static_cast<uint64_t>(waited), std::memory_order_relaxed);
            stall_count_.fetch_add(1, std::memory_order_relaxed);
        } else {
            not_full_cv_.wait(lock, [&] {
                return closed_ || queue_.size() < max_queue_batches_ || !status_.ok();
            });
        }
        if (!status_.ok()) {
            return;
        }
        if (closed_) {
            return;
        }
        queue_.push_back({batch, batch_bytes});
        current_bytes_ += batch_bytes;
        if (current_bytes_ > peak_bytes_) {
            peak_bytes_ = current_bytes_;
        }
        not_empty_cv_.notify_one();
    }

    arrow::Status pop(std::shared_ptr<arrow::RecordBatch>* out) {
        std::unique_lock<std::mutex> lock(mu_);
        not_empty_cv_.wait(lock, [&] {
            return closed_ || !queue_.empty() || !status_.ok();
        });
        if (!status_.ok()) {
            return status_;
        }
        if (queue_.empty() && closed_) {
            *out = nullptr;
            return arrow::Status::OK();
        }
        *out = queue_.front().batch;
        current_bytes_ -= queue_.front().bytes;
        queue_.pop_front();
        not_full_cv_.notify_one();
        return arrow::Status::OK();
    }

    void close() {
        std::lock_guard<std::mutex> lock(mu_);
        closed_ = true;
        not_empty_cv_.notify_all();
        not_full_cv_.notify_all();
    }

    void set_error(const arrow::Status& status) {
        std::lock_guard<std::mutex> lock(mu_);
        status_ = status;
        not_empty_cv_.notify_all();
        not_full_cv_.notify_all();
    }

    std::pair<uint64_t, uint64_t> stall_stats() const {
        return {stall_ns_.load(std::memory_order_relaxed),
                stall_count_.load(std::memory_order_relaxed)};
    }

    size_t queue_size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return queue_.size();
    }

    int64_t current_bytes() const {
        std::lock_guard<std::mutex> lock(mu_);
        return current_bytes_;
    }

    int64_t peak_bytes() const {
        std::lock_guard<std::mutex> lock(mu_);
        return peak_bytes_;
    }

private:
    struct QueueItem {
        std::shared_ptr<arrow::RecordBatch> batch;
        int64_t bytes;
    };

    mutable std::mutex mu_;
    std::condition_variable not_empty_cv_;
    std::condition_variable not_full_cv_;
    std::deque<QueueItem> queue_;
    size_t max_queue_batches_;
    bool closed_ = false;
    arrow::Status status_ = arrow::Status::OK();
    std::atomic<uint64_t> stall_ns_{0};
    std::atomic<uint64_t> stall_count_{0};
    int64_t current_bytes_ = 0;
    int64_t peak_bytes_ = 0;
};

class StreamRecordBatchReader : public arrow::RecordBatchReader {
public:
    StreamRecordBatchReader(std::shared_ptr<arrow::Schema> schema,
                            std::shared_ptr<StreamState> state)
        : schema_(std::move(schema)),
          state_(std::move(state)) {}

    std::shared_ptr<arrow::Schema> schema() const override {
        return schema_;
    }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        return state_->pop(out);
    }

private:
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<StreamState> state_;
};

int64_t estimate_batch_bytes(const arrow::RecordBatch& batch) {
    int64_t total = 0;
    for (int i = 0; i < batch.num_columns(); ++i) {
        const auto& array = batch.column(i);
        if (!array) {
            continue;
        }
        const auto& data = array->data();
        if (!data) {
            continue;
        }
        for (const auto& buf : data->buffers) {
            if (buf) {
                total += static_cast<int64_t>(buf->size());
            }
        }
    }
    return total;
}

LanceWriter::LanceWriter(const std::string& dataset_path,
                         const std::string& dataset_name)
    : dataset_path_(dataset_path),
      dataset_name_(dataset_name) {
    // Ensure path ends with .lance
    const std::string suffix = ".lance";
    if (dataset_path_.length() < suffix.length() ||
        dataset_path_.substr(dataset_path_.length() - suffix.length()) != suffix) {
        dataset_path_ += ".lance";
    }
}

LanceWriter::~LanceWriter() {
    if (rust_writer_ != nullptr) {
        try {
            close();
        } catch (...) {
            // Suppress exceptions in destructor
        }
    }
}

void LanceWriter::set_write_params(int64_t max_rows_per_file,
                                   int64_t max_rows_per_group,
                                   int64_t max_bytes_per_file,
                                   bool skip_auto_cleanup) {
    max_rows_per_file_ = max_rows_per_file;
    max_rows_per_group_ = max_rows_per_group;
    max_bytes_per_file_ = max_bytes_per_file;
    skip_auto_cleanup_ = skip_auto_cleanup;

    if (rust_writer_ != nullptr) {
        int result = lance_writer_set_write_params(
            reinterpret_cast<::LanceWriter*>(rust_writer_),
            max_rows_per_file_,
            max_rows_per_group_,
            max_bytes_per_file_,
            skip_auto_cleanup_ ? 1 : 0);
        if (result != 0) {
            throw std::runtime_error("Failed to configure Lance write parameters");
        }
    }
}

void LanceWriter::initialize_lance_dataset(
    const std::shared_ptr<arrow::RecordBatch>& first_batch) {
    if (schema_locked_) {
        return;
    }

    schema_ = first_batch->schema();
    schema_locked_ = true;

    // Create dataset directory structure
    try {
        fs::create_directories(dataset_path_);
        fs::create_directories(dataset_path_ + "/data");
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to create dataset directory: " +
                                std::string(e.what()));
    }

    // Initialize Rust FFI writer
    auto* raw_writer = lance_writer_create(dataset_path_.c_str(), nullptr, streaming_enabled_ ? 1 : 0);
    rust_writer_ = reinterpret_cast<void*>(raw_writer);

    if (rust_writer_ == nullptr) {
        throw std::runtime_error("Failed to create Lance writer via FFI");
    }

    if (max_rows_per_file_ > 0 || max_rows_per_group_ > 0 || max_bytes_per_file_ > 0 || skip_auto_cleanup_) {
        int result = lance_writer_set_write_params(
            reinterpret_cast<::LanceWriter*>(rust_writer_),
            max_rows_per_file_,
            max_rows_per_group_,
            max_bytes_per_file_,
            skip_auto_cleanup_ ? 1 : 0);
        if (result != 0) {
            throw std::runtime_error("Failed to configure Lance write parameters");
        }
    }

    if (streaming_enabled_) {
        auto state = std::make_shared<StreamState>(stream_queue_depth_);
        auto reader = std::make_shared<StreamRecordBatchReader>(schema_, state);
        auto* stream = reinterpret_cast<ArrowArrayStream*>(std::malloc(sizeof(ArrowArrayStream)));
        if (stream == nullptr) {
            throw std::runtime_error("Failed to allocate ArrowArrayStream");
        }
        std::memset(stream, 0, sizeof(ArrowArrayStream));
        auto status = arrow::ExportRecordBatchReader(reader, stream);
        if (!status.ok()) {
            std::free(stream);
            throw std::runtime_error("Failed to export ArrowArrayStream: " + status.ToString());
        }
        int result = lance_writer_start_stream(reinterpret_cast<::LanceWriter*>(rust_writer_), stream);
        if (result != 0) {
            std::free(stream);
            throw std::runtime_error("Failed to start Lance streaming write");
        }
        streaming_started_ = true;
        stream_state_ = std::move(state);
        stream_reader_ = std::move(reader);
    }

    std::cout << "Lance: Initialized dataset at " << dataset_path_ << "\n";
}

std::pair<void*, void*> LanceWriter::batch_to_ffi(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    // Convert RecordBatch and Schema to Arrow C Data Interface format
    // using Arrow's built-in export functions

    auto* arrow_array = new ArrowArray();
    auto* arrow_schema = new ArrowSchema();

    // Export RecordBatch as ArrowArray (includes schema)
    auto status = arrow::ExportRecordBatch(*batch, arrow_array, arrow_schema);
    if (!status.ok()) {
        delete arrow_array;
        delete arrow_schema;
        throw std::runtime_error("Failed to export RecordBatch to C Data Interface: " +
                                status.ToString());
    }

    return std::make_pair(reinterpret_cast<void*>(arrow_array),
                         reinterpret_cast<void*>(arrow_schema));
}

// NOTE: free_ffi_structures() removed as of PR #5 fix
//
// FFI ownership is transferred to Rust FFI layer via lance_writer_write_batch().
// The Rust side calls FFI_ArrowSchema::from_raw() and FFI_ArrowArray::from_raw(),
// which take ownership and call release callbacks when dropped.
//
// C++ must NOT call release() or delete these pointers after passing them to Rust.
// Doing so would cause double-free / use-after-free.
//
// Reference: Arrow C Data Interface specification - ownership transfers to callee

void LanceWriter::write_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        throw std::runtime_error("Received null batch");
    }

    if (batch->num_rows() == 0) {
        return;
    }

    // Initialize on first batch
    if (!schema_locked_) {
        initialize_lance_dataset(batch);
        start_time_ = std::chrono::steady_clock::now();
        last_report_time_ = start_time_;
        last_report_row_count_ = 0;
        last_report_batch_count_ = 0;
        last_report_byte_count_ = 0;
        total_byte_count_ = 0;
    }

    // Validate schema consistency
    if (!batch->schema()->Equals(*schema_)) {
        throw std::runtime_error(
            "Batch schema does not match table schema. "
            "Expected: " + schema_->ToString() + ", "
            "Got: " + batch->schema()->ToString());
    }

    if (streaming_enabled_) {
        if (!streaming_started_ || stream_state_ == nullptr) {
            throw std::runtime_error("Lance streaming writer not initialized");
        }
        int64_t batch_bytes = estimate_batch_bytes(*batch);
        stream_state_->push(batch, batch_bytes);
        row_count_ += batch->num_rows();
        batch_count_++;
        total_byte_count_ += batch_bytes;
        if (batch_count_ % 100 == 0 || batch_count_ <= 3) {
            auto now = std::chrono::steady_clock::now();
            auto elapsed_total = std::chrono::duration_cast<std::chrono::duration<double>>(now - start_time_).count();
            auto elapsed_window = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_report_time_).count();
            if (elapsed_window >= 1.0) {
                int64_t rows_window = row_count_ - last_report_row_count_;
                int32_t batches_window = batch_count_ - last_report_batch_count_;
                int64_t bytes_window = total_byte_count_ - last_report_byte_count_;
                double total_rps = elapsed_total > 0.0 ? (static_cast<double>(row_count_) / elapsed_total) : 0.0;
                double window_rps = elapsed_window > 0.0 ? (static_cast<double>(rows_window) / elapsed_window) : 0.0;
                double window_mbps = elapsed_window > 0.0 ? (static_cast<double>(bytes_window) / elapsed_window) / (1024.0 * 1024.0) : 0.0;

                auto stalls = stream_state_->stall_stats();
                double stall_ms = static_cast<double>(stalls.first) / 1e6;
                double queue_mb = static_cast<double>(stream_state_->current_bytes()) / (1024.0 * 1024.0);
                double peak_mb = static_cast<double>(stream_state_->peak_bytes()) / (1024.0 * 1024.0);

                std::cout << "Lance: Streamed batch " << batch_count_ << ", "
                          << row_count_ << " rows total"
                          << " (avg " << static_cast<long long>(total_rps) << " rows/s"
                          << ", window " << static_cast<long long>(window_rps) << " rows/s"
                          << ", " << window_mbps << " MB/s"
                          << ", queue " << stream_state_->queue_size()
                          << " (" << queue_mb << " MB, peak " << peak_mb << " MB)"
                          << ", stalls " << stalls.second << " (" << stall_ms << " ms)";
                if (elapsed_window > 0.0) {
                    double batches_per_s = batches_window / elapsed_window;
                    std::cout << ", " << batches_per_s << " batches/s";
                }
                std::cout << ")\n";

                last_report_time_ = now;
                last_report_row_count_ = row_count_;
                last_report_batch_count_ = batch_count_;
                last_report_byte_count_ = total_byte_count_;
            }
        }
        return;
    }

    // Convert batch to Arrow C Data Interface format
    auto [array_ptr, schema_ptr] = batch_to_ffi(batch);

    try {
        // Stream batch directly to Rust writer
        // NOTE: FFI ownership is transferred to Rust (via from_raw calls in Rust FFI)
        // Do NOT call free_ffi_structures() - Rust handles cleanup via Drop trait
        auto* raw_writer = reinterpret_cast<::LanceWriter*>(rust_writer_);
        int result = lance_writer_write_batch(raw_writer, array_ptr, schema_ptr);

        if (result != 0) {
            throw std::runtime_error(
                "Failed to write batch to Lance writer (error code: " +
                std::to_string(result) + ")");
        }

        row_count_ += batch->num_rows();
        batch_count_++;
        total_byte_count_ += estimate_batch_bytes(*batch);

        if (batch_count_ % 100 == 0 || batch_count_ <= 3) {
            auto now = std::chrono::steady_clock::now();
            auto elapsed_total = std::chrono::duration_cast<std::chrono::duration<double>>(now - start_time_).count();
            auto elapsed_window = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_report_time_).count();
            if (elapsed_window >= 1.0) {
                int64_t rows_window = row_count_ - last_report_row_count_;
                int32_t batches_window = batch_count_ - last_report_batch_count_;
                int64_t bytes_window = total_byte_count_ - last_report_byte_count_;
                double total_rps = elapsed_total > 0.0 ? (static_cast<double>(row_count_) / elapsed_total) : 0.0;
                double window_rps = elapsed_window > 0.0 ? (static_cast<double>(rows_window) / elapsed_window) : 0.0;
                double window_mbps = elapsed_window > 0.0 ? (static_cast<double>(bytes_window) / elapsed_window) / (1024.0 * 1024.0) : 0.0;

                std::cout << "Lance: Streamed batch " << batch_count_ << ", "
                          << row_count_ << " rows total"
                          << " (avg " << static_cast<long long>(total_rps) << " rows/s"
                          << ", window " << static_cast<long long>(window_rps) << " rows/s"
                          << ", " << window_mbps << " MB/s";
                if (elapsed_window > 0.0) {
                    double batches_per_s = batches_window / elapsed_window;
                    std::cout << ", " << batches_per_s << " batches/s";
                }
                std::cout << ")\n";

                last_report_time_ = now;
                last_report_row_count_ = row_count_;
                last_report_batch_count_ = batch_count_;
                last_report_byte_count_ = total_byte_count_;
            }
        }
    } catch (...) {
        // Exception handling: FFI structures now owned by Rust if call was initiated
        // Rust will clean up via Drop when writer closes or errors out
        throw;
    }
}


void LanceWriter::close() {
    if (rust_writer_ == nullptr) {
        return;
    }

    try {
        if (streaming_enabled_ && stream_state_) {
            stream_state_->close();
        }

        // Close Rust writer (handles metadata creation)
        auto* raw_writer = reinterpret_cast<::LanceWriter*>(rust_writer_);
        int result = lance_writer_close(raw_writer);
        if (result != 0) {
            throw std::runtime_error(
                "Lance writer close returned error code: " + std::to_string(result));
        }

        if (stream_state_) {
            auto stats = stream_state_->stall_stats();
            double stall_ms = static_cast<double>(stats.first) / 1e6;
            std::cout << "Lance: Stream stalls " << stats.second << " times, "
                      << stall_ms << " ms total\n";
        }

        std::cout << "Lance dataset finalized: " << dataset_path_ << "\n"
                  << "  Total rows: " << row_count_ << "\n"
                  << "  Total batches: " << batch_count_ << "\n";

        // Clean up
        lance_writer_destroy(raw_writer);
        rust_writer_ = nullptr;
        stream_state_.reset();
        stream_reader_.reset();
    } catch (const std::exception& e) {
        if (rust_writer_ != nullptr) {
            lance_writer_destroy(reinterpret_cast<::LanceWriter*>(rust_writer_));
            rust_writer_ = nullptr;
        }
        throw;
    }
}

}  // namespace tpch
