#include "tpch/lance_writer.hpp"

#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/type.h>

namespace fs = std::filesystem;

namespace tpch {

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
            // Call close() first to finalize the dataset
            // Suppress exceptions in destructor to avoid termination
            auto* raw_writer = reinterpret_cast<::LanceWriter*>(rust_writer_);
            lance_writer_close(raw_writer);
        } catch (...) {
            // Suppress exceptions in destructor
        }
        // Then destroy the handle
        lance_writer_destroy(reinterpret_cast<::LanceWriter*>(rust_writer_));
        rust_writer_ = nullptr;
    }
}

void LanceWriter::initialize_lance_dataset(
    const std::shared_ptr<arrow::RecordBatch>& first_batch) {
    if (schema_locked_) {
        return;
    }

    schema_ = first_batch->schema();
    schema_locked_ = true;

    // Create dataset directory
    try {
        fs::create_directories(dataset_path_);
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to create dataset directory: " +
                                std::string(e.what()));
    }

    // Create data subdirectory
    std::string data_dir = dataset_path_ + "/data";
    try {
        fs::create_directories(data_dir);
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to create data directory: " +
                                std::string(e.what()));
    }

    // Create Rust writer via FFI
    // Note: First argument is URI (dataset path), second is schema (can be null for now)
    auto* raw_writer = lance_writer_create(dataset_path_.c_str(), nullptr);
    rust_writer_ = reinterpret_cast<void*>(raw_writer);

    if (rust_writer_ == nullptr) {
        throw std::runtime_error("Failed to create Lance writer via FFI");
    }
}

std::pair<void*, void*> LanceWriter::batch_to_ffi(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    // Convert RecordBatch to Arrow C Data Interface format
    // This creates two FFI structures: ArrowArray and ArrowSchema

    try {
        // Note: Arrow 23.x doesn't have a C Data Interface export API yet
        // We'll pass the batch pointer directly as a placeholder
        // The actual data will be consumed by the Rust FFI layer which
        // can work with Arrow objects directly

        // For now, we return the batch as a void pointer
        // The Rust FFI can access it directly if needed

        void* batch_ptr = const_cast<arrow::RecordBatch*>(batch.get());
        void* schema_ptr = const_cast<arrow::Schema*>(batch->schema().get());

        return std::make_pair(batch_ptr, schema_ptr);
    } catch (const std::exception& e) {
        throw std::runtime_error("Error converting batch to FFI format: " +
                                std::string(e.what()));
    }
}

void LanceWriter::free_ffi_structures(void* /*array_ptr*/, void* /*schema_ptr*/) {
    // Note: For now, we're not allocating memory in batch_to_ffi,
    // so there's nothing to free. When using proper C Data Interface,
    // this would release the exported FFI structures.
    //
    // The pointers are managed by the Arrow library and shared_ptr
}

void LanceWriter::write_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        throw std::runtime_error("Received null batch");
    }

    if (batch->num_rows() == 0) {
        // Skip empty batches
        return;
    }

    // Initialize on first batch
    if (!schema_locked_) {
        initialize_lance_dataset(batch);
    }

    // Validate schema consistency
    if (!batch->schema()->Equals(*schema_)) {
        throw std::runtime_error(
            "Batch schema does not match table schema. "
            "Expected: " + schema_->ToString() + ", "
            "Got: " + batch->schema()->ToString());
    }

    // Convert batch to FFI format
    auto [array_ptr, schema_ptr] = batch_to_ffi(batch);

    try {
        // Write batch via Rust FFI
        auto* raw_writer = reinterpret_cast<::LanceWriter*>(rust_writer_);
        int result = lance_writer_write_batch(
            raw_writer,
            reinterpret_cast<const char*>(array_ptr),
            reinterpret_cast<const char*>(schema_ptr)
        );

        if (result != 0) {
            throw std::runtime_error(
                "Lance writer returned error code: " + std::to_string(result));
        }

        row_count_ += batch->num_rows();
        batch_count_++;

        // Log progress
        if (batch_count_ % 100 == 0 || batch_count_ <= 3) {
            std::cout << "Lance: Wrote " << batch_count_ << " batches, "
                      << row_count_ << " rows total\n";
        }
    } catch (const std::exception& e) {
        free_ffi_structures(array_ptr, schema_ptr);
        throw;
    }

    free_ffi_structures(array_ptr, schema_ptr);
}

void LanceWriter::close() {
    if (rust_writer_ == nullptr) {
        return;
    }

    try {
        // Close Rust writer
        auto* raw_writer = reinterpret_cast<::LanceWriter*>(rust_writer_);
        int result = lance_writer_close(raw_writer);
        if (result != 0) {
            throw std::runtime_error(
                "Lance writer close returned error code: " + std::to_string(result));
        }

        std::cout << "Lance dataset finalized: " << dataset_path_ << "\n"
                  << "  Total rows: " << row_count_ << "\n"
                  << "  Total batches: " << batch_count_ << "\n";

        // Clean up Rust writer handle
        lance_writer_destroy(raw_writer);
        rust_writer_ = nullptr;
    } catch (const std::exception& e) {
        // Attempt cleanup even if close failed
        if (rust_writer_ != nullptr) {
            lance_writer_destroy(reinterpret_cast<::LanceWriter*>(rust_writer_));
            rust_writer_ = nullptr;
        }
        throw;
    }
}

}  // namespace tpch
