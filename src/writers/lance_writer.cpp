#include "tpch/lance_writer.hpp"

#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <arrow/type.h>
#include <arrow/record_batch.h>
#include <arrow/c/bridge.h>
#include <arrow/c/abi.h>

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
            close();
        } catch (...) {
            // Suppress exceptions in destructor
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
    auto* raw_writer = lance_writer_create(dataset_path_.c_str(), nullptr);
    rust_writer_ = reinterpret_cast<void*>(raw_writer);

    if (rust_writer_ == nullptr) {
        throw std::runtime_error("Failed to create Lance writer via FFI");
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

void LanceWriter::free_ffi_structures(void* array_ptr, void* schema_ptr) {
    // Free the ArrowArray and ArrowSchema structs that were allocated in batch_to_ffi()
    // The release callbacks handle releasing the actual data
    if (array_ptr != nullptr) {
        auto* arr = reinterpret_cast<ArrowArray*>(array_ptr);
        if (arr->release != nullptr) {
            arr->release(arr);
        }
        delete arr;
    }
    if (schema_ptr != nullptr) {
        auto* sch = reinterpret_cast<ArrowSchema*>(schema_ptr);
        if (sch->release != nullptr) {
            sch->release(sch);
        }
        delete sch;
    }
}

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
    }

    // Validate schema consistency
    if (!batch->schema()->Equals(*schema_)) {
        throw std::runtime_error(
            "Batch schema does not match table schema. "
            "Expected: " + schema_->ToString() + ", "
            "Got: " + batch->schema()->ToString());
    }

    // Convert batch to Arrow C Data Interface format
    auto [array_ptr, schema_ptr] = batch_to_ffi(batch);

    try {
        // Stream batch directly to Rust writer
        auto* raw_writer = reinterpret_cast<::LanceWriter*>(rust_writer_);
        int result = lance_writer_write_batch(raw_writer, array_ptr, schema_ptr);

        if (result != 0) {
            free_ffi_structures(array_ptr, schema_ptr);
            throw std::runtime_error(
                "Failed to write batch to Lance writer (error code: " +
                std::to_string(result) + ")");
        }

        // Clean up FFI structures after successful write
        free_ffi_structures(array_ptr, schema_ptr);

        row_count_ += batch->num_rows();
        batch_count_++;

        if (batch_count_ % 100 == 0 || batch_count_ <= 3) {
            std::cout << "Lance: Streamed batch " << batch_count_ << ", "
                      << row_count_ << " rows total\n";
        }
    } catch (...) {
        // Make sure to clean up FFI structures on exception
        free_ffi_structures(array_ptr, schema_ptr);
        throw;
    }
}


void LanceWriter::close() {
    if (rust_writer_ == nullptr) {
        return;
    }

    try {
        // Batches are already streamed to Rust writer, nothing to flush

        // Close Rust writer (handles metadata creation)
        auto* raw_writer = reinterpret_cast<::LanceWriter*>(rust_writer_);
        int result = lance_writer_close(raw_writer);
        if (result != 0) {
            throw std::runtime_error(
                "Lance writer close returned error code: " + std::to_string(result));
        }

        std::cout << "Lance dataset finalized: " << dataset_path_ << "\n"
                  << "  Total rows: " << row_count_ << "\n"
                  << "  Total batches: " << batch_count_ << "\n";

        // Clean up
        lance_writer_destroy(raw_writer);
        rust_writer_ = nullptr;
    } catch (const std::exception& e) {
        if (rust_writer_ != nullptr) {
            lance_writer_destroy(reinterpret_cast<::LanceWriter*>(rust_writer_));
            rust_writer_ = nullptr;
        }
        throw;
    }
}

}  // namespace tpch
