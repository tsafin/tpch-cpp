#include <iostream>
#include <sstream>
#include <memory>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

// Minimal Arrow includes to avoid protobuf symbol pollution
// These specific headers should NOT pull in protobuf infrastructure
#include <arrow/record_batch.h>
#include <arrow/array.h>
#include <arrow/type.h>

#include <orc/OrcFile.hh>

#include "tpch/orc_writer.hpp"
#include "tpch/async_io.hpp"

#ifdef TPCH_ENABLE_ASYNC_IO

namespace tpch {

/**
 * ORC OutputStream backed by io_uring for async disk writes.
 *
 * Matches the Rust io_uring_store design:
 *   - 512 KB chunks (same as Rust CHUNK_SIZE)
 *   - 8-buffer circular ring: up to 7 SQEs in-flight while filling the 8th
 *   - submit_and_wait(1) in the drain path: single syscall instead of two
 *   - sysfs-calibrated queue depth (nr_requests/2, clamped [8,128])
 *   - IORING_SETUP_ATTACH_WQ shared worker pool (via AsyncIOContext ctor)
 */
class OrcIoUringStream : public orc::OutputStream {
    // 512 KB per SQE — matches Rust CHUNK_SIZE
    static constexpr size_t kChunkSize = 512 * 1024;
    // Circular pool of staging buffers.
    // At steady state kNumBufs-1 = 7 SQEs are in-flight while we fill the 8th.
    static constexpr int kNumBufs = 8;

    std::string filepath_;
    int fd_ = -1;
    uint64_t file_offset_ = 0;

    std::array<std::vector<uint8_t>, kNumBufs> pool_;
    int write_idx_ = 0;  // slot currently being filled
    int read_idx_  = 0;  // oldest submitted slot (FIFO order)

    std::unique_ptr<AsyncIOContext> context_;

public:
    explicit OrcIoUringStream(const std::string& filepath)
        : filepath_(filepath) {
        fd_ = ::open(filepath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) {
            throw std::runtime_error("OrcIoUringStream: open failed: " + std::string(strerror(errno)));
        }
        // Calibrate queue depth from the target block device via sysfs.
        uint32_t qd = AsyncIOContext::calibrate_queue_depth(filepath.c_str());
        context_ = std::make_unique<AsyncIOContext>(qd);
        for (auto& b : pool_) b.reserve(kChunkSize);
    }

    ~OrcIoUringStream() override {
        try { close(); } catch (...) {}
    }

    void write(const void* buf, size_t length) override {
        if (fd_ < 0) {
            return;  // Silently ignore writes after close (double-close guard)
        }
        const uint8_t* ptr = static_cast<const uint8_t*>(buf);
        while (length > 0) {
            auto& cur = pool_[write_idx_];
            size_t space   = kChunkSize - cur.size();
            size_t to_copy = std::min(length, space);
            cur.insert(cur.end(), ptr, ptr + to_copy);
            ptr    += to_copy;
            length -= to_copy;
            if (cur.size() >= kChunkSize) {
                submit_head();
            }
        }
    }

    void close() override {
        if (fd_ < 0) return;

        // Submit any partial buffer.
        if (!pool_[write_idx_].empty()) {
            submit_head();
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
        ::close(fd_);
        fd_ = -1;
    }

    uint64_t getLength() const override {
        return file_offset_ + static_cast<uint64_t>(pool_[write_idx_].size());
    }

    uint64_t getNaturalWriteSize() const override {
        return kChunkSize;
    }

    const std::string& getName() const override {
        return filepath_;
    }

private:
    // Submit pool_[write_idx_] to io_uring, then advance write_idx_.
    // When the pool is full, drain the oldest slot with submit_and_wait(1).
    void submit_head() {
        auto& cur = pool_[write_idx_];

        context_->queue_write(fd_, cur.data(), cur.size(),
                              static_cast<off_t>(file_offset_), 0);
        file_offset_ += cur.size();

        if (context_->pending_count() + 1 >= kNumBufs) {
            // Pool full: submit new SQE + wait for oldest in 1 syscall.
            context_->submit_and_wait(1);
            pool_[read_idx_].clear();
            read_idx_ = (read_idx_ + 1) % kNumBufs;
        } else {
            context_->submit_queued();
        }

        write_idx_ = (write_idx_ + 1) % kNumBufs;
    }
};

}  // namespace tpch

#endif  // TPCH_ENABLE_ASYNC_IO

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
    } else if (type->id() == arrow::Type::DICTIONARY) {
        return "string";  // dict8<int8, utf8> expanded to string on write
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
    } else if (array->type()->id() == arrow::Type::DICTIONARY) {
        // Expand dict8<int8, utf8> to ORC string column via index lookup.
        // string_view points into the dictionary buffer which outlives this function.
        auto dict_array = std::static_pointer_cast<arrow::DictionaryArray>(array);
        auto dict_values = std::static_pointer_cast<arrow::StringArray>(dict_array->dictionary());
        auto* string_col = dynamic_cast<orc::StringVectorBatch*>(col_batch);
        if (!string_col) {
            throw std::runtime_error("Failed to cast ORC column to StringVectorBatch");
        }
        for (size_t i = 0; i < size; ++i) {
            if (dict_array->IsNull(static_cast<int64_t>(i))) {
                string_col->notNull[i] = 0;
            } else {
                string_col->notNull[i] = 1;
                auto idx = dict_array->GetValueIndex(static_cast<int64_t>(i));
                auto sv = dict_values->GetView(idx);
                string_col->data[i] = const_cast<char*>(sv.data());
                string_col->length[i] = static_cast<int64_t>(sv.length());
            }
        }
    } else {
        throw std::runtime_error("Unsupported Arrow type for ORC column copy");
    }
}

}  // anonymous namespace

ORCWriter::ORCWriter(const std::string& filepath)
    : filepath_(filepath), orc_writer_(nullptr), orc_output_stream_(nullptr),
      orc_type_(nullptr), orc_io_uring_stream_(nullptr) {
    // Constructor doesn't create writer yet - we wait for first batch to get schema
}

ORCWriter::~ORCWriter() {
    try {
        close();
    } catch (...) {
        // Suppress exceptions in destructor
    }

    // Delete writer first (it owns references to stream)
    if (orc_writer_) {
        // Writer's destructor handles cleanup
        delete reinterpret_cast<orc::Writer*>(orc_writer_);
        orc_writer_ = nullptr;
    }

    // Delete output stream (unique_ptr will be deleted, closing the file)
    if (orc_output_stream_) {
        delete reinterpret_cast<std::unique_ptr<orc::OutputStream>*>(orc_output_stream_);
        orc_output_stream_ = nullptr;
    }

    // Delete ORC type
    if (orc_type_) {
        delete reinterpret_cast<std::unique_ptr<orc::Type>*>(orc_type_);
        orc_type_ = nullptr;
    }

    // Delete io_uring stream (if used)
#ifdef TPCH_ENABLE_ASYNC_IO
    if (orc_io_uring_stream_) {
        delete reinterpret_cast<OrcIoUringStream*>(orc_io_uring_stream_);
        orc_io_uring_stream_ = nullptr;
    }
#endif
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
            // Create ORC type from schema string - must be stored as member to stay alive
            auto orc_type_local = orc::Type::buildTypeFromString(orc_schema_str);
            orc_type_ = new std::unique_ptr<orc::Type>(std::move(orc_type_local));

            // Create writer options
            orc::WriterOptions writer_options;
            writer_options.setStripeSize(64 * 1024 * 1024);  // 64MB stripes
            writer_options.setRowIndexStride(10000);
            // Always attempt dictionary encoding for strings (threshold = 1.0 = always).
            // TPC-H string columns have 2-40 unique values, so dictionary is always beneficial.
            writer_options.setDictionaryKeySizeThreshold(1.0);

            auto* orc_type_ptr = reinterpret_cast<std::unique_ptr<orc::Type>*>(orc_type_);

#ifdef TPCH_ENABLE_ASYNC_IO
            if (use_io_uring_) {
                // io_uring path: OrcIoUringStream owns the fd and the ring
                auto* uring_stream = new OrcIoUringStream(filepath_);
                orc_io_uring_stream_ = uring_stream;
                auto writer = orc::createWriter(**orc_type_ptr, uring_stream, writer_options);
                orc_writer_ = writer.release();
            } else {
#endif
            // Create output file stream using ORC factory function - must be stored as member to stay alive
            auto out_stream_local = orc::writeLocalFile(filepath_);
            orc_output_stream_ = new std::unique_ptr<orc::OutputStream>(std::move(out_stream_local));

            // Create ORC writer using factory function
            auto* out_stream_ptr = reinterpret_cast<std::unique_ptr<orc::OutputStream>*>(orc_output_stream_);
            auto writer = orc::createWriter(**orc_type_ptr, out_stream_ptr->get(), writer_options);
            orc_writer_ = writer.release();
#ifdef TPCH_ENABLE_ASYNC_IO
            }
#endif

        } catch (const std::exception& e) {
            schema_locked_ = false;

            // Clean up on error
            if (orc_type_) {
                delete reinterpret_cast<std::unique_ptr<orc::Type>*>(orc_type_);
                orc_type_ = nullptr;
            }
            if (orc_output_stream_) {
                delete reinterpret_cast<std::unique_ptr<orc::OutputStream>*>(orc_output_stream_);
                orc_output_stream_ = nullptr;
            }

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

        // CRITICAL: Set numElements on struct batch and all column batches
        struct_batch->numElements = static_cast<uint64_t>(num_rows);
        for (int col_idx = 0; col_idx < num_cols; ++col_idx) {
            struct_batch->fields[col_idx]->numElements = static_cast<uint64_t>(num_rows);
        }

        // Write the batch
        writer->add(*root_batch);

    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to write ORC batch: ") + e.what());
    }
}

void ORCWriter::enable_io_uring(bool enable) {
    use_io_uring_ = enable;
}

void ORCWriter::close() {
    if (orc_writer_) {
        try {
            auto* writer = reinterpret_cast<orc::Writer*>(orc_writer_);
            writer->close();
        } catch (const std::exception& e) {
            // Log but don't rethrow - the data has been written
            std::cerr << "Warning closing ORC writer: " << e.what() << std::endl;
        }
    }
}

}  // namespace tpch
