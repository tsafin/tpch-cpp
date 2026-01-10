#include "tpch/multi_table_writer.hpp"
#include "tpch/shared_async_io.hpp"
#include "tpch/csv_writer.hpp"
#include "tpch/parquet_writer.hpp"
#include "tpch/async_io.hpp"
#ifdef TPCH_ENABLE_ORC
#include "tpch/orc_writer.hpp"
#endif
#include <sys/stat.h>
#include <stdexcept>
#include <sstream>

namespace tpch {

MultiTableWriter::MultiTableWriter(const std::string& output_dir, const std::string& format,
                                   bool use_async_io)
    : output_dir_(output_dir), format_(format), use_async_io_(use_async_io) {
    // Create output directory if it doesn't exist
    mkdir(output_dir.c_str(), 0755);

    // Initialize async I/O context if requested
    if (use_async_io) {
        async_ctx_ = std::make_shared<SharedAsyncIOContext>(512);
    }
}

MultiTableWriter::~MultiTableWriter() {
    try {
        finish_all();
    } catch (...) {
        // Suppress exceptions in destructor
    }
}

std::string MultiTableWriter::get_table_filename(TableType table_type) const {
    std::string table_name = table_type_name(table_type);
    std::string filename = table_name + "." + format_;

    if (!output_dir_.empty() && output_dir_.back() == '/') {
        return output_dir_ + filename;
    }
    return output_dir_ + "/" + filename;
}

WriterPtr MultiTableWriter::create_writer(TableType /*table_type*/, const std::string& filepath) {
    WriterPtr writer;

    if (format_ == "csv") {
        writer = std::make_unique<CSVWriter>(filepath);
    } else if (format_ == "parquet") {
        writer = std::make_unique<ParquetWriter>(filepath);
    }
#ifdef TPCH_ENABLE_ORC
    else if (format_ == "orc") {
        writer = std::make_unique<ORCWriter>(filepath);
    }
#endif
    else {
        throw std::invalid_argument("Unknown format: " + format_);
    }

    // Set async context if available
    if (use_async_io_ && async_ctx_) {
        // For SharedAsyncIOContext support, we would need to enhance writers
        // For now, use the existing AsyncIOContext capability if the writer supports it
        // This is a bridge - writers will need SharedAsyncIOContext support in next phase
    }

    return writer;
}

void MultiTableWriter::start_tables(const std::vector<TableType>& tables) {
    for (TableType table : tables) {
        int table_idx = static_cast<int>(table);
        if (table_writers_.find(table_idx) != table_writers_.end()) {
            continue;  // Already initialized
        }

        std::string filepath = get_table_filename(table);
        WriterPtr writer = create_writer(table, filepath);

        TableWriter tw;
        tw.writer = std::move(writer);
        tw.table_type = table;
        tw.initialized = true;

        table_writers_[table_idx] = std::move(tw);
    }
}

void MultiTableWriter::write_batch(TableType table_type, const std::shared_ptr<arrow::RecordBatch>& batch) {
    int table_idx = static_cast<int>(table_type);
    auto it = table_writers_.find(table_idx);
    if (it == table_writers_.end()) {
        throw std::runtime_error("Table not initialized");
    }

    if (it->second.writer) {
        it->second.writer->write_batch(batch);
    }
}

void MultiTableWriter::finish_all() {
    // Flush async I/O if enabled
    if (async_ctx_) {
        async_ctx_->flush();
    }

    // Close all writers
    for (auto& entry : table_writers_) {
        if (entry.second.writer) {
            entry.second.writer->close();
        }
    }

    // Close all async files
    if (async_ctx_) {
        async_ctx_->close_all();
    }
}

WriterInterface* MultiTableWriter::get_writer(TableType table_type) {
    int table_idx = static_cast<int>(table_type);
    auto it = table_writers_.find(table_idx);
    if (it == table_writers_.end()) {
        return nullptr;
    }
    return it->second.writer.get();
}

std::shared_ptr<SharedAsyncIOContext> MultiTableWriter::get_async_context() const {
    return async_ctx_;
}

int MultiTableWriter::pending_io_count() const {
    if (!async_ctx_) {
        return 0;
    }
    return async_ctx_->pending_count();
}

void MultiTableWriter::set_async_io_enabled(bool enabled) {
    use_async_io_ = enabled;
    if (!enabled && async_ctx_) {
        async_ctx_->flush();
        async_ctx_->close_all();
        async_ctx_.reset();
    }
}

}  // namespace tpch
