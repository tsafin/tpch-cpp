#include "tpch/parquet_writer.hpp"

#include <iostream>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

namespace tpch {

ParquetWriter::ParquetWriter(const std::string& filepath)
    : filepath_(filepath), closed_(false) {}

ParquetWriter::~ParquetWriter() {
    if (!closed_) {
        try {
            close();
        } catch (...) {
            // Suppress exceptions in destructor
        }
    }
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
        // Create output file stream
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

        closed_ = true;
    } catch (const std::exception& e) {
        closed_ = true;
        throw;
    }
}

}  // namespace tpch
