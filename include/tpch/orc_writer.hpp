#ifndef TPCH_ORC_WRITER_HPP
#define TPCH_ORC_WRITER_HPP

#include <memory>
#include <string>
#include <arrow/record_batch.h>

#include "writer_interface.hpp"

namespace tpch {

/**
 * ORC writer implementation using Apache ORC C++ library.
 * Writes Arrow RecordBatch data to ORC files with compression and schema support.
 */
class ORCWriter : public WriterInterface {
public:
    /**
     * Create an ORC writer for the specified filepath.
     * The file will be created or overwritten.
     *
     * @param filepath Path to the output ORC file
     * @throws std::runtime_error if ORC initialization fails
     */
    explicit ORCWriter(const std::string& filepath);

    ~ORCWriter() override;

    /**
     * Write a batch of rows to the ORC file.
     * The schema is inferred from the first batch and locked for subsequent batches.
     *
     * @param batch Arrow RecordBatch to write
     * @throws std::runtime_error if write fails or schema is inconsistent
     */
    void write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    /**
     * Finalize and close the output file.
     */
    void close() override;

private:
    std::string filepath_;
    std::shared_ptr<arrow::RecordBatch> first_batch_;
    bool schema_locked_ = false;

    // Opaque ORC writer implementation (void* to avoid exposing ORC headers)
    // In the implementation file, this will be cast to orc::Writer*
    void* orc_writer_;
};

}  // namespace tpch

#endif  // TPCH_ORC_WRITER_HPP
