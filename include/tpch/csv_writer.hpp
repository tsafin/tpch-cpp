#ifndef TPCH_CSV_WRITER_HPP
#define TPCH_CSV_WRITER_HPP

#include <fstream>
#include <memory>
#include <string>

#include "writer_interface.hpp"

namespace tpch {

/**
 * CSV writer implementation using Arrow's CSV API.
 * Writes Arrow RecordBatch data to CSV files with proper escaping and quoting.
 */
class CSVWriter : public WriterInterface {
public:
    /**
     * Create a CSV writer for the specified filepath.
     * The file will be created or overwritten.
     *
     * @param filepath Path to the output CSV file
     */
    explicit CSVWriter(const std::string& filepath);

    ~CSVWriter() override;

    /**
     * Write a batch of rows to the CSV file.
     * The header will be written on the first batch call.
     *
     * @param batch Arrow RecordBatch to write
     */
    void write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    /**
     * Finalize and close the output file.
     */
    void close() override;

private:
    std::string filepath_;
    std::ofstream output_;
    bool header_written_ = false;

    /**
     * Write CSV header (field names) to the output.
     *
     * @param batch First batch, used to extract field names from schema
     */
    void write_header(const std::shared_ptr<arrow::RecordBatch>& batch);

    /**
     * Escape a string value for CSV output.
     * Quotes the value if it contains special characters (comma, quote, newline).
     *
     * @param value String to escape
     * @return Escaped string safe for CSV output
     */
    static std::string escape_csv_value(const std::string& value);
};

}  // namespace tpch

#endif  // TPCH_CSV_WRITER_HPP
