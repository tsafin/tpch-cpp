#include <iostream>
#include <memory>
#include <sys/stat.h>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

using arrow::Array;
using arrow::ArrayBuilder;
using arrow::Buffer;
using arrow::DoubleBuilder;
using arrow::Field;
using arrow::Int64Builder;
using arrow::Schema;
using arrow::StringBuilder;
using arrow::Table;

int main() {
  // 1. Create Arrow schema for TPC-H lineitem-like table
  auto schema = arrow::schema({
      arrow::field("l_orderkey", arrow::int64()),
      arrow::field("l_partkey", arrow::int64()),
      arrow::field("l_quantity", arrow::float64()),
      arrow::field("l_extendedprice", arrow::float64()),
      arrow::field("l_discount", arrow::float64()),
      arrow::field("l_tax", arrow::float64()),
      arrow::field("l_returnflag", arrow::utf8()),
      arrow::field("l_linestatus", arrow::utf8()),
  });

  // 2. Build sample data using Arrow builders
  Int64Builder orderkey_builder;
  Int64Builder partkey_builder;
  DoubleBuilder quantity_builder;
  DoubleBuilder extendedprice_builder;
  DoubleBuilder discount_builder;
  DoubleBuilder tax_builder;
  StringBuilder returnflag_builder;
  StringBuilder linestatus_builder;

  // Add sample rows
  const int num_rows = 100;
  for (int i = 0; i < num_rows; ++i) {
    // Simulate TPC-H lineitem data
    ARROW_RETURN_NOT_OK(orderkey_builder.Append(i + 1));
    ARROW_RETURN_NOT_OK(partkey_builder.Append(i % 200 + 1));
    ARROW_RETURN_NOT_OK(quantity_builder.Append(10.0 + (i % 50)));
    ARROW_RETURN_NOT_OK(
        extendedprice_builder.Append(100.0 * (10.0 + (i % 50))));
    ARROW_RETURN_NOT_OK(discount_builder.Append(0.05 + (i % 10) * 0.01));
    ARROW_RETURN_NOT_OK(tax_builder.Append(0.06 + (i % 8) * 0.01));

    // Return flags: 'A', 'N', 'R'
    const char flags[] = {'A', 'N', 'R'};
    ARROW_RETURN_NOT_OK(
        returnflag_builder.Append(std::string(1, flags[i % 3])));

    // Line status: 'O', 'F'
    const char statuses[] = {'O', 'F'};
    ARROW_RETURN_NOT_OK(
        linestatus_builder.Append(std::string(1, statuses[i % 2])));
  }

  // 3. Build arrays from builders
  std::shared_ptr<Array> orderkey_array, partkey_array, quantity_array,
      extendedprice_array, discount_array, tax_array, returnflag_array,
      linestatus_array;

  ARROW_ASSIGN_OR_RAISE(orderkey_array, orderkey_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(partkey_array, partkey_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(quantity_array, quantity_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(extendedprice_array, extendedprice_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(discount_array, discount_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(tax_array, tax_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(returnflag_array, returnflag_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(linestatus_array, linestatus_builder.Finish());

  // 4. Create Arrow Table
  auto table = arrow::Table::Make(schema, {orderkey_array, partkey_array,
                                           quantity_array, extendedprice_array,
                                           discount_array, tax_array,
                                           returnflag_array, linestatus_array});

  // 5. Write to Parquet file
  const std::string output_file = "/tmp/simple_lineitem.parquet";

  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(output_file));

  // Set write options
  parquet::WriterProperties::Builder prop_builder;
  prop_builder.compression(parquet::Compression::SNAPPY);
  auto write_props = prop_builder.build();

  parquet::ArrowWriterProperties::Builder arrow_prop_builder;
  auto arrow_props = arrow_prop_builder.build();

  // Write table
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile,
                                 1024 * 1024, write_props, arrow_props));

  // 6. Print summary
  std::cout << "=== Simple Arrow/Parquet Example ===" << std::endl;
  std::cout << "Output file: " << output_file << std::endl;
  std::cout << "Rows written: " << table->num_rows() << std::endl;
  std::cout << "Columns: " << table->num_columns() << std::endl;

  // Get file size
  struct stat statbuf;
  if (stat(output_file.c_str(), &statbuf) == 0) {
    std::cout << "File size: " << statbuf.st_size << " bytes" << std::endl;
  }

  std::cout << "Schema:" << std::endl;
  std::cout << schema->ToString() << std::endl;

  return 0;
}
