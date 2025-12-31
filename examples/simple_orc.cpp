#include <iostream>
#include <memory>
#include <vector>

#include <arrow/api.h>

#include "tpch/orc_writer.hpp"

using arrow::DoubleBuilder;
using arrow::Int64Builder;
using arrow::StringBuilder;

arrow::Status RunORCExample() {
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
  std::shared_ptr<arrow::Array> orderkey_array, partkey_array, quantity_array,
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

  // 4. Create Arrow RecordBatch
  auto batch = arrow::RecordBatch::Make(
      schema, num_rows,
      {orderkey_array, partkey_array, quantity_array, extendedprice_array,
       discount_array, tax_array, returnflag_array, linestatus_array});

  // 5. Write to ORC file using ORCWriter
  const std::string output_file = "/tmp/simple_lineitem.orc";

  try {
    tpch::ORCWriter orc_writer(output_file);
    orc_writer.write_batch(batch);
    orc_writer.close();

    // 6. Print summary
    std::cout << "=== Simple ORC Writer Example ===" << std::endl;
    std::cout << "Output file: " << output_file << std::endl;
    std::cout << "Rows written: " << batch->num_rows() << std::endl;
    std::cout << "Columns: " << batch->num_columns() << std::endl;
    std::cout << "Schema:" << std::endl;
    std::cout << schema->ToString() << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return arrow::Status::UnknownError(e.what());
  }

  return arrow::Status::OK();
}

int main() {
  auto status = RunORCExample();
  if (!status.ok()) {
    std::cerr << "Error: " << status.message() << std::endl;
    return 1;
  }
  return 0;
}
