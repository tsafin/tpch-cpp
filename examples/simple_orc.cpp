#include <iostream>
#include <memory>
#include <orc/OrcFile.hh>

int main() {
  std::cout << "=== Simple ORC Writer Example ===" << std::endl;

  try {
    // Create ORC schema for TPC-H lineitem-like table
    std::string orc_schema_str = "struct<l_orderkey:bigint,l_partkey:bigint,"
                                 "l_quantity:double,l_extendedprice:double,"
                                 "l_discount:double,l_tax:double,"
                                 "l_returnflag:string,l_linestatus:string>";

    // Build ORC type from schema string
    auto orc_type = orc::Type::buildTypeFromString(orc_schema_str);

    // Create output file stream
    auto out_stream = orc::writeLocalFile("/tmp/simple_lineitem.orc");

    // Create writer options
    orc::WriterOptions writer_options;
    writer_options.setStripeSize(64 * 1024 * 1024);  // 64MB stripes
    writer_options.setRowIndexStride(10000);

    // Create ORC writer
    auto writer = orc::createWriter(*orc_type, out_stream.get(), writer_options);

    // Create row batch for 100 rows
    const uint64_t num_rows = 100;
    auto root_batch = writer->createRowBatch(num_rows);

    // Cast root batch to StructVectorBatch (the container for all columns)
    auto* struct_batch = dynamic_cast<orc::StructVectorBatch*>(root_batch.get());
    if (!struct_batch) {
      throw std::runtime_error("Root batch is not a StructVectorBatch");
    }

    // Get column batches
    auto* orderkey_col = dynamic_cast<orc::LongVectorBatch*>(struct_batch->fields[0]);
    auto* partkey_col = dynamic_cast<orc::LongVectorBatch*>(struct_batch->fields[1]);
    auto* quantity_col = dynamic_cast<orc::DoubleVectorBatch*>(struct_batch->fields[2]);
    auto* extendedprice_col = dynamic_cast<orc::DoubleVectorBatch*>(struct_batch->fields[3]);
    auto* discount_col = dynamic_cast<orc::DoubleVectorBatch*>(struct_batch->fields[4]);
    auto* tax_col = dynamic_cast<orc::DoubleVectorBatch*>(struct_batch->fields[5]);
    auto* returnflag_col = dynamic_cast<orc::StringVectorBatch*>(struct_batch->fields[6]);
    auto* linestatus_col = dynamic_cast<orc::StringVectorBatch*>(struct_batch->fields[7]);

    if (!orderkey_col || !partkey_col || !quantity_col || !extendedprice_col ||
        !discount_col || !tax_col || !returnflag_col || !linestatus_col) {
      throw std::runtime_error("Failed to cast column batches");
    }

    // Fill in sample data
    for (uint64_t i = 0; i < num_rows; ++i) {
      // Orderkey: 1, 2, 3, ...
      orderkey_col->data[i] = i + 1;
      orderkey_col->notNull[i] = 1;

      // Partkey: 1..200 (cycling)
      partkey_col->data[i] = (i % 200) + 1;
      partkey_col->notNull[i] = 1;

      // Quantity: 10.0 to 59.0
      quantity_col->data[i] = 10.0 + (i % 50);
      quantity_col->notNull[i] = 1;

      // Extended price: quantity * 100
      extendedprice_col->data[i] = (10.0 + (i % 50)) * 100.0;
      extendedprice_col->notNull[i] = 1;

      // Discount: 0.05 to 0.14
      discount_col->data[i] = 0.05 + (i % 10) * 0.01;
      discount_col->notNull[i] = 1;

      // Tax: 0.06 to 0.13
      tax_col->data[i] = 0.06 + (i % 8) * 0.01;
      tax_col->notNull[i] = 1;

      // Return flags: A, N, R
      const char* flags_str[] = {"A", "N", "R"};
      auto flag_str = flags_str[i % 3];
      returnflag_col->data[i] = const_cast<char*>(flag_str);
      returnflag_col->length[i] = 1;
      returnflag_col->notNull[i] = 1;

      // Line status: O, F
      const char* status_str[] = {"O", "F"};
      auto status = status_str[i % 2];
      linestatus_col->data[i] = const_cast<char*>(status);
      linestatus_col->length[i] = 1;
      linestatus_col->notNull[i] = 1;
    }

    // Set the number of elements in the root batch
    struct_batch->numElements = num_rows;
    orderkey_col->numElements = num_rows;
    partkey_col->numElements = num_rows;
    quantity_col->numElements = num_rows;
    extendedprice_col->numElements = num_rows;
    discount_col->numElements = num_rows;
    tax_col->numElements = num_rows;
    returnflag_col->numElements = num_rows;
    linestatus_col->numElements = num_rows;

    // Write the batch
    writer->add(*struct_batch);

    // Close writer
    writer->close();

    std::cout << "Output file: /tmp/simple_lineitem.orc" << std::endl;
    std::cout << "Rows written: " << num_rows << std::endl;
    std::cout << "Columns: 8" << std::endl;
    std::cout << "Schema: " << orc_schema_str << std::endl;
    std::cout << "Success!" << std::endl;

    return 0;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
}
