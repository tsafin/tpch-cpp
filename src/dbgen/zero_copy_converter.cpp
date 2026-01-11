#include "tpch/zero_copy_converter.hpp"
#include "tpch/performance_counters.hpp"

#include <cstring>
#include <vector>

namespace tpch {

arrow::Result<std::shared_ptr<arrow::StringArray>>
ZeroCopyConverter::build_string_array(std::span<const std::string_view> views) {
    const int64_t count = static_cast<int64_t>(views.size());

    if (count == 0) {
        return std::make_shared<arrow::StringArray>(0, nullptr, nullptr);
    }

    // Calculate total size needed
    int64_t total_size = 0;
    for (const auto& view : views) {
        total_size += static_cast<int64_t>(view.size());
    }

    // Allocate Arrow buffers (single allocation each)
    ARROW_ASSIGN_OR_RAISE(auto value_buffer,
        arrow::AllocateBuffer(total_size));
    ARROW_ASSIGN_OR_RAISE(auto offset_buffer,
        arrow::AllocateBuffer((count + 1) * sizeof(int32_t)));

    // Fill buffers
    uint8_t* value_ptr = value_buffer->mutable_data();
    auto* offset_ptr = reinterpret_cast<int32_t*>(offset_buffer->mutable_data());

    offset_ptr[0] = 0;
    int32_t current_offset = 0;

    for (int64_t i = 0; i < count; ++i) {
        const auto& view = views[i];
        // Direct memcpy from dbgen memory to Arrow buffer
        if (!view.empty()) {
            std::memcpy(value_ptr + current_offset, view.data(), view.size());
        }
        current_offset += static_cast<int32_t>(view.size());
        offset_ptr[i + 1] = current_offset;
    }

    // Construct array from buffers
    return std::make_shared<arrow::StringArray>(
        count,
        std::move(offset_buffer),
        std::move(value_buffer)
    );
}

arrow::Result<std::shared_ptr<arrow::Array>>
ZeroCopyConverter::build_int64_array(std::span<const int64_t> values) {
    const int64_t count = static_cast<int64_t>(values.size());

    if (count == 0) {
        return std::make_shared<arrow::Int64Array>(0, nullptr);
    }

    const int64_t size_bytes = count * sizeof(int64_t);

    // Single memcpy (safer than zero-copy, still efficient)
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(size_bytes));
    std::memcpy(buffer->mutable_data(), values.data(), size_bytes);

    return std::make_shared<arrow::Int64Array>(count, std::move(buffer));
}

arrow::Result<std::shared_ptr<arrow::Array>>
ZeroCopyConverter::build_double_array(std::span<const double> values) {
    const int64_t count = static_cast<int64_t>(values.size());

    if (count == 0) {
        return std::make_shared<arrow::DoubleArray>(0, nullptr);
    }

    const int64_t size_bytes = count * sizeof(double);

    // Single memcpy (safer than zero-copy, still efficient)
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(size_bytes));
    std::memcpy(buffer->mutable_data(), values.data(), size_bytes);

    return std::make_shared<arrow::DoubleArray>(count, std::move(buffer));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ZeroCopyConverter::lineitem_to_recordbatch(
    std::span<const line_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }

    // Extract data into temporary contiguous arrays
    // These vectors provide contiguous memory for efficient memcpy
    std::vector<int64_t> orderkeys;
    std::vector<int64_t> partkeys;
    std::vector<int64_t> suppkeys;
    std::vector<int64_t> linenumbers;
    std::vector<double> quantities;
    std::vector<double> extendedprices;
    std::vector<double> discounts;
    std::vector<double> taxes;

    std::vector<std::string_view> returnflags;
    std::vector<std::string_view> linestatuses;
    std::vector<std::string_view> shipdates;
    std::vector<std::string_view> commitdates;
    std::vector<std::string_view> receiptdates;
    std::vector<std::string_view> shipinstructs;
    std::vector<std::string_view> shipmodes;
    std::vector<std::string_view> comments;

    // Reserve space
    orderkeys.reserve(count);
    partkeys.reserve(count);
    suppkeys.reserve(count);
    linenumbers.reserve(count);
    quantities.reserve(count);
    extendedprices.reserve(count);
    discounts.reserve(count);
    taxes.reserve(count);

    returnflags.reserve(count);
    linestatuses.reserve(count);
    shipdates.reserve(count);
    commitdates.reserve(count);
    receiptdates.reserve(count);
    shipinstructs.reserve(count);
    shipmodes.reserve(count);
    comments.reserve(count);

    // Single pass: extract all fields (good cache locality)
    for (const line_t& line : batch) {
        // Numeric fields (values, not copies)
        orderkeys.push_back(line.okey);
        partkeys.push_back(line.partkey);
        suppkeys.push_back(line.suppkey);
        linenumbers.push_back(line.lcnt);

        // Convert from fixed-point (cents) to floating-point (dollars)
        quantities.push_back(static_cast<double>(line.quantity) / 100.0);
        extendedprices.push_back(static_cast<double>(line.eprice) / 100.0);
        discounts.push_back(static_cast<double>(line.discount) / 100.0);
        taxes.push_back(static_cast<double>(line.tax) / 100.0);

        // String fields (views, not copies!)
        returnflags.emplace_back(line.rflag, 1);  // Single char
        linestatuses.emplace_back(line.lstatus, 1);  // Single char
        shipdates.emplace_back(line.sdate, strlen_fast(line.sdate));
        commitdates.emplace_back(line.cdate, strlen_fast(line.cdate));
        receiptdates.emplace_back(line.rdate, strlen_fast(line.rdate));
        shipinstructs.emplace_back(line.shipinstruct, strlen_fast(line.shipinstruct));
        shipmodes.emplace_back(line.shipmode, strlen_fast(line.shipmode));
        comments.emplace_back(line.comment, line.clen);  // clen is pre-computed
    }

    // Build Arrow arrays (single allocation per array type)
    ARROW_ASSIGN_OR_RAISE(auto orderkey_array, build_int64_array(orderkeys));
    ARROW_ASSIGN_OR_RAISE(auto partkey_array, build_int64_array(partkeys));
    ARROW_ASSIGN_OR_RAISE(auto suppkey_array, build_int64_array(suppkeys));
    ARROW_ASSIGN_OR_RAISE(auto linenumber_array, build_int64_array(linenumbers));
    ARROW_ASSIGN_OR_RAISE(auto quantity_array, build_double_array(quantities));
    ARROW_ASSIGN_OR_RAISE(auto extendedprice_array, build_double_array(extendedprices));
    ARROW_ASSIGN_OR_RAISE(auto discount_array, build_double_array(discounts));
    ARROW_ASSIGN_OR_RAISE(auto tax_array, build_double_array(taxes));

    ARROW_ASSIGN_OR_RAISE(auto returnflag_array,
        build_string_array(returnflags));
    ARROW_ASSIGN_OR_RAISE(auto linestatus_array,
        build_string_array(linestatuses));
    ARROW_ASSIGN_OR_RAISE(auto commitdate_array,
        build_string_array(commitdates));
    ARROW_ASSIGN_OR_RAISE(auto shipdate_array,
        build_string_array(shipdates));
    ARROW_ASSIGN_OR_RAISE(auto receiptdate_array,
        build_string_array(receiptdates));
    ARROW_ASSIGN_OR_RAISE(auto shipinstruct_array,
        build_string_array(shipinstructs));
    ARROW_ASSIGN_OR_RAISE(auto shipmode_array,
        build_string_array(shipmodes));
    ARROW_ASSIGN_OR_RAISE(auto comment_array,
        build_string_array(comments));

    // Assemble RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        orderkey_array,
        partkey_array,
        suppkey_array,
        linenumber_array,
        quantity_array,
        extendedprice_array,
        discount_array,
        tax_array,
        returnflag_array,
        linestatus_array,
        commitdate_array,
        shipdate_array,
        receiptdate_array,
        shipinstruct_array,
        shipmode_array,
        comment_array,
    };

    return arrow::RecordBatch::Make(schema, count, arrays);
}

} // namespace tpch
