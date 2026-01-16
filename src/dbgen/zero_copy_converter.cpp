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

// ============================================================================
// Phase 14.2.3: Wrapped Array Builders (True Zero-Copy with Buffer::Wrap)
// ============================================================================

arrow::Result<std::shared_ptr<arrow::Array>>
ZeroCopyConverter::build_int64_array_wrapped(std::shared_ptr<std::vector<int64_t>> vec_ptr) {
    const int64_t count = static_cast<int64_t>(vec_ptr->size());

    if (count == 0) {
        return std::make_shared<arrow::Int64Array>(0, nullptr);
    }

    const int64_t size_bytes = count * sizeof(int64_t);

    // Wrap existing vector memory WITHOUT copying!
    // The BufferLifetimeManager (which holds vec_ptr via shared_ptr) ensures
    // the vector stays alive until Arrow buffer is freed.
    // Note: vec_ptr must be kept alive externally (via BufferLifetimeManager)
    auto buffer = arrow::Buffer::Wrap(
        reinterpret_cast<const uint8_t*>(vec_ptr->data()),
        size_bytes
    );

    return std::make_shared<arrow::Int64Array>(count, std::move(buffer));
}

arrow::Result<std::shared_ptr<arrow::Array>>
ZeroCopyConverter::build_double_array_wrapped(std::shared_ptr<std::vector<double>> vec_ptr) {
    const int64_t count = static_cast<int64_t>(vec_ptr->size());

    if (count == 0) {
        return std::make_shared<arrow::DoubleArray>(0, nullptr);
    }

    const int64_t size_bytes = count * sizeof(double);

    // Wrap existing vector memory WITHOUT copying!
    // The BufferLifetimeManager (which holds vec_ptr via shared_ptr) ensures
    // the vector stays alive until Arrow buffer is freed.
    // Note: vec_ptr must be kept alive externally (via BufferLifetimeManager)
    auto buffer = arrow::Buffer::Wrap(
        reinterpret_cast<const uint8_t*>(vec_ptr->data()),
        size_bytes
    );

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

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ZeroCopyConverter::orders_to_recordbatch(
    std::span<const order_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }

    // Extract data into temporary contiguous arrays
    std::vector<int64_t> orderkeys;
    std::vector<int64_t> custkeys;
    std::vector<double> totalprices;
    std::vector<int64_t> shippriorities;

    std::vector<std::string_view> orderstatuses;
    std::vector<std::string_view> orderdates;
    std::vector<std::string_view> orderpriorities;
    std::vector<std::string_view> clerks;
    std::vector<std::string_view> comments;

    // Reserve space
    orderkeys.reserve(count);
    custkeys.reserve(count);
    totalprices.reserve(count);
    shippriorities.reserve(count);
    orderstatuses.reserve(count);
    orderdates.reserve(count);
    orderpriorities.reserve(count);
    clerks.reserve(count);
    comments.reserve(count);

    // Single pass: extract all fields
    for (const order_t& order : batch) {
        // Numeric fields
        orderkeys.push_back(order.okey);
        custkeys.push_back(order.custkey);
        totalprices.push_back(static_cast<double>(order.totalprice) / 100.0);
        shippriorities.push_back(order.spriority);

        // String fields (views, not copies!)
        orderstatuses.emplace_back(&order.orderstatus, 1);  // Single char
        orderdates.emplace_back(order.odate, strlen_fast(order.odate));
        orderpriorities.emplace_back(order.opriority, strlen_fast(order.opriority));
        clerks.emplace_back(order.clerk, strlen_fast(order.clerk));
        comments.emplace_back(order.comment, order.clen);  // clen is pre-computed
    }

    // Build Arrow arrays
    ARROW_ASSIGN_OR_RAISE(auto orderkey_array, build_int64_array(orderkeys));
    ARROW_ASSIGN_OR_RAISE(auto custkey_array, build_int64_array(custkeys));
    ARROW_ASSIGN_OR_RAISE(auto totalprice_array, build_double_array(totalprices));
    ARROW_ASSIGN_OR_RAISE(auto shippriority_array, build_int64_array(shippriorities));

    ARROW_ASSIGN_OR_RAISE(auto orderstatus_array, build_string_array(orderstatuses));
    ARROW_ASSIGN_OR_RAISE(auto orderdate_array, build_string_array(orderdates));
    ARROW_ASSIGN_OR_RAISE(auto orderpriority_array, build_string_array(orderpriorities));
    ARROW_ASSIGN_OR_RAISE(auto clerk_array, build_string_array(clerks));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(comments));

    // Assemble RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        orderkey_array,
        custkey_array,
        orderstatus_array,
        totalprice_array,
        orderdate_array,
        orderpriority_array,
        clerk_array,
        shippriority_array,
        comment_array
    };

    return arrow::RecordBatch::Make(schema, count, arrays);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ZeroCopyConverter::customer_to_recordbatch(
    std::span<const customer_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }

    // Extract data into temporary contiguous arrays
    std::vector<int64_t> custkeys;
    std::vector<int64_t> nationkeys;
    std::vector<double> acctbals;

    std::vector<std::string_view> names;
    std::vector<std::string_view> addresses;
    std::vector<std::string_view> phones;
    std::vector<std::string_view> mktsegments;
    std::vector<std::string_view> comments;

    // Reserve space
    custkeys.reserve(count);
    nationkeys.reserve(count);
    acctbals.reserve(count);
    names.reserve(count);
    addresses.reserve(count);
    phones.reserve(count);
    mktsegments.reserve(count);
    comments.reserve(count);

    // Single pass: extract all fields
    for (const customer_t& cust : batch) {
        // Numeric fields
        custkeys.push_back(cust.custkey);
        nationkeys.push_back(cust.nation_code);
        acctbals.push_back(static_cast<double>(cust.acctbal) / 100.0);

        // String fields (views, not copies!)
        names.emplace_back(cust.name, strlen_fast(cust.name));
        addresses.emplace_back(cust.address, cust.alen);
        phones.emplace_back(cust.phone, strlen_fast(cust.phone));
        mktsegments.emplace_back(cust.mktsegment, strlen_fast(cust.mktsegment));
        comments.emplace_back(cust.comment, cust.clen);
    }

    // Build Arrow arrays
    ARROW_ASSIGN_OR_RAISE(auto custkey_array, build_int64_array(custkeys));
    ARROW_ASSIGN_OR_RAISE(auto nationkey_array, build_int64_array(nationkeys));
    ARROW_ASSIGN_OR_RAISE(auto acctbal_array, build_double_array(acctbals));

    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(names));
    ARROW_ASSIGN_OR_RAISE(auto address_array, build_string_array(addresses));
    ARROW_ASSIGN_OR_RAISE(auto phone_array, build_string_array(phones));
    ARROW_ASSIGN_OR_RAISE(auto mktsegment_array, build_string_array(mktsegments));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(comments));

    // Assemble RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        custkey_array,
        name_array,
        address_array,
        nationkey_array,
        phone_array,
        acctbal_array,
        mktsegment_array,
        comment_array
    };

    return arrow::RecordBatch::Make(schema, count, arrays);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ZeroCopyConverter::part_to_recordbatch(
    std::span<const part_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }

    // Extract data into temporary contiguous arrays
    std::vector<int64_t> partkeys;
    std::vector<int64_t> sizes;
    std::vector<double> retailprices;

    std::vector<std::string_view> names;
    std::vector<std::string_view> mfgrs;
    std::vector<std::string_view> brands;
    std::vector<std::string_view> types;
    std::vector<std::string_view> containers;
    std::vector<std::string_view> comments;

    // Reserve space
    partkeys.reserve(count);
    sizes.reserve(count);
    retailprices.reserve(count);
    names.reserve(count);
    mfgrs.reserve(count);
    brands.reserve(count);
    types.reserve(count);
    containers.reserve(count);
    comments.reserve(count);

    // Single pass: extract all fields
    for (const part_t& part : batch) {
        // Numeric fields
        partkeys.push_back(part.partkey);
        sizes.push_back(part.size);
        retailprices.push_back(static_cast<double>(part.retailprice) / 100.0);

        // String fields (views, not copies!)
        names.emplace_back(part.name, part.nlen);  // nlen is pre-computed
        mfgrs.emplace_back(part.mfgr, part.mlen);  // mlen is pre-computed
        brands.emplace_back(part.brand, part.blen);  // blen is pre-computed
        types.emplace_back(part.type, part.tlen);  // tlen is pre-computed
        containers.emplace_back(part.container, part.cnlen);  // cnlen is pre-computed
        comments.emplace_back(part.comment, part.clen);  // clen is pre-computed
    }

    // Build Arrow arrays
    ARROW_ASSIGN_OR_RAISE(auto partkey_array, build_int64_array(partkeys));
    ARROW_ASSIGN_OR_RAISE(auto size_array, build_int64_array(sizes));
    ARROW_ASSIGN_OR_RAISE(auto retailprice_array, build_double_array(retailprices));

    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(names));
    ARROW_ASSIGN_OR_RAISE(auto mfgr_array, build_string_array(mfgrs));
    ARROW_ASSIGN_OR_RAISE(auto brand_array, build_string_array(brands));
    ARROW_ASSIGN_OR_RAISE(auto type_array, build_string_array(types));
    ARROW_ASSIGN_OR_RAISE(auto container_array, build_string_array(containers));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(comments));

    // Assemble RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        partkey_array,
        name_array,
        mfgr_array,
        brand_array,
        type_array,
        size_array,
        container_array,
        retailprice_array,
        comment_array
    };

    return arrow::RecordBatch::Make(schema, count, arrays);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ZeroCopyConverter::partsupp_to_recordbatch(
    std::span<const partsupp_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }

    // Extract data into temporary contiguous arrays
    std::vector<int64_t> partkeys;
    std::vector<int64_t> suppkeys;
    std::vector<int64_t> availqtys;
    std::vector<double> supplycosts;

    std::vector<std::string_view> comments;

    // Reserve space
    partkeys.reserve(count);
    suppkeys.reserve(count);
    availqtys.reserve(count);
    supplycosts.reserve(count);
    comments.reserve(count);

    // Single pass: extract all fields
    for (const partsupp_t& ps : batch) {
        // Numeric fields
        partkeys.push_back(ps.partkey);
        suppkeys.push_back(ps.suppkey);
        availqtys.push_back(ps.qty);
        supplycosts.push_back(static_cast<double>(ps.scost) / 100.0);

        // String fields (views, not copies!)
        comments.emplace_back(ps.comment, ps.clen);  // clen is pre-computed
    }

    // Build Arrow arrays
    ARROW_ASSIGN_OR_RAISE(auto partkey_array, build_int64_array(partkeys));
    ARROW_ASSIGN_OR_RAISE(auto suppkey_array, build_int64_array(suppkeys));
    ARROW_ASSIGN_OR_RAISE(auto availqty_array, build_int64_array(availqtys));
    ARROW_ASSIGN_OR_RAISE(auto supplycost_array, build_double_array(supplycosts));

    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(comments));

    // Assemble RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        partkey_array,
        suppkey_array,
        availqty_array,
        supplycost_array,
        comment_array
    };

    return arrow::RecordBatch::Make(schema, count, arrays);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ZeroCopyConverter::supplier_to_recordbatch(
    std::span<const supplier_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }

    // Extract data into temporary contiguous arrays
    std::vector<int64_t> suppkeys;
    std::vector<int64_t> nationkeys;
    std::vector<double> acctbals;

    std::vector<std::string_view> names;
    std::vector<std::string_view> addresses;
    std::vector<std::string_view> phones;
    std::vector<std::string_view> comments;

    // Reserve space
    suppkeys.reserve(count);
    nationkeys.reserve(count);
    acctbals.reserve(count);
    names.reserve(count);
    addresses.reserve(count);
    phones.reserve(count);
    comments.reserve(count);

    // Single pass: extract all fields
    for (const supplier_t& supp : batch) {
        // Numeric fields
        suppkeys.push_back(supp.suppkey);
        nationkeys.push_back(supp.nation_code);
        acctbals.push_back(static_cast<double>(supp.acctbal) / 100.0);

        // String fields (views, not copies!)
        names.emplace_back(supp.name, strlen_fast(supp.name));
        addresses.emplace_back(supp.address, supp.alen);
        phones.emplace_back(supp.phone, strlen_fast(supp.phone));
        comments.emplace_back(supp.comment, supp.clen);
    }

    // Build Arrow arrays
    ARROW_ASSIGN_OR_RAISE(auto suppkey_array, build_int64_array(suppkeys));
    ARROW_ASSIGN_OR_RAISE(auto nationkey_array, build_int64_array(nationkeys));
    ARROW_ASSIGN_OR_RAISE(auto acctbal_array, build_double_array(acctbals));

    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(names));
    ARROW_ASSIGN_OR_RAISE(auto address_array, build_string_array(addresses));
    ARROW_ASSIGN_OR_RAISE(auto phone_array, build_string_array(phones));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(comments));

    // Assemble RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        suppkey_array,
        name_array,
        address_array,
        nationkey_array,
        phone_array,
        acctbal_array,
        comment_array
    };

    return arrow::RecordBatch::Make(schema, count, arrays);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ZeroCopyConverter::nation_to_recordbatch(
    std::span<const code_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }

    // Extract data into temporary contiguous arrays
    std::vector<int64_t> nationkeys;
    std::vector<int64_t> regionkeys;

    std::vector<std::string_view> names;
    std::vector<std::string_view> comments;

    // Reserve space
    nationkeys.reserve(count);
    regionkeys.reserve(count);
    names.reserve(count);
    comments.reserve(count);

    // Single pass: extract all fields
    for (const code_t& nation : batch) {
        // Numeric fields
        nationkeys.push_back(nation.code);
        regionkeys.push_back(nation.join);

        // String fields (views, not copies!)
        names.emplace_back(nation.text, strlen_fast(nation.text));
        comments.emplace_back(nation.comment, nation.clen);
    }

    // Build Arrow arrays
    ARROW_ASSIGN_OR_RAISE(auto nationkey_array, build_int64_array(nationkeys));
    ARROW_ASSIGN_OR_RAISE(auto regionkey_array, build_int64_array(regionkeys));

    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(names));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(comments));

    // Assemble RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        nationkey_array,
        name_array,
        regionkey_array,
        comment_array
    };

    return arrow::RecordBatch::Make(schema, count, arrays);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ZeroCopyConverter::region_to_recordbatch(
    std::span<const code_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }

    // Extract data into temporary contiguous arrays
    std::vector<int64_t> regionkeys;

    std::vector<std::string_view> names;
    std::vector<std::string_view> comments;

    // Reserve space
    regionkeys.reserve(count);
    names.reserve(count);
    comments.reserve(count);

    // Single pass: extract all fields
    for (const code_t& region : batch) {
        // Numeric fields
        regionkeys.push_back(region.code);

        // String fields (views, not copies!)
        names.emplace_back(region.text, strlen_fast(region.text));
        comments.emplace_back(region.comment, region.clen);
    }

    // Build Arrow arrays
    ARROW_ASSIGN_OR_RAISE(auto regionkey_array, build_int64_array(regionkeys));

    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(names));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(comments));

    // Assemble RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        regionkey_array,
        name_array,
        comment_array
    };

    return arrow::RecordBatch::Make(schema, count, arrays);
}

// ============================================================================
// Phase 14.2.3: Wrapped Converters (True Zero-Copy with Buffer::Wrap)
// ============================================================================

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::lineitem_to_recordbatch_wrapped(
    std::span<const line_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        auto empty_batch = arrow::RecordBatch::Make(schema, 0, empty_arrays);
        return ManagedRecordBatch(empty_batch, nullptr);
    }

    // Create lifetime manager to hold vectors alive
    auto lifetime_mgr = std::make_shared<BufferLifetimeManager>();

    // Create managed vectors (shared_ptr extends lifetime)
    auto orderkeys = lifetime_mgr->create_int64_buffer(count);
    auto partkeys = lifetime_mgr->create_int64_buffer(count);
    auto suppkeys = lifetime_mgr->create_int64_buffer(count);
    auto linenumbers = lifetime_mgr->create_int64_buffer(count);
    auto quantities = lifetime_mgr->create_double_buffer(count);
    auto extendedprices = lifetime_mgr->create_double_buffer(count);
    auto discounts = lifetime_mgr->create_double_buffer(count);
    auto taxes = lifetime_mgr->create_double_buffer(count);

    auto returnflags = lifetime_mgr->create_string_view_buffer(count);
    auto linestatuses = lifetime_mgr->create_string_view_buffer(count);
    auto shipdates = lifetime_mgr->create_string_view_buffer(count);
    auto commitdates = lifetime_mgr->create_string_view_buffer(count);
    auto receiptdates = lifetime_mgr->create_string_view_buffer(count);
    auto shipinstructs = lifetime_mgr->create_string_view_buffer(count);
    auto shipmodes = lifetime_mgr->create_string_view_buffer(count);
    auto comments = lifetime_mgr->create_string_view_buffer(count);

    // Single pass: extract all fields into managed vectors
    for (const line_t& line : batch) {
        // Numeric fields (will use Buffer::Wrap)
        orderkeys->push_back(line.okey);
        partkeys->push_back(line.partkey);
        suppkeys->push_back(line.suppkey);
        linenumbers->push_back(line.lcnt);
        quantities->push_back(static_cast<double>(line.quantity) / 100.0);
        extendedprices->push_back(static_cast<double>(line.eprice) / 100.0);
        discounts->push_back(static_cast<double>(line.discount) / 100.0);
        taxes->push_back(static_cast<double>(line.tax) / 100.0);

        // String fields (zero-copy views, but vectors need lifetime extension)
        returnflags->emplace_back(&line.rflag[0], 1);
        linestatuses->emplace_back(&line.lstatus[0], 1);
        shipdates->emplace_back(line.sdate, strlen_fast(line.sdate));
        commitdates->emplace_back(line.cdate, strlen_fast(line.cdate));
        receiptdates->emplace_back(line.rdate, strlen_fast(line.rdate));
        shipinstructs->emplace_back(line.shipinstruct, strlen_fast(line.shipinstruct));
        shipmodes->emplace_back(line.shipmode, strlen_fast(line.shipmode));
        comments->emplace_back(line.comment, line.clen);
    }

    // Build Arrow arrays using wrapped builders (NO MEMCPY for numeric!)
    ARROW_ASSIGN_OR_RAISE(auto orderkey_array, build_int64_array_wrapped(orderkeys));
    ARROW_ASSIGN_OR_RAISE(auto partkey_array, build_int64_array_wrapped(partkeys));
    ARROW_ASSIGN_OR_RAISE(auto suppkey_array, build_int64_array_wrapped(suppkeys));
    ARROW_ASSIGN_OR_RAISE(auto linenumber_array, build_int64_array_wrapped(linenumbers));
    ARROW_ASSIGN_OR_RAISE(auto quantity_array, build_double_array_wrapped(quantities));
    ARROW_ASSIGN_OR_RAISE(auto extendedprice_array, build_double_array_wrapped(extendedprices));
    ARROW_ASSIGN_OR_RAISE(auto discount_array, build_double_array_wrapped(discounts));
    ARROW_ASSIGN_OR_RAISE(auto tax_array, build_double_array_wrapped(taxes));

    // String arrays still use regular builder (memcpy required for non-contiguous data)
    ARROW_ASSIGN_OR_RAISE(auto returnflag_array, build_string_array(*returnflags));
    ARROW_ASSIGN_OR_RAISE(auto linestatus_array, build_string_array(*linestatuses));
    ARROW_ASSIGN_OR_RAISE(auto shipdate_array, build_string_array(*shipdates));
    ARROW_ASSIGN_OR_RAISE(auto commitdate_array, build_string_array(*commitdates));
    ARROW_ASSIGN_OR_RAISE(auto receiptdate_array, build_string_array(*receiptdates));
    ARROW_ASSIGN_OR_RAISE(auto shipinstruct_array, build_string_array(*shipinstructs));
    ARROW_ASSIGN_OR_RAISE(auto shipmode_array, build_string_array(*shipmodes));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(*comments));

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
        comment_array
    };

    auto record_batch = arrow::RecordBatch::Make(schema, count, arrays);

    // Return batch + lifetime manager (keeps vectors alive until Parquet encoding completes)
    return ManagedRecordBatch(record_batch, lifetime_mgr);
}

// Placeholder implementations for remaining tables (to be completed in Phase 2)
arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::orders_to_recordbatch_wrapped(
    std::span<const order_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {
    // TODO: Phase 2 - Implement with Buffer::Wrap for numeric arrays
    // For now, use regular converter and wrap in ManagedRecordBatch
    ARROW_ASSIGN_OR_RAISE(auto batch_result, orders_to_recordbatch(batch, schema));
    return ManagedRecordBatch(batch_result, nullptr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::customer_to_recordbatch_wrapped(
    std::span<const customer_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {
    // TODO: Phase 2
    ARROW_ASSIGN_OR_RAISE(auto batch_result, customer_to_recordbatch(batch, schema));
    return ManagedRecordBatch(batch_result, nullptr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::part_to_recordbatch_wrapped(
    std::span<const part_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {
    // TODO: Phase 2
    ARROW_ASSIGN_OR_RAISE(auto batch_result, part_to_recordbatch(batch, schema));
    return ManagedRecordBatch(batch_result, nullptr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::partsupp_to_recordbatch_wrapped(
    std::span<const partsupp_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {
    // TODO: Phase 2
    ARROW_ASSIGN_OR_RAISE(auto batch_result, partsupp_to_recordbatch(batch, schema));
    return ManagedRecordBatch(batch_result, nullptr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::supplier_to_recordbatch_wrapped(
    std::span<const supplier_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {
    // TODO: Phase 2
    ARROW_ASSIGN_OR_RAISE(auto batch_result, supplier_to_recordbatch(batch, schema));
    return ManagedRecordBatch(batch_result, nullptr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::nation_to_recordbatch_wrapped(
    std::span<const code_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {
    // TODO: Phase 2
    ARROW_ASSIGN_OR_RAISE(auto batch_result, nation_to_recordbatch(batch, schema));
    return ManagedRecordBatch(batch_result, nullptr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::region_to_recordbatch_wrapped(
    std::span<const code_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {
    // TODO: Phase 2
    ARROW_ASSIGN_OR_RAISE(auto batch_result, region_to_recordbatch(batch, schema));
    return ManagedRecordBatch(batch_result, nullptr);
}

} // namespace tpch
