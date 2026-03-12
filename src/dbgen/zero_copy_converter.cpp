#include "tpch/zero_copy_converter.hpp"
#include "tpch/performance_counters.hpp"

#include <cstring>
#include <vector>

namespace {

// Build a static string dictionary from a list of C strings.
// Used to construct the value arrays for DictionaryArrays.
std::shared_ptr<arrow::Array> make_string_dict(const std::vector<const char*>& values) {
    arrow::StringBuilder builder;
    for (const char* v : values) {
        builder.Append(v, strlen(v)).ok();
    }
    return *builder.Finish();
}

}  // namespace

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

arrow::Result<std::shared_ptr<arrow::Array>>
ZeroCopyConverter::build_dict_int8_array(
    std::span<const int8_t> indices,
    const std::shared_ptr<arrow::Array>& dictionary)
{
    const int64_t count = static_cast<int64_t>(indices.size());

    ARROW_ASSIGN_OR_RAISE(auto index_buf, arrow::AllocateBuffer(count * sizeof(int8_t)));
    std::memcpy(index_buf->mutable_data(), indices.data(), count * sizeof(int8_t));

    auto index_array = std::make_shared<arrow::Int8Array>(count, std::move(index_buf));
    auto dict_type = arrow::dictionary(arrow::int8(), arrow::utf8());
    return arrow::DictionaryArray::FromArrays(dict_type, index_array, dictionary);
}

arrow::Result<std::shared_ptr<arrow::Array>>
ZeroCopyConverter::build_dict_int16_array(
    std::span<const int16_t> indices,
    const std::shared_ptr<arrow::Array>& dictionary)
{
    const int64_t count = static_cast<int64_t>(indices.size());

    ARROW_ASSIGN_OR_RAISE(auto index_buf, arrow::AllocateBuffer(count * sizeof(int16_t)));
    std::memcpy(index_buf->mutable_data(), indices.data(), count * sizeof(int16_t));

    auto index_array = std::make_shared<arrow::Int16Array>(count, std::move(index_buf));
    auto dict_type = arrow::dictionary(arrow::int16(), arrow::utf8());
    return arrow::DictionaryArray::FromArrays(dict_type, index_array, dictionary);
}

std::shared_ptr<arrow::Array>
ZeroCopyConverter::get_dict_for_field(const std::string& name) {
    static auto returnflag   = make_string_dict({"A", "N", "R"});
    static auto linestatus   = make_string_dict({"F", "O"});
    static auto shipinstruct = make_string_dict({
        "COLLECT COD", "DELIVER IN PERSON", "NONE", "TAKE BACK RETURN"});
    static auto shipmode     = make_string_dict({
        "AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"});
    static auto orderstatus  = make_string_dict({"F", "O", "P"});
    static auto orderpriority = make_string_dict({
        "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"});
    static auto mktsegment   = make_string_dict({
        "AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"});
    static auto mfgr         = make_string_dict({
        "Manufacturer#1", "Manufacturer#2", "Manufacturer#3",
        "Manufacturer#4", "Manufacturer#5"});
    static auto brand        = make_string_dict({
        "Brand#11", "Brand#12", "Brand#13", "Brand#14", "Brand#15",
        "Brand#21", "Brand#22", "Brand#23", "Brand#24", "Brand#25",
        "Brand#31", "Brand#32", "Brand#33", "Brand#34", "Brand#35",
        "Brand#41", "Brand#42", "Brand#43", "Brand#44", "Brand#45",
        "Brand#51", "Brand#52", "Brand#53", "Brand#54", "Brand#55"});
    static auto container    = make_string_dict({
        "SM BOX",    "SM BAG",    "SM JAR",    "SM PKG",    "SM PACK",    "SM CAN",    "SM DRUM",    "SM CUP",
        "MED BOX",   "MED BAG",   "MED JAR",   "MED PKG",   "MED PACK",   "MED CAN",   "MED DRUM",   "MED CUP",
        "LG BOX",    "LG BAG",    "LG JAR",    "LG PKG",    "LG PACK",    "LG CAN",    "LG DRUM",    "LG CUP",
        "JUMBO BOX", "JUMBO BAG", "JUMBO JAR", "JUMBO PKG", "JUMBO PACK", "JUMBO CAN", "JUMBO DRUM", "JUMBO CUP",
        "WRAP BOX",  "WRAP BAG",  "WRAP JAR",  "WRAP PKG",  "WRAP PACK",  "WRAP CAN",  "WRAP DRUM",  "WRAP CUP"});

    // Date dictionary: all TPC-H dates 1992-01-01 .. 1998-12-31 in day order
    static auto date_dict = []() -> std::shared_ptr<arrow::Array> {
        arrow::StringBuilder b;
        static const int days_in_month[2][12] = {
            {31,28,31,30,31,30,31,31,30,31,30,31},  // non-leap
            {31,29,31,30,31,30,31,31,30,31,30,31}   // leap
        };
        for (int y = 1992; y <= 1998; ++y) {
            bool leap = (y % 4 == 0);
            for (int m = 1; m <= 12; ++m) {
                for (int d = 1; d <= days_in_month[leap ? 1 : 0][m - 1]; ++d) {
                    char buf[11];
                    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, d);
                    b.Append(buf, 10).ok();
                }
            }
        }
        return *b.Finish();
    }();

    // p_type dictionary: 150 = 6×5×5 syllable combinations in index order
    static auto ptype_dict = []() -> std::shared_ptr<arrow::Array> {
        static const char* s1[] = {"STANDARD","SMALL","MEDIUM","LARGE","ECONOMY","PROMO"};
        static const char* s2[] = {"ANODIZED","BURNISHED","PLATED","POLISHED","BRUSHED"};
        static const char* s3[] = {"TIN","NICKEL","BRASS","STEEL","COPPER"};
        arrow::StringBuilder b;
        for (int i = 0; i < 6; ++i)
            for (int j = 0; j < 5; ++j)
                for (int k = 0; k < 5; ++k) {
                    std::string v = std::string(s1[i]) + " " + s2[j] + " " + s3[k];
                    b.Append(v).ok();
                }
        return *b.Finish();
    }();

    if (name == "l_returnflag")     return returnflag;
    if (name == "l_linestatus")     return linestatus;
    if (name == "l_shipinstruct")   return shipinstruct;
    if (name == "l_shipmode")       return shipmode;
    if (name == "o_orderstatus")    return orderstatus;
    if (name == "o_orderpriority")  return orderpriority;
    if (name == "c_mktsegment")     return mktsegment;
    if (name == "p_mfgr")           return mfgr;
    if (name == "p_brand")          return brand;
    if (name == "p_container")      return container;
    if (name == "l_shipdate" || name == "l_commitdate" || name == "l_receiptdate" ||
        name == "o_orderdate")  return date_dict;
    if (name == "p_type")       return ptype_dict;
    return nullptr;
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

    // Dict-encoded columns (int8 indices, zero Lance stats overhead)
    std::vector<int8_t> returnflag_idxs;
    std::vector<int8_t> linestatus_idxs;
    std::vector<int8_t> shipinstruct_idxs;
    std::vector<int8_t> shipmode_idxs;
    // Date fields: dict16-encoded (2556 unique values, fit in int16)
    std::vector<int16_t> shipdate_idxs;
    std::vector<int16_t> commitdate_idxs;
    std::vector<int16_t> receiptdate_idxs;
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

    returnflag_idxs.reserve(count);
    linestatus_idxs.reserve(count);
    shipdate_idxs.reserve(count);
    commitdate_idxs.reserve(count);
    receiptdate_idxs.reserve(count);
    shipinstruct_idxs.reserve(count);
    shipmode_idxs.reserve(count);
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

        // Dict-encoded: encode to int8 index (O(1) switch, no string alloc)
        returnflag_idxs.push_back(encode_returnflag(line.rflag[0]));
        linestatus_idxs.push_back(encode_linestatus(line.lstatus[0]));
        shipinstruct_idxs.push_back(encode_shipinstruct(line.shipinstruct));
        shipmode_idxs.push_back(encode_shipmode(line.shipmode));
        // Date fields: encode to int16 index (dict16)
        shipdate_idxs.push_back(encode_date(line.sdate));
        commitdate_idxs.push_back(encode_date(line.cdate));
        receiptdate_idxs.push_back(encode_date(line.rdate));
        comments.emplace_back(line.comment, line.clen);
    }

    // Build Arrow arrays
    ARROW_ASSIGN_OR_RAISE(auto orderkey_array, build_int64_array(orderkeys));
    ARROW_ASSIGN_OR_RAISE(auto partkey_array, build_int64_array(partkeys));
    ARROW_ASSIGN_OR_RAISE(auto suppkey_array, build_int64_array(suppkeys));
    ARROW_ASSIGN_OR_RAISE(auto linenumber_array, build_int64_array(linenumbers));
    ARROW_ASSIGN_OR_RAISE(auto quantity_array, build_double_array(quantities));
    ARROW_ASSIGN_OR_RAISE(auto extendedprice_array, build_double_array(extendedprices));
    ARROW_ASSIGN_OR_RAISE(auto discount_array, build_double_array(discounts));
    ARROW_ASSIGN_OR_RAISE(auto tax_array, build_double_array(taxes));

    ARROW_ASSIGN_OR_RAISE(auto returnflag_array,
        build_dict_int8_array(returnflag_idxs, get_dict_for_field("l_returnflag")));
    ARROW_ASSIGN_OR_RAISE(auto linestatus_array,
        build_dict_int8_array(linestatus_idxs, get_dict_for_field("l_linestatus")));
    ARROW_ASSIGN_OR_RAISE(auto commitdate_array,
        build_dict_int16_array(commitdate_idxs, get_dict_for_field("l_commitdate")));
    ARROW_ASSIGN_OR_RAISE(auto shipdate_array,
        build_dict_int16_array(shipdate_idxs, get_dict_for_field("l_shipdate")));
    ARROW_ASSIGN_OR_RAISE(auto receiptdate_array,
        build_dict_int16_array(receiptdate_idxs, get_dict_for_field("l_receiptdate")));
    ARROW_ASSIGN_OR_RAISE(auto shipinstruct_array,
        build_dict_int8_array(shipinstruct_idxs, get_dict_for_field("l_shipinstruct")));
    ARROW_ASSIGN_OR_RAISE(auto shipmode_array,
        build_dict_int8_array(shipmode_idxs, get_dict_for_field("l_shipmode")));
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

    std::vector<int8_t> orderstatus_idxs;
    std::vector<int8_t> orderpriority_idxs;
    std::vector<int16_t> orderdate_idxs;
    std::vector<std::string_view> clerks;
    std::vector<std::string_view> comments;

    // Reserve space
    orderkeys.reserve(count);
    custkeys.reserve(count);
    totalprices.reserve(count);
    shippriorities.reserve(count);
    orderstatus_idxs.reserve(count);
    orderpriority_idxs.reserve(count);
    orderdate_idxs.reserve(count);
    clerks.reserve(count);
    comments.reserve(count);

    // Single pass: extract all fields
    for (const order_t& order : batch) {
        // Numeric fields
        orderkeys.push_back(order.okey);
        custkeys.push_back(order.custkey);
        totalprices.push_back(static_cast<double>(order.totalprice) / 100.0);
        shippriorities.push_back(order.spriority);

        // Dict-encoded
        orderstatus_idxs.push_back(encode_orderstatus(order.orderstatus));
        orderpriority_idxs.push_back(encode_orderpriority(order.opriority));
        // orderdate: dict16-encoded (2556 unique values, fit in int16)
        orderdate_idxs.push_back(encode_date(order.odate));
        clerks.emplace_back(order.clerk, strlen_fast(order.clerk));
        comments.emplace_back(order.comment, order.clen);
    }

    // Build Arrow arrays
    ARROW_ASSIGN_OR_RAISE(auto orderkey_array, build_int64_array(orderkeys));
    ARROW_ASSIGN_OR_RAISE(auto custkey_array, build_int64_array(custkeys));
    ARROW_ASSIGN_OR_RAISE(auto totalprice_array, build_double_array(totalprices));
    ARROW_ASSIGN_OR_RAISE(auto shippriority_array, build_int64_array(shippriorities));

    ARROW_ASSIGN_OR_RAISE(auto orderstatus_array,
        build_dict_int8_array(orderstatus_idxs, get_dict_for_field("o_orderstatus")));
    ARROW_ASSIGN_OR_RAISE(auto orderdate_array,
        build_dict_int16_array(orderdate_idxs, get_dict_for_field("o_orderdate")));
    ARROW_ASSIGN_OR_RAISE(auto orderpriority_array,
        build_dict_int8_array(orderpriority_idxs, get_dict_for_field("o_orderpriority")));
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
    std::vector<int8_t> mktsegment_idxs;
    std::vector<std::string_view> comments;

    // Reserve space
    custkeys.reserve(count);
    nationkeys.reserve(count);
    acctbals.reserve(count);
    names.reserve(count);
    addresses.reserve(count);
    phones.reserve(count);
    mktsegment_idxs.reserve(count);
    comments.reserve(count);

    // Single pass: extract all fields
    for (const customer_t& cust : batch) {
        // Numeric fields
        custkeys.push_back(cust.custkey);
        nationkeys.push_back(cust.nation_code);
        acctbals.push_back(static_cast<double>(cust.acctbal) / 100.0);

        // String fields
        names.emplace_back(cust.name, strlen_fast(cust.name));
        addresses.emplace_back(cust.address, cust.alen);
        phones.emplace_back(cust.phone, strlen_fast(cust.phone));
        mktsegment_idxs.push_back(encode_mktsegment(cust.mktsegment));
        comments.emplace_back(cust.comment, cust.clen);
    }

    // Build Arrow arrays
    ARROW_ASSIGN_OR_RAISE(auto custkey_array, build_int64_array(custkeys));
    ARROW_ASSIGN_OR_RAISE(auto nationkey_array, build_int64_array(nationkeys));
    ARROW_ASSIGN_OR_RAISE(auto acctbal_array, build_double_array(acctbals));

    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(names));
    ARROW_ASSIGN_OR_RAISE(auto address_array, build_string_array(addresses));
    ARROW_ASSIGN_OR_RAISE(auto phone_array, build_string_array(phones));
    ARROW_ASSIGN_OR_RAISE(auto mktsegment_array,
        build_dict_int8_array(mktsegment_idxs, get_dict_for_field("c_mktsegment")));
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
    std::vector<int8_t> mfgr_idxs;
    std::vector<int8_t> brand_idxs;
    std::vector<int16_t> type_idxs;
    std::vector<int8_t> container_idxs;
    std::vector<std::string_view> comments;

    // Reserve space
    partkeys.reserve(count);
    sizes.reserve(count);
    retailprices.reserve(count);
    names.reserve(count);
    mfgr_idxs.reserve(count);
    brand_idxs.reserve(count);
    type_idxs.reserve(count);
    container_idxs.reserve(count);
    comments.reserve(count);

    // Single pass: extract all fields
    for (const part_t& part : batch) {
        // Numeric fields
        partkeys.push_back(part.partkey);
        sizes.push_back(part.size);
        retailprices.push_back(static_cast<double>(part.retailprice) / 100.0);

        // Dict-encoded
        mfgr_idxs.push_back(encode_mfgr(part.mfgr));
        brand_idxs.push_back(encode_brand(part.brand));
        container_idxs.push_back(encode_container(part.container));
        // p_type: dict16-encoded (150 values)
        type_idxs.push_back(encode_ptype(part.type));
        names.emplace_back(part.name, part.nlen);
        comments.emplace_back(part.comment, part.clen);
    }

    // Build Arrow arrays
    ARROW_ASSIGN_OR_RAISE(auto partkey_array, build_int64_array(partkeys));
    ARROW_ASSIGN_OR_RAISE(auto size_array, build_int64_array(sizes));
    ARROW_ASSIGN_OR_RAISE(auto retailprice_array, build_double_array(retailprices));

    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(names));
    ARROW_ASSIGN_OR_RAISE(auto mfgr_array,
        build_dict_int8_array(mfgr_idxs, get_dict_for_field("p_mfgr")));
    ARROW_ASSIGN_OR_RAISE(auto brand_array,
        build_dict_int8_array(brand_idxs, get_dict_for_field("p_brand")));
    ARROW_ASSIGN_OR_RAISE(auto type_array,
        build_dict_int16_array(type_idxs, get_dict_for_field("p_type")));
    ARROW_ASSIGN_OR_RAISE(auto container_array,
        build_dict_int8_array(container_idxs, get_dict_for_field("p_container")));
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

    // Dict-encoded string columns (plain vectors — int8 values, no lifetime extension needed)
    std::vector<int8_t> returnflag_idxs; returnflag_idxs.reserve(count);
    std::vector<int8_t> linestatus_idxs; linestatus_idxs.reserve(count);
    std::vector<int8_t> shipinstruct_idxs; shipinstruct_idxs.reserve(count);
    std::vector<int8_t> shipmode_idxs; shipmode_idxs.reserve(count);
    // Date fields: dict16-encoded (2556 unique values, fit in int16)
    std::vector<int16_t> shipdate_idxs; shipdate_idxs.reserve(count);
    std::vector<int16_t> commitdate_idxs; commitdate_idxs.reserve(count);
    std::vector<int16_t> receiptdate_idxs; receiptdate_idxs.reserve(count);
    auto comments     = lifetime_mgr->create_string_view_buffer(count);

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

        // Dict-encoded: encode to int8 index
        returnflag_idxs.push_back(encode_returnflag(line.rflag[0]));
        linestatus_idxs.push_back(encode_linestatus(line.lstatus[0]));
        shipinstruct_idxs.push_back(encode_shipinstruct(line.shipinstruct));
        shipmode_idxs.push_back(encode_shipmode(line.shipmode));
        // Date fields: encode to int16 index (dict16)
        shipdate_idxs.push_back(encode_date(line.sdate));
        commitdate_idxs.push_back(encode_date(line.cdate));
        receiptdate_idxs.push_back(encode_date(line.rdate));
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

    // Dict-encoded string columns
    ARROW_ASSIGN_OR_RAISE(auto returnflag_array,
        build_dict_int8_array(returnflag_idxs, get_dict_for_field("l_returnflag")));
    ARROW_ASSIGN_OR_RAISE(auto linestatus_array,
        build_dict_int8_array(linestatus_idxs, get_dict_for_field("l_linestatus")));
    ARROW_ASSIGN_OR_RAISE(auto shipinstruct_array,
        build_dict_int8_array(shipinstruct_idxs, get_dict_for_field("l_shipinstruct")));
    ARROW_ASSIGN_OR_RAISE(auto shipmode_array,
        build_dict_int8_array(shipmode_idxs, get_dict_for_field("l_shipmode")));
    // dict16 date columns
    ARROW_ASSIGN_OR_RAISE(auto shipdate_array,
        build_dict_int16_array(shipdate_idxs, get_dict_for_field("l_shipdate")));
    ARROW_ASSIGN_OR_RAISE(auto commitdate_array,
        build_dict_int16_array(commitdate_idxs, get_dict_for_field("l_commitdate")));
    ARROW_ASSIGN_OR_RAISE(auto receiptdate_array,
        build_dict_int16_array(receiptdate_idxs, get_dict_for_field("l_receiptdate")));
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

// ============================================================================
// Phase 3: Wrapped Converters for Remaining Tables
// ============================================================================

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::orders_to_recordbatch_wrapped(
    std::span<const order_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        auto empty_batch = arrow::RecordBatch::Make(schema, 0, empty_arrays);
        return ManagedRecordBatch(empty_batch, nullptr);
    }

    // Create lifetime manager
    auto lifetime_mgr = std::make_shared<BufferLifetimeManager>();

    // Create managed vectors for numeric fields
    auto orderkeys = lifetime_mgr->create_int64_buffer(count);
    auto custkeys = lifetime_mgr->create_int64_buffer(count);
    auto totalprices = lifetime_mgr->create_double_buffer(count);
    auto shippriorities = lifetime_mgr->create_int64_buffer(count);

    // Dict-encoded string columns
    std::vector<int8_t> orderstatus_idxs; orderstatus_idxs.reserve(count);
    std::vector<int8_t> orderpriority_idxs; orderpriority_idxs.reserve(count);
    // orderdate: dict16-encoded (2556 unique values, fit in int16)
    std::vector<int16_t> orderdate_idxs; orderdate_idxs.reserve(count);
    auto clerks     = lifetime_mgr->create_string_view_buffer(count);
    auto comments   = lifetime_mgr->create_string_view_buffer(count);

    // Extract data into managed vectors
    for (const order_t& order : batch) {
        // Numeric fields
        orderkeys->push_back(order.okey);
        custkeys->push_back(order.custkey);
        totalprices->push_back(static_cast<double>(order.totalprice) / 100.0);
        shippriorities->push_back(order.spriority);

        // Dict-encoded
        orderstatus_idxs.push_back(encode_orderstatus(order.orderstatus));
        orderpriority_idxs.push_back(encode_orderpriority(order.opriority));
        // orderdate: dict16 index
        orderdate_idxs.push_back(encode_date(order.odate));
        clerks->emplace_back(order.clerk, strlen_fast(order.clerk));
        comments->emplace_back(order.comment, order.clen);
    }

    // Build arrays using wrapped builders for numeric
    ARROW_ASSIGN_OR_RAISE(auto orderkey_array, build_int64_array_wrapped(orderkeys));
    ARROW_ASSIGN_OR_RAISE(auto custkey_array, build_int64_array_wrapped(custkeys));
    ARROW_ASSIGN_OR_RAISE(auto totalprice_array, build_double_array_wrapped(totalprices));
    ARROW_ASSIGN_OR_RAISE(auto shippriority_array, build_int64_array_wrapped(shippriorities));

    ARROW_ASSIGN_OR_RAISE(auto orderstatus_array,
        build_dict_int8_array(orderstatus_idxs, get_dict_for_field("o_orderstatus")));
    ARROW_ASSIGN_OR_RAISE(auto orderdate_array,
        build_dict_int16_array(orderdate_idxs, get_dict_for_field("o_orderdate")));
    ARROW_ASSIGN_OR_RAISE(auto orderpriority_array,
        build_dict_int8_array(orderpriority_idxs, get_dict_for_field("o_orderpriority")));
    ARROW_ASSIGN_OR_RAISE(auto clerk_array, build_string_array(*clerks));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(*comments));

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

    auto record_batch = arrow::RecordBatch::Make(schema, count, arrays);
    return ManagedRecordBatch(record_batch, lifetime_mgr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::customer_to_recordbatch_wrapped(
    std::span<const customer_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        auto empty_batch = arrow::RecordBatch::Make(schema, 0, empty_arrays);
        return ManagedRecordBatch(empty_batch, nullptr);
    }

    // Create lifetime manager
    auto lifetime_mgr = std::make_shared<BufferLifetimeManager>();

    // Create managed vectors for numeric fields
    auto custkeys = lifetime_mgr->create_int64_buffer(count);
    auto nationkeys = lifetime_mgr->create_int64_buffer(count);
    auto acctbals = lifetime_mgr->create_double_buffer(count);

    // Create managed vectors for string fields
    auto names     = lifetime_mgr->create_string_view_buffer(count);
    auto addresses = lifetime_mgr->create_string_view_buffer(count);
    auto phones    = lifetime_mgr->create_string_view_buffer(count);
    std::vector<int8_t> mktsegment_idxs; mktsegment_idxs.reserve(count);
    auto comments  = lifetime_mgr->create_string_view_buffer(count);

    // Extract data into managed vectors
    for (const customer_t& cust : batch) {
        // Numeric fields
        custkeys->push_back(cust.custkey);
        nationkeys->push_back(cust.nation_code);
        acctbals->push_back(static_cast<double>(cust.acctbal) / 100.0);

        // String fields
        names->emplace_back(cust.name, strlen_fast(cust.name));
        addresses->emplace_back(cust.address, cust.alen);
        phones->emplace_back(cust.phone, strlen_fast(cust.phone));
        mktsegment_idxs.push_back(encode_mktsegment(cust.mktsegment));
        comments->emplace_back(cust.comment, cust.clen);
    }

    // Build arrays using wrapped builders for numeric
    ARROW_ASSIGN_OR_RAISE(auto custkey_array, build_int64_array_wrapped(custkeys));
    ARROW_ASSIGN_OR_RAISE(auto nationkey_array, build_int64_array_wrapped(nationkeys));
    ARROW_ASSIGN_OR_RAISE(auto acctbal_array, build_double_array_wrapped(acctbals));

    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(*names));
    ARROW_ASSIGN_OR_RAISE(auto address_array, build_string_array(*addresses));
    ARROW_ASSIGN_OR_RAISE(auto phone_array, build_string_array(*phones));
    ARROW_ASSIGN_OR_RAISE(auto mktsegment_array,
        build_dict_int8_array(mktsegment_idxs, get_dict_for_field("c_mktsegment")));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(*comments));

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

    auto record_batch = arrow::RecordBatch::Make(schema, count, arrays);
    return ManagedRecordBatch(record_batch, lifetime_mgr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::part_to_recordbatch_wrapped(
    std::span<const part_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        auto empty_batch = arrow::RecordBatch::Make(schema, 0, empty_arrays);
        return ManagedRecordBatch(empty_batch, nullptr);
    }

    // Create lifetime manager
    auto lifetime_mgr = std::make_shared<BufferLifetimeManager>();

    // Create managed vectors for numeric fields
    auto partkeys = lifetime_mgr->create_int64_buffer(count);
    auto sizes = lifetime_mgr->create_int64_buffer(count);
    auto retailprices = lifetime_mgr->create_double_buffer(count);

    // Create managed vectors for string fields
    auto names     = lifetime_mgr->create_string_view_buffer(count);
    std::vector<int8_t> mfgr_idxs;      mfgr_idxs.reserve(count);
    std::vector<int8_t> brand_idxs;     brand_idxs.reserve(count);
    // p_type: dict16-encoded (150 values)
    std::vector<int16_t> type_idxs;     type_idxs.reserve(count);
    std::vector<int8_t> container_idxs; container_idxs.reserve(count);
    auto comments  = lifetime_mgr->create_string_view_buffer(count);

    // Extract data into managed vectors
    for (const part_t& part : batch) {
        // Numeric fields
        partkeys->push_back(part.partkey);
        sizes->push_back(part.size);
        retailprices->push_back(static_cast<double>(part.retailprice) / 100.0);

        // Dict-encoded
        mfgr_idxs.push_back(encode_mfgr(part.mfgr));
        brand_idxs.push_back(encode_brand(part.brand));
        container_idxs.push_back(encode_container(part.container));
        // p_type: dict16 index
        type_idxs.push_back(encode_ptype(part.type));
        names->emplace_back(part.name, part.nlen);
        comments->emplace_back(part.comment, part.clen);
    }

    // Build arrays using wrapped builders for numeric
    ARROW_ASSIGN_OR_RAISE(auto partkey_array, build_int64_array_wrapped(partkeys));
    ARROW_ASSIGN_OR_RAISE(auto size_array, build_int64_array_wrapped(sizes));
    ARROW_ASSIGN_OR_RAISE(auto retailprice_array, build_double_array_wrapped(retailprices));

    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(*names));
    ARROW_ASSIGN_OR_RAISE(auto mfgr_array,
        build_dict_int8_array(mfgr_idxs, get_dict_for_field("p_mfgr")));
    ARROW_ASSIGN_OR_RAISE(auto brand_array,
        build_dict_int8_array(brand_idxs, get_dict_for_field("p_brand")));
    ARROW_ASSIGN_OR_RAISE(auto type_array,
        build_dict_int16_array(type_idxs, get_dict_for_field("p_type")));
    ARROW_ASSIGN_OR_RAISE(auto container_array,
        build_dict_int8_array(container_idxs, get_dict_for_field("p_container")));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(*comments));

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

    auto record_batch = arrow::RecordBatch::Make(schema, count, arrays);
    return ManagedRecordBatch(record_batch, lifetime_mgr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::partsupp_to_recordbatch_wrapped(
    std::span<const partsupp_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        auto empty_batch = arrow::RecordBatch::Make(schema, 0, empty_arrays);
        return ManagedRecordBatch(empty_batch, nullptr);
    }

    // Create lifetime manager
    auto lifetime_mgr = std::make_shared<BufferLifetimeManager>();

    // Create managed vectors for numeric fields
    auto partkeys = lifetime_mgr->create_int64_buffer(count);
    auto suppkeys = lifetime_mgr->create_int64_buffer(count);
    auto availqtys = lifetime_mgr->create_int64_buffer(count);
    auto supplycosts = lifetime_mgr->create_double_buffer(count);

    // Create managed vectors for string fields
    auto comments = lifetime_mgr->create_string_view_buffer(count);

    // Extract data into managed vectors
    for (const partsupp_t& ps : batch) {
        // Numeric fields
        partkeys->push_back(ps.partkey);
        suppkeys->push_back(ps.suppkey);
        availqtys->push_back(ps.qty);
        supplycosts->push_back(static_cast<double>(ps.scost) / 100.0);

        // String fields
        comments->emplace_back(ps.comment, ps.clen);
    }

    // Build arrays using wrapped builders for numeric
    ARROW_ASSIGN_OR_RAISE(auto partkey_array, build_int64_array_wrapped(partkeys));
    ARROW_ASSIGN_OR_RAISE(auto suppkey_array, build_int64_array_wrapped(suppkeys));
    ARROW_ASSIGN_OR_RAISE(auto availqty_array, build_int64_array_wrapped(availqtys));
    ARROW_ASSIGN_OR_RAISE(auto supplycost_array, build_double_array_wrapped(supplycosts));

    // String arrays still use regular builder
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(*comments));

    // Assemble RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        partkey_array,
        suppkey_array,
        availqty_array,
        supplycost_array,
        comment_array
    };

    auto record_batch = arrow::RecordBatch::Make(schema, count, arrays);
    return ManagedRecordBatch(record_batch, lifetime_mgr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::supplier_to_recordbatch_wrapped(
    std::span<const supplier_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        auto empty_batch = arrow::RecordBatch::Make(schema, 0, empty_arrays);
        return ManagedRecordBatch(empty_batch, nullptr);
    }

    // Create lifetime manager
    auto lifetime_mgr = std::make_shared<BufferLifetimeManager>();

    // Create managed vectors for numeric fields
    auto suppkeys = lifetime_mgr->create_int64_buffer(count);
    auto nationkeys = lifetime_mgr->create_int64_buffer(count);
    auto acctbals = lifetime_mgr->create_double_buffer(count);

    // Create managed vectors for string fields
    auto names = lifetime_mgr->create_string_view_buffer(count);
    auto addresses = lifetime_mgr->create_string_view_buffer(count);
    auto phones = lifetime_mgr->create_string_view_buffer(count);
    auto comments = lifetime_mgr->create_string_view_buffer(count);

    // Extract data into managed vectors
    for (const supplier_t& supp : batch) {
        // Numeric fields
        suppkeys->push_back(supp.suppkey);
        nationkeys->push_back(supp.nation_code);
        acctbals->push_back(static_cast<double>(supp.acctbal) / 100.0);

        // String fields
        names->emplace_back(supp.name, strlen_fast(supp.name));
        addresses->emplace_back(supp.address, supp.alen);
        phones->emplace_back(supp.phone, strlen_fast(supp.phone));
        comments->emplace_back(supp.comment, supp.clen);
    }

    // Build arrays using wrapped builders for numeric
    ARROW_ASSIGN_OR_RAISE(auto suppkey_array, build_int64_array_wrapped(suppkeys));
    ARROW_ASSIGN_OR_RAISE(auto nationkey_array, build_int64_array_wrapped(nationkeys));
    ARROW_ASSIGN_OR_RAISE(auto acctbal_array, build_double_array_wrapped(acctbals));

    // String arrays still use regular builder
    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(*names));
    ARROW_ASSIGN_OR_RAISE(auto address_array, build_string_array(*addresses));
    ARROW_ASSIGN_OR_RAISE(auto phone_array, build_string_array(*phones));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(*comments));

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

    auto record_batch = arrow::RecordBatch::Make(schema, count, arrays);
    return ManagedRecordBatch(record_batch, lifetime_mgr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::nation_to_recordbatch_wrapped(
    std::span<const code_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        auto empty_batch = arrow::RecordBatch::Make(schema, 0, empty_arrays);
        return ManagedRecordBatch(empty_batch, nullptr);
    }

    // Create lifetime manager
    auto lifetime_mgr = std::make_shared<BufferLifetimeManager>();

    // Create managed vectors for numeric fields
    auto nationkeys = lifetime_mgr->create_int64_buffer(count);
    auto regionkeys = lifetime_mgr->create_int64_buffer(count);

    // Create managed vectors for string fields
    auto names = lifetime_mgr->create_string_view_buffer(count);
    auto comments = lifetime_mgr->create_string_view_buffer(count);

    // Extract data into managed vectors
    for (const code_t& nation : batch) {
        // Numeric fields
        nationkeys->push_back(nation.code);
        regionkeys->push_back(nation.join);

        // String fields
        names->emplace_back(nation.text, strlen_fast(nation.text));
        comments->emplace_back(nation.comment, nation.clen);
    }

    // Build arrays using wrapped builders for numeric
    ARROW_ASSIGN_OR_RAISE(auto nationkey_array, build_int64_array_wrapped(nationkeys));
    ARROW_ASSIGN_OR_RAISE(auto regionkey_array, build_int64_array_wrapped(regionkeys));

    // String arrays still use regular builder
    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(*names));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(*comments));

    // Assemble RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        nationkey_array,
        name_array,
        regionkey_array,
        comment_array
    };

    auto record_batch = arrow::RecordBatch::Make(schema, count, arrays);
    return ManagedRecordBatch(record_batch, lifetime_mgr);
}

arrow::Result<ManagedRecordBatch>
ZeroCopyConverter::region_to_recordbatch_wrapped(
    std::span<const code_t> batch,
    const std::shared_ptr<arrow::Schema>& schema) {

    const int64_t count = static_cast<int64_t>(batch.size());

    if (count == 0) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        auto empty_batch = arrow::RecordBatch::Make(schema, 0, empty_arrays);
        return ManagedRecordBatch(empty_batch, nullptr);
    }

    // Create lifetime manager
    auto lifetime_mgr = std::make_shared<BufferLifetimeManager>();

    // Create managed vectors for numeric fields
    auto regionkeys = lifetime_mgr->create_int64_buffer(count);

    // Create managed vectors for string fields
    auto names = lifetime_mgr->create_string_view_buffer(count);
    auto comments = lifetime_mgr->create_string_view_buffer(count);

    // Extract data into managed vectors
    for (const code_t& region : batch) {
        // Numeric fields
        regionkeys->push_back(region.code);

        // String fields
        names->emplace_back(region.text, strlen_fast(region.text));
        comments->emplace_back(region.comment, region.clen);
    }

    // Build arrays using wrapped builders for numeric
    ARROW_ASSIGN_OR_RAISE(auto regionkey_array, build_int64_array_wrapped(regionkeys));

    // String arrays still use regular builder
    ARROW_ASSIGN_OR_RAISE(auto name_array, build_string_array(*names));
    ARROW_ASSIGN_OR_RAISE(auto comment_array, build_string_array(*comments));

    // Assemble RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        regionkey_array,
        name_array,
        comment_array
    };

    auto record_batch = arrow::RecordBatch::Make(schema, count, arrays);
    return ManagedRecordBatch(record_batch, lifetime_mgr);
}

} // namespace tpch
