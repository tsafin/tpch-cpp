// Unit test: verify BatchIteratorImpl preserves exact row sequence

#include <gtest/gtest.h>

#include <cstring>
#include <iostream>
#include <vector>

#include "tpch/dbgen_wrapper.hpp"

using namespace tpch;

TEST(DBGenBatchIterator, LineitemBatchBoundary) {
    // Use scale factor 1 and request 11000 rows to force batch boundary at 10000
    DBGenWrapper dbgen_base(1, false);
    DBGenWrapper dbgen_iter(1, false);

    // Use smaller batch to keep test quick while exercising the boundary
    const long max_rows = 1100;
    const size_t batch_size = 1000;

    // Baseline: generate rows via callback
    
    std::vector<line_t> baseline;
    baseline.reserve(max_rows);
    dbgen_base.generate_lineitem([&](const void* row) { baseline.push_back(*static_cast<const line_t*>(row)); },
                                 max_rows);
    

    // Now generate using batch iterator with a smaller batch_size to force boundary
    
    auto iter = dbgen_iter.generate_lineitem_batches(batch_size, max_rows);
    std::vector<line_t> batched;
    batched.reserve(max_rows);
    size_t batch_idx = 0;
    while (iter.has_next()) {
        auto batch = iter.next();
        ++batch_idx;
        
        for (const auto& r : batch.rows) {
            batched.push_back(r);
        }
    }
    

    ASSERT_EQ(baseline.size(), batched.size());

    for (size_t i = 0; i < baseline.size(); ++i) {
        int cmp = std::memcmp(&baseline[i], &batched[i], sizeof(line_t));
        if (cmp != 0) {
            const auto& a = baseline[i];
            const auto& b = batched[i];
            std::cerr << "Row mismatch at index " << i << "\n";
            std::cerr << " baseline: okey=" << a.okey << " partkey=" << a.partkey << " suppkey=" << a.suppkey
                      << " lcnt=" << a.lcnt << " qty=" << a.quantity << " eprice=" << a.eprice << " disc=" << a.discount
                      << " tax=" << a.tax << " rflag=" << a.rflag[0] << " lstatus=" << a.lstatus[0]
                      << " cdate=" << a.cdate << " sdate=" << a.sdate << " rdate=" << a.rdate
                      << " comment=" << a.comment << "\n";
            std::cerr << " batched : okey=" << b.okey << " partkey=" << b.partkey << " suppkey=" << b.suppkey
                      << " lcnt=" << b.lcnt << " qty=" << b.quantity << " eprice=" << b.eprice << " disc=" << b.discount
                      << " tax=" << b.tax << " rflag=" << b.rflag[0] << " lstatus=" << b.lstatus[0]
                      << " cdate=" << b.cdate << " sdate=" << b.sdate << " rdate=" << b.rdate
                      << " comment=" << b.comment << "\n";
            std::cerr << " baseline clen=" << a.clen << " batched clen=" << b.clen
                      << " sizeof(line_t)=" << sizeof(line_t) << "\n";
        }
        ASSERT_EQ(cmp, 0) << "Row mismatch at index " << i;
    }
}
