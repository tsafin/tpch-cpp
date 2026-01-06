# Phase 9: Real DBGen Integration - Planning Documents

This directory contains comprehensive planning documentation for Phase 9: Real DBGen Integration & Scale Factor Support.

---

## 📚 Documentation Map

### Start Here → [PHASE_9_SUMMARY.md](PHASE_9_SUMMARY.md)
**5-minute executive summary**
- What we're building
- Why this matters
- Key insight (callback pattern)
- Risk assessment
- Success criteria

⬇️

### Then Read → [PHASE_9_PLAN.md](PHASE_9_PLAN.md)
**Detailed technical specification**
- Current state analysis (what works, what's missing)
- Architecture: how dbgen_wrapper connects to writers
- 7-step implementation plan
- Test matrix
- Converter implementation details
- Rollback plan
- Success criteria & blockers

⬇️

### For Implementation → [QUICK_REFERENCE.md](QUICK_REFERENCE.md)
**Copy-paste guide with code snippets**
- File changes summary
- Step-by-step implementation with code
- Testing commands
- Debugging tips
- Performance notes

⬇️

### For Deep Dive → [INTEGRATION_ARCHITECTURE.md](INTEGRATION_ARCHITECTURE.md)
**System design & data flow**
- Detailed architecture diagrams
- Data flow: callback pattern
- Component interaction
- Call sequences with line numbers
- C struct definitions
- Type conversions
- Performance expectations
- Integration checklist

---

## 🎯 Quick Navigation

**If you want to...**

- **Understand what we're building**: Read PHASE_9_SUMMARY.md (5 min)
- **See the full specification**: Read PHASE_9_PLAN.md (20 min)
- **Get implementation code**: Use QUICK_REFERENCE.md (copy-paste)
- **Understand the system design**: Read INTEGRATION_ARCHITECTURE.md (15 min)
- **Debug a specific issue**: Check QUICK_REFERENCE.md Debugging Tips

---

## 📋 Document Contents at a Glance

| Document | Length | Audience | Purpose |
|----------|--------|----------|---------|
| PHASE_9_SUMMARY.md | 500 lines | Managers, Architects | High-level overview & decisions |
| PHASE_9_PLAN.md | 700 lines | Architects, Lead Engineers | Complete specification |
| QUICK_REFERENCE.md | 600 lines | Engineers | Implementation guide |
| INTEGRATION_ARCHITECTURE.md | 600 lines | Engineers, Reviewers | System design details |

---

## 🔄 The Big Picture

### Phase 8 (Previous)
```
tpch_benchmark → hardcoded synthetic data → Parquet/CSV
(~0.02 sec, fake data)
```

### Phase 9 (This Phase)
```
tpch_benchmark → DBGenWrapper → official dbgen → Arrow builders → Parquet/CSV
(~5 sec, authentic TPC-H data)
```

### What Exists
- ✅ dbgen_wrapper.hpp/cpp (complete, Phase 7)
- ✅ libdbgen.a (compiled, Phase 8.2)
- ✅ Arrow/Parquet writers (working, Phases 1-3)
- ✅ CLI infrastructure (existing, Phase 8.3)

### What We Need to Add
- ❌ Converter functions (C struct → Arrow builders)
- ❌ Main.cpp integration logic
- ❌ Multi-table dispatch
- ❌ Schema routing

---

## 📝 Implementation at a Glance

### Files to Create
```
include/tpch/dbgen_converter.hpp      80 lines   (function declarations)
src/dbgen/dbgen_converter.cpp         400 lines  (implementation)
```

### Files to Modify
```
CMakeLists.txt                        2 lines    (add compilation flags)
src/main.cpp                          300 lines  (integration logic)
```

### Total New Code
```
~500-700 lines across 4 files
```

---

## 🚀 Getting Started

### 1. Read the Overview
```bash
cat PHASE_9_SUMMARY.md  # 5 minutes
```

### 2. Review the Plan
```bash
cat PHASE_9_PLAN.md     # 20 minutes
```

### 3. Follow the Quick Reference
```bash
cat QUICK_REFERENCE.md  # Use as copy-paste guide
```

### 4. Build & Test
```bash
# See QUICK_REFERENCE.md "Testing Commands" section
cd /home/tsafin/src/tpch-cpp
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
./tpch_benchmark --use-dbgen --scale-factor 1 --table lineitem --verbose
```

---

## ✅ Success Criteria

After Phase 9 is complete, ALL of these should be true:

- [ ] Lineitem generation: 6,000,000 rows at SF=1
- [ ] All 8 TPC-H tables supported
- [ ] Row counts match TPC-H specification
- [ ] Output files (Parquet/CSV) are valid
- [ ] Performance: ≥1M rows/second
- [ ] Scale factors work: SF=1, 10, 100
- [ ] No data corruption (verified)
- [ ] Backward compatibility maintained

---

## 🔍 Key Concepts

### 1. Callback Pattern (The Core Innovation)
Instead of trying to integrate dbgen directly, use callbacks:
```cpp
dbgen.generate_lineitem([&](const void* row) {
    append_row_to_builders(row, builders);  // Convert to Arrow
});
```

### 2. Type Conversions
| C Type | Arrow Type | Notes |
|--------|-----------|-------|
| int64 | int64 | Direct |
| char[N] | utf8 | String conversion |
| quantity (stored as int) | float64 | Divide by 100 |

### 3. Batching Strategy
- Accumulate rows in Arrow builders (10K rows per batch)
- When batch full: convert builders to RecordBatch → write to file
- Reset builders for next batch
- Reduces memory while maintaining I/O efficiency

### 4. Scale Factor Support
Row counts scale linearly with TPC-H formulas:
```
SF=1:   lineitem=6M, orders=1.5M, customer=150K, ...
SF=10:  lineitem=60M, orders=15M, customer=1.5M, ...
SF=100: lineitem=600M, orders=150M, customer=15M, ...
```

---

## 📊 Dependency Graph

```
Phase 9: Real DBGen Integration
    ├─ Depends on Phase 8.2 (libdbgen.a compiled) ✅
    ├─ Depends on Phase 7 (dbgen_wrapper complete) ✅
    ├─ Depends on Phases 1-3 (Arrow/Parquet) ✅
    ├─ Depends on Phase 8.3 (CLI infrastructure) ✅
    │
    └─ Enables Phase 10 (Distributed/Parallel generation)
        └─ Enables Phase 11 (Query Integration & Optimization)
```

---

## 🧪 Testing Strategy

### Phase 9.1: Connectivity
- Test: Callback mechanism works
- Test: Row counting works
- Output: Any data (doesn't need to be correct)

### Phase 9.2: Lineitem
- Test: 6M rows generated for SF=1
- Test: Schema is correct
- Test: Row count verified with pyarrow
- Output: Valid Parquet file

### Phase 9.3: All Tables
- Test: Each table generates correct row count
- Test: Schemas match specification
- Output: 8 valid Parquet files

### Phase 9.4: Scale Factors
- Test: SF=1, 10, 100 all work
- Test: Row counts scale linearly
- Output: Correct row counts at each SF

### Phase 9.5: Format Support
- Test: CSV format
- Test: Parquet format
- Test: ORC format (if enabled)
- Output: All formats readable by standard tools

---

## 🐛 Debugging Quick Links

| Problem | See |
|---------|-----|
| Compilation errors | QUICK_REFERENCE.md: "If compilation fails" |
| Wrong row counts | QUICK_REFERENCE.md: "If row counts are wrong" |
| Data corruption | QUICK_REFERENCE.md: "If data is corrupted" |
| Unreadable output | QUICK_REFERENCE.md: "If Parquet file is unreadable" |

---

## 📈 Performance Notes

For lineitem at SF=1 (6M rows):

| Phase | Time | Throughput | Data Source |
|-------|------|-----------|------------|
| Phase 8 (Synthetic) | 0.02 sec | N/A | Hardcoded |
| Phase 9 (Real dbgen) | 5-10 sec | 1M rows/sec | Official TPC-H |

Phase 9 is ~250x slower than synthetic, but produces authentic benchmark data.

---

## 📞 Questions?

Refer to specific document:

1. **"What are we building?"** → PHASE_9_SUMMARY.md
2. **"How does it work?"** → INTEGRATION_ARCHITECTURE.md
3. **"How do I build it?"** → QUICK_REFERENCE.md
4. **"What are the details?"** → PHASE_9_PLAN.md

---

## 📦 Related Files

Outside these planning documents, also reference:

- `include/tpch/dbgen_wrapper.hpp` - Complete DBGenWrapper API
- `src/dbgen/dbgen_wrapper.cpp` - Implementation details
- `third_party/dbgen/Makefile` - dbgen build configuration
- `CMakeLists.txt` - Build system configuration
- `TODO.md` - Project progress tracking

---

## ✨ Key Achievements of Planning

✅ Comprehensive specification (7-step plan)
✅ Clear implementation roadmap
✅ Ready-to-use code snippets
✅ Test strategy & validation approach
✅ Risk assessment & mitigation
✅ Performance expectations set
✅ Debugging guide provided
✅ Success criteria defined

**All planning is complete. Ready for implementation.** 🚀

---

## 📄 Document Version

Created: January 6, 2026
Version: 1.0
Status: Ready for Review

For latest updates, check git history:
```bash
git log --oneline -- PHASE_9_*
```

