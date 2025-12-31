# TPC-H C++ Data Generator

A high-performance TPC-H data generator with multiple output format support (Parquet, ORC, CSV) and optional asynchronous I/O capabilities using Linux io_uring.

## Features

- **Multiple Output Formats**
  - Apache Parquet (columnar, compressed)
  - Apache ORC (columnar, optimized for Hive/Spark)
  - CSV (row-oriented, human-readable)

- **Apache Arrow Integration**
  - Central in-memory columnar representation
  - Unified API for all output formats
  - Zero-copy conversions between formats

- **Optional Async I/O**
  - Linux io_uring support for high-throughput writes
  - Optional feature (graceful fallback to synchronous I/O)
  - 20-50% throughput improvement over synchronous writes

- **TPC-H Reference Implementation**
  - Official dbgen integration via git submodule
  - All 8 TPC-H tables supported (lineitem, orders, customer, part, partsupp, supplier, nation, region)
  - Configurable scale factors (1, 10, 100, 1000, ...)

- **Performance-Focused**
  - Target: 1M+ rows/second for lineitem table
  - Cross-architecture support considerations
  - Benchmarking harness included

## Quick Start

### Prerequisites

- **OS**: Linux (WSL2 supported)
- **Compiler**: GCC 11+ or Clang 13+
- **CMake**: 3.22+
- **Packages**: libarrow-dev, libparquet-dev, liborc-dev

### Installation

Install system dependencies:

```bash
./scripts/install_deps.sh
```

### Build

```bash
# Configure
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..

# Build
make -j$(nproc)

# Optional: Install
make install
```

### Usage

Generate TPC-H data in Parquet format:

```bash
./tpch_benchmark --scale-factor 1 --format parquet --output data/
```

With async I/O (if enabled):

```bash
./tpch_benchmark --scale-factor 1 --format parquet --output data/ --async-io
```

See `./tpch_benchmark --help` for all options.

## Project Structure

```
tpch-cpp/
├── CMakeLists.txt              # Root build configuration
├── README.md                   # This file
├── .gitignore                  # Git ignore patterns
├── cmake/                      # CMake modules
│   ├── FindArrow.cmake         # Apache Arrow discovery
│   ├── FindORC.cmake           # Apache ORC discovery
│   └── CompilerWarnings.cmake  # Compiler configuration
├── include/tpch/               # Public headers
│   ├── writer_interface.hpp
│   ├── parquet_writer.hpp
│   ├── csv_writer.hpp
│   ├── orc_writer.hpp
│   ├── async_io.hpp
│   └── dbgen_wrapper.hpp
├── src/                        # Implementation
│   ├── writers/
│   │   ├── parquet_writer.cpp
│   │   ├── csv_writer.cpp
│   │   └── orc_writer.cpp
│   ├── async/
│   │   └── io_uring_context.cpp
│   ├── dbgen/
│   │   └── dbgen_wrapper.cpp
│   └── main.cpp                # Benchmark driver
├── examples/                   # Standalone examples
│   ├── simple_arrow_parquet.cpp
│   ├── simple_csv.cpp
│   ├── simple_orc.cpp
│   ├── async_io_demo.cpp
│   └── CMakeLists.txt
├── third_party/                # External dependencies
│   └── dbgen/                  # TPC-H dbgen (git submodule)
├── tests/                      # Unit tests
└── scripts/                    # Helper scripts
    ├── install_deps.sh         # Dependency installation
    └── benchmark.sh            # Benchmarking harness
```

## Build Options

Configure with CMake:

```bash
cmake -DCMAKE_BUILD_TYPE=Release \
      -DTPCH_BUILD_EXAMPLES=ON \
      -DTPCH_ENABLE_ASAN=OFF \
      -DTPCH_ENABLE_ASYNC_IO=ON \
      ..
```

| Option | Default | Description |
|--------|---------|-------------|
| `TPCH_BUILD_EXAMPLES` | ON | Build example applications |
| `TPCH_BUILD_TESTS` | OFF | Build unit tests |
| `TPCH_ENABLE_ASAN` | OFF | Enable AddressSanitizer |
| `TPCH_ENABLE_ASYNC_IO` | OFF | Enable async I/O with io_uring |

## Dependencies

### Required

| Library | Version | Ubuntu Package |
|---------|---------|----------------|
| Apache Arrow | >= 10.0 | libarrow-dev |
| Apache Parquet | >= 10.0 | libparquet-dev |
| Apache ORC | >= 1.8 | liborc-dev |
| CMake | >= 3.22 | cmake |
| GCC/Clang | >= 11 | build-essential |

### Optional

| Library | Version | Ubuntu Package | Purpose |
|---------|---------|----------------|---------|
| liburing | >= 2.1 | liburing-dev | Async I/O (optional) |

## Performance Targets

- **Lineitem (largest table)**: 1M+ rows/second
- **All tables average**: 500K+ rows/second
- **Parquet write rate**: > 100 MB/second
- **ORC write rate**: > 100 MB/second
- **CSV write rate**: > 50 MB/second
- **Async I/O improvement**: 20-50% over synchronous

## Development

### Running Examples

After building with `TPCH_BUILD_EXAMPLES=ON`:

```bash
# Parquet example
./examples/simple_arrow_parquet

# CSV example
./examples/simple_csv

# ORC example
./examples/simple_orc

# Async I/O demo (if enabled)
./examples/async_io_demo
```

### Benchmarking

Use the included benchmarking harness:

```bash
./scripts/benchmark.sh
```

This runs comprehensive benchmarks across all scale factors and formats.

## Validation

- Output files readable by standard tools:
  - Parquet: `pyarrow.parquet.read_table()`
  - ORC: Apache Spark, `orc.read()`
  - CSV: pandas, Excel, awk, etc.
- Round-trip testing: write → read → verify data integrity
- Performance benchmarks: throughput and scalability analysis

## Architecture Notes

### Apache Arrow Central Format

Uses Arrow as the central in-memory columnar format:
- Unified API across all output formats
- Zero-copy conversions
- Industry standard for analytics
- Better memory efficiency than row-oriented

### C++17 Standard

- Full C++17 support in GCC 11.4
- Modern features without C++20 complexity
- Smart pointers, optional, structured bindings

### CMake Build System

- Multiple external dependencies
- Cross-platform potential
- Clean configuration management

### Modular Writer Interface

- Abstract `WriterInterface` base class
- Format-specific implementations (Parquet, ORC, CSV)
- Easy to extend with new formats
- Runtime polymorphism for format selection

### Optional Async I/O

- Linux io_uring support for high-throughput I/O
- Graceful fallback to synchronous I/O
- Compile-time flag for portability
- 20-50% throughput improvement target

## Future Enhancements

- Additional formats: Avro, Arrow IPC, Protobuf
- Distributed parallel generation
- Query integration with DuckDB/Polars
- Direct I/O (O_DIRECT) support
- Advanced observability and metrics

## Contributing

See the main monorepo documentation for contribution guidelines.

## License

See LICENSE file (inherits from monorepo)

## Contact

For questions or issues, contact the maintainers at the Database Internals meetups.
