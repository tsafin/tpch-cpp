cmake_minimum_required(VERSION 3.22)
project(tpch-cpp
    VERSION 0.1.0
    DESCRIPTION "High-performance TPC-H data generator with multiple format support"
    LANGUAGES CXX C
)

# Set C++ standard (C++20 required for std::span in Phase 13.4 zero-copy)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set C standard for dbgen (C code)
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Add CMake modules directory
list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Build options
option(TPCH_BUILD_EXAMPLES "Build example applications" ON)
option(TPCH_BUILD_TESTS "Build unit tests" OFF)
option(TPCH_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(TPCH_ENABLE_ASYNC_IO "Enable async I/O with io_uring" OFF)
option(TPCH_ENABLE_ORC "Enable ORC file format support" OFF)
option(TPCH_ENABLE_PAIMON "Enable Apache Paimon table format support" OFF)
option(TPCH_ENABLE_ICEBERG "Enable Apache Iceberg table format support" OFF)
option(TPCH_ENABLE_LANCE "Enable Lance columnar format support (requires Rust)" OFF)
option(TPCH_ENABLE_PERF_COUNTERS "Enable performance counters instrumentation" OFF)

# Compiler configuration
include(cmake/CompilerWarnings.cmake)

# Apply compiler flags based on configuration
if(CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    # Enable aggressive optimizations with SIMD support
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(-O3 -march=native)

        # Enable auto-vectorization and report optimizations
        add_compile_options(
            -ftree-vectorize              # Enable loop vectorization
            -fopt-info-vec-optimized      # Report successful vectorizations
        )

        # Check for AVX2 support (preferred)
        include(CheckCXXCompilerFlag)
        check_cxx_compiler_flag("-mavx2" COMPILER_SUPPORTS_AVX2)

        if(COMPILER_SUPPORTS_AVX2)
            message(STATUS "Enabling AVX2 SIMD optimizations")
            add_compile_options(-mavx2 -mfma)
        else()
            # Fallback to SSE4.2 (required for SIMD string utils)
            check_cxx_compiler_flag("-msse4.2" COMPILER_SUPPORTS_SSE42)
            if(COMPILER_SUPPORTS_SSE42)
                message(STATUS "Enabling SSE4.2 SIMD optimizations")
                add_compile_options(-msse4.2)
            else()
                message(WARNING "No SIMD support detected - performance will be degraded")
            endif()
        endif()
    endif()
elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O0 -g)
endif()

if(TPCH_ENABLE_ASAN)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
endif()

# Find required packages from system
# Note: We need to find zstd before Arrow to avoid CMake configuration issues
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(ZSTD libzstd)
endif()

# Find dependencies before Arrow/Parquet to ensure targets are available
# and suppress warnings from Arrow's internal dependency checks
find_package(zstd REQUIRED)
set(zstdAlt_FOUND TRUE) # Prevent Arrow from trying to find zstd again

find_package(lz4 REQUIRED)
set(lz4Alt_FOUND TRUE)

find_package(re2 REQUIRED)
set(re2Alt_FOUND TRUE)

find_package(Thrift REQUIRED)
set(ThriftAlt_FOUND TRUE)

# Protobuf handling:
# - Arrow is built WITHOUT protobuf (-DARROW_WITH_PROTOBUF=OFF)
# - ORC is built with protobuf STATICALLY EMBEDDED (-DORC_PREFER_STATIC_PROTOBUF=ON)
# - We do NOT link protobuf separately to avoid conflicts with ORC's embedded protobuf
# - ORC's CMake target (ORC::orc) handles all protobuf dependencies internally

find_package(Snappy QUIET)
find_package(ZLIB QUIET)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

# Use local xsimd (modern version from third_party)
set(XSIMD_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/xsimd/include")
if(EXISTS "${XSIMD_INCLUDE_DIR}/xsimd/xsimd.hpp")
    message(STATUS "Using local xsimd from: ${XSIMD_INCLUDE_DIR}")
    set(xsimd_FOUND TRUE)
else()
    message(STATUS "xsimd not found in third_party - will use manual SIMD intrinsics")
    set(xsimd_FOUND FALSE)
endif()

if(TPCH_ENABLE_ORC)
    find_package(ORC QUIET)
endif()

# Paimon support: Note that our implementation uses Parquet as backing format
# and does not require paimon-cpp C++ library, so we don't need to find it
if(TPCH_ENABLE_PAIMON)
    set(Paimon_FOUND TRUE)
endif()

# Iceberg support: Similar to Paimon, our implementation uses Parquet as backing format
# and does not require iceberg-cpp C++ library, so we don't need to find it
if(TPCH_ENABLE_ICEBERG)
    set(Iceberg_FOUND TRUE)
endif()

# Lance support: Requires Rust toolchain to build FFI library
if(TPCH_ENABLE_LANCE)
    # Check for Rust toolchain
    find_program(Cargo_EXECUTABLE cargo)
    if(Cargo_EXECUTABLE)
        message(STATUS "Found Rust toolchain: ${Cargo_EXECUTABLE}")
        set(Lance_FOUND TRUE)
    else()
        message(FATAL_ERROR "Lance requested (TPCH_ENABLE_LANCE=ON) but Rust toolchain not found. "
                "Install with: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh")
    endif()
endif()

# Print found status
message(STATUS "Arrow found: ${Arrow_LIBRARY}")
message(STATUS "Parquet found: ${Parquet_LIBRARY}")

if(TPCH_ENABLE_ORC)
    if(ORC_FOUND)
        message(STATUS "ORC found: ${ORC_LIBRARY}")
    else()
        message(FATAL_ERROR "ORC requested (TPCH_ENABLE_ORC=ON) but not found - install with: sudo apt-get install liborc-dev")
    endif()
else()
    message(STATUS "ORC support disabled (TPCH_ENABLE_ORC=OFF)")
endif()

if(TPCH_ENABLE_PAIMON)
    message(STATUS "Paimon support enabled (uses Parquet backing format)")
else()
    message(STATUS "Paimon support disabled (TPCH_ENABLE_PAIMON=OFF)")
endif()

if(TPCH_ENABLE_ICEBERG)
    message(STATUS "Iceberg support enabled (uses Parquet backing format)")
else()
    message(STATUS "Iceberg support disabled (TPCH_ENABLE_ICEBERG=OFF)")
endif()

if(TPCH_ENABLE_LANCE)
    if(Lance_FOUND)
        message(STATUS "Lance support enabled (Rust FFI bridge)")
    else()
        message(FATAL_ERROR "Lance support requested but Rust toolchain not found")
    endif()
else()
    message(STATUS "Lance support disabled (TPCH_ENABLE_LANCE=OFF)")
endif()

if(TPCH_ENABLE_ASYNC_IO)
    find_package(Uring QUIET)
    if(Uring_FOUND)
        message(STATUS "Found liburing: ${Uring_LIBRARY}")
    else()
        message(WARNING "liburing not found - install with: sudo apt-get install liburing-dev")
    endif()
endif()

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Build dbgen objects
add_subdirectory(third_party/dbgen EXCLUDE_FROM_ALL)
include_directories(${DBGEN_INCLUDE_DIRS})

# Copy TPC-H distribution file to build directory
# Required by dbgen for loading nations, regions, and other lookup tables
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/tpch/dbgen/dists.dss"
    "${CMAKE_BINARY_DIR}/dists.dss"
    COPYONLY
)
message(STATUS "Copied dists.dss to build directory")

# Build Rust FFI library for Lance support (if enabled)
if(TPCH_ENABLE_LANCE AND Lance_FOUND)
    set(LANCE_FFI_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/lance-ffi")
    set(LANCE_FFI_BUILD_DIR "${CMAKE_BINARY_DIR}/rust")

    # Determine output library path based on platform and Rust target
    if(APPLE)
        set(RUST_TARGET "aarch64-apple-darwin")
        set(LANCE_FFI_LIB "${LANCE_FFI_BUILD_DIR}/${RUST_TARGET}/release/liblance_ffi.a")
    else()
        # Use default native build for Linux (x86_64-unknown-linux-gnu)
        set(RUST_TARGET "")
        set(LANCE_FFI_LIB "${LANCE_FFI_BUILD_DIR}/release/liblance_ffi.a")
    endif()

    set(LANCE_FFI_LIB_FINAL "${CMAKE_BINARY_DIR}/liblance_ffi.a")

    # Check for snap Rust - both cargo and rustc should come from snap together
    find_program(SNAP_RUSTC rustc PATHS /snap/bin NO_DEFAULT_PATH)
    if(SNAP_RUSTC)
        message(STATUS "Detected Rust from snap: ${SNAP_RUSTC}")
    else()
        message(STATUS "Using system Rust compiler")
    endif()

    # Build Rust FFI library using cargo
    # Note: Cargo target directory is set to build/rust to keep artifacts out of source tree
    # All build artifacts go to CMAKE_BINARY_DIR/rust, NOT source tree
    # On macOS, we specify the target triple to ensure correct output directory
    set(CARGO_BUILD_CMD "${Cargo_EXECUTABLE}" build --release
            --manifest-path "${LANCE_FFI_DIR}/Cargo.toml"
            --target-dir "${LANCE_FFI_BUILD_DIR}")

    if(RUST_TARGET)
        list(APPEND CARGO_BUILD_CMD --target "${RUST_TARGET}")
    endif()

    add_custom_command(
        OUTPUT "${LANCE_FFI_LIB_FINAL}"
        COMMAND ${CMAKE_COMMAND} -E echo "Building Lance FFI library with Rust cargo ..."
        COMMAND ${CMAKE_COMMAND} -E make_directory "${LANCE_FFI_BUILD_DIR}"
        COMMAND ${CARGO_BUILD_CMD}
        COMMAND ${CMAKE_COMMAND} -E copy "${LANCE_FFI_LIB}" "${LANCE_FFI_LIB_FINAL}"
        WORKING_DIRECTORY "${LANCE_FFI_DIR}"
        DEPENDS "${LANCE_FFI_DIR}/Cargo.toml"
                "${LANCE_FFI_DIR}/Cargo.lock"
                "${LANCE_FFI_DIR}/src/lib.rs"
        COMMENT "Compiling Lance FFI library with Rust (target: ${RUST_TARGET})"
        VERBATIM
    )

    # Create custom target for Rust build
    add_custom_target(lance_ffi ALL DEPENDS "${LANCE_FFI_LIB_FINAL}")

    # Import the Rust library as an external target
    add_library(lance_ffi_lib STATIC IMPORTED GLOBAL)
    set_target_properties(lance_ffi_lib PROPERTIES
        IMPORTED_LOCATION "${LANCE_FFI_LIB_FINAL}"
    )
    add_dependencies(lance_ffi_lib lance_ffi)

    message(STATUS "Rust FFI build configured for Lance support")
    message(STATUS "  Rust target directory: ${LANCE_FFI_BUILD_DIR}")
    message(STATUS "  Output library: ${LANCE_FFI_LIB_FINAL}")
endif()

# Source files for main library
set(TPCH_CORE_SOURCES
    src/writers/csv_writer.cpp
    src/writers/parquet_writer.cpp
    src/multi_table_writer.cpp
    src/dbgen/dbgen_wrapper.cpp
    src/dbgen/dbgen_converter.cpp
    src/dbgen/zero_copy_converter.cpp  # Phase 13.4: Zero-copy optimizations
    src/util/builder_pool.cpp
    ${DBGEN_OBJECTS}
)

# Add async IO sources only if enabled
if(TPCH_ENABLE_ASYNC_IO)
    list(APPEND TPCH_CORE_SOURCES
        src/async/io_uring_context.cpp
        src/async/shared_async_io.cpp
    )
else()
    # Use stub implementation when async IO is disabled
    list(APPEND TPCH_CORE_SOURCES
        src/async/async_io_stub.cpp
    )
endif()

# Add ORC writer if enabled and available
if(TPCH_ENABLE_ORC AND ORC_FOUND)
    list(APPEND TPCH_CORE_SOURCES
        src/writers/orc_writer.cpp
    )
endif()

# Add Paimon writer if enabled and available
if(TPCH_ENABLE_PAIMON AND Paimon_FOUND)
    list(APPEND TPCH_CORE_SOURCES
        src/writers/paimon_writer.cpp
    )
endif()

# Add Iceberg writer if enabled and available
if(TPCH_ENABLE_ICEBERG AND Iceberg_FOUND)
    list(APPEND TPCH_CORE_SOURCES
        src/writers/iceberg_writer.cpp
    )
endif()

# Add Lance writer if enabled and available
if(TPCH_ENABLE_LANCE AND Lance_FOUND)
    list(APPEND TPCH_CORE_SOURCES
        src/writers/lance_writer.cpp
    )
endif()

# Create core library
add_library(tpch_core STATIC ${TPCH_CORE_SOURCES})
target_include_directories(tpch_core PUBLIC include)

# Link required libraries (Arrow and Parquet are mandatory)
target_link_libraries(tpch_core PUBLIC
    Arrow::arrow
    Arrow::parquet
    dbgen_objs
)

# Link xsimd if available (header-only library)
if(xsimd_FOUND)
    target_include_directories(tpch_core PUBLIC ${XSIMD_INCLUDE_DIR})
    target_compile_definitions(tpch_core PUBLIC TPCH_USE_XSIMD)
    message(STATUS "xsimd SIMD optimizations enabled")
endif()

# Link ORC if enabled and available
# Note: ORC was built with static protobuf (-DORC_PREFER_STATIC_PROTOBUF=ON)
# ORC's static library (.a) requires explicit protobuf linking even though
# protobuf is embedded, because the symbols are referenced by ORC object files
if(TPCH_ENABLE_ORC AND ORC_FOUND)
    target_link_libraries(tpch_core PRIVATE ORC::orc)

    # Link protobuf - required for ORC static library symbols
    # This does NOT cause conflicts because we don't use protobuf ourselves
    # and ORC's protobuf definitions are self-contained
    find_package(Protobuf QUIET)
    if(Protobuf_FOUND)
        if(TARGET protobuf::libprotobuf)
            target_link_libraries(tpch_core PRIVATE protobuf::libprotobuf)
        elseif(Protobuf_LIBRARIES)
            target_link_libraries(tpch_core PRIVATE ${Protobuf_LIBRARIES})
        endif()
    endif()
endif()

# Paimon linking not needed - uses Parquet backing format directly

# Link Lance FFI library if enabled
if(TPCH_ENABLE_LANCE AND Lance_FOUND)
    target_link_libraries(tpch_core PRIVATE lance_ffi_lib)

    # Lance FFI requires Rust runtime libraries
    # These are typically available on Unix-like systems
    find_library(DL_LIB NAMES dl)
    if(DL_LIB)
        target_link_libraries(tpch_core PRIVATE ${DL_LIB})
    endif()

    find_library(PTHREAD_LIB NAMES pthread)
    if(PTHREAD_LIB)
        target_link_libraries(tpch_core PRIVATE ${PTHREAD_LIB})
    endif()

    find_library(M_LIB NAMES m)
    if(M_LIB)
        target_link_libraries(tpch_core PRIVATE ${M_LIB})
    endif()
endif()

# Arrow has dependencies on system libraries that need to be linked AFTER ORC
# Link compression libraries for Arrow's support
find_library(ZLIB_LIB NAMES z)
if(ZLIB_LIB)
    target_link_libraries(tpch_core PRIVATE ${ZLIB_LIB})
endif()

find_library(ZSTD_LIB NAMES zstd)
if(ZSTD_LIB)
    target_link_libraries(tpch_core PRIVATE ${ZSTD_LIB})
endif()

find_library(LZ4_LIB NAMES lz4)
if(LZ4_LIB)
    target_link_libraries(tpch_core PRIVATE ${LZ4_LIB})
endif()

# Snappy must come at the very end due to linker symbol ordering requirements
find_library(SNAPPY_LIB NAMES snappy)
if(SNAPPY_LIB)
    target_link_libraries(tpch_core PRIVATE ${SNAPPY_LIB})
endif()


# Define compilation flags for enabled features
target_compile_definitions(tpch_core PUBLIC TPCH_DEPS_AVAILABLE)

if(TPCH_ENABLE_ORC)
    target_compile_definitions(tpch_core PUBLIC TPCH_ENABLE_ORC)
endif()

if(TPCH_ENABLE_PAIMON)
    target_compile_definitions(tpch_core PUBLIC TPCH_ENABLE_PAIMON)
endif()

if(TPCH_ENABLE_ICEBERG)
    target_compile_definitions(tpch_core PUBLIC TPCH_ENABLE_ICEBERG)
endif()

if(TPCH_ENABLE_LANCE)
    target_compile_definitions(tpch_core PUBLIC TPCH_ENABLE_LANCE)
endif()

if(TPCH_ENABLE_ASYNC_IO AND Uring_FOUND)
    target_link_libraries(tpch_core PUBLIC Uring::uring)
    target_compile_definitions(tpch_core PUBLIC TPCH_ENABLE_ASYNC_IO)
endif()

if(TPCH_ENABLE_PERF_COUNTERS)
    target_compile_definitions(tpch_core PUBLIC TPCH_ENABLE_PERF_COUNTERS)
    message(STATUS "Performance counters enabled")
endif()

# Main benchmark executable
add_executable(tpch_benchmark src/main.cpp)
target_link_libraries(tpch_benchmark PRIVATE tpch_core)

# Ensure main executable sees the same compilation flags
if(TPCH_ENABLE_ORC)
    target_compile_definitions(tpch_benchmark PRIVATE TPCH_ENABLE_ORC)
endif()
if(TPCH_ENABLE_PAIMON)
    target_compile_definitions(tpch_benchmark PRIVATE TPCH_ENABLE_PAIMON)
endif()
if(TPCH_ENABLE_ICEBERG)
    target_compile_definitions(tpch_benchmark PRIVATE TPCH_ENABLE_ICEBERG)
endif()
if(TPCH_ENABLE_LANCE)
    target_compile_definitions(tpch_benchmark PRIVATE TPCH_ENABLE_LANCE)
endif()
if(TPCH_ENABLE_ASYNC_IO AND Uring_FOUND)
    target_compile_definitions(tpch_benchmark PRIVATE TPCH_ENABLE_ASYNC_IO)
endif()
if(TPCH_ENABLE_PERF_COUNTERS)
    target_compile_definitions(tpch_benchmark PRIVATE TPCH_ENABLE_PERF_COUNTERS)
endif()

# Examples
if(TPCH_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Tests
if(TPCH_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Install targets
install(TARGETS tpch_benchmark
    RUNTIME DESTINATION bin
)
install(DIRECTORY include/tpch/
    DESTINATION include/tpch
)
