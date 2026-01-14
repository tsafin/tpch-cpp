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

find_package(Protobuf REQUIRED)
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

# Print found status
message(STATUS "Arrow found: ${Arrow_LIBRARY}")
message(STATUS "Parquet found: ${Parquet_LIBRARY}")

if(TPCH_ENABLE_ORC)
    if(ORC_FOUND)
        message(STATUS "ORC found: ${ORC_LIBRARY}")
        if(NOT Protobuf_FOUND)
            message(WARNING "Protobuf not found - ORC may not link correctly")
        endif()
    else()
        message(FATAL_ERROR "ORC requested (TPCH_ENABLE_ORC=ON) but not found - install with: sudo apt-get install liborc-dev")
    endif()
else()
    message(STATUS "ORC support disabled (TPCH_ENABLE_ORC=OFF)")
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
# ORC references protobuf symbols, so we must link protobuf
if(TPCH_ENABLE_ORC AND ORC_FOUND)
    target_link_libraries(tpch_core PRIVATE ORC::orc)

    if(TARGET protobuf::libprotobuf)
        target_link_libraries(tpch_core PRIVATE protobuf::libprotobuf)
    elseif(TARGET Protobuf::protobuf)
        target_link_libraries(tpch_core PRIVATE Protobuf::protobuf)
    else()
        target_link_libraries(tpch_core PRIVATE ${Protobuf_LIBRARIES})
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
