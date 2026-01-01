cmake_minimum_required(VERSION 3.22)
project(tpch-cpp
    VERSION 0.1.0
    DESCRIPTION "High-performance TPC-H data generator with multiple format support"
    LANGUAGES CXX C
)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
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

# Compiler configuration
include(cmake/CompilerWarnings.cmake)

# Apply compiler flags based on configuration
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3 -march=native)
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

# Source files for main library
set(TPCH_CORE_SOURCES
    src/writers/csv_writer.cpp
    src/writers/parquet_writer.cpp
    src/async/io_uring_context.cpp
    # src/dbgen/dbgen_wrapper.cpp  # Deferred - dbgen integration pending
)

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
    # dbgen linking deferred - wrapper uses synthetic data generation instead
)

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

# Examples
if(TPCH_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Tests
if(TPCH_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Third-party: dbgen
add_subdirectory(third_party/dbgen)

# Install targets
install(TARGETS tpch_benchmark
    RUNTIME DESTINATION bin
)
install(DIRECTORY include/tpch/
    DESTINATION include/tpch
)
