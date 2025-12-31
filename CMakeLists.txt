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
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Build options
option(TPCH_BUILD_EXAMPLES "Build example applications" ON)
option(TPCH_BUILD_TESTS "Build unit tests" OFF)
option(TPCH_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(TPCH_ENABLE_ASYNC_IO "Enable async I/O with io_uring" OFF)

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
find_package(Arrow QUIET)
find_package(Parquet QUIET)
find_package(ORC QUIET)

# Print found status
if(Arrow_FOUND)
    message(STATUS "Arrow found: ${Arrow_LIBRARY}")
else()
    message(WARNING "Arrow not found - install with: sudo apt-get install libarrow-dev")
endif()

if(ORC_FOUND)
    message(STATUS "ORC found: ${ORC_LIBRARY}")
else()
    message(WARNING "ORC not found - install with: sudo apt-get install liborc-dev")
endif()

if(TPCH_ENABLE_ASYNC_IO)
    find_package(uring QUIET)
    if(NOT uring_FOUND)
        message(WARNING "liburing not found - install with: sudo apt-get install liburing-dev")
    endif()
endif()

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Source files for main library
set(TPCH_CORE_SOURCES
    src/writers/csv_writer.cpp
    src/writers/parquet_writer.cpp
    src/writers/orc_writer.cpp
    src/async/io_uring_context.cpp
    src/dbgen/dbgen_wrapper.cpp
)

# Create core library
add_library(tpch_core STATIC ${TPCH_CORE_SOURCES})
target_include_directories(tpch_core PUBLIC include)

# Link libraries only if found
if(Arrow_FOUND AND Parquet_FOUND)
    target_link_libraries(tpch_core PUBLIC
        Arrow::arrow
        Arrow::parquet
    )

    if(ORC_FOUND)
        target_link_libraries(tpch_core PUBLIC ORC::orc)
    endif()

    target_compile_definitions(tpch_core PUBLIC TPCH_DEPS_AVAILABLE)
else()
    message(STATUS "Building without external dependencies (development mode)")
    message(STATUS "  Arrow_FOUND: ${Arrow_FOUND}")
    message(STATUS "  Parquet_FOUND: ${Parquet_FOUND}")
    message(STATUS "  ORC_FOUND: ${ORC_FOUND}")
endif()

if(TPCH_ENABLE_ASYNC_IO AND uring_FOUND)
    target_link_libraries(tpch_core PUBLIC uring::uring)
    target_compile_definitions(tpch_core PUBLIC TPCH_ENABLE_ASYNC_IO)
endif()

# Main benchmark executable
add_executable(tpch_benchmark src/main.cpp)
target_link_libraries(tpch_benchmark PRIVATE tpch_core)

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
