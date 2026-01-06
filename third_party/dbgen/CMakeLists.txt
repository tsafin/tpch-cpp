# TPC-H dbgen integration
# Compile dbgen object files and provide them for linking

# Check if dbgen source directory exists
set(DBGEN_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../tpch/dbgen")
if(NOT EXISTS "${DBGEN_SOURCE_DIR}")
    message(FATAL_ERROR "dbgen source not found at ${DBGEN_SOURCE_DIR}. Make sure git submodule is initialized: git submodule update --init --recursive")
endif()

# Get all dbgen C source files except those with main() functions
file(GLOB DBGEN_SOURCES "${DBGEN_SOURCE_DIR}/*.c")

# Filter out files that define main() or are not needed for row generation
list(FILTER DBGEN_SOURCES EXCLUDE REGEX "(driver|qgen|dss_pc|print|tpch_kit)\\.c$")

# Create object library with dbgen sources
add_library(dbgen_objs OBJECT ${DBGEN_SOURCES})
target_include_directories(dbgen_objs PUBLIC "${DBGEN_SOURCE_DIR}")
set_target_properties(dbgen_objs PROPERTIES C_STANDARD 99 C_EXTENSIONS OFF)

# Expose the objects and include path
set(DBGEN_OBJECTS $<TARGET_OBJECTS:dbgen_objs> PARENT_SCOPE)
set(DBGEN_INCLUDE_DIRS "${DBGEN_SOURCE_DIR}" PARENT_SCOPE)

message(STATUS "dbgen configured with source: ${DBGEN_SOURCE_DIR}")
message(STATUS "dbgen object library created from sources")
