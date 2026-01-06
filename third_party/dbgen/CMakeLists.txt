# TPC-H dbgen integration
# Compile core dbgen object files with proper configuration for embedded use

# Check if dbgen source directory exists
set(DBGEN_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../tpch/dbgen")
if(NOT EXISTS "${DBGEN_SOURCE_DIR}")
    message(FATAL_ERROR "dbgen source not found at ${DBGEN_SOURCE_DIR}. Make sure git submodule is initialized: git submodule update --init --recursive")
endif()

# Core dbgen sources (exclude driver, qgen, text, and print utilities)
# We exclude text.c since we don't generate text descriptions
# We exclude print.c since we convert directly to Arrow
set(DBGEN_CORE_SOURCES
    ${DBGEN_SOURCE_DIR}/build.c
    ${DBGEN_SOURCE_DIR}/bm_utils.c
    ${DBGEN_SOURCE_DIR}/rnd.c
    ${DBGEN_SOURCE_DIR}/rng64.c
    ${DBGEN_SOURCE_DIR}/permute.c
    ${DBGEN_SOURCE_DIR}/speed_seed.c
    ${DBGEN_SOURCE_DIR}/bcd2.c
    ${DBGEN_SOURCE_DIR}/tpch_init.c
    "${CMAKE_SOURCE_DIR}/src/dbgen/dbgen_stubs.c"
)

# Create object library with core dbgen sources
add_library(dbgen_objs OBJECT ${DBGEN_CORE_SOURCES})

# Critical: Define LINUX and TPCH for proper compilation
# These ensure DSS_HUGE is correctly defined and TPCH-specific features are enabled
# EMBEDDED_DBGEN disables multi-stream parallel code that requires external tdefs array
target_compile_definitions(dbgen_objs PRIVATE
    LINUX=1
    TPCH=1
    EMBEDDED_DBGEN=1
)

# Include path for all dbgen headers
target_include_directories(dbgen_objs PUBLIC "${DBGEN_SOURCE_DIR}")

# Set C standard and ensure no extensions
set_target_properties(dbgen_objs PROPERTIES
    C_STANDARD 99
    C_EXTENSIONS OFF
)

# Expose the objects and include path to parent CMakeLists.txt
set(DBGEN_OBJECTS $<TARGET_OBJECTS:dbgen_objs> PARENT_SCOPE)
set(DBGEN_INCLUDE_DIRS "${DBGEN_SOURCE_DIR}" PARENT_SCOPE)

message(STATUS "dbgen configured:")
message(STATUS "  Source directory: ${DBGEN_SOURCE_DIR}")
message(STATUS "  Core sources: ${DBGEN_CORE_SOURCES}")
message(STATUS "  Configuration: LINUX=1, TPCH=1")
message(STATUS "  Object library: dbgen_objs")
