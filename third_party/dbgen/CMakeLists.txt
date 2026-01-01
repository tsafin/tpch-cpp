# TPC-H dbgen integration
# Wraps the dbgen Makefile-based build system with CMake

# Check if dbgen source directory exists
set(DBGEN_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../tpch/dbgen")
if(NOT EXISTS "${DBGEN_SOURCE_DIR}")
    message(FATAL_ERROR "dbgen source not found at ${DBGEN_SOURCE_DIR}. Make sure git submodule is initialized: git submodule update --init --recursive")
endif()

# Custom build target for dbgen using its Makefile
# dbgen uses makefile.suite which needs to be copied to Makefile
add_custom_target(dbgen_build ALL
    COMMAND cp makefile.suite Makefile
    COMMAND ${CMAKE_MAKE_PROGRAM} CC=${CMAKE_C_COMPILER} CFLAGS="-O2 -std=c99" all
    WORKING_DIRECTORY "${DBGEN_SOURCE_DIR}"
    COMMENT "Building TPC-H dbgen"
)

# Create imported library target
add_library(dbgen STATIC IMPORTED)
set_target_properties(dbgen PROPERTIES
    IMPORTED_LOCATION "${DBGEN_SOURCE_DIR}/libdbgen.a"
    INTERFACE_INCLUDE_DIRECTORIES "${DBGEN_SOURCE_DIR}"
)
add_dependencies(dbgen dbgen_build)

message(STATUS "dbgen configured with source: ${DBGEN_SOURCE_DIR}")
