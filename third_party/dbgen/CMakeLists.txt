# TPC-H dbgen integration
# Wraps the dbgen Makefile-based build system with CMake

# Check if dbgen source directory exists
# The dbgen source is in third_party/tpch/dbgen (from the git submodule)
set(DBGEN_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../tpch/dbgen")
set(DBGEN_LIB "${DBGEN_SOURCE_DIR}/libdbgen.a")
if(NOT EXISTS "${DBGEN_SOURCE_DIR}")
    message(FATAL_ERROR "dbgen source not found at ${DBGEN_SOURCE_DIR}. Make sure git submodule is initialized: git submodule update --init --recursive")
endif()

include(ExternalProject)

ExternalProject_Add(tpch_dbgen
    SOURCE_DIR "${DBGEN_SOURCE_DIR}"
    # 1. Copy the suite to a real Makefile
    # 2. Use sed to replace placeholders (e.g., setting DATABASE=SQLSERVER)
    PATCH_COMMAND cp makefile.suite Makefile
          COMMAND sed -i "s/DATABASE[[:space:]]*=/DATABASE = ORACLE/g" Makefile
          COMMAND sed -i "s/MACHINE[[:space:]]*=/MACHINE = LINUX/g" Makefile
          COMMAND sed -i "s/WORKLOAD[[:space:]]*=/WORKLOAD = TPCH/g" Makefile
          COMMAND sed -i "s/CC[[:space:]]*=/CC = gcc/g" Makefile
    CONFIGURE_COMMAND ""  # No ./configure script
    BUILD_COMMAND sh -c "make && ar rcs ${DBGEN_LIB} *.o"
    INSTALL_COMMAND ""
    BUILD_IN_SOURCE 1
)

# Create the logical target
add_library(dbgen_lib STATIC IMPORTED GLOBAL)

# Set the path to the library file and include headers
set_target_properties(dbgen_lib PROPERTIES
    IMPORTED_LOCATION "${DBGEN_LIB}"
    INTERFACE_INCLUDE_DIRECTORIES "${DBGEN_SOURCE_DIR}"
)

message(STATUS "dbgen configured with source: ${DBGEN_SOURCE_DIR}")
