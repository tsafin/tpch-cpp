# Find Thrift (Apache Thrift)
#
# This module defines:
#  Thrift_FOUND
#  Thrift_INCLUDE_DIR
#  Thrift_LIBRARY
#  Thrift_VERSION
#  Thrift::thrift

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(PC_THRIFT QUIET thrift)
endif()

find_path(Thrift_INCLUDE_DIR
    NAMES thrift/Thrift.h
    HINTS ${PC_THRIFT_INCLUDEDIR} ${PC_THRIFT_INCLUDE_DIRS}
    PATH_SUFFIXES include
)

find_library(Thrift_LIBRARY
    NAMES thrift
    HINTS ${PC_THRIFT_LIBDIR} ${PC_THRIFT_LIBRARY_DIRS}
    PATH_SUFFIXES lib
)

if(PC_THRIFT_VERSION)
    set(Thrift_VERSION ${PC_THRIFT_VERSION})
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Thrift
    REQUIRED_VARS Thrift_LIBRARY Thrift_INCLUDE_DIR
    VERSION_VAR Thrift_VERSION
)

if(Thrift_FOUND)
    if(NOT TARGET Thrift::thrift)
        add_library(Thrift::thrift UNKNOWN IMPORTED)
        set_target_properties(Thrift::thrift PROPERTIES
            IMPORTED_LOCATION "${Thrift_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${Thrift_INCLUDE_DIR}"
        )
    endif()
endif()

mark_as_advanced(Thrift_INCLUDE_DIR Thrift_LIBRARY)