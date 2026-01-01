# Find re2

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(PC_RE2 QUIET re2)
endif()

find_path(re2_INCLUDE_DIR NAMES re2/re2.h
    HINTS ${PC_RE2_INCLUDEDIR} ${PC_RE2_INCLUDE_DIRS}
)

find_library(re2_LIBRARY NAMES re2
    HINTS ${PC_RE2_LIBDIR} ${PC_RE2_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(re2
    REQUIRED_VARS re2_LIBRARY re2_INCLUDE_DIR
    VERSION_VAR PC_RE2_VERSION
)

if(re2_FOUND)
    if(NOT TARGET re2::re2)
        add_library(re2::re2 UNKNOWN IMPORTED)
        set_target_properties(re2::re2 PROPERTIES
            IMPORTED_LOCATION "${re2_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${re2_INCLUDE_DIR}"
        )
    endif()
endif()