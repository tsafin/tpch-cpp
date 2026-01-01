# Find zstd

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(PC_ZSTD QUIET libzstd)
endif()

find_path(zstd_INCLUDE_DIR NAMES zstd.h
    HINTS ${PC_ZSTD_INCLUDEDIR} ${PC_ZSTD_INCLUDE_DIRS}
)

find_library(zstd_LIBRARY NAMES zstd
    HINTS ${PC_ZSTD_LIBDIR} ${PC_ZSTD_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(zstd
    REQUIRED_VARS zstd_LIBRARY zstd_INCLUDE_DIR
    VERSION_VAR PC_ZSTD_VERSION
)

if(zstd_FOUND)
    if(NOT TARGET zstd::libzstd_shared)
        add_library(zstd::libzstd_shared UNKNOWN IMPORTED)
        set_target_properties(zstd::libzstd_shared PROPERTIES
            IMPORTED_LOCATION "${zstd_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${zstd_INCLUDE_DIR}"
        )
    endif()
endif()