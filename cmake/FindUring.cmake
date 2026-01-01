# Find liburing (Linux io_uring library)
#
# Sets:
#  uring_FOUND - whether liburing was found
#  uring_INCLUDE_DIRS - location of uring.h
#  uring_LIBRARIES - location of liburing.so or liburing.a
#  uring::uring - imported target

find_path(uring_INCLUDE_DIR
    NAMES liburing.h
    PATHS /usr/include /usr/local/include
)

find_library(uring_LIBRARY
    NAMES uring
    PATHS /usr/lib /usr/local/lib /usr/lib/x86_64-linux-gnu
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Uring
    DEFAULT_MSG
    uring_INCLUDE_DIR
    uring_LIBRARY
)

if(Uring_FOUND)
    set(Uring_INCLUDE_DIRS ${uring_INCLUDE_DIR})
    set(Uring_LIBRARIES ${uring_LIBRARY})

    # Create imported target
    if(NOT TARGET Uring::uring)
        add_library(Uring::uring UNKNOWN IMPORTED)
        set_target_properties(Uring::uring PROPERTIES
            IMPORTED_LOCATION "${uring_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${uring_INCLUDE_DIR}"
        )
    endif()
else()
    set(Uring_INCLUDE_DIRS)
    set(Uring_LIBRARIES)
endif()

mark_as_advanced(uring_INCLUDE_DIR uring_LIBRARY)
