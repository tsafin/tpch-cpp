# Unit tests for TPC-H C++

if(TPCH_BUILD_TESTS)
    # Enable testing
    enable_testing()

    # Use vendored Google Test only
    set(GTEST_VENDOR_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../third_party/googletest")
    if(NOT EXISTS "${GTEST_VENDOR_DIR}/CMakeLists.txt")
        message(FATAL_ERROR "Vendored googletest not found at ${GTEST_VENDOR_DIR}. "
                            "Run: git submodule update --init --recursive")
    endif()

    # For Windows: Prevent overriding the parent project's compiler/linker settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    add_subdirectory(
        "${GTEST_VENDOR_DIR}"
        "${CMAKE_BINARY_DIR}/_deps/googletest-build"
        EXCLUDE_FROM_ALL
    )

    include(GoogleTest)

    # Buffer lifetime manager unit tests
    add_executable(buffer_lifetime_manager_test
        buffer_lifetime_manager_test.cpp
    )

    target_link_libraries(buffer_lifetime_manager_test
        PRIVATE
            tpch_core
            GTest::gtest_main
    )

    target_include_directories(buffer_lifetime_manager_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
    )

    # Register tests
    gtest_discover_tests(buffer_lifetime_manager_test)

    # DBGen batch iterator tests
    add_executable(dbgen_batch_iterator_test
        dbgen_batch_iterator_test.cpp
    )

    target_link_libraries(dbgen_batch_iterator_test
        PRIVATE
            tpch_core
            GTest::gtest_main
    )

    target_include_directories(dbgen_batch_iterator_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
    )

    gtest_discover_tests(dbgen_batch_iterator_test)

    # Paimon writer tests (only if Paimon is enabled)
    if(TPCH_ENABLE_PAIMON)
        add_executable(paimon_writer_test
            paimon_writer_test.cpp
        )

        target_link_libraries(paimon_writer_test
            PRIVATE
                tpch_core
                GTest::gtest_main
        )

        target_include_directories(paimon_writer_test
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/../include
        )

        gtest_discover_tests(paimon_writer_test)
    endif()

    # Lance writer tests (only if Lance FFI is enabled)
    if(TPCH_ENABLE_LANCE)
        add_executable(lance_writer_test
            lance_writer_test.cpp
        )

        target_link_libraries(lance_writer_test
            PRIVATE
                tpch_core
                GTest::gtest_main
        )

        target_include_directories(lance_writer_test
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/../include
        )

        gtest_discover_tests(lance_writer_test)
    endif()

    message(STATUS "Test targets configured successfully")
endif()
