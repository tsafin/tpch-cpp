# Example programs demonstrating TPC-H functionality

if(TPCH_BUILD_EXAMPLES)
    # Placeholder examples - will be implemented in later phases

    # Future: simple_arrow_parquet
    # add_executable(simple_arrow_parquet simple_arrow_parquet.cpp)
    # target_link_libraries(simple_arrow_parquet PRIVATE tpch_core)

    # Future: simple_csv
    # add_executable(simple_csv simple_csv.cpp)
    # target_link_libraries(simple_csv PRIVATE tpch_core)

    # Future: simple_orc
    # add_executable(simple_orc simple_orc.cpp)
    # target_link_libraries(simple_orc PRIVATE tpch_core)

    # Future: async_io_demo
    # if(TPCH_ENABLE_ASYNC_IO)
    #     add_executable(async_io_demo async_io_demo.cpp)
    #     target_link_libraries(async_io_demo PRIVATE tpch_core)
    # endif()
endif()
