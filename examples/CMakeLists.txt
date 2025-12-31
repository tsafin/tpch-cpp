# Example programs demonstrating TPC-H functionality

if(TPCH_BUILD_EXAMPLES)
    # simple_arrow_parquet - Demonstrates Arrow schema and Parquet writing
    if(Arrow_FOUND AND Parquet_FOUND)
        add_executable(simple_arrow_parquet simple_arrow_parquet.cpp)
        target_link_libraries(simple_arrow_parquet PRIVATE
            Arrow::arrow
            Arrow::parquet
        )
    else()
        message(WARNING "Arrow not found - simple_arrow_parquet example will not be built")
    endif()

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
