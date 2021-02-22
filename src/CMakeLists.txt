add_executable(sqlplusplus main.cpp oracle_helpers.cpp cli_args.cpp)
target_link_libraries(sqlplusplus odpi linenoise tabulate mpark_variant fmt)
