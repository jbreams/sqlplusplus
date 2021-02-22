add_executable(sqlsqrt main.cpp oracle_helpers.cpp cli_args.cpp)
target_link_libraries(sqlsqrt odpi linenoise tabulate mpark_variant fmt)
