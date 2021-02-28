add_executable(sqlplusplus main.cpp oracle_helpers.cpp cli_args.cpp table.cpp)
target_link_libraries(sqlplusplus odpi linenoise mpark_variant fmt tsl_hat_trie)
