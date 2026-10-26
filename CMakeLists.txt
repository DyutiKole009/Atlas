cmake_minimum_required(VERSION 3.20)

project(Atlas
    VERSION 0.1.0
    DESCRIPTION "A low-latency stock-exchange simulator"
    LANGUAGES CXX
)

find_package(Threads REQUIRED)

function(atlas_configure_target target)
    target_compile_features(${target} PRIVATE cxx_std_20)
    set_target_properties(${target} PROPERTIES CXX_EXTENSIONS OFF)

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${target} PRIVATE
            -Wall
            -Wextra
            -Wpedantic
        )
    endif()
endfunction()

add_library(atlas_matching
    src/order_book.cpp
    src/storage.cpp
)
target_include_directories(atlas_matching PUBLIC include)
atlas_configure_target(atlas_matching)

add_library(atlas_protocol src/protocol.cpp)
target_include_directories(atlas_protocol PUBLIC include)
atlas_configure_target(atlas_protocol)

add_library(atlas_raft src/raft.cpp src/raft_storage.cpp src/raft_transport.cpp)
target_include_directories(atlas_raft PUBLIC include)
target_link_libraries(atlas_raft PUBLIC atlas_protocol)
atlas_configure_target(atlas_raft)

add_library(atlas_node_lib src/node.cpp src/replicated_exchange.cpp)
target_include_directories(atlas_node_lib PUBLIC include)
target_link_libraries(atlas_node_lib PUBLIC atlas_matching atlas_protocol atlas_raft Threads::Threads)
atlas_configure_target(atlas_node_lib)

add_executable(atlas src/main.cpp)
atlas_configure_target(atlas)

add_executable(atlas_cli src/cli_main.cpp)
target_link_libraries(atlas_cli PRIVATE atlas_matching)
atlas_configure_target(atlas_cli)

add_executable(atlas-node src/node_main.cpp)
target_link_libraries(atlas-node PRIVATE atlas_node_lib)
atlas_configure_target(atlas-node)

add_executable(atlas-client src/client_main.cpp)
target_link_libraries(atlas-client PRIVATE atlas_protocol)
atlas_configure_target(atlas-client)

add_executable(atlas-bench src/bench_main.cpp)
target_link_libraries(atlas-bench PRIVATE atlas_node_lib atlas_matching)
atlas_configure_target(atlas-bench)
target_compile_definitions(atlas-bench PRIVATE
    ATLAS_COMPILER_ID="${CMAKE_CXX_COMPILER_ID}"
    ATLAS_COMPILER_VERSION="${CMAKE_CXX_COMPILER_VERSION}"
    ATLAS_BUILD_MODE="$<CONFIG>"
)

add_executable(atlas_matching_tests tests/order_book_tests.cpp)
target_link_libraries(atlas_matching_tests PRIVATE atlas_matching)
atlas_configure_target(atlas_matching_tests)

add_executable(atlas_domain_tests tests/domain_tests.cpp)
target_link_libraries(atlas_domain_tests PRIVATE atlas_matching)
atlas_configure_target(atlas_domain_tests)

add_executable(atlas_wal_tests tests/wal_tests.cpp)
target_link_libraries(atlas_wal_tests PRIVATE atlas_matching)
atlas_configure_target(atlas_wal_tests)

add_executable(atlas_snapshot_tests tests/snapshot_tests.cpp)
target_link_libraries(atlas_snapshot_tests PRIVATE atlas_matching)
atlas_configure_target(atlas_snapshot_tests)

add_executable(atlas_protocol_tests tests/protocol_tests.cpp)
target_link_libraries(atlas_protocol_tests PRIVATE atlas_protocol)
atlas_configure_target(atlas_protocol_tests)

add_executable(atlas_node_tests tests/node_tests.cpp)
target_link_libraries(atlas_node_tests PRIVATE atlas_node_lib)
atlas_configure_target(atlas_node_tests)

add_executable(atlas_replicated_node_tests tests/replicated_node_tests.cpp)
target_link_libraries(atlas_replicated_node_tests PRIVATE atlas_node_lib)
add_dependencies(atlas_replicated_node_tests atlas-node)
atlas_configure_target(atlas_replicated_node_tests)

add_executable(atlas_raft_tests tests/raft_tests.cpp)
target_link_libraries(atlas_raft_tests PRIVATE atlas_raft)
atlas_configure_target(atlas_raft_tests)

add_executable(atlas_raft_fault_tests tests/raft_fault_tests.cpp)
target_link_libraries(atlas_raft_fault_tests PRIVATE atlas_node_lib)
atlas_configure_target(atlas_raft_fault_tests)

include(CTest)
if(BUILD_TESTING)
    add_test(NAME atlas_smoke COMMAND atlas)
    set_tests_properties(atlas_smoke PROPERTIES
        PASS_REGULAR_EXPRESSION "Project Atlas starting"
    )
    add_test(NAME atlas_matching_tests COMMAND atlas_matching_tests)
    add_test(NAME atlas_domain_tests COMMAND atlas_domain_tests)
    add_test(NAME atlas_wal_tests COMMAND atlas_wal_tests)
    add_test(NAME atlas_snapshot_tests COMMAND atlas_snapshot_tests)
    add_test(NAME atlas_protocol_tests COMMAND atlas_protocol_tests)
    add_test(NAME atlas_node_tests COMMAND atlas_node_tests)
    add_test(NAME atlas_replicated_node_tests COMMAND atlas_replicated_node_tests)
    add_test(NAME atlas_raft_tests COMMAND atlas_raft_tests)
    add_test(NAME atlas_raft_fault_tests COMMAND atlas_raft_fault_tests)
    set_tests_properties(atlas_replicated_node_tests atlas_raft_fault_tests PROPERTIES TIMEOUT 45)
endif()
