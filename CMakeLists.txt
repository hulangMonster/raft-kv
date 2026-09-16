cmake_minimum_required(VERSION 3.16)
project(raftkv VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

add_compile_options(-Wall -Wextra)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

find_package(Threads REQUIRED)

add_library(raftkv_core STATIC
  src/codec.cpp
  src/wal.cpp
  src/store.cpp
  src/server.cpp
)
target_include_directories(raftkv_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(raftkv_core PUBLIC Threads::Threads)

add_executable(raftkv_server src/main_server.cpp)
target_link_libraries(raftkv_server PRIVATE raftkv_core)

add_executable(raftkv_cli src/main_client.cpp)
target_link_libraries(raftkv_cli PRIVATE raftkv_core)

enable_testing()
add_executable(raftkv_tests
  tests/test_main.cpp
  tests/test_store.cpp
  tests/test_codec.cpp
)
target_link_libraries(raftkv_tests PRIVATE raftkv_core)
add_test(NAME raftkv_unit COMMAND raftkv_tests)

# ---- M2: Raft core (new sources only; M1 targets above are unchanged) ----
add_library(raftkv_raft STATIC
  src/raft/cluster_config.cpp
  src/raft/memory_adapters.cpp
  src/raft/raft_node.cpp
  src/raft/file_log_store.cpp
  src/raft/message.cpp
  src/raft/metrics.cpp
  src/raft/transport_tcp.cpp
  src/raft/memory_snapshot_store.cpp
  src/raft/file_snapshot_store.cpp
  src/kv/kv_state_machine.cpp
)
target_include_directories(raftkv_raft PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(raftkv_raft PUBLIC Threads::Threads)

add_executable(raftkv_raft_node src/main_raft_node.cpp)
target_link_libraries(raftkv_raft_node PRIVATE raftkv_raft)

add_executable(raftkv_raft_cli src/main_raft_client.cpp)
target_link_libraries(raftkv_raft_cli PRIVATE raftkv_raft)

# GTest is a test-only dependency. Use the existing install under /usr/local
# (found on the dev VM); do NOT vendor it into runtime/core targets.
find_path(GTEST_INCLUDE_DIR gtest/gtest.h PATHS /usr/local/include)
find_library(GTEST_LIBRARY NAMES gtest PATHS /usr/local/lib)

if(GTEST_INCLUDE_DIR AND GTEST_LIBRARY)
  add_executable(raftkv_raft_tests
    tests/raft_gtest_main.cpp
    tests/raft_election_test.cpp
    tests/raft_log_test.cpp
    tests/raft_commit_test.cpp
    tests/raft_idempotent_test.cpp
    tests/raft_restart_test.cpp
    tests/raft_snapshot_test.cpp
    tests/raft_membership_test.cpp
    tests/raft_perf_test.cpp
  )
  target_include_directories(raftkv_raft_tests PRIVATE
    ${GTEST_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests
  )
  target_link_libraries(raftkv_raft_tests PRIVATE
    raftkv_raft
    ${GTEST_LIBRARY}
    Threads::Threads
  )
  add_test(NAME raftkv_raft COMMAND raftkv_raft_tests)
else()
  message(WARNING "GTest not found under /usr/local; skipping raftkv_raft_tests")
endif()
