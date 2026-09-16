# CMake generated Testfile for 
# Source directory: /home/tengyujie/raft-kv
# Build directory: /home/tengyujie/raft-kv/build-tsan
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(raftkv_unit "/home/tengyujie/raft-kv/build-tsan/bin/raftkv_tests")
set_tests_properties(raftkv_unit PROPERTIES  _BACKTRACE_TRIPLES "/home/tengyujie/raft-kv/CMakeLists.txt;52;add_test;/home/tengyujie/raft-kv/CMakeLists.txt;0;")
add_test(raftkv_raft "/home/tengyujie/raft-kv/build-tsan/bin/raftkv_raft_tests")
set_tests_properties(raftkv_raft PROPERTIES  _BACKTRACE_TRIPLES "/home/tengyujie/raft-kv/CMakeLists.txt;105;add_test;/home/tengyujie/raft-kv/CMakeLists.txt;0;")
