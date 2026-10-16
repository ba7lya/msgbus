# SPDX-License-Identifier: MIT
#
# The benchmark suite: one executable named `msgbus_benchmark`, after the project, the same way the
# tests name theirs. Every source under this directory is compiled into that one binary rather than
# getting an executable each as the examples do, because Google Benchmark owns the entry point
# (`BENCHMARK_MAIN`) and a second `main` would collide.

file(GLOB MSGBUS_BENCHMARK_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cxx")

add_executable(msgbus_benchmark ${MSGBUS_BENCHMARK_SOURCES})
target_link_libraries(msgbus_benchmark PRIVATE ba7lya::msgbus benchmark::benchmark
    $<$<BOOL:${ba7lya.msgbus_SANITIZER}>:ba7lya.msgbus_sanitizer>)
set_target_properties(msgbus_benchmark PROPERTIES FOLDER "benchmark")
