# SPDX-License-Identifier: MIT
#
# The unit-test suite: each source registers itself with CTest through gtest_discover_tests, so a
# new test file needs no change here beyond the list below.

file(GLOB MSGBUS_TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/ut/*.cxx")

add_executable(ba7lya.msgbus.ut ${MSGBUS_TEST_SOURCES})

target_link_libraries(ba7lya.msgbus.ut PRIVATE ba7lya::msgbus GTest::gtest_main
    $<$<BOOL:${ba7lya.msgbus_SANITIZER}>:ba7lya.msgbus_sanitizer>)

gtest_discover_tests(ba7lya.msgbus.ut)
