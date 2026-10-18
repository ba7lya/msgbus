# SPDX-License-Identifier: MIT
#
# The examples: one executable per source, so each file under this directory is a program the
# reader can build and run on its own. The binaries are named `msgbus_example_<file>` after the
# project, matching how the tests and benchmarks name theirs.

file(GLOB MSGBUS_EXAMPLE_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cxx")

foreach(MSGBUS_EXAMPLE_SOURCE IN LISTS MSGBUS_EXAMPLE_SOURCES)
    get_filename_component(MSGBUS_EXAMPLE_NAME "${MSGBUS_EXAMPLE_SOURCE}" NAME_WE)
    add_executable("msgbus_example_${MSGBUS_EXAMPLE_NAME}" "${MSGBUS_EXAMPLE_SOURCE}")
    target_link_libraries("msgbus_example_${MSGBUS_EXAMPLE_NAME}" PRIVATE ba7lya::msgbus
        $<$<BOOL:${ba7lya.msgbus_SANITIZER}>:ba7lya.msgbus_sanitizer>)
    set_target_properties("msgbus_example_${MSGBUS_EXAMPLE_NAME}" PROPERTIES FOLDER "examples")
endforeach()
