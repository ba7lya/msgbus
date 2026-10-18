# SPDX-License-Identifier: MIT

# --- cmake version ---
cmake_minimum_required(VERSION 3.21)

# --- project ---
project(ba7lya.msgbus LANGUAGES CXX)

# --- build options ---
option(${PROJECT_NAME}_BUILD_BENCHMARK "Build benchmarks"    OFF)
option(${PROJECT_NAME}_BUILD_EXAMPLE   "Build examples"      OFF)
option(${PROJECT_NAME}_BUILD_DOC       "Build Documentation" OFF)
option(${PROJECT_NAME}_BUILD_TEST      "Build Test"          OFF)

# --- log switch ---
# `include/logger.hxx` reads this one macro, and ON is the default: a trace that is absent because
# nobody asked for it is a failure nobody notices, while a trace that is present and switched off at
# run time costs one comparison. Turning it off is what a build does when the program's own logging
# already covers the bus; it removes every spdlog call from `ba7lya.msgbus`'s headers without
# touching a call site.
option(${PROJECT_NAME}_BUILD_LOG "Compile the bus trace in" ON)

# --- sanitizer ---
# One of: "" (off) | address | thread | undefined. A build-time check rather than a shipping
# feature, so it is a cache variable instead of an install-time option: the flag must never
# reach a downstream consumer. Spelled the pkg-config way ('address') rather than the compiler's
# ('asan') because the presets, the CI matrix and the diagnostics all say the full word.
set(${PROJECT_NAME}_SANITIZER "" CACHE STRING "Sanitizer to build the tests, examples and benchmarks with")
set_property(CACHE ${PROJECT_NAME}_SANITIZER PROPERTY STRINGS "" address thread undefined)

# --- includes ---
include(CTest)
include(GNUInstallDirs)
include(GoogleTest)

# --- dependencies ---
if(${PROJECT_NAME}_BUILD_BENCHMARK)
    find_package(benchmark CONFIG REQUIRED)
endif()
if(${PROJECT_NAME}_BUILD_TEST)
    find_package(GTest CONFIG REQUIRED)
endif()
find_package(spdlog CONFIG REQUIRED)

# --- os ---
if (WIN32)
    add_definitions(-DWINVER=0x0A00)       # Windows 10
    add_definitions(-D_WIN32_WINNT=0x0A00) # Windows 10
    add_definitions(-DNOMINMAX)            # disable C2589 min()/max() error
endif()

# --- output layout ---
# Every runtime artifact of a configuration lands in one directory: `${CMAKE_BINARY_DIR}/bin/<config>`.
# One directory rather than the default per-target tree (`build/<preset>/tests/Debug/`) for two
# reasons. The loader is the first: the tests link a shared GTest, so `gtest.dll` is placed beside each
# executable and a single directory means one copy instead of one per subdirectory. The second is that
# a smoke run and a bug report can then name one path -- `build/<preset>/bin/<config>/<target>` -- for
# every configuration of every preset, which is what CI's `smoke the benchmarks` / `smoke the examples`
# steps already assume.
#
# Ninja Multi-Config expands the `$<CONFIG>` generator expression per configuration, so one setting
# covers Debug and Release; single-config generators have it expand to the one configuration they were
# configured for, which keeps the layout flat but not wrong. The libraries (here: none, this target is
# the library) are placed alongside so the library cannot end up split from the
# executables that load it.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/$<CONFIG>")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/$<CONFIG>")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/$<CONFIG>")

# --- modules ---
add_subdirectory(thirdparty)

# --- target ---
# A static library, not an interface target: the bus's implementation lives in `src/`, so a consumer
# compiles the headers it names and links the rest. `include/` is the whole of the public API; `src/` is
# added for the library's own translation units, and for the tests and the benchmark that exercise the
# internals directly.
file(GLOB MSGBUS_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cxx")

add_library(${PROJECT_NAME} STATIC ${MSGBUS_SOURCES})
add_library(ba7lya::msgbus ALIAS ${PROJECT_NAME})
target_include_directories(${PROJECT_NAME}
    PUBLIC ${PROJECT_SOURCE_DIR}/include/
    PRIVATE ${PROJECT_SOURCE_DIR}/src/)
target_link_libraries(${PROJECT_NAME} PUBLIC spdlog::spdlog)
target_compile_features(${PROJECT_NAME} PUBLIC cxx_std_23)
target_compile_options(${PROJECT_NAME} PUBLIC "$<$<CXX_COMPILER_ID:MSVC>:/utf-8>")
# `/EHsc` is not optional here. `executor` runs its handlers on a `std::jthread` worker where a
# user handler may throw, and `drain`/`flush` deliberately throw `std::invalid_argument` when
# called from that same worker -- so the library's own contract is "a C++ exception crosses a
# frame and is caught". Without unwind semantics MSVC still compiles (warning C4530 from
# `chrono`/`__msvc_ostream.hpp` is the only hint) and then aborts at the first throw with a
# `_Throw_Cpp_error` rethrow inside the unwind path, which is what turned the unit-test suite
# red on `win-msvc-2026-dbg` while every other configuration stayed green.
target_compile_options(${PROJECT_NAME} PUBLIC "$<$<CXX_COMPILER_ID:MSVC>:/EHsc>")

# The log switch travels as a public compile definition, unlike the sanitizer flags below:
# `logger.hxx` is a public header, so which build of it a translation unit sees is part of the
# library's interface, and every consumer has to agree with the one the library was configured for.
# It is passed as `0`/`1` rather than only-when-on, so the header's own fallback never decides: a
# consumer that defines the macro for itself overrides the build, and one that does not gets the
# switch answered the same way `option()` was.
target_compile_definitions(${PROJECT_NAME} PUBLIC
    "BA7LYA_MSGBUS_LOG_ENABLED=$<BOOL:${${PROJECT_NAME}_BUILD_LOG}>")

# --- sanitizer flags ---
# A dedicated INTERFACE target, linked PRIVATEly by ${PROJECT_NAME} and by every executable: the sanitizer
# is a build-time check, so its compile options must not reach a downstream consumer's build.
# The library carries them too, because its objects are what the executables link: MSVC records the
# sanitizer in `#pragma detect_mismatch` (`annotate_string` and friends), so an instrumented executable
# linked against an uninstrumented library fails with LNK2038 rather than producing a green build that
# checked only the half of the code that lives in headers.
if(${PROJECT_NAME}_SANITIZER)
    add_library(${PROJECT_NAME}_sanitizer INTERFACE)

    if(MSVC)
        # cl.exe knows /fsanitize=address only. It SILENTLY IGNORES thread and undefined
        # (warning D9002, exit code 0), so an unsupported request has to fail here, loudly,
        # rather than produce a green build that checks nothing at all.
        if(NOT ${PROJECT_NAME}_SANITIZER STREQUAL "address")
            message(FATAL_ERROR
                "MSVC supports ${PROJECT_NAME}_SANITIZER=address only; "
                "'${${PROJECT_NAME}_SANITIZER}' is not a sanitizer cl.exe knows. "
                "Use the linux-*-x64 presets for thread and undefined.")
        endif()
        target_compile_options(${PROJECT_NAME}_sanitizer INTERFACE /fsanitize=address /Zi)
        target_link_options(${PROJECT_NAME}_sanitizer INTERFACE /INCREMENTAL:NO)
    else()
        target_compile_options(${PROJECT_NAME}_sanitizer INTERFACE
            -fsanitize=${${PROJECT_NAME}_SANITIZER} -fno-omit-frame-pointer -g)
        target_link_options(${PROJECT_NAME}_sanitizer INTERFACE
            -fsanitize=${${PROJECT_NAME}_SANITIZER})
    endif()

    # Ubuntu's gcc ships -D_FORTIFY_SOURCE=3 in its default spec, and it refuses to work
    # alongside asan instrumentation. Only GNU defines it unprompted, so only GNU needs the
    # undefine. The runtime *_OPTIONS knobs are set per CI step, not here: the build system has
    # no business choosing a leak-detection policy.
    target_compile_options(${PROJECT_NAME}_sanitizer INTERFACE
        "$<$<COMPILE_LANGUAGE:CXX>:$<$<CXX_COMPILER_ID:GNU>:-U_FORTIFY_SOURCE>>")

    # The library compiles objects of its own, so it is instrumented like the executables that link it.
    # PRIVATE keeps the flags out of a consumer's compile line; what a consumer needs is the runtime at
    # link time, which a static library's private dependency carries as LINK_ONLY anyway.
    target_link_libraries(${PROJECT_NAME} PRIVATE ${PROJECT_NAME}_sanitizer)
endif()

# --- benchmark ---
if(${PROJECT_NAME}_BUILD_BENCHMARK)
    add_subdirectory(benchmark)
endif()

# --- examples ---
if(${PROJECT_NAME}_BUILD_EXAMPLE)
    add_subdirectory(examples)
endif()

# --- unit tests ---
if(${PROJECT_NAME}_BUILD_TEST)
    enable_testing()
    add_subdirectory(tests)
endif()
