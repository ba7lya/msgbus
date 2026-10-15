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

# --- modules ---
add_subdirectory(thirdparty)

# --- target ---
add_library(${PROJECT_NAME} INTERFACE)
add_library(ba7lya::msgbus ALIAS ${PROJECT_NAME})
target_include_directories(${PROJECT_NAME} INTERFACE ${PROJECT_SOURCE_DIR}/include/)
target_link_libraries(${PROJECT_NAME} INTERFACE spdlog::spdlog)
target_compile_features(${PROJECT_NAME} INTERFACE cxx_std_23)
target_compile_options(${PROJECT_NAME} INTERFACE "$<$<CXX_COMPILER_ID:MSVC>:/utf-8>")

# --- sanitizer flags ---
# A dedicated INTERFACE target, never linked by ${PROJECT_NAME} itself: this library is
# header-only, so anything on its INTERFACE would propagate into every downstream consumer's
# compile options. Keeping it a separate target is what makes that impossible -- and since no
# install(EXPORT) rule ever names this target, its flags cannot leak through the package either.
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
                "Use the linux-gcc-x64 / linux-clang-x64 presets for thread and undefined.")
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
