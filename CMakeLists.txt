cmake_minimum_required(VERSION 3.28)

project(ws-eventbus
    VERSION 0.1.0
    DESCRIPTION "Typed pub/sub + command dispatch + correlation tracking for C++23"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---------------------------------------------------------------------------
# ws-error is composed, not vendored (tech spec section 14.3: a base lib may
# compose another). If the consumer already provides it, use theirs — that is
# what keeps a program from carrying two copies of the error contract.
# The FetchContent name is deliberately private so it can never collide with a
# consumer's own `ws-error` declaration.
# ---------------------------------------------------------------------------
if(NOT TARGET ws-error)
    include(FetchContent)
    FetchContent_Declare(ws_eventbus_dep_ws_error
        GIT_REPOSITORY https://github.com/ws-base-libs/ws-error
        GIT_TAG        v0.1.0
        GIT_SHALLOW    TRUE)
    FetchContent_MakeAvailable(ws_eventbus_dep_ws_error)
endif()

add_library(ws-eventbus STATIC src/eventbus.cpp)
add_library(ws-eventbus::ws-eventbus ALIAS ws-eventbus)

target_include_directories(ws-eventbus PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(ws-eventbus PUBLIC ws-error)
target_compile_features(ws-eventbus PUBLIC cxx_std_23)

# ---------------------------------------------------------------------------
# Tests build only when this library is the top-level project, so consuming it
# through FetchContent never drags our test binaries or our GoogleTest into
# someone else's build (and never collides with their copy of it).
# ---------------------------------------------------------------------------
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(WS_EVENTBUS_STANDALONE ON)
else()
    set(WS_EVENTBUS_STANDALONE OFF)
endif()

option(WS_EVENTBUS_BUILD_TESTS "Build ws-eventbus's standalone test suite" ${WS_EVENTBUS_STANDALONE})

if(WS_EVENTBUS_BUILD_TESTS)
    enable_testing()

    include(FetchContent)
    FetchContent_Declare(ws_eventbus_gtest
        GIT_REPOSITORY https://github.com/google/googletest
        GIT_TAG        v1.14.0
        GIT_SHALLOW    TRUE)
    FetchContent_MakeAvailable(ws_eventbus_gtest)

    foreach(suite pubsub command correlation)
        add_executable(${suite}_test tests/test_${suite}.cpp)
        target_link_libraries(${suite}_test PRIVATE ws-eventbus GTest::gtest_main)
        target_compile_options(${suite}_test PRIVATE -Wall -Wextra -Werror)
        add_test(NAME ws-eventbus.${suite} COMMAND ${suite}_test)
    endforeach()
endif()
