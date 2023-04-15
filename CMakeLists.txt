#Copyright (c) 2023. ByteDance Inc. All rights reserved.
cmake_minimum_required(VERSION 3.10)

project(mpdtest C CXX)

set(CXX_FLAGS
        -g
        -Wall
        -Wextra
        #-Werror
        -std=c++11
        -Wno-unused-parameter
        -Wno-format
        -Wno-unused-private-field
        )

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    list(APPEND CXX_FLAGS "-Wthread-safety")
endif ()


string(REPLACE ";" " " CMAKE_CXX_FLAGS "${CXX_FLAGS}")

#add_definitions(-DCLOSE_LOG)
add_definitions(-DPARTNER_ISURECLOUD)
add_definitions(-DLINUX_X86_PLAT)
add_definitions(-DCACHE_SIZE_16K=50000)
add_definitions(-DSPDLOG_LEVEL_TRACE=0)
add_definitions(-DOPEN_LAB_TRACE)

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -D_DEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)


message(STATUS "CXX_FLAGS = " ${CMAKE_CXX_FLAGS} " " ${CMAKE_CXX_FLAGS_${BUILD_TYPE}})

find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost COMPONENTS system thread chrono date_time regex REQUIRED)


include_directories(
        ${PROJECT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/mpd/
        ${PROJECT_SOURCE_DIR}/mpd/app
        ${PROJECT_SOURCE_DIR}/mpd/basefw
        ${PROJECT_SOURCE_DIR}/mpd/basefw/base
        ${PROJECT_SOURCE_DIR}/mpd/download/transportcontroller/
        ${PROJECT_SOURCE_DIR}/mpd/third_lib/json/single_include
)

link_directories(
        ${PROJECT_SOURCE_DIR}/mpd/lib/debug/
        )
message(STATUS ${PROJECT_SOURCE_DIR}/mpd/lib/debug/)

add_subdirectory(mpd)