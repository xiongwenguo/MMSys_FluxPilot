#Copyright (c) 2023. ByteDance Inc. All rights reserved.
set(BOOST_LIBRARIES boost_system boost_thread boost_chrono boost_date_time boost_regex)
##############################################
set(APP_SOURCES
        MPDtest.cpp)
include_directories(${PROJECT_SOURCE_DIR}/demo/utils
                    ${PROJECT_SOURCE_DIR}/demo)
set(DEMO_SOURCES
        ${PROJECT_SOURCE_DIR}/demo/demotransportcontroller.cpp
        ${PROJECT_SOURCE_DIR}/demo/utils/defaultclock.cpp
        ${PROJECT_SOURCE_DIR}/demo/utils/thirdparty/quiche/quic_clock.cpp
        ${PROJECT_SOURCE_DIR}/demo/utils/thirdparty/quiche/quic_time.cpp
        ${PROJECT_SOURCE_DIR}/demo/utils/thirdparty/quiche/rtt_stats.cpp)

add_executable(MPDtest ${APP_SOURCES} ${DEMO_SOURCES}) # client node
target_link_libraries(MPDtest libp2p_lab_module.a ${BOOST_LIBRARIES} pthread ssl crypto dl)


