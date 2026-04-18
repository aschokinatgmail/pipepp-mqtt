cmake_minimum_required(VERSION 3.20)

find_package(GTest REQUIRED)
include(GoogleTest)

add_executable(pipepp-mqtt-unit-tests
    unit/test_mqtt_config.cpp
    unit/test_mqtt_error.cpp
    unit/test_mqtt_source.cpp
    unit/test_mqtt_uri.cpp
)
target_link_libraries(pipepp-mqtt-unit-tests PRIVATE pipepp-mqtt GTest::gtest_main)
target_include_directories(pipepp-mqtt-unit-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(pipepp-mqtt-unit-tests PRIVATE -fno-rtti -fno-exceptions)
target_link_libraries(pipepp-mqtt-unit-tests PRIVATE -pthread)
target_compile_definitions(pipepp-mqtt-unit-tests PRIVATE
    PIPEPP_MQTT_TEST_BROKER="${PIPEPP_MQTT_TEST_BROKER}"
    PIPEPP_MQTT_TEST_PORT=${PIPEPP_MQTT_TEST_PORT}
)
gtest_discover_tests(pipepp-mqtt-unit-tests TEST_LIST UNIT_TESTS)

add_executable(pipepp-mqtt-integration-tests
    integration/test_mqtt_connect.cpp
    integration/test_mqtt_pubsub.cpp
)
target_link_libraries(pipepp-mqtt-integration-tests PRIVATE pipepp-mqtt GTest::gtest_main)
target_include_directories(pipepp-mqtt-integration-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(pipepp-mqtt-integration-tests PRIVATE -fno-rtti -fno-exceptions)
target_link_libraries(pipepp-mqtt-integration-tests PRIVATE -pthread)
target_compile_definitions(pipepp-mqtt-integration-tests PRIVATE
    PIPEPP_MQTT_TEST_BROKER="${PIPEPP_MQTT_TEST_BROKER}"
    PIPEPP_MQTT_TEST_PORT=${PIPEPP_MQTT_TEST_PORT}
)
gtest_discover_tests(pipepp-mqtt-integration-tests TEST_LIST INTEGRATION_TESTS)
