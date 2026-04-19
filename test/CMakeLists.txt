cmake_minimum_required(VERSION 3.20)

find_package(GTest REQUIRED)
find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(pipepp-mqtt-unit-tests
    unit/test_mqtt_config.cpp
    unit/test_mqtt_error.cpp
    unit/test_mqtt_source.cpp
    unit/test_mqtt_uri.cpp
    unit/test_mqtt_phase3.cpp
    unit/test_mqtt_coverage.cpp
)
target_link_libraries(pipepp-mqtt-unit-tests PRIVATE pipepp-mqtt GTest::gtest_main Threads::Threads)
target_include_directories(pipepp-mqtt-unit-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(pipepp-mqtt-unit-tests PRIVATE -fno-rtti -fno-exceptions)
elseif(MSVC)
    target_compile_options(pipepp-mqtt-unit-tests PRIVATE /GR- /EHs-c-)
endif()
target_compile_definitions(pipepp-mqtt-unit-tests PRIVATE
    PIPEPP_MQTT_TEST_BROKER="${PIPEPP_MQTT_TEST_BROKER}"
    PIPEPP_MQTT_TEST_PORT=${PIPEPP_MQTT_TEST_PORT}
    PIPEPP_MQTT_TEST_TLS_PORT=${PIPEPP_MQTT_TEST_TLS_PORT}
    PIPEPP_MQTT_TEST_CA_CERT="${PIPEPP_MQTT_TEST_CA_CERT}"
)
if(PahoMqttCpp_FOUND)
    target_compile_definitions(pipepp-mqtt-unit-tests PRIVATE PIPEPP_MQTT_HAS_PAHO_CPP=1)
else()
    target_compile_definitions(pipepp-mqtt-unit-tests PRIVATE PIPEPP_MQTT_STUB=1)
endif()
gtest_discover_tests(pipepp-mqtt-unit-tests TEST_LIST UNIT_TESTS)

add_executable(pipepp-mqtt-integration-tests
    integration/test_mqtt_connect.cpp
    integration/test_mqtt_pubsub.cpp
    integration/test_mqtt_tls_auth.cpp
)
target_link_libraries(pipepp-mqtt-integration-tests PRIVATE pipepp-mqtt GTest::gtest_main Threads::Threads)
target_include_directories(pipepp-mqtt-integration-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(pipepp-mqtt-integration-tests PRIVATE -fno-rtti -fno-exceptions)
elseif(MSVC)
    target_compile_options(pipepp-mqtt-integration-tests PRIVATE /GR- /EHs-c-)
endif()
target_compile_definitions(pipepp-mqtt-integration-tests PRIVATE
    PIPEPP_MQTT_TEST_BROKER="${PIPEPP_MQTT_TEST_BROKER}"
    PIPEPP_MQTT_TEST_PORT=${PIPEPP_MQTT_TEST_PORT}
    PIPEPP_MQTT_TEST_TLS_PORT=${PIPEPP_MQTT_TEST_TLS_PORT}
    PIPEPP_MQTT_TEST_CA_CERT="${PIPEPP_MQTT_TEST_CA_CERT}"
)
gtest_discover_tests(pipepp-mqtt-integration-tests TEST_LIST INTEGRATION_TESTS
    PROPERTIES RUN_SERIAL TRUE)

# Fault injection tests: compile mqtt_source.cpp against mock Paho headers
add_executable(pipepp-mqtt-fault-tests
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/mqtt_source.cpp
    unit/test_mqtt_fault_injection.cpp
)
target_include_directories(pipepp-mqtt-fault-tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/mocks
    ${PIPEPP_CORE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)
target_compile_definitions(pipepp-mqtt-fault-tests PRIVATE
    PIPEPP_MQTT_HAS_PAHO_CPP=1
)
target_link_libraries(pipepp-mqtt-fault-tests PRIVATE GTest::gtest_main Threads::Threads)
target_compile_features(pipepp-mqtt-fault-tests PRIVATE cxx_std_20)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(pipepp-mqtt-fault-tests PRIVATE -fno-rtti)
endif()
if(PIPEPP_MQTT_ENABLE_COVERAGE)
    target_compile_options(pipepp-mqtt-fault-tests PRIVATE --coverage -g -O0)
    target_link_libraries(pipepp-mqtt-fault-tests PRIVATE --coverage)
endif()
gtest_discover_tests(pipepp-mqtt-fault-tests TEST_LIST FAULT_TESTS)
