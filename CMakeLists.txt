cmake_minimum_required(VERSION 3.20)
project(pipepp-mqtt VERSION 0.1.0 LANGUAGES CXX)

if(NOT TARGET pipepp::core)
    if(DEFINED PIPEPP_CORE_DIR)
        add_subdirectory(${PIPEPP_CORE_DIR} ${CMAKE_BINARY_DIR}/pipepp-core)
    else()
        find_package(pipepp-core REQUIRED)
    endif()
endif()

add_library(pipepp-mqtt
    src/mqtt_source.cpp
)
target_include_directories(pipepp-mqtt PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(pipepp-mqtt PUBLIC cxx_std_20)
target_link_libraries(pipepp-mqtt PUBLIC pipepp::core)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(pipepp-mqtt PUBLIC -fno-rtti)
    set_source_files_properties(src/mqtt_source.cpp PROPERTIES COMPILE_FLAGS "-fexceptions")
elseif(MSVC)
    target_compile_options(pipepp-mqtt PUBLIC /GR- /EHs-c-)
    set_source_files_properties(src/mqtt_source.cpp PROPERTIES COMPILE_FLAGS "/EHsc")
endif()

find_package(PahoMqttCpp QUIET)
if(PahoMqttCpp_FOUND)
    target_link_libraries(pipepp-mqtt PRIVATE paho-mqtt3a paho-mqttpp3)
    target_compile_definitions(pipepp-mqtt PUBLIC PIPEPP_MQTT_HAS_PAHO_CPP=1)
else()
    find_package(PahoMqttC QUIET)
    if(PahoMqttC_FOUND)
        target_link_libraries(pipepp-mqtt PRIVATE paho-mqtt3a)
        target_compile_definitions(pipepp-mqtt PUBLIC PIPEPP_MQTT_HAS_PAHO_C=1)
    else()
        message(WARNING "Paho MQTT not found. pipepp-mqtt will have stub implementations.")
        target_compile_definitions(pipepp-mqtt PUBLIC PIPEPP_MQTT_STUB=1)
    endif()
endif()

add_library(pipepp::mqtt ALIAS pipepp-mqtt)

include(GNUInstallDirs)
install(TARGETS pipepp-mqtt
    EXPORT pipepp-mqtt-targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(DIRECTORY include/pipepp DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT pipepp-mqtt-targets
    FILE pipepp-mqtt-targets.cmake
    NAMESPACE pipepp::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/pipepp-mqtt
)

set(PIPEPP_MQTT_TEST_BROKER "bms-logging-server.local" CACHE STRING "MQTT broker for integration tests")
set(PIPEPP_MQTT_TEST_PORT 1883 CACHE STRING "MQTT broker port for integration tests")

if(PIPEPP_MQTT_BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()
