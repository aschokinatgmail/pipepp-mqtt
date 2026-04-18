#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt_source.hpp>

#ifndef PIPEPP_MQTT_TEST_BROKER
#define PIPEPP_MQTT_TEST_BROKER "bms-logging-server.local"
#endif

#ifndef PIPEPP_MQTT_TEST_PORT
#define PIPEPP_MQTT_TEST_PORT 1883
#endif

using namespace pipepp::mqtt;

TEST(MqttConnectTest, Placeholder) {
    GTEST_SKIP() << "Integration tests require Phase 2 implementation";
}
