#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt_source.hpp>
#include <pipepp/core/uri.hpp>

#ifndef PIPEPP_MQTT_TEST_BROKER
#define PIPEPP_MQTT_TEST_BROKER "bms-logging-server.local"
#endif

#ifndef PIPEPP_MQTT_TEST_PORT
#define PIPEPP_MQTT_TEST_PORT 1883
#endif

using namespace pipepp::mqtt;
using namespace pipepp::core;

static std::string broker_uri() {
    return "mqtt://" PIPEPP_MQTT_TEST_BROKER ":" +
           std::to_string(PIPEPP_MQTT_TEST_PORT);
}

static std::string unique_topic(const char* suffix) {
    return std::string("pipepp-test/") +
           std::to_string(::getpid()) + "/" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
           "/" + suffix;
}

TEST(MqttConnectTest, ConnectAndDisconnect) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse(broker_uri());
    auto r = src.connect(uri.view());
    if (!r.has_value()) {
        GTEST_SKIP() << "Broker unreachable at " << broker_uri();
    }
    EXPECT_TRUE(src.is_connected());

    auto dr = src.disconnect();
    EXPECT_TRUE(dr.has_value());
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttConnectTest, ConnectWithUriParams) {
    mqtt_source<mqtt_default_config> src;
    auto full = broker_uri() + "/?keepalive=30&clean=1";
    auto uri = basic_uri<mqtt_default_config>::parse(full);
    auto r = src.connect(uri.view());
    if (!r.has_value()) {
        GTEST_SKIP() << "Broker unreachable";
    }
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

TEST(MqttConnectTest, ConnectFailureInvalidHost) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://invalid.host.that.does.not.exist:19999");
    auto r = src.connect(uri.view());
    EXPECT_FALSE(r.has_value());
}
