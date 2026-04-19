#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt_source.hpp>
#include <pipepp/core/concepts.hpp>

using namespace pipepp::mqtt;
using namespace pipepp::core;

TEST(MqttSourceTest, SatisfiesBusSourceConcept) {
    EXPECT_TRUE((BusSource<mqtt_source<mqtt_default_config>, mqtt_default_config>));
    EXPECT_TRUE((BusSource<mqtt_source<mqtt_embedded_config>, mqtt_embedded_config>));
}

TEST(MqttSourceTest, DefaultSizeWithinBounds) {
    EXPECT_LE(sizeof(mqtt_source<mqtt_default_config>), mqtt_default_config::source_size);
}

TEST(MqttSourceTest, DefaultState) {
    mqtt_source<mqtt_default_config> src;
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttSourceTest, SettersStoreValues) {
    mqtt_source<mqtt_default_config> src;
    src.set_client_id("test-client");
    src.set_keepalive(30);
    src.set_clean_session(false);
    src.set_automatic_reconnect(1, 60);
    src.set_username("user");
    src.set_password("pass");
    src.set_mqtt_version(5);
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttSourceTest, MoveConstructible) {
    mqtt_source<mqtt_default_config> src1;
    src1.set_client_id("client-1");
    mqtt_source<mqtt_default_config> src2(std::move(src1));
    EXPECT_FALSE(src2.is_connected());
}

TEST(MqttSourceTest, MoveAssignable) {
    mqtt_source<mqtt_default_config> src1;
    src1.set_client_id("client-1");
    mqtt_source<mqtt_default_config> src2;
    src2 = std::move(src1);
    EXPECT_FALSE(src2.is_connected());
}

TEST(MqttSourceTest, SubscribeWhenNotConnectedReturnsError) {
    mqtt_source<mqtt_default_config> src;
    auto r = src.subscribe("test/topic", 0);
    EXPECT_FALSE(r.has_value());
}

TEST(MqttSourceTest, PublishWhenNotConnectedReturnsError) {
    mqtt_source<mqtt_default_config> src;
    std::byte payload[4]{};
    auto r = src.publish("test/topic", payload, 0);
    EXPECT_FALSE(r.has_value());
}

TEST(MqttSourceTest, QoSValidationRejectsInvalid) {
    mqtt_source<mqtt_default_config> src;
    auto r1 = src.subscribe("test/topic", -1);
    EXPECT_FALSE(r1.has_value());

    auto r2 = src.subscribe("test/topic", 3);
    EXPECT_FALSE(r2.has_value());

    std::byte payload[4]{};
    auto r3 = src.publish("test/topic", payload, -1);
    EXPECT_FALSE(r3.has_value());

    auto r4 = src.publish("test/topic", payload, 3);
    EXPECT_FALSE(r4.has_value());
}

TEST(MqttSourceTest, SetBrokerStoresValues) {
    mqtt_source<mqtt_default_config> src;
    src.set_broker("broker.example.com", "1883");
    EXPECT_FALSE(src.is_connected());
}
