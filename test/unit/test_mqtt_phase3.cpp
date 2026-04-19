#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt_options.hpp>
#include <pipepp/mqtt/mqtt_registry.hpp>
#include <pipepp/mqtt/mqtt_source.hpp>
#include <pipepp/core/source_registry.hpp>

using namespace pipepp::mqtt;
using namespace pipepp::core;

TEST(MqttOptionsTest, ConnectOptionsDefaults) {
    mqtt_connect_options<mqtt_default_config> opts;
    EXPECT_EQ(opts.keepalive, 60);
    EXPECT_TRUE(opts.clean_session);
    EXPECT_FALSE(opts.auto_reconnect);
    EXPECT_EQ(opts.mqtt_version, 4);
    EXPECT_TRUE(opts.client_id.empty());
    EXPECT_TRUE(opts.username.empty());
    EXPECT_TRUE(opts.password.empty());
}

TEST(MqttOptionsTest, WillOptionsDefaults) {
    mqtt_will_options<mqtt_default_config> opts;
    EXPECT_TRUE(opts.topic.empty());
    EXPECT_EQ(opts.payload_len, 0u);
    EXPECT_EQ(opts.qos, 0);
    EXPECT_FALSE(opts.retained);
}

TEST(MqttOptionsTest, SslOptionsDefaults) {
    mqtt_ssl_options<mqtt_default_config> opts;
    EXPECT_TRUE(opts.trust_store.empty());
    EXPECT_TRUE(opts.key_store.empty());
    EXPECT_TRUE(opts.private_key.empty());
    EXPECT_TRUE(opts.verify);
}

TEST(MqttOptionsTest, EmbeddedConfigOptions) {
    mqtt_connect_options<mqtt_embedded_config> opts;
    EXPECT_EQ(opts.keepalive, 30);
}

TEST(MqttRegistryTest, RegisterMqttSchemes) {
    source_registry<mqtt_default_config> registry;
    register_mqtt(registry);
    EXPECT_TRUE(registry.has_scheme("mqtt"));
    EXPECT_TRUE(registry.has_scheme("mqtts"));
    EXPECT_EQ(registry.size(), 2u);
}

TEST(MqttRegistryTest, CreateMqttSource) {
    source_registry<mqtt_default_config> registry;
    register_mqtt(registry);
    auto src = registry.create("mqtt://broker:1883");
    EXPECT_TRUE(static_cast<bool>(src));
}

TEST(MqttRegistryTest, CreateMqttsSource) {
    source_registry<mqtt_default_config> registry;
    register_mqtt(registry);
    auto src = registry.create("mqtts://broker:8883");
    EXPECT_TRUE(static_cast<bool>(src));
}

TEST(MqttRegistryTest, UnknownSchemeReturnsEmpty) {
    source_registry<mqtt_default_config> registry;
    register_mqtt(registry);
    auto src = registry.create("unknown://broker");
    EXPECT_FALSE(static_cast<bool>(src));
}

TEST(MqttSourcePhase3Test, SetWillStoresCorrectly) {
    mqtt_source<mqtt_default_config> src;
    std::byte payload[] = {std::byte{0xDE}, std::byte{0xAD}};
    src.set_will("status/offline", payload, 1, true);
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttSourcePhase3Test, SetSslStoresCorrectly) {
    mqtt_source<mqtt_default_config> src;
    src.set_ssl("/etc/ssl/ca.crt", "/etc/ssl/client.crt", "/etc/ssl/client.key");
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttSourcePhase3Test, SetMqttVersion) {
    mqtt_source<mqtt_default_config> src;
    src.set_mqtt_version(5);
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttSourcePhase3Test, ConsumerConfigSatisfiesBusSource) {
    EXPECT_TRUE((BusSource<mqtt_source<mqtt_default_consumer_config>, mqtt_default_consumer_config>));
    EXPECT_TRUE((BusSource<mqtt_source<mqtt_embedded_consumer_config>, mqtt_embedded_consumer_config>));
}
