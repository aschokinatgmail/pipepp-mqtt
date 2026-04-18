#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt_config.hpp>
#include <type_traits>

using namespace pipepp::mqtt;
using namespace pipepp::core;

TEST(MqttConfigTest, DefaultConfigPollModeIsCallback) {
    EXPECT_TRUE((std::is_same_v<mqtt_default_config::poll_mode, mqtt_callback_tag>));
}

TEST(MqttConfigTest, EmbeddedConfigPollModeIsCallback) {
    EXPECT_TRUE((std::is_same_v<mqtt_embedded_config::poll_mode, mqtt_callback_tag>));
}

TEST(MqttConfigTest, DefaultConfigValues) {
    EXPECT_EQ(mqtt_default_config::default_keepalive, 60);
    EXPECT_TRUE(mqtt_default_config::default_clean_session);
    EXPECT_EQ(mqtt_default_config::source_size, 256u);
    EXPECT_EQ(mqtt_default_config::max_client_id_len, 64u);
    EXPECT_EQ(mqtt_default_config::max_broker_addr_len, 256u);
}

TEST(MqttConfigTest, EmbeddedConfigValues) {
    EXPECT_EQ(mqtt_embedded_config::default_keepalive, 30);
    EXPECT_TRUE(mqtt_embedded_config::default_clean_session);
    EXPECT_EQ(mqtt_embedded_config::source_size, 128u);
    EXPECT_EQ(mqtt_embedded_config::max_client_id_len, 32u);
    EXPECT_EQ(mqtt_embedded_config::max_broker_addr_len, 128u);
}

TEST(MqttConfigTest, DefaultConfigInheritsCoreDefaults) {
    EXPECT_GE(mqtt_default_config::max_topic_len, 64u);
    EXPECT_GE(mqtt_default_config::max_payload_len, 256u);
    EXPECT_GE(mqtt_default_config::max_stages, 4u);
}
