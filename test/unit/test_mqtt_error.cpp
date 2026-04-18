#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt_error.hpp>
#include <cstring>

using namespace pipepp::mqtt;

TEST(MqttErrorTest, ErrorCodeValues) {
    EXPECT_EQ(static_cast<int>(mqtt_error_code::none), 0);
    EXPECT_EQ(static_cast<int>(mqtt_error_code::protocol_error), 1);
    EXPECT_EQ(static_cast<int>(mqtt_error_code::invalid_client_id), 2);
    EXPECT_EQ(static_cast<int>(mqtt_error_code::broker_unavailable), 3);
    EXPECT_EQ(static_cast<int>(mqtt_error_code::auth_failed), 4);
    EXPECT_EQ(static_cast<int>(mqtt_error_code::qos_not_supported), 5);
    EXPECT_EQ(static_cast<int>(mqtt_error_code::payload_too_large), 6);
    EXPECT_EQ(static_cast<int>(mqtt_error_code::will_invalid), 7);
    EXPECT_EQ(static_cast<int>(mqtt_error_code::ssl_handshake_failed), 8);
}

TEST(MqttErrorTest, ErrorMessageNonNull) {
    EXPECT_NE(mqtt_error_message(mqtt_error_code::none), nullptr);
    EXPECT_NE(mqtt_error_message(mqtt_error_code::protocol_error), nullptr);
    EXPECT_NE(mqtt_error_message(mqtt_error_code::auth_failed), nullptr);
    EXPECT_NE(mqtt_error_message(mqtt_error_code::ssl_handshake_failed), nullptr);
}

TEST(MqttErrorTest, ErrorMessageContent) {
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::none), "no mqtt error");
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::protocol_error), "mqtt protocol error");
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::auth_failed), "mqtt authentication failed");
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::ssl_handshake_failed), "mqtt ssl handshake failed");
}
