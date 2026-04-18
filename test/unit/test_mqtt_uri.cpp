#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt_source.hpp>
#include <pipepp/core/uri.hpp>

using namespace pipepp::mqtt;
using namespace pipepp::core;

TEST(MqttUriTest, ParseMqttScheme) {
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://broker.example.com:1883");
    ASSERT_TRUE(uri);
    EXPECT_EQ(uri.view().scheme, "mqtt");
    EXPECT_EQ(uri.view().host, "broker.example.com");
    EXPECT_EQ(uri.view().port, "1883");
}

TEST(MqttUriTest, ParseMqttsScheme) {
    auto uri = basic_uri<mqtt_default_config>::parse("mqtts://broker.example.com:8883");
    ASSERT_TRUE(uri);
    EXPECT_EQ(uri.view().scheme, "mqtts");
    EXPECT_EQ(uri.view().host, "broker.example.com");
    EXPECT_EQ(uri.view().port, "8883");
}

TEST(MqttUriTest, ParseUserinfo) {
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://user:pass@broker:1883");
    ASSERT_TRUE(uri);
    auto info = uri.view().userinfo();
    EXPECT_FALSE(info.empty());
    auto colon = info.find(':');
    ASSERT_NE(colon, std::string_view::npos);
    EXPECT_EQ(info.substr(0, colon), "user");
    EXPECT_EQ(info.substr(colon + 1), "pass");
}

TEST(MqttUriTest, ParseQueryParams) {
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://broker/?keepalive=30&clean=0&version=5");
    ASSERT_TRUE(uri);
    EXPECT_EQ(uri.view().query, "keepalive=30&clean=0&version=5");
}

TEST(MqttUriTest, ParseQueryClientId) {
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://broker/?clientid=my-client");
    ASSERT_TRUE(uri);
    EXPECT_EQ(uri.view().query, "clientid=my-client");
}

TEST(MqttUriTest, EmptyUriFallsBackToDefaults) {
    auto uri = basic_uri<mqtt_default_config>::parse("");
    EXPECT_FALSE(uri);
}

TEST(MqttUriTest, HostOnlyNoPort) {
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://broker.local");
    ASSERT_TRUE(uri);
    EXPECT_EQ(uri.view().host, "broker.local");
    EXPECT_TRUE(uri.view().port.empty());
}

TEST(MqttUriTest, SourceSetBrokerFallback) {
    mqtt_source<mqtt_default_config> src;
    src.set_broker("fallback.local", "2883");
    EXPECT_FALSE(src.is_connected());
}
