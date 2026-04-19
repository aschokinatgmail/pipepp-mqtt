#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt.hpp>
#include <pipepp/core/concepts.hpp>

#ifndef PIPEPP_MQTT_TEST_BROKER
#define PIPEPP_MQTT_TEST_BROKER "bms-logging-server.local"
#endif

#ifndef PIPEPP_MQTT_TEST_PORT
#define PIPEPP_MQTT_TEST_PORT 1883
#endif

using namespace pipepp::mqtt;
using namespace pipepp::core;

template<typename Config>
static std::string make_broker_uri() {
    return "mqtt://" PIPEPP_MQTT_TEST_BROKER ":" +
           std::to_string(PIPEPP_MQTT_TEST_PORT);
}

template<typename Config>
static bool try_connect(mqtt_source<Config>& src) {
    auto uri = basic_uri<Config>::parse(make_broker_uri<Config>());
    auto r = src.connect(uri.view());
    return r.has_value();
}

template<typename Config>
static void exercise_all() {
    mqtt_source<Config> src;
    EXPECT_FALSE(src.is_connected());

    src.set_client_id("cov-client");
    src.set_keepalive(45);
    src.set_clean_session(false);
    src.set_automatic_reconnect(2, 60);
    src.set_username("cov-user");
    src.set_password("cov-pass");
    src.set_mqtt_version(5);
    src.set_broker("cov-broker.local", "2883");

    std::byte will_data[] = {std::byte{0x01}};
    src.set_will("cov/will", will_data, 1, true);
    src.set_ssl("/cov/ca.crt", "/cov/client.crt", "/cov/client.key");

#ifdef PIPEPP_MQTT_STUB
    auto r = src.connect();
    EXPECT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());

    auto sr = src.subscribe("cov/topic", 0);
    EXPECT_TRUE(sr.has_value());

    std::byte pub_data[] = {std::byte{0x42}};
    auto pr = src.publish("cov/topic", pub_data, 0);
    EXPECT_TRUE(pr.has_value());

    bool cb_called = false;
    src.set_message_callback([&](const message_view&) { cb_called = true; });

    src.poll();

    auto dr = src.disconnect();
    EXPECT_TRUE(dr.has_value());
    EXPECT_FALSE(src.is_connected());
#else
    auto uri = basic_uri<Config>::parse(make_broker_uri<Config>());
    auto r = src.connect(uri.view());
    if (!r.has_value()) {
        GTEST_SKIP() << "Broker unreachable for coverage test";
    }
    EXPECT_TRUE(src.is_connected());

    auto sr = src.subscribe("cov/topic", 0);
    EXPECT_TRUE(sr.has_value());

    std::byte pub_data[] = {std::byte{0x42}};
    auto pr = src.publish("cov/topic", pub_data, 0);
    EXPECT_TRUE(pr.has_value());

    bool cb_called = false;
    src.set_message_callback([&](const message_view&) { cb_called = true; });

    src.poll();

    auto dr = src.disconnect();
    EXPECT_TRUE(dr.has_value());
    EXPECT_FALSE(src.is_connected());
#endif

    mqtt_source<Config> src2(std::move(src));
    EXPECT_FALSE(src2.is_connected());

    mqtt_source<Config> src3;
    src3 = std::move(src2);
    EXPECT_FALSE(src3.is_connected());
}

template<typename Config>
static void exercise_connected_destructor() {
    mqtt_source<Config> src;
#ifdef PIPEPP_MQTT_STUB
    src.connect();
    EXPECT_TRUE(src.is_connected());
#else
    auto uri = basic_uri<Config>::parse(make_broker_uri<Config>());
    auto r = src.connect(uri.view());
    if (!r.has_value()) {
        GTEST_SKIP() << "Broker unreachable";
    }
    EXPECT_TRUE(src.is_connected());
#endif
}

template<typename Config>
static void exercise_valid_qos() {
    mqtt_source<Config> src;
#ifdef PIPEPP_MQTT_STUB
    src.connect();
#else
    auto uri = basic_uri<Config>::parse(make_broker_uri<Config>());
    auto r = src.connect(uri.view());
    if (!r.has_value()) {
        GTEST_SKIP() << "Broker unreachable";
    }
#endif

    EXPECT_TRUE(src.subscribe("qos/0", 0).has_value());
    EXPECT_TRUE(src.subscribe("qos/1", 1).has_value());
    EXPECT_TRUE(src.subscribe("qos/2", 2).has_value());

    std::byte p[] = {std::byte{0x01}};
    EXPECT_TRUE(src.publish("qos/0", p, 0).has_value());
    EXPECT_TRUE(src.publish("qos/1", p, 1).has_value());
    EXPECT_TRUE(src.publish("qos/2", p, 2).has_value());

    src.disconnect();
}

TEST(MqttCoverage, DefaultConfigAllMethods) {
    exercise_all<mqtt_default_config>();
}

TEST(MqttCoverage, EmbeddedConfigAllMethods) {
    exercise_all<mqtt_embedded_config>();
}

TEST(MqttCoverage, DefaultConsumerConfigAllMethods) {
    exercise_all<mqtt_default_consumer_config>();
}

TEST(MqttCoverage, EmbeddedConsumerConfigAllMethods) {
    exercise_all<mqtt_embedded_consumer_config>();
}

TEST(MqttCoverage, ConnectedDestructorDefault) {
    exercise_connected_destructor<mqtt_default_config>();
}

TEST(MqttCoverage, ConnectedDestructorEmbedded) {
    exercise_connected_destructor<mqtt_embedded_config>();
}

TEST(MqttCoverage, ConnectedDestructorConsumer) {
    exercise_connected_destructor<mqtt_default_consumer_config>();
}

TEST(MqttCoverage, ConnectedDestructorEmbeddedConsumer) {
    exercise_connected_destructor<mqtt_embedded_consumer_config>();
}

TEST(MqttCoverage, ValidQoSDefault) {
    exercise_valid_qos<mqtt_default_config>();
}

TEST(MqttCoverage, ValidQoSEmbedded) {
    exercise_valid_qos<mqtt_embedded_config>();
}

TEST(MqttCoverage, ValidQoSConsumer) {
    exercise_valid_qos<mqtt_default_consumer_config>();
}

TEST(MqttCoverage, ValidQoSEmbeddedConsumer) {
    exercise_valid_qos<mqtt_embedded_consumer_config>();
}

TEST(MqttCoverage, AllErrorMessages) {
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::none), "no mqtt error");
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::protocol_error), "mqtt protocol error");
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::invalid_client_id), "invalid mqtt client id");
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::broker_unavailable), "mqtt broker unavailable");
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::auth_failed), "mqtt authentication failed");
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::qos_not_supported), "mqtt qos not supported");
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::payload_too_large), "mqtt payload too large");
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::will_invalid), "mqtt will message invalid");
    EXPECT_STREQ(mqtt_error_message(mqtt_error_code::ssl_handshake_failed), "mqtt ssl handshake failed");
}

template<typename Config>
static void exercise_move_assign_connected_dst() {
    mqtt_source<Config> src1;
#ifdef PIPEPP_MQTT_STUB
    src1.connect();
#else
    auto uri1 = basic_uri<Config>::parse(make_broker_uri<Config>());
    auto r1 = src1.connect(uri1.view());
    if (!r1.has_value()) {
        GTEST_SKIP() << "Broker unreachable";
    }
#endif
    mqtt_source<Config> src2;
#ifdef PIPEPP_MQTT_STUB
    src2.connect();
#else
    auto uri2 = basic_uri<Config>::parse(make_broker_uri<Config>());
    auto r2 = src2.connect(uri2.view());
    if (!r2.has_value()) {
        GTEST_SKIP() << "Broker unreachable";
    }
#endif
    EXPECT_TRUE(src2.is_connected());
    src2 = std::move(src1);
    EXPECT_TRUE(src2.is_connected());
}

TEST(MqttCoverage, MoveAssignConnectedDefault) {
    mqtt_source<mqtt_default_config> src1;
#ifdef PIPEPP_MQTT_STUB
    src1.connect();
#else
    auto uri = basic_uri<mqtt_default_config>::parse(make_broker_uri<mqtt_default_config>());
    auto r = src1.connect(uri.view());
    if (!r.has_value()) {
        GTEST_SKIP() << "Broker unreachable";
    }
#endif
    EXPECT_TRUE(src1.is_connected());
    mqtt_source<mqtt_default_config> src2;
    src2 = std::move(src1);
    EXPECT_TRUE(src2.is_connected());
}

TEST(MqttCoverage, MoveAssignConnectedEmbedded) {
    mqtt_source<mqtt_embedded_config> src1;
#ifdef PIPEPP_MQTT_STUB
    src1.connect();
#else
    auto uri = basic_uri<mqtt_embedded_config>::parse(make_broker_uri<mqtt_embedded_config>());
    auto r = src1.connect(uri.view());
    if (!r.has_value()) {
        GTEST_SKIP() << "Broker unreachable";
    }
#endif
    mqtt_source<mqtt_embedded_config> src2;
    src2 = std::move(src1);
    EXPECT_TRUE(src2.is_connected());
}

TEST(MqttCoverage, MoveAssignConnectedDstDefault) {
    exercise_move_assign_connected_dst<mqtt_default_config>();
}

TEST(MqttCoverage, MoveAssignConnectedDstEmbedded) {
    exercise_move_assign_connected_dst<mqtt_embedded_config>();
}

TEST(MqttCoverage, MoveAssignConnectedDstConsumer) {
    exercise_move_assign_connected_dst<mqtt_default_consumer_config>();
}

TEST(MqttCoverage, MoveAssignConnectedDstEmbeddedConsumer) {
    exercise_move_assign_connected_dst<mqtt_embedded_consumer_config>();
}

template<typename Config>
static void exercise_self_assign() {
    mqtt_source<Config> src;
#ifdef PIPEPP_MQTT_STUB
    src.connect();
#else
    auto uri = basic_uri<Config>::parse(make_broker_uri<Config>());
    auto r = src.connect(uri.view());
    if (!r.has_value()) {
        GTEST_SKIP() << "Broker unreachable";
    }
#endif
    src = std::move(src);
}

template<typename Config>
static void exercise_move_assign_from_moved() {
    mqtt_source<Config> a;
    mqtt_source<Config> b(std::move(a));
    mqtt_source<Config> c;
    a = std::move(c);
}

TEST(MqttCoverage, SelfAssignDefault) { exercise_self_assign<mqtt_default_config>(); }
TEST(MqttCoverage, SelfAssignEmbedded) { exercise_self_assign<mqtt_embedded_config>(); }
TEST(MqttCoverage, SelfAssignConsumer) { exercise_self_assign<mqtt_default_consumer_config>(); }
TEST(MqttCoverage, SelfAssignEmbeddedConsumer) { exercise_self_assign<mqtt_embedded_consumer_config>(); }

TEST(MqttCoverage, MoveAssignFromMovedDefault) { exercise_move_assign_from_moved<mqtt_default_config>(); }
TEST(MqttCoverage, MoveAssignFromMovedEmbedded) { exercise_move_assign_from_moved<mqtt_embedded_config>(); }
TEST(MqttCoverage, MoveAssignFromMovedConsumer) { exercise_move_assign_from_moved<mqtt_default_consumer_config>(); }
TEST(MqttCoverage, MoveAssignFromMovedEmbeddedConsumer) { exercise_move_assign_from_moved<mqtt_embedded_consumer_config>(); }

TEST(MqttCoverage, ErrorMessageDefaultCase) {
    auto ec = static_cast<mqtt_error_code>(255);
    const char* msg = mqtt_error_message(ec);
    EXPECT_NE(msg, nullptr);
    EXPECT_STREQ(msg, "unknown mqtt error");
}

template<typename Config>
static void exercise_set_will_overflow() {
    mqtt_source<Config> src;
    std::byte large[Config::max_payload_len + 64]{};
    src.set_will("overflow/topic", large, 0, false);
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttCoverage, SetWillOverflowDefault) { exercise_set_will_overflow<mqtt_default_config>(); }
TEST(MqttCoverage, SetWillOverflowEmbedded) { exercise_set_will_overflow<mqtt_embedded_config>(); }
TEST(MqttCoverage, SetWillOverflowConsumer) { exercise_set_will_overflow<mqtt_default_consumer_config>(); }
TEST(MqttCoverage, SetWillOverflowEmbeddedConsumer) { exercise_set_will_overflow<mqtt_embedded_consumer_config>(); }
