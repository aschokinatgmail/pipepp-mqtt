#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt.hpp>
#include <pipepp/core/concepts.hpp>
#include <chrono>
#include <cstdint>

using namespace pipepp::mqtt;
using namespace pipepp::core;

static constexpr const char* kBroker = PIPEPP_MQTT_TEST_BROKER;
static constexpr int kPort = PIPEPP_MQTT_TEST_PORT;

template<typename Config>
static void connect_to_broker(mqtt_source<Config>& src) {
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value()) << "connect() failed — is broker running at "
                               << kBroker << ":" << kPort << "?";
    EXPECT_TRUE(src.is_connected());
}

static std::string unique_cov_topic(const char* suffix) {
    return std::string("cov/") +
           std::to_string(::getpid()) + "/" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
           "/" + suffix;
}

template<typename Config>
static void exercise_all() {
    mqtt_source<Config> src;
    EXPECT_FALSE(src.is_connected());

    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-cli-" + uid).c_str());
    src.set_keepalive(45);
    src.set_clean_session(false);
    src.set_automatic_reconnect(2, 60);
    src.set_mqtt_version(4);
    src.set_broker(kBroker, std::to_string(kPort).c_str());

    auto will_topic = unique_cov_topic("will");
    std::byte will_data[] = {std::byte{0x01}};
    src.set_will(will_topic.c_str(), will_data, 1, true);

    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());

    auto pub_topic = unique_cov_topic("pub");
    auto sr = src.subscribe(pub_topic.c_str(), 0);
    EXPECT_TRUE(sr.has_value());

    bool cb_called = false;
    src.set_message_callback([&](const message_view&) { cb_called = true; });

    std::byte pub_data[] = {std::byte{0x42}};
    auto pr = src.publish(pub_topic.c_str(), pub_data, 0);
    EXPECT_TRUE(pr.has_value());

    for (int i = 0; i < 20 && !cb_called; ++i) {
        src.poll();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_TRUE(cb_called);

    auto dr = src.disconnect();
    EXPECT_TRUE(dr.has_value());
    EXPECT_FALSE(src.is_connected());

    mqtt_source<Config> src2(std::move(src));
    EXPECT_FALSE(src2.is_connected());

    mqtt_source<Config> src3;
    src3 = std::move(src2);
    EXPECT_FALSE(src3.is_connected());
}

template<typename Config>
static void exercise_connected_destructor() {
    mqtt_source<Config> src;
    connect_to_broker(src);
}

template<typename Config>
static void exercise_valid_qos() {
    mqtt_source<Config> src;
    connect_to_broker(src);

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
    connect_to_broker(src1);
    mqtt_source<Config> src2;
    connect_to_broker(src2);
    EXPECT_TRUE(src2.is_connected());
    src2 = std::move(src1);
    EXPECT_TRUE(src2.is_connected());
}

TEST(MqttCoverage, MoveAssignConnectedDefault) {
    mqtt_source<mqtt_default_config> src1;
    connect_to_broker(src1);
    EXPECT_TRUE(src1.is_connected());
    mqtt_source<mqtt_default_config> src2;
    src2 = std::move(src1);
    EXPECT_TRUE(src2.is_connected());
}

TEST(MqttCoverage, MoveAssignConnectedEmbedded) {
    mqtt_source<mqtt_embedded_config> src1;
    connect_to_broker(src1);
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
    connect_to_broker(src);
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

TEST(MqttCoverage, SettersNoOpAfterConnect) {
    mqtt_source<mqtt_default_config> src;
    connect_to_broker(src);

    src.set_keepalive(99);
    src.set_clean_session(false);
    src.set_mqtt_version(5);
    src.set_client_id("changed");

    EXPECT_TRUE(src.is_connected());

    std::byte data[] = {std::byte{0x01}};
    auto pr = src.publish("setter/test", data, 0);
    EXPECT_TRUE(pr.has_value());

    src.disconnect();
}
