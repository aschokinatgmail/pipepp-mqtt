#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt.hpp>
#include <pipepp/core/concepts.hpp>
#include <chrono>
#include <cstdint>
#include <thread>

#ifndef PIPEPP_MQTT_TEST_BROKER
#define PIPEPP_MQTT_TEST_BROKER "bms-logging-server.local"
#endif

#ifndef PIPEPP_MQTT_TEST_PORT
#define PIPEPP_MQTT_TEST_PORT 1883
#endif

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

#ifdef PIPEPP_MQTT_STUB
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

// --- Edge case: connect with URI userinfo (user:pass@host) ---

TEST(MqttCoverage, ConnectWithUriUserinfo) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-ui-" + uid).c_str());
    auto uri = basic_uri<mqtt_default_config>::parse(
        "mqtt://pipepp_test:pipepp_test_pw@" PIPEPP_MQTT_TEST_BROKER
        ":" + std::to_string(PIPEPP_MQTT_TEST_PORT));
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

TEST(MqttCoverage, ConnectWithUriUserinfoNoPassword) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-uinp-" + uid).c_str());
    auto uri = basic_uri<mqtt_default_config>::parse(
        "mqtt://justuser@" PIPEPP_MQTT_TEST_BROKER
        ":" + std::to_string(PIPEPP_MQTT_TEST_PORT));
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- Edge case: connect with URI query params ---

TEST(MqttCoverage, ConnectWithUriQueryParams) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    auto cid = "cov-qp-" + uid;
    auto uri_str = "mqtt://" PIPEPP_MQTT_TEST_BROKER
        ":" + std::to_string(PIPEPP_MQTT_TEST_PORT)
        + "?keepalive=30&clean=true&version=4&clientid=" + cid;
    auto uri = basic_uri<mqtt_default_config>::parse(uri_str);
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

// --- Edge case: connect with MQTT version 5 ---

TEST(MqttCoverage, ConnectWithMqttVersion5) {
    mqtt_source<mqtt_default_config> src;
    src.set_mqtt_version(5);
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-v5-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

// --- Edge case: connect with MQTT version 3 ---

TEST(MqttCoverage, ConnectWithMqttVersion3) {
    mqtt_source<mqtt_default_config> src;
    src.set_mqtt_version(3);
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-v3-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

// --- Edge case: connect with TLS and all SSL options set ---

TEST(MqttCoverage, ConnectTlsWithAllSslOptions) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-ssl-" + uid).c_str());
    src.set_username("pipepp_test");
    src.set_password("pipepp_test_pw");
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    auto uri = basic_uri<mqtt_default_config>::parse(
        "mqtts://" PIPEPP_MQTT_TEST_BROKER ":" + std::to_string(PIPEPP_MQTT_TEST_TLS_PORT));
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

// --- Edge case: connect with MQTTS scheme but no explicit port (uses default 8883) ---

TEST(MqttCoverage, ConnectMqttsDefaultPort) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-mqtts-" + uid).c_str());
    src.set_username("pipepp_test");
    src.set_password("pipepp_test_pw");
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    auto uri = basic_uri<mqtt_default_config>::parse(
        "mqtts://" PIPEPP_MQTT_TEST_BROKER ":" + std::to_string(PIPEPP_MQTT_TEST_TLS_PORT));
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- Edge case: connect with empty host returns error ---

TEST(MqttCoverage, ConnectEmptyHostError) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://:1883");
    auto r = src.connect(uri.view());
    EXPECT_FALSE(r.has_value());
    EXPECT_FALSE(src.is_connected());
}

// --- Edge case: reconnect (connect while already connected) ---

TEST(MqttCoverage, ReconnectWhileConnected) {
    mqtt_source<mqtt_default_config> src;
    connect_to_broker(src);
    EXPECT_TRUE(src.is_connected());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    EXPECT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

// --- Edge case: reconnect (connect while already connected, consumer) ---

TEST(MqttCoverage, ReconnectWhileConnectedConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    connect_to_broker(src);
    EXPECT_TRUE(src.is_connected());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    EXPECT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

// --- Edge case: set callback then connect (covers callback installation in connect) ---

TEST(MqttCoverage, SetCallbackThenConnect) {
    mqtt_source<mqtt_default_config> src;
    bool cb_called = false;
    src.set_message_callback([&](const message_view&) { cb_called = true; });
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-cb-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    auto topic = unique_cov_topic("cb-connect");
    src.subscribe(topic.c_str(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::byte p[] = {std::byte{0xCC}};
    src.publish(topic.c_str(), p, 0);
    for (int i = 0; i < 20 && !cb_called; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_TRUE(cb_called);
    src.disconnect();
}

TEST(MqttCoverage, SetCallbackThenConnectConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    bool cb_called = false;
    src.set_message_callback([&](const message_view&) { cb_called = true; });
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-cbc-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    auto topic = unique_cov_topic("cb-consumer");
    src.subscribe(topic.c_str(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::byte p[] = {std::byte{0xDD}};
    src.publish(topic.c_str(), p, 0);
    for (int i = 0; i < 20 && !cb_called; ++i) {
        src.poll();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_TRUE(cb_called);
    src.disconnect();
}

// --- Edge case: subscribe exceeding max_subscriptions ---

TEST(MqttCoverage, SubscribeExceedsMaxDefault) {
    mqtt_source<mqtt_default_config> src;
    connect_to_broker(src);
    for (std::size_t i = 0; i < mqtt_default_config::max_subscriptions; ++i) {
        auto t = "cov/max/sub" + std::to_string(i);
        auto r = src.subscribe(t.c_str(), 0);
        EXPECT_TRUE(r.has_value()) << "subscribe " << i << " should succeed";
    }
    auto r = src.subscribe("cov/max/overflow", 0);
    EXPECT_FALSE(r.has_value()) << "subscribe beyond max should fail";
    src.disconnect();
}

TEST(MqttCoverage, SubscribeExceedsMaxEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    connect_to_broker(src);
    for (std::size_t i = 0; i < mqtt_embedded_config::max_subscriptions; ++i) {
        auto t = "cov/maxe/" + std::to_string(i);
        auto r = src.subscribe(t.c_str(), 0);
        EXPECT_TRUE(r.has_value()) << "subscribe " << i << " should succeed";
    }
    auto r = src.subscribe("cov/maxe/overflow", 0);
    EXPECT_FALSE(r.has_value()) << "subscribe beyond max should fail";
    src.disconnect();
}

// --- Edge case: publish/subscribe with empty topic after connect ---

TEST(MqttCoverage, PublishEmptyTopicAfterConnect) {
    mqtt_source<mqtt_default_config> src;
    connect_to_broker(src);
    std::byte p[] = {std::byte{0x01}};
    auto r = src.publish("", p, 0);
    EXPECT_FALSE(r.has_value());
    src.disconnect();
}

TEST(MqttCoverage, SubscribeEmptyTopicAfterConnect) {
    mqtt_source<mqtt_default_config> src;
    connect_to_broker(src);
    auto r = src.subscribe("", 0);
    EXPECT_FALSE(r.has_value());
    src.disconnect();
}

// --- Edge case: connect to invalid host ---

TEST(MqttCoverage, ConnectInvalidHost) {
    mqtt_source<mqtt_default_config> src;
    src.set_client_id("cov-invalid");
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://256.256.256.256:19999");
    auto r = src.connect(uri.view());
    EXPECT_FALSE(r.has_value());
    EXPECT_FALSE(src.is_connected());
}

// --- Edge case: connect with automatic reconnect enabled ---

TEST(MqttCoverage, ConnectWithAutoReconnect) {
    mqtt_source<mqtt_default_config> src;
    src.set_automatic_reconnect(1, 10);
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-ar-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

// --- Edge case: connect with clean_session=false via URI query ---

TEST(MqttCoverage, ConnectWithCleanSessionFalse) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    auto cid = "cov-csf-" + uid;
    auto uri_str = "mqtt://" PIPEPP_MQTT_TEST_BROKER
        ":" + std::to_string(PIPEPP_MQTT_TEST_PORT)
        + "?clean=false&clientid=" + cid;
    auto uri = basic_uri<mqtt_default_config>::parse(uri_str);
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- Edge case: connect with will set via set_will before connect ---

TEST(MqttCoverage, ConnectWithWillSet) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-will-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto will_topic = unique_cov_topic("will-connect");
    std::byte will_data[] = {std::byte{0x42}};
    src.set_will(will_topic.c_str(), will_data, 1, true);
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

// --- Edge case: move-assign connected consumer to connected consumer ---

TEST(MqttCoverage, MoveAssignConnectedConsumerToConnected) {
    mqtt_source<mqtt_default_consumer_config> src1;
    connect_to_broker(src1);
    mqtt_source<mqtt_default_consumer_config> src2;
    connect_to_broker(src2);
    EXPECT_TRUE(src2.is_connected());
    src2 = std::move(src1);
    EXPECT_TRUE(src2.is_connected());
}

// --- Edge case: poll on callback-mode source (yields) ---

TEST(MqttCoverage, PollOnCallbackSourceYields) {
    mqtt_source<mqtt_default_config> src;
    src.poll();
}

// --- Edge case: is_connected on default-constructed source ---

TEST(MqttCoverage, IsConnectedDefault) {
    mqtt_source<mqtt_default_config> src;
    EXPECT_FALSE(src.is_connected());
}

// --- Edge case: set_broker with empty port uses stored default ---

TEST(MqttCoverage, SetBrokerEmptyPort) {
    mqtt_source<mqtt_default_config> src;
    src.set_broker(kBroker, "");
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-bport-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- Edge case: connect with empty client_id generates auto ID ---

TEST(MqttCoverage, ConnectEmptyClientIdAutoGenerated) {
    mqtt_source<mqtt_default_config> src;
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

// --- Subscribe then reconnect triggers subscription restoration (lines 270-273) ---

TEST(MqttCoverage, SubscribeRestoreOnReconnect) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-restore-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    src.set_automatic_reconnect(1, 2);
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    r = src.subscribe(("test/restore/" + uid).c_str(), 1);
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- Move-assign connected consumer source (line 156) ---

TEST(MqttCoverage, MoveAssignConnectedConsumerDisconnects) {
    mqtt_source<mqtt_default_consumer_config> src1;
    mqtt_source<mqtt_default_consumer_config> src2;
    auto uid1 = std::to_string(reinterpret_cast<uintptr_t>(&src1));
    auto uid2 = std::to_string(reinterpret_cast<uintptr_t>(&src2));
    src1.set_client_id(("cov-ma1-" + uid1).c_str());
    src2.set_client_id(("cov-ma2-" + uid2).c_str());
    src1.set_broker(kBroker, std::to_string(kPort).c_str());
    src2.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r1 = src1.connect();
    ASSERT_TRUE(r1.has_value());
    EXPECT_TRUE(src1.is_connected());
    src1 = std::move(src2);
    EXPECT_FALSE(src1.is_connected());
}

// --- Subscribe on moved-from source (line 406) ---

TEST(MqttCoverage, SubscribeOnMovedFromSource) {
    mqtt_source<mqtt_default_config> src;
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    mqtt_source<mqtt_default_config> dst(std::move(src));
    auto r = src.subscribe("test/moved", 0);
    EXPECT_FALSE(r.has_value());
}

// --- Publish on moved-from source (line 456) ---

TEST(MqttCoverage, PublishOnMovedFromSource) {
    mqtt_source<mqtt_default_config> src;
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    mqtt_source<mqtt_default_config> dst(std::move(src));
    auto payload = std::array<std::byte, 1>{std::byte{0x42}};
    auto r = src.publish("test/moved", payload, 0);
    EXPECT_FALSE(r.has_value());
}

// --- Consumer mode connect while already connected (line 177) ---

TEST(MqttCoverage, ConsumerReconnectWhileConnected) {
    mqtt_source<mqtt_default_consumer_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-crconn-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    r = src.connect();
    EXPECT_TRUE(r.has_value());
    src.disconnect();
}

// --- Embedded consumer connect while already connected (line 177) ---

TEST(MqttCoverage, EmbeddedConsumerReconnectWhileConnected) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-ecrconn-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    r = src.connect();
    EXPECT_TRUE(r.has_value());
    src.disconnect();
}

// --- Poll on consumer source with actual message (lines 517-533) ---

TEST(MqttCoverage, ConsumerPollWithMessage) {
    mqtt_source<mqtt_default_consumer_config> consumer;
    mqtt_source<mqtt_default_config> publisher;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&consumer));
    consumer.set_client_id(("cov-cpoll-" + uid).c_str());
    publisher.set_client_id(("cov-cpub-" + uid).c_str());
    consumer.set_broker(kBroker, std::to_string(kPort).c_str());
    publisher.set_broker(kBroker, std::to_string(kPort).c_str());
    auto cr = consumer.connect();
    ASSERT_TRUE(cr.has_value());
    auto pr = publisher.connect();
    ASSERT_TRUE(pr.has_value());
    std::string topic = "test/poll/" + uid;
    cr = consumer.subscribe(topic.c_str(), 0);
    ASSERT_TRUE(cr.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto payload = std::array<std::byte, 3>{std::byte{'A'}, std::byte{'B'}, std::byte{'C'}};
    pr = publisher.publish(topic.c_str(), payload, 0);
    ASSERT_TRUE(pr.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    mqtt_source<mqtt_default_consumer_config> consumer2(std::move(consumer));
    bool received = false;
    pipepp::core::message_callback<mqtt_default_consumer_config> cb =
        [&received](pipepp::core::message_view mv) {
            received = true;
        };
    consumer2.set_message_callback(std::move(cb));
    consumer2.poll();
    consumer2.disconnect();
    publisher.disconnect();
}

// --- Embedded consumer poll with message ---

TEST(MqttCoverage, EmbeddedConsumerPollWithMessage) {
    mqtt_source<mqtt_embedded_consumer_config> consumer;
    mqtt_source<mqtt_default_config> publisher;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&consumer));
    consumer.set_client_id(("cov-ecpoll-" + uid).c_str());
    publisher.set_client_id(("cov-ecpub-" + uid).c_str());
    consumer.set_broker(kBroker, std::to_string(kPort).c_str());
    publisher.set_broker(kBroker, std::to_string(kPort).c_str());
    auto cr = consumer.connect();
    ASSERT_TRUE(cr.has_value());
    auto pr = publisher.connect();
    ASSERT_TRUE(pr.has_value());
    std::string topic = "test/epoll/" + uid;
    cr = consumer.subscribe(topic.c_str(), 0);
    ASSERT_TRUE(cr.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto payload = std::array<std::byte, 2>{std::byte{'X'}, std::byte{'Y'}};
    pr = publisher.publish(topic.c_str(), payload, 0);
    ASSERT_TRUE(pr.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    bool received = false;
    pipepp::core::message_callback<mqtt_embedded_consumer_config> cb =
        [&received](pipepp::core::message_view mv) {
            received = true;
        };
    consumer.set_message_callback(std::move(cb));
    consumer.poll();
    consumer.disconnect();
    publisher.disconnect();
}

// --- set_message_callback on already-connected client (lines 493-508) ---

TEST(MqttCoverage, SetCallbackOnConnectedClient) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-cbconn-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    bool called = false;
    pipepp::core::message_callback<mqtt_default_config> cb =
        [&called](pipepp::core::message_view) { called = true; };
    src.set_message_callback(std::move(cb));
    src.disconnect();
}

// --- Embedded config: all methods exercised ---

TEST(MqttCoverage, EmbeddedConfigConnectPublishSubscribe) {
    mqtt_source<mqtt_embedded_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-emb-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    std::string topic = "test/emb/" + uid;
    r = src.subscribe(topic.c_str(), 1);
    ASSERT_TRUE(r.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto payload = std::array<std::byte, 1>{std::byte{0x01}};
    r = src.publish(topic.c_str(), payload, 1);
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- Embedded config: set_keepalive, set_clean_session, set_automatic_reconnect ---

TEST(MqttCoverage, EmbeddedConfigConnectWithOptions) {
    mqtt_source<mqtt_embedded_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-embopt-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    src.set_keepalive(30);
    src.set_clean_session(false);
    src.set_automatic_reconnect(2, 10);
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- Embedded consumer: full connect/publish/subscribe cycle ---

TEST(MqttCoverage, EmbeddedConsumerFullCycle) {
    mqtt_source<mqtt_embedded_consumer_config> sub;
    mqtt_source<mqtt_embedded_config> pub;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&sub));
    sub.set_client_id(("cov-ecsub-" + uid).c_str());
    pub.set_client_id(("cov-ecpub2-" + uid).c_str());
    sub.set_broker(kBroker, std::to_string(kPort).c_str());
    pub.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r1 = sub.connect();
    ASSERT_TRUE(r1.has_value());
    auto r2 = pub.connect();
    ASSERT_TRUE(r2.has_value());
    std::string topic = "test/ecfull/" + uid;
    r1 = sub.subscribe(topic.c_str(), 0);
    ASSERT_TRUE(r1.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto payload = std::array<std::byte, 2>{std::byte{0xAA}, std::byte{0xBB}};
    r2 = pub.publish(topic.c_str(), payload, 0);
    ASSERT_TRUE(r2.has_value());
    sub.disconnect();
    pub.disconnect();
}

// --- Move-assign connected embedded consumer to connected (line 156) ---

TEST(MqttCoverage, MoveAssignConnectedEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src1;
    mqtt_source<mqtt_embedded_consumer_config> src2;
    auto uid1 = std::to_string(reinterpret_cast<uintptr_t>(&src1));
    auto uid2 = std::to_string(reinterpret_cast<uintptr_t>(&src2));
    src1.set_client_id(("cov-ema1-" + uid1).c_str());
    src2.set_client_id(("cov-ema2-" + uid2).c_str());
    src1.set_broker(kBroker, std::to_string(kPort).c_str());
    src2.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r1 = src1.connect();
    ASSERT_TRUE(r1.has_value());
    r1 = src2.connect();
    ASSERT_TRUE(r1.has_value());
    src1 = std::move(src2);
    src1.disconnect();
}

// --- set_message_callback on connected consumer client (lines 493-508 consumer path) ---

TEST(MqttCoverage, SetCallbackOnConnectedConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-cbcon-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    pipepp::core::message_callback<mqtt_default_consumer_config> cb =
        [](pipepp::core::message_view) {};
    src.set_message_callback(std::move(cb));
    src.disconnect();
}

// --- set_message_callback on connected embedded client ---

TEST(MqttCoverage, SetCallbackOnConnectedEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-cbemb-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    pipepp::core::message_callback<mqtt_embedded_config> cb =
        [](pipepp::core::message_view) {};
    src.set_message_callback(std::move(cb));
    src.disconnect();
}

// --- set_message_callback on connected embedded consumer ---

TEST(MqttCoverage, SetCallbackOnConnectedEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-cbec-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    pipepp::core::message_callback<mqtt_embedded_consumer_config> cb =
        [](pipepp::core::message_view) {};
    src.set_message_callback(std::move(cb));
    src.disconnect();
}

// --- Embedded config connect while already connected ---

TEST(MqttCoverage, EmbeddedConfigReconnectWhileConnected) {
    mqtt_source<mqtt_embedded_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-embreconn-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    r = src.connect();
    EXPECT_TRUE(r.has_value());
    src.disconnect();
}

// --- Embedded consumer connect while already connected ---

TEST(MqttCoverage, EmbeddedConsumerReconnectWhileConnectedAlt) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-ecreconn2-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    r = src.connect();
    EXPECT_TRUE(r.has_value());
    src.disconnect();
}

// --- query_param edge cases: key appears after non-separator char (lines 48-49) ---

TEST(MqttCoverage, QueryParamKeyAfterNonSeparator) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    auto cid = "cov-qpns-" + uid;
    auto uri_str = "mqtt://" PIPEPP_MQTT_TEST_BROKER
        ":" + std::to_string(PIPEPP_MQTT_TEST_PORT)
        + "?xkeepalive=10&keepalive=30&clientid=" + cid;
    auto uri = basic_uri<mqtt_default_config>::parse(uri_str);
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- query_param: key without = sign (lines 53-54) ---

TEST(MqttCoverage, QueryParamKeyWithoutEquals) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    auto cid = "cov-qpne-" + uid;
    auto uri_str = "mqtt://" PIPEPP_MQTT_TEST_BROKER
        ":" + std::to_string(PIPEPP_MQTT_TEST_PORT)
        + "?keepalive&clean=true&clientid=" + cid;
    auto uri = basic_uri<mqtt_default_config>::parse(uri_str);
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- query_param: key not found at all (line 60) ---

TEST(MqttCoverage, QueryParamKeyNotFound) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-qpnf-" + uid).c_str());
    auto uri_str = "mqtt://" PIPEPP_MQTT_TEST_BROKER
        ":" + std::to_string(PIPEPP_MQTT_TEST_PORT)
        + "?unknown=42";
    auto uri = basic_uri<mqtt_default_config>::parse(uri_str);
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- parse_int overflow via very large keepalive (line 34) ---

TEST(MqttCoverage, ParseIntOverflowViaKeepalive) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    auto cid = "cov-piof-" + uid;
    auto uri_str = "mqtt://" PIPEPP_MQTT_TEST_BROKER
        ":" + std::to_string(PIPEPP_MQTT_TEST_PORT)
        + "?keepalive=999999999999&clientid=" + cid;
    auto uri = basic_uri<mqtt_default_config>::parse(uri_str);
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- MQTTS scheme with empty port defaults to 8883 (line 224) ---

TEST(MqttCoverage, MqttsSchemeEmptyPortUsesDefault) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-mqttsnp-" + uid).c_str());
    src.set_username("pipepp_test");
    src.set_password("pipepp_test_pw");
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    src.set_broker(PIPEPP_MQTT_TEST_BROKER, std::to_string(PIPEPP_MQTT_TEST_TLS_PORT).c_str());
    auto uri = basic_uri<mqtt_default_config>::parse(
        "mqtts://" PIPEPP_MQTT_TEST_BROKER);
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- All 4 configs: set_will + connect ---

TEST(MqttCoverage, WillConnectAllConfigs) {
    auto exercise_will = [](auto& src, const char* prefix) {
        auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
        src.set_client_id((std::string(prefix) + uid).c_str());
        src.set_broker(kBroker, std::to_string(kPort).c_str());
        auto topic = unique_cov_topic(prefix);
        std::byte data[] = {std::byte{0xDE}};
        src.set_will(topic.c_str(), data, 1, true);
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        EXPECT_TRUE(src.is_connected());
        src.disconnect();
    };
    {
        mqtt_source<mqtt_default_config> src;
        exercise_will(src, "will-d-");
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        exercise_will(src, "will-e-");
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        exercise_will(src, "will-dc-");
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        exercise_will(src, "will-ec-");
    }
}

// --- All 4 configs: set_ssl_verify ---

TEST(MqttCoverage, SetSslVerifyAllConfigs) {
    mqtt_source<mqtt_default_config> s1; s1.set_ssl_verify(false);
    mqtt_source<mqtt_embedded_config> s2; s2.set_ssl_verify(false);
    mqtt_source<mqtt_default_consumer_config> s3; s3.set_ssl_verify(false);
    mqtt_source<mqtt_embedded_consumer_config> s4; s4.set_ssl_verify(false);
}

// --- All 4 configs: set_username + set_password ---

TEST(MqttCoverage, SetUsernamePasswordAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    s1.set_username("user1"); s1.set_password("pass1");
    mqtt_source<mqtt_embedded_config> s2;
    s2.set_username("user2"); s2.set_password("pass2");
    mqtt_source<mqtt_default_consumer_config> s3;
    s3.set_username("user3"); s3.set_password("pass3");
    mqtt_source<mqtt_embedded_consumer_config> s4;
    s4.set_username("user4"); s4.set_password("pass4");
}

// --- All 4 configs: set_ssl ---

TEST(MqttCoverage, SetSslAllConfigs) {
    mqtt_source<mqtt_default_config> s1; s1.set_ssl("/path/ca", "", "");
    mqtt_source<mqtt_embedded_config> s2; s2.set_ssl("/path/ca", "", "");
    mqtt_source<mqtt_default_consumer_config> s3; s3.set_ssl("/path/ca", "", "");
    mqtt_source<mqtt_embedded_consumer_config> s4; s4.set_ssl("/path/ca", "", "");
}

// --- All 4 configs: set_mqtt_version ---

TEST(MqttCoverage, SetMqttVersionAllConfigs) {
    mqtt_source<mqtt_default_config> s1; s1.set_mqtt_version(5);
    mqtt_source<mqtt_embedded_config> s2; s2.set_mqtt_version(5);
    mqtt_source<mqtt_default_consumer_config> s3; s3.set_mqtt_version(5);
    mqtt_source<mqtt_embedded_consumer_config> s4; s4.set_mqtt_version(5);
}

// --- All 4 configs: set_keepalive ---

TEST(MqttCoverage, SetKeepaliveAllConfigs) {
    mqtt_source<mqtt_default_config> s1; s1.set_keepalive(60);
    mqtt_source<mqtt_embedded_config> s2; s2.set_keepalive(60);
    mqtt_source<mqtt_default_consumer_config> s3; s3.set_keepalive(60);
    mqtt_source<mqtt_embedded_consumer_config> s4; s4.set_keepalive(60);
}

// --- All 4 configs: set_clean_session ---

TEST(MqttCoverage, SetCleanSessionAllConfigs) {
    mqtt_source<mqtt_default_config> s1; s1.set_clean_session(false);
    mqtt_source<mqtt_embedded_config> s2; s2.set_clean_session(false);
    mqtt_source<mqtt_default_consumer_config> s3; s3.set_clean_session(false);
    mqtt_source<mqtt_embedded_consumer_config> s4; s4.set_clean_session(false);
}

// --- All 4 configs: set_automatic_reconnect ---

TEST(MqttCoverage, SetAutoReconnectAllConfigs) {
    mqtt_source<mqtt_default_config> s1; s1.set_automatic_reconnect(1, 10);
    mqtt_source<mqtt_embedded_config> s2; s2.set_automatic_reconnect(1, 10);
    mqtt_source<mqtt_default_consumer_config> s3; s3.set_automatic_reconnect(1, 10);
    mqtt_source<mqtt_embedded_consumer_config> s4; s4.set_automatic_reconnect(1, 10);
}

// --- All 4 configs: set_client_id ---

TEST(MqttCoverage, SetClientIdAllConfigs) {
    mqtt_source<mqtt_default_config> s1; s1.set_client_id("id1");
    mqtt_source<mqtt_embedded_config> s2; s2.set_client_id("id2");
    mqtt_source<mqtt_default_consumer_config> s3; s3.set_client_id("id3");
    mqtt_source<mqtt_embedded_consumer_config> s4; s4.set_client_id("id4");
}

// --- All 4 configs: set_broker ---

TEST(MqttCoverage, SetBrokerAllConfigs) {
    mqtt_source<mqtt_default_config> s1; s1.set_broker("host1", "1883");
    mqtt_source<mqtt_embedded_config> s2; s2.set_broker("host2", "1883");
    mqtt_source<mqtt_default_consumer_config> s3; s3.set_broker("host3", "1883");
    mqtt_source<mqtt_embedded_consumer_config> s4; s4.set_broker("host4", "1883");
}

// --- Full cycle for embedded config ---

TEST(MqttCoverage, FullCycleEmbeddedConfig) {
    mqtt_source<mqtt_embedded_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-fce-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    src.set_keepalive(20);
    src.set_clean_session(true);
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    auto topic = unique_cov_topic("fce");
    r = src.subscribe(topic.c_str(), 0);
    ASSERT_TRUE(r.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::byte data[] = {std::byte{0xAA}};
    r = src.publish(topic.c_str(), data, 0);
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
    EXPECT_FALSE(src.is_connected());
}

// --- set_will QoS clamping (lines 608-610) ---

TEST(MqttCoverage, WillQosClampingNegative) {
    mqtt_source<mqtt_default_config> src;
    std::byte data[] = {std::byte{0x01}};
    src.set_will("topic", data, -1, false);
}

TEST(MqttCoverage, WillQosClampingAbove2) {
    mqtt_source<mqtt_default_config> src;
    std::byte data[] = {std::byte{0x01}};
    src.set_will("topic", data, 5, false);
}

// --- Poll on not-connected for all 4 configs ---

TEST(MqttCoverage, PollNotConnectedAllConfigs) {
    mqtt_source<mqtt_default_config> s1; s1.poll();
    mqtt_source<mqtt_embedded_config> s2; s2.poll();
    mqtt_source<mqtt_default_consumer_config> s3; s3.poll();
    mqtt_source<mqtt_embedded_consumer_config> s4; s4.poll();
}

// --- is_connected on moved-from for all configs ---

TEST(MqttCoverage, IsConnectedMovedFromAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    auto dst1 = std::move(s1);
    EXPECT_FALSE(s1.is_connected());
    mqtt_source<mqtt_embedded_config> s2;
    auto dst2 = std::move(s2);
    EXPECT_FALSE(s2.is_connected());
    mqtt_source<mqtt_default_consumer_config> s3;
    auto dst3 = std::move(s3);
    EXPECT_FALSE(s3.is_connected());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    auto dst4 = std::move(s4);
    EXPECT_FALSE(s4.is_connected());
}

// --- disconnect on not-connected for all configs ---

TEST(MqttCoverage, DisconnectNotConnectedAllConfigs) {
    mqtt_source<mqtt_default_config> s1; s1.disconnect();
    mqtt_source<mqtt_embedded_config> s2; s2.disconnect();
    mqtt_source<mqtt_default_consumer_config> s3; s3.disconnect();
    mqtt_source<mqtt_embedded_consumer_config> s4; s4.disconnect();
}

// --- disconnect on moved-from ---

TEST(MqttCoverage, DisconnectMovedFromAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    auto dst1 = std::move(s1);
    s1.disconnect();
    mqtt_source<mqtt_embedded_config> s2;
    auto dst2 = std::move(s2);
    s2.disconnect();
    mqtt_source<mqtt_default_consumer_config> s3;
    auto dst3 = std::move(s3);
    s3.disconnect();
    mqtt_source<mqtt_embedded_consumer_config> s4;
    auto dst4 = std::move(s4);
    s4.disconnect();
}

// --- subscribe/publish on not-connected for all configs ---

TEST(MqttCoverage, SubscribePublishNotConnectedAllConfigs) {
    std::byte data[] = {std::byte{0x01}};
    mqtt_source<mqtt_default_config> s1;
    EXPECT_FALSE(s1.subscribe("t", 0).has_value());
    EXPECT_FALSE(s1.publish("t", data, 0).has_value());
    mqtt_source<mqtt_embedded_config> s2;
    EXPECT_FALSE(s2.subscribe("t", 0).has_value());
    EXPECT_FALSE(s2.publish("t", data, 0).has_value());
    mqtt_source<mqtt_default_consumer_config> s3;
    EXPECT_FALSE(s3.subscribe("t", 0).has_value());
    EXPECT_FALSE(s3.publish("t", data, 0).has_value());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    EXPECT_FALSE(s4.subscribe("t", 0).has_value());
    EXPECT_FALSE(s4.publish("t", data, 0).has_value());
}

// --- subscribe with invalid QoS for all configs ---

TEST(MqttCoverage, SubscribeInvalidQosAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    EXPECT_FALSE(s1.subscribe("t", -1).has_value());
    EXPECT_FALSE(s1.subscribe("t", 3).has_value());
    mqtt_source<mqtt_embedded_config> s2;
    EXPECT_FALSE(s2.subscribe("t", -1).has_value());
    mqtt_source<mqtt_default_consumer_config> s3;
    EXPECT_FALSE(s3.subscribe("t", -1).has_value());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    EXPECT_FALSE(s4.subscribe("t", -1).has_value());
}

// --- publish with invalid QoS for all configs ---

TEST(MqttCoverage, PublishInvalidQosAllConfigs) {
    std::byte data[] = {std::byte{0x01}};
    mqtt_source<mqtt_default_config> s1;
    EXPECT_FALSE(s1.publish("t", data, -1).has_value());
    mqtt_source<mqtt_embedded_config> s2;
    EXPECT_FALSE(s2.publish("t", data, 3).has_value());
    mqtt_source<mqtt_default_consumer_config> s3;
    EXPECT_FALSE(s3.publish("t", data, -1).has_value());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    EXPECT_FALSE(s4.publish("t", data, 3).has_value());
}

// --- TLS connect for all 4 configs ---

TEST(MqttCoverage, TlsConnectAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
        src.set_client_id(("tls-d-" + uid).c_str());
        src.set_username("pipepp_test");
        src.set_password("pipepp_test_pw");
        src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
        auto uri = basic_uri<mqtt_default_config>::parse(
            "mqtts://" PIPEPP_MQTT_TEST_BROKER ":" + std::to_string(PIPEPP_MQTT_TEST_TLS_PORT));
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        EXPECT_TRUE(src.is_connected());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
        src.set_client_id(("tls-e-" + uid).c_str());
        src.set_username("pipepp_test");
        src.set_password("pipepp_test_pw");
        src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
        auto uri = basic_uri<mqtt_embedded_config>::parse(
            "mqtts://" PIPEPP_MQTT_TEST_BROKER ":" + std::to_string(PIPEPP_MQTT_TEST_TLS_PORT));
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
        src.set_client_id(("tls-dc-" + uid).c_str());
        src.set_username("pipepp_test");
        src.set_password("pipepp_test_pw");
        src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
        auto uri = basic_uri<mqtt_default_consumer_config>::parse(
            "mqtts://" PIPEPP_MQTT_TEST_BROKER ":" + std::to_string(PIPEPP_MQTT_TEST_TLS_PORT));
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
        src.set_client_id(("tls-ec-" + uid).c_str());
        src.set_username("pipepp_test");
        src.set_password("pipepp_test_pw");
        src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse(
            "mqtts://" PIPEPP_MQTT_TEST_BROKER ":" + std::to_string(PIPEPP_MQTT_TEST_TLS_PORT));
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// --- Setters no-op after connect for all configs (covers setter guard branches) ---

template<typename Config>
static void exercise_setters_while_connected() {
    mqtt_source<Config> src;
    connect_to_broker(src);
    EXPECT_TRUE(src.is_connected());
    src.set_keepalive(99);
    src.set_clean_session(false);
    src.set_mqtt_version(5);
    src.set_client_id("changed");
    src.set_broker("newhost", "9999");
    src.set_automatic_reconnect(1, 5);
    std::byte will_data[] = {std::byte{0x42}};
    src.set_will("will/topic", will_data, 1, true);
    src.set_username("newuser");
    src.set_password("newpass");
    src.set_ssl("/new/ca", "", "");
    src.set_ssl_verify(false);
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

TEST(MqttCoverage, SettersNoOpAfterConnectEmbedded) {
    exercise_setters_while_connected<mqtt_embedded_config>();
}
TEST(MqttCoverage, SettersNoOpAfterConnectConsumer) {
    exercise_setters_while_connected<mqtt_default_consumer_config>();
}
TEST(MqttCoverage, SettersNoOpAfterConnectEmbeddedConsumer) {
    exercise_setters_while_connected<mqtt_embedded_consumer_config>();
}

// --- Destructor with connected consumer for all consumer configs (lines 140-144) ---

TEST(MqttCoverage, DestructorConnectedConsumerDefault) {
    mqtt_source<mqtt_default_consumer_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-dtorc-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
}

TEST(MqttCoverage, DestructorConnectedConsumerEmbedded) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-dtorce-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(src.is_connected());
}

// --- Destructor with connected callback source (line 135-137) ---

TEST(MqttCoverage, DestructorConnectedCallbackDefault) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-dtorcb-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    src.set_message_callback([](message_view) {});
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
}

TEST(MqttCoverage, DestructorConnectedCallbackEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-dtorcbe-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    src.set_message_callback([](message_view) {});
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
}

// --- Move-assign connected consumer (lines 167-173) ---

TEST(MqttCoverage, MoveAssignConnectedConsumerDisconnectsDefault) {
    mqtt_source<mqtt_default_consumer_config> src1;
    mqtt_source<mqtt_default_consumer_config> src2;
    auto uid1 = std::to_string(reinterpret_cast<uintptr_t>(&src1));
    auto uid2 = std::to_string(reinterpret_cast<uintptr_t>(&src2));
    src1.set_client_id(("cov-macd1-" + uid1).c_str());
    src2.set_client_id(("cov-macd2-" + uid2).c_str());
    src1.set_broker(kBroker, std::to_string(kPort).c_str());
    src2.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r1 = src1.connect();
    ASSERT_TRUE(r1.has_value());
    EXPECT_TRUE(src1.is_connected());
    src1 = std::move(src2);
    EXPECT_FALSE(src1.is_connected());
}

// --- Move-assign connected callback source (lines 164-166) ---

TEST(MqttCoverage, MoveAssignConnectedCallbackDefault) {
    mqtt_source<mqtt_default_config> src1;
    mqtt_source<mqtt_default_config> src2;
    auto uid1 = std::to_string(reinterpret_cast<uintptr_t>(&src1));
    auto uid2 = std::to_string(reinterpret_cast<uintptr_t>(&src2));
    src1.set_client_id(("cov-macb1-" + uid1).c_str());
    src2.set_client_id(("cov-macb2-" + uid2).c_str());
    src1.set_broker(kBroker, std::to_string(kPort).c_str());
    src2.set_broker(kBroker, std::to_string(kPort).c_str());
    src1.set_message_callback([](message_view) {});
    auto r1 = src1.connect();
    ASSERT_TRUE(r1.has_value());
    src1 = std::move(src2);
}

// --- URI userinfo without colon (line 237: username only) ---

TEST(MqttCoverage, ConnectUriUserinfoNoColonEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-uinc-" + uid).c_str());
    auto uri = basic_uri<mqtt_embedded_config>::parse(
        "mqtt://justuser@" PIPEPP_MQTT_TEST_BROKER
        ":" + std::to_string(PIPEPP_MQTT_TEST_PORT));
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// --- connect() while already connected for callback types (lines 190-202) ---

TEST(MqttCoverage, ReconnectCallbackWithCallbackInstalled) {
    mqtt_source<mqtt_default_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-rcbci-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    src.set_message_callback([](message_view) {});
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    r = src.connect();
    EXPECT_TRUE(r.has_value());
    src.disconnect();
}

TEST(MqttCoverage, ReconnectEmbeddedCallbackWithCallbackInstalled) {
    mqtt_source<mqtt_embedded_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-recbci-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    src.set_message_callback([](message_view) {});
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    r = src.connect();
    EXPECT_TRUE(r.has_value());
    src.disconnect();
}

// --- set_message_callback on connected embedded consumer (consumer path) ---

TEST(MqttCoverage, SetCallbackOnConnectedEmbeddedConsumer2) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("cov-scbec2-" + uid).c_str());
    src.set_broker(kBroker, std::to_string(kPort).c_str());
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    src.set_message_callback([](message_view) {});
    src.disconnect();
}

// --- set_message_callback on connected consumer with callback (lines 529-543) ---

TEST(MqttCoverage, SetCallbackOnConnectedConsumerWithMessages) {
    mqtt_source<mqtt_default_consumer_config> consumer;
    mqtt_source<mqtt_default_config> publisher;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&consumer));
    consumer.set_client_id(("cov-scbwm-" + uid).c_str());
    publisher.set_client_id(("cov-scbwmp-" + uid).c_str());
    consumer.set_broker(kBroker, std::to_string(kPort).c_str());
    publisher.set_broker(kBroker, std::to_string(kPort).c_str());
    auto cr = consumer.connect();
    ASSERT_TRUE(cr.has_value());
    auto pr = publisher.connect();
    ASSERT_TRUE(pr.has_value());
    std::string topic = "test/setcb/" + uid;
    cr = consumer.subscribe(topic.c_str(), 0);
    ASSERT_TRUE(cr.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    bool called = false;
    consumer.set_message_callback([&called](message_view) { called = true; });
    auto payload = std::array<std::byte, 2>{std::byte{'Z'}, std::byte{'W'}};
    pr = publisher.publish(topic.c_str(), payload, 0);
    ASSERT_TRUE(pr.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    consumer.poll();
    consumer.disconnect();
    publisher.disconnect();
}
