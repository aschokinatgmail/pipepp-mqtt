#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt_source.hpp>
#include <pipepp/core/uri.hpp>
#include <atomic>
#include <chrono>
#include <thread>

#ifndef PIPEPP_MQTT_TEST_BROKER
#define PIPEPP_MQTT_TEST_BROKER "localhost"
#endif

#ifndef PIPEPP_MQTT_TEST_TLS_PORT
#define PIPEPP_MQTT_TEST_TLS_PORT 8883
#endif

#ifndef PIPEPP_MQTT_TEST_CA_CERT
#define PIPEPP_MQTT_TEST_CA_CERT "/etc/mosquitto/certs/ca.crt"
#endif

using namespace pipepp::mqtt;
using namespace pipepp::core;

static std::string tls_broker_uri() {
    return "mqtts://" PIPEPP_MQTT_TEST_BROKER ":" +
           std::to_string(PIPEPP_MQTT_TEST_TLS_PORT);
}

static std::string unique_topic(const char* suffix) {
    return std::string("pipepp-tls-test/") +
           std::to_string(::getpid()) + "/" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
           "/" + suffix;
}

TEST(MqttTlsAuthTest, TlsConnectWithAuth) {
    mqtt_source<mqtt_default_config> src;
    src.set_username("pipepp_test");
    src.set_password("pipepp_test_pw");
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    src.set_ssl_verify(false);

    auto uri = basic_uri<mqtt_default_config>::parse(tls_broker_uri());
    auto r = src.connect(uri.view());
    if (!r.has_value()) {
        GTEST_SKIP() << "TLS broker unreachable at " << tls_broker_uri();
    }
    EXPECT_TRUE(src.is_connected());

    auto dr = src.disconnect();
    EXPECT_TRUE(dr.has_value());
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttTlsAuthTest, TlsConnectAuthViaUri) {
    mqtt_source<mqtt_default_config> src;
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    src.set_ssl_verify(false);

    auto uri_str = "mqtts://pipepp_test:pipepp_test_pw@" PIPEPP_MQTT_TEST_BROKER ":" +
                   std::to_string(PIPEPP_MQTT_TEST_TLS_PORT);
    auto uri = basic_uri<mqtt_default_config>::parse(uri_str);
    auto r = src.connect(uri.view());
    if (!r.has_value()) {
        GTEST_SKIP() << "TLS broker unreachable";
    }
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
}

TEST(MqttTlsAuthTest, TlsAuthRejectBadCredentials) {
    mqtt_source<mqtt_default_config> src;
    src.set_username("pipepp_test");
    src.set_password("wrong_password");
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    src.set_ssl_verify(false);

    auto uri = basic_uri<mqtt_default_config>::parse(tls_broker_uri());
    auto r = src.connect(uri.view());
    if (!r.has_value()) {
        EXPECT_FALSE(r.has_value());
        return;
    }
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttTlsAuthTest, TlsAuthRejectNoCredentials) {
    mqtt_source<mqtt_default_config> src;
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    src.set_ssl_verify(false);

    auto uri = basic_uri<mqtt_default_config>::parse(tls_broker_uri());
    auto r = src.connect(uri.view());
    if (!r.has_value()) {
        EXPECT_FALSE(r.has_value());
        return;
    }
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttTlsAuthTest, TlsPubSubWithAuth) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;

    pub.set_username("pipepp_test");
    pub.set_password("pipepp_test_pw");
    pub.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    pub.set_ssl_verify(false);

    sub.set_username("pipepp_test");
    sub.set_password("pipepp_test_pw");
    sub.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    sub.set_ssl_verify(false);

    auto uri = basic_uri<mqtt_default_config>::parse(tls_broker_uri());

    auto r1 = pub.connect(uri.view());
    if (!r1.has_value()) {
        GTEST_SKIP() << "TLS broker unreachable";
    }
    auto r2 = sub.connect(uri.view());
    ASSERT_TRUE(r2.has_value());

    auto topic = unique_topic("tls-pubsub");
    std::atomic<bool> received{false};
    std::string received_payload;

    sub.set_message_callback([&](const message_view& mv) {
        received_payload = std::string(reinterpret_cast<const char*>(mv.payload.data()), mv.payload.size());
        received.store(true);
    });

    auto sr = sub.subscribe(topic, 1);
    ASSERT_TRUE(sr.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte payload[] = {std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};
    auto pr = pub.publish(topic, payload, 1);
    ASSERT_TRUE(pr.has_value());

    for (int i = 0; i < 50 && !received.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_TRUE(received.load());
    EXPECT_EQ(received_payload, "\xDE\xAD\xBE\xEF");

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttTlsAuthTest, TlsConsumerModeWithAuth) {
    mqtt_source<mqtt_default_consumer_config> pub;
    mqtt_source<mqtt_default_consumer_config> sub;

    pub.set_username("pipepp_test");
    pub.set_password("pipepp_test_pw");
    pub.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    pub.set_ssl_verify(false);

    sub.set_username("pipepp_test");
    sub.set_password("pipepp_test_pw");
    sub.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    sub.set_ssl_verify(false);

    auto uri = basic_uri<mqtt_default_consumer_config>::parse(tls_broker_uri());

    auto r1 = pub.connect(uri.view());
    if (!r1.has_value()) {
        GTEST_SKIP() << "TLS broker unreachable";
    }
    auto r2 = sub.connect(uri.view());
    ASSERT_TRUE(r2.has_value());

    auto topic = unique_topic("tls-consumer");
    std::atomic<bool> received{false};

    sub.set_message_callback([&](const message_view& mv) {
        received.store(true);
    });

    auto sr = sub.subscribe(topic, 0);
    ASSERT_TRUE(sr.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte payload[] = {std::byte{0xCA}, std::byte{0xFE}};
    auto pr = pub.publish(topic, payload, 0);
    ASSERT_TRUE(pr.has_value());

    for (int i = 0; i < 50 && !received.load(); ++i) {
        sub.poll();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_TRUE(received.load());

    pub.disconnect();
    sub.disconnect();
}
