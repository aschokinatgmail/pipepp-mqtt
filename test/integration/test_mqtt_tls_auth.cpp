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
    return std::string("mqtts://") + PIPEPP_MQTT_TEST_BROKER + ":" +
           std::to_string(PIPEPP_MQTT_TEST_TLS_PORT);
}

static std::string unique_topic(const char* suffix) {
    return std::string("pipepp-tls-test/") +
           std::to_string(::getpid()) + "/" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
           "/" + suffix;
}

static auto try_tls_connect(mqtt_source<mqtt_default_config>& src,
                            const char* user = "pipepp_test",
                            const char* pass = "pipepp_test_pw") -> bool {
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("tls-" + uid).c_str());
    src.set_username(user);
    src.set_password(pass);
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    auto uri = basic_uri<mqtt_default_config>::parse(tls_broker_uri());
    auto r = src.connect(uri.view());
    return r.has_value();
}

static auto try_tls_connect_consumer(mqtt_source<mqtt_default_consumer_config>& src,
                                      const char* user = "pipepp_test",
                                      const char* pass = "pipepp_test_pw") -> bool {
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("tls-c-" + uid).c_str());
    src.set_username(user);
    src.set_password(pass);
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    auto uri = basic_uri<mqtt_default_consumer_config>::parse(tls_broker_uri());
    auto r = src.connect(uri.view());
    return r.has_value();
}

TEST(MqttTlsAuthTest, ConnectWithTlsAndAuth) {
    mqtt_source<mqtt_default_config> src;
    ASSERT_TRUE(try_tls_connect(src)) << "TLS+auth connect failed";
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttTlsAuthTest, ConnectWithTlsWrongPasswordFails) {
    mqtt_source<mqtt_default_config> src;
    EXPECT_FALSE(try_tls_connect(src, "pipepp_test", "wrong_password"));
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttTlsAuthTest, ConnectWithTlsWrongUsernameFails) {
    mqtt_source<mqtt_default_config> src;
    EXPECT_FALSE(try_tls_connect(src, "wrong_user", "pipepp_test_pw"));
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttTlsAuthTest, ConnectWithTlsNoAuthFails) {
    mqtt_source<mqtt_default_config> src;
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    auto uri = basic_uri<mqtt_default_config>::parse(tls_broker_uri());
    auto r = src.connect(uri.view());
    EXPECT_FALSE(r.has_value());
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttTlsAuthTest, PubSubOverTlsCallbackMode) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(try_tls_connect(pub)) << "Publisher TLS connect failed";
    ASSERT_TRUE(try_tls_connect(sub)) << "Subscriber TLS connect failed";

    auto topic = unique_topic("tls-callback");
    std::atomic<bool> received{false};
    std::string received_topic;
    std::vector<std::byte> received_payload;

    sub.set_message_callback([&](const message_view& mv) {
        received_topic = std::string(mv.topic);
        received_payload.assign(mv.payload.begin(), mv.payload.end());
        received.store(true);
    });

    auto sr = sub.subscribe(topic, 1);
    ASSERT_TRUE(sr.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte payload[] = {std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};
    auto pr = pub.publish(topic, payload, 1);
    ASSERT_TRUE(pr.has_value());

    for (int i = 0; i < 50 && !received.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(received.load()) << "Message not received over TLS";
    EXPECT_EQ(received_topic, topic);
    EXPECT_EQ(received_payload.size(), 4u);
    EXPECT_EQ(received_payload[0], std::byte{0xDE});
    EXPECT_EQ(received_payload[3], std::byte{0xEF});

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttTlsAuthTest, PubSubOverTlsConsumerMode) {
    mqtt_source<mqtt_default_consumer_config> pub;
    mqtt_source<mqtt_default_consumer_config> sub;
    ASSERT_TRUE(try_tls_connect_consumer(pub)) << "Publisher TLS connect failed";
    ASSERT_TRUE(try_tls_connect_consumer(sub)) << "Subscriber TLS connect failed";

    auto topic = unique_topic("tls-consumer");
    std::atomic<bool> received{false};

    sub.set_message_callback([&](const message_view&) {
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

TEST(MqttTlsAuthTest, QoS1OverTls) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(try_tls_connect(pub));
    ASSERT_TRUE(try_tls_connect(sub));

    auto topic = unique_topic("tls-qos1");
    std::atomic<bool> received{false};
    sub.set_message_callback([&](const message_view&) { received.store(true); });
    sub.subscribe(topic, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte p[] = {std::byte{0x01}};
    auto pr = pub.publish(topic, p, 1);
    ASSERT_TRUE(pr.has_value());

    for (int i = 0; i < 50 && !received.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(received.load());

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttTlsAuthTest, QoS2OverTls) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(try_tls_connect(pub));
    ASSERT_TRUE(try_tls_connect(sub));

    auto topic = unique_topic("tls-qos2");
    std::atomic<bool> received{false};
    sub.set_message_callback([&](const message_view&) { received.store(true); });
    sub.subscribe(topic, 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte p[] = {std::byte{0x02}};
    auto pr = pub.publish(topic, p, 2);
    ASSERT_TRUE(pr.has_value());

    for (int i = 0; i < 50 && !received.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(received.load());

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttTlsAuthTest, MultipleMessagesOverTls) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(try_tls_connect(pub));
    ASSERT_TRUE(try_tls_connect(sub));

    auto topic = unique_topic("tls-multi");
    std::atomic<int> count{0};
    sub.set_message_callback([&](const message_view&) { count.fetch_add(1); });
    sub.subscribe(topic, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    for (int i = 0; i < 10; ++i) {
        std::byte p = static_cast<std::byte>(i);
        auto pr = pub.publish(topic, {&p, 1}, 1);
        ASSERT_TRUE(pr.has_value());
    }

    for (int i = 0; i < 50 && count.load() < 10; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(count.load(), 10);

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttTlsAuthTest, LargePayloadOverTls) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(try_tls_connect(pub));
    ASSERT_TRUE(try_tls_connect(sub));

    auto topic = unique_topic("tls-large");
    std::atomic<bool> received{false};
    std::size_t received_size = 0;
    sub.set_message_callback([&](const message_view& mv) {
        received_size = mv.payload.size();
        received.store(true);
    });
    sub.subscribe(topic, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::vector<std::byte> large(4096, std::byte{0xAB});
    auto pr = pub.publish(topic, large, 1);
    ASSERT_TRUE(pr.has_value());

    for (int i = 0; i < 50 && !received.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(received.load());
    EXPECT_EQ(received_size, 4096u);

    pub.disconnect();
    sub.disconnect();
}
