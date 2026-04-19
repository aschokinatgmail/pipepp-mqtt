#include <gtest/gtest.h>
#include "test_helpers.hpp"

TEST(MqttTlsAuthTest, ConnectWithTlsAndAuth) {
    mqtt_source<mqtt_default_config> src;
    ASSERT_TRUE(pipepp_test::try_tls_connect(src)) << "TLS+auth connect failed";
    EXPECT_TRUE(src.is_connected());
    src.disconnect();
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttTlsAuthTest, ConnectWithTlsWrongPasswordFails) {
    mqtt_source<mqtt_default_config> src;
    EXPECT_FALSE(pipepp_test::try_tls_connect(src, "tls", "pipepp_test", "wrong_password"));
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttTlsAuthTest, ConnectWithTlsWrongUsernameFails) {
    mqtt_source<mqtt_default_config> src;
    EXPECT_FALSE(pipepp_test::try_tls_connect(src, "tls", "wrong_user", "pipepp_test_pw"));
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttTlsAuthTest, ConnectWithTlsNoAuthFails) {
    mqtt_source<mqtt_default_config> src;
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    auto uri = basic_uri<mqtt_default_config>::parse(pipepp_test::tls_broker_uri());
    auto r = src.connect(uri.view());
    EXPECT_FALSE(r.has_value());
    EXPECT_FALSE(src.is_connected());
}

TEST(MqttTlsAuthTest, PubSubOverTlsCallbackMode) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(pipepp_test::try_tls_connect(pub)) << "Publisher TLS connect failed";
    ASSERT_TRUE(pipepp_test::try_tls_connect(sub)) << "Subscriber TLS connect failed";

    auto topic = pipepp_test::unique_topic("pipepp-tls-test/", "tls-callback");
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

    ASSERT_TRUE(pipepp_test::wait_for_message(received)) << "Message not received over TLS";
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
    ASSERT_TRUE(pipepp_test::try_tls_connect(pub, "tls-c")) << "Publisher TLS connect failed";
    ASSERT_TRUE(pipepp_test::try_tls_connect(sub, "tls-c")) << "Subscriber TLS connect failed";

    auto topic = pipepp_test::unique_topic("pipepp-tls-test/", "tls-consumer");
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
    ASSERT_TRUE(pipepp_test::try_tls_connect(pub));
    ASSERT_TRUE(pipepp_test::try_tls_connect(sub));

    auto topic = pipepp_test::unique_topic("pipepp-tls-test/", "tls-qos1");
    std::atomic<bool> received{false};
    sub.set_message_callback([&](const message_view&) { received.store(true); });
    sub.subscribe(topic, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte p[] = {std::byte{0x01}};
    auto pr = pub.publish(topic, p, 1);
    ASSERT_TRUE(pr.has_value());

    EXPECT_TRUE(pipepp_test::wait_for_message(received));

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttTlsAuthTest, QoS2OverTls) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(pipepp_test::try_tls_connect(pub));
    ASSERT_TRUE(pipepp_test::try_tls_connect(sub));

    auto topic = pipepp_test::unique_topic("pipepp-tls-test/", "tls-qos2");
    std::atomic<bool> received{false};
    sub.set_message_callback([&](const message_view&) { received.store(true); });
    sub.subscribe(topic, 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte p[] = {std::byte{0x02}};
    auto pr = pub.publish(topic, p, 2);
    ASSERT_TRUE(pr.has_value());

    EXPECT_TRUE(pipepp_test::wait_for_message(received));

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttTlsAuthTest, MultipleMessagesOverTls) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(pipepp_test::try_tls_connect(pub));
    ASSERT_TRUE(pipepp_test::try_tls_connect(sub));

    auto topic = pipepp_test::unique_topic("pipepp-tls-test/", "tls-multi");
    std::atomic<int> count{0};
    sub.set_message_callback([&](const message_view&) { count.fetch_add(1); });
    sub.subscribe(topic, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    for (int i = 0; i < 10; ++i) {
        std::byte p = static_cast<std::byte>(i);
        auto pr = pub.publish(topic, {&p, 1}, 1);
        ASSERT_TRUE(pr.has_value());
    }

    EXPECT_TRUE(pipepp_test::wait_for_count(count, 10));
    EXPECT_EQ(count.load(), 10);

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttTlsAuthTest, LargePayloadOverTls) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(pipepp_test::try_tls_connect(pub));
    ASSERT_TRUE(pipepp_test::try_tls_connect(sub));

    auto topic = pipepp_test::unique_topic("pipepp-tls-test/", "tls-large");
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

    ASSERT_TRUE(pipepp_test::wait_for_message(received));
    EXPECT_EQ(received_size, 4096u);

    pub.disconnect();
    sub.disconnect();
}
