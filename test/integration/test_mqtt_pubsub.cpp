#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt_source.hpp>
#include <pipepp/core/uri.hpp>
#include <atomic>
#include <chrono>
#include <thread>

#ifndef PIPEPP_MQTT_TEST_BROKER
#define PIPEPP_MQTT_TEST_BROKER "bms-logging-server.local"
#endif

#ifndef PIPEPP_MQTT_TEST_PORT
#define PIPEPP_MQTT_TEST_PORT 1883
#endif

using namespace pipepp::mqtt;
using namespace pipepp::core;

static std::string broker_uri() {
    return "mqtt://" PIPEPP_MQTT_TEST_BROKER ":" +
           std::to_string(PIPEPP_MQTT_TEST_PORT);
}

static std::string unique_topic(const char* suffix) {
    return std::string("pipepp-test/") +
           std::to_string(::getpid()) + "/" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
           "/" + suffix;
}

static auto try_connect(mqtt_source<mqtt_default_config>& src) -> bool {
    auto uri = basic_uri<mqtt_default_config>::parse(broker_uri());
    auto r = src.connect(uri.view());
    return r.has_value();
}

static auto try_connect_consumer(mqtt_source<mqtt_default_consumer_config>& src) -> bool {
    auto uri = basic_uri<mqtt_default_consumer_config>::parse(broker_uri());
    auto r = src.connect(uri.view());
    return r.has_value();
}

TEST(MqttPubSubTest, PublishSubscribeCallbackMode) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    if (!try_connect(pub) || !try_connect(sub)) {
        GTEST_SKIP() << "Broker unreachable";
    }

    auto topic = unique_topic("callback");
    std::atomic<bool> received{false};
    std::string received_topic;
    std::string received_payload;

    sub.set_message_callback([&](const message_view& mv) {
        received_topic = std::string(mv.topic);
        received_payload = std::string(reinterpret_cast<const char*>(mv.payload.data()), mv.payload.size());
        received.store(true);
    });

    auto sr = sub.subscribe(topic, 0);
    ASSERT_TRUE(sr.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte payload[] = {std::byte{0x48}, std::byte{0x69}};
    auto pr = pub.publish(topic, payload, 0);
    ASSERT_TRUE(pr.has_value());

    for (int i = 0; i < 50 && !received.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_TRUE(received.load());

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttPubSubTest, PublishSubscribeConsumerMode) {
    mqtt_source<mqtt_default_consumer_config> pub;
    mqtt_source<mqtt_default_consumer_config> sub;
    if (!try_connect_consumer(pub) || !try_connect_consumer(sub)) {
        GTEST_SKIP() << "Broker unreachable";
    }

    auto topic = unique_topic("consumer");
    std::atomic<bool> received{false};

    sub.set_message_callback([&](const message_view& mv) {
        received.store(true);
    });

    auto sr = sub.subscribe(topic, 0);
    ASSERT_TRUE(sr.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte payload[] = {std::byte{0x48}, std::byte{0x69}};
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

TEST(MqttPubSubTest, QoS0PubSub) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    if (!try_connect(pub) || !try_connect(sub)) {
        GTEST_SKIP() << "Broker unreachable";
    }

    auto topic = unique_topic("qos0");
    std::atomic<bool> received{false};
    sub.set_message_callback([&](const message_view&) { received.store(true); });
    sub.subscribe(topic, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte p[] = {std::byte{1}};
    pub.publish(topic, p, 0);

    for (int i = 0; i < 50 && !received.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(received.load());

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttPubSubTest, QoS1PubSub) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    if (!try_connect(pub) || !try_connect(sub)) {
        GTEST_SKIP() << "Broker unreachable";
    }

    auto topic = unique_topic("qos1");
    std::atomic<bool> received{false};
    sub.set_message_callback([&](const message_view&) { received.store(true); });
    sub.subscribe(topic, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte p[] = {std::byte{2}};
    pub.publish(topic, p, 1);

    for (int i = 0; i < 50 && !received.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(received.load());

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttPubSubTest, QoS2PubSub) {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    if (!try_connect(pub) || !try_connect(sub)) {
        GTEST_SKIP() << "Broker unreachable";
    }

    auto topic = unique_topic("qos2");
    std::atomic<bool> received{false};
    sub.set_message_callback([&](const message_view&) { received.store(true); });
    sub.subscribe(topic, 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte p[] = {std::byte{3}};
    pub.publish(topic, p, 2);

    for (int i = 0; i < 50 && !received.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(received.load());

    pub.disconnect();
    sub.disconnect();
}
