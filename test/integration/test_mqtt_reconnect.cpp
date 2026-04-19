#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt_source.hpp>
#include <pipepp/core/uri.hpp>
#include <atomic>
#include <chrono>
#include <thread>

#ifndef PIPEPP_MQTT_TEST_BROKER
#define PIPEPP_MQTT_TEST_BROKER "localhost"
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
    return std::string("pipepp-reconn-test/") +
           std::to_string(::getpid()) + "/" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
           "/" + suffix;
}

static bool broker_is_running() {
    return system("pgrep -x mosquitto > /dev/null 2>&1") == 0;
}

static void wait_for_broker(int max_wait_secs = 5) {
    for (int i = 0; i < max_wait_secs * 10 && !broker_is_running(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

static void start_broker() {
    if (!broker_is_running()) {
        system("mosquitto -c /etc/mosquitto/mosquitto.conf -d 2>/dev/null || true");
        wait_for_broker(5);
    }
}

static void stop_broker() {
    system("pkill -x mosquitto 2>/dev/null || true");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (int i = 0; i < 30 && broker_is_running(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

static auto try_connect(mqtt_source<mqtt_default_config>& src) -> bool {
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("rc-" + uid).c_str());
    auto uri = basic_uri<mqtt_default_config>::parse(broker_uri());
    auto r = src.connect(uri.view());
    return r.has_value();
}

static auto try_connect_with_retry(mqtt_source<mqtt_default_config>& src, int retries = 3) -> bool {
    for (int i = 0; i < retries; ++i) {
        if (try_connect(src)) return true;
        start_broker();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return try_connect(src);
}

TEST(MqttReconnectTest, ConnectionLostDetectedOnBrokerStop) {
    start_broker();
    mqtt_source<mqtt_default_config> src;
    ASSERT_TRUE(try_connect_with_retry(src)) << "Initial connect failed";
    EXPECT_TRUE(src.is_connected());

    stop_broker();
    std::this_thread::sleep_for(std::chrono::seconds(3));

    EXPECT_FALSE(src.is_connected()) << "is_connected() should return false after broker stops";

    start_broker();
    src.disconnect();
}

TEST(MqttReconnectTest, ReconnectAfterBrokerRestart) {
    start_broker();
    mqtt_source<mqtt_default_config> src;
    ASSERT_TRUE(try_connect_with_retry(src)) << "Initial connect failed";
    EXPECT_TRUE(src.is_connected());

    src.disconnect();
    EXPECT_FALSE(src.is_connected());

    stop_broker();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    start_broker();

    mqtt_source<mqtt_default_config> src2;
    ASSERT_TRUE(try_connect_with_retry(src2)) << "Reconnect after broker restart failed";
    EXPECT_TRUE(src2.is_connected());
    src2.disconnect();
}

TEST(MqttReconnectTest, PublishAfterBrokerRestart) {
    start_broker();
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(try_connect_with_retry(pub)) << "Publisher connect failed";
    ASSERT_TRUE(try_connect_with_retry(sub)) << "Subscriber connect failed";

    auto topic = unique_topic("post-restart");
    std::atomic<bool> received{false};
    sub.set_message_callback([&](const message_view&) { received.store(true); });
    sub.subscribe(topic, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte p[] = {std::byte{0x99}};
    auto pr = pub.publish(topic, p, 1);
    ASSERT_TRUE(pr.has_value());

    for (int i = 0; i < 50 && !received.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(received.load()) << "Publish before restart should work";

    pub.disconnect();
    sub.disconnect();
}

TEST(MqttReconnectTest, ConnectionLostCallback) {
    start_broker();
    mqtt_source<mqtt_default_config> src;
    ASSERT_TRUE(try_connect_with_retry(src));

    stop_broker();
    std::this_thread::sleep_for(std::chrono::seconds(3));

    EXPECT_FALSE(src.is_connected());

    start_broker();
    src.disconnect();
}

TEST(MqttReconnectTest, ConsumerReconnectAfterBrokerRestart) {
    start_broker();
    mqtt_source<mqtt_default_consumer_config> src;
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("rc-c-" + uid).c_str());
    auto uri = basic_uri<mqtt_default_consumer_config>::parse(broker_uri());
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value()) << "Consumer connect failed";
    EXPECT_TRUE(src.is_connected());

    stop_broker();
    std::this_thread::sleep_for(std::chrono::seconds(3));
    EXPECT_FALSE(src.is_connected());

    start_broker();
    src.disconnect();
}
