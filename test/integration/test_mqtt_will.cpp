#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt_source.hpp>
#include <pipepp/core/uri.hpp>
#include <atomic>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <cstdio>

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
    return std::string("pipepp-will-test/") +
           std::to_string(::getpid()) + "/" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
           "/" + suffix;
}

static bool broker_is_running() {
    return system("pgrep -x mosquitto > /dev/null 2>&1") == 0;
}

static void start_broker() {
    if (!broker_is_running()) {
        system("mosquitto -c /etc/mosquitto/mosquitto.conf -d 2>/dev/null || true");
        for (int i = 0; i < 50 && !broker_is_running(); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

static auto try_connect(mqtt_source<mqtt_default_config>& src) -> bool {
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id(("will-" + uid).c_str());
    auto uri = basic_uri<mqtt_default_config>::parse(broker_uri());
    auto r = src.connect(uri.view());
    return r.has_value();
}

static auto try_connect_with_retry(mqtt_source<mqtt_default_config>& src, int retries = 3) -> bool {
    for (int i = 0; i < retries; ++i) {
        start_broker();
        if (try_connect(src)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return try_connect(src);
}

static std::string capture_cmd(const std::string& cmd, int timeout_ms) {
    std::string tmpf = "/tmp/will_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count()) + ".txt";
    std::string full = "timeout " + std::to_string(timeout_ms / 1000) +
        " bash -c '" + cmd + "' > " + tmpf + " 2>/dev/null";
    system(full.c_str());
    FILE* f = fopen(tmpf.c_str(), "r");
    if (!f) return "";
    char buf[4096] = {};
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    unlink(tmpf.c_str());
    return std::string(buf, n);
}

static std::string test_will_delivery(const std::string& will_topic,
                                       const std::string& will_payload,
                                       bool retain = false) {
    std::string client_id = "will-" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    std::string output_file = "/tmp/will_out_" + client_id + ".txt";
    std::string retain_flag = retain ? " --will-retain " : " ";

    std::string cmd =
        "rm -f " + output_file + "; "
        "mosquitto_sub -h " PIPEPP_MQTT_TEST_BROKER
        " -p " + std::to_string(PIPEPP_MQTT_TEST_PORT) +
        " -t '" + will_topic + "' -C 1 -W 10 > " + output_file + " 2>/dev/null & "
        "SUB_PID=$!; "
        "sleep 0.5; "
        "mosquitto_sub -h " PIPEPP_MQTT_TEST_BROKER
        " -p " + std::to_string(PIPEPP_MQTT_TEST_PORT) +
        " -t '__will_keepalive_" + client_id + "__' "
        " --will-topic '" + will_topic + "' "
        " --will-payload '" + will_payload + "' " +
        retain_flag +
        " -i '" + client_id + "' "
        " -W 30 & "
        "WILL_PID=$!; "
        "sleep 1; "
        "kill -9 $WILL_PID 2>/dev/null; "
        "wait $WILL_PID 2>/dev/null; "
        "wait $SUB_PID 2>/dev/null; "
        "cat " + output_file;

    return capture_cmd(cmd, 15000);
}

TEST(MqttWillTest, WillMessageDeliveredOnUncleanDisconnect) {
    start_broker();
    auto will_topic = unique_topic("will/notify");
    std::string will_payload = "WILL_NOTIFY_12345";

    auto result = test_will_delivery(will_topic, will_payload);
    EXPECT_NE(result.find(will_payload), std::string::npos)
        << "Will message was not delivered on unclean disconnect. Got: '" << result << "'";
}

TEST(MqttWillTest, WillMessageWithRetained) {
    start_broker();
    auto will_topic = unique_topic("will/retained");

    auto result = test_will_delivery(will_topic, "retained_will", true);
    EXPECT_FALSE(result.empty()) << "Retained will message was not delivered";

    auto retained = capture_cmd(
        "mosquitto_sub -h " PIPEPP_MQTT_TEST_BROKER
        " -p " + std::to_string(PIPEPP_MQTT_TEST_PORT) +
        " -t '" + will_topic + "' -C 1 -W 2", 5000);
    EXPECT_FALSE(retained.empty()) << "Retained will should still be available after disconnect";
}

TEST(MqttWillTest, NoWillWhenNotSet) {
    start_broker();
    auto topic = unique_topic("will/notset");

    mqtt_source<mqtt_default_config> no_will_src;
    ASSERT_TRUE(try_connect_with_retry(no_will_src));

    mqtt_source<mqtt_default_config> monitor;
    ASSERT_TRUE(try_connect_with_retry(monitor));

    std::atomic<bool> received{false};
    monitor.set_message_callback([&](const message_view&) { received.store(true); });
    monitor.subscribe(topic, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    no_will_src.disconnect();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    EXPECT_FALSE(received.load()) << "No will message should be delivered when none was set";

    monitor.disconnect();
}

TEST(MqttWillTest, WillPayloadPreserved) {
    start_broker();
    auto will_topic = unique_topic("will/payload");
    std::string will_payload = "PAYLOAD_5_BYTES_ABCDE";

    auto result = test_will_delivery(will_topic, will_payload);
    ASSERT_FALSE(result.empty()) << "Will payload not received";
    EXPECT_NE(result.find(will_payload), std::string::npos)
        << "Will payload content mismatch. Got: '" << result << "'";
}
