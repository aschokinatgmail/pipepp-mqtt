#include <gtest/gtest.h>
#include "test_helpers.hpp"
#include <cstdio>

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
    pipepp_test::start_broker();
    auto will_topic = pipepp_test::unique_topic("pipepp-will-test/", "will/notify");
    std::string will_payload = "WILL_NOTIFY_12345";

    auto result = test_will_delivery(will_topic, will_payload);
    EXPECT_NE(result.find(will_payload), std::string::npos)
        << "Will message was not delivered on unclean disconnect. Got: '" << result << "'";
}

TEST(MqttWillTest, WillMessageWithRetained) {
    pipepp_test::start_broker();
    auto will_topic = pipepp_test::unique_topic("pipepp-will-test/", "will/retained");

    auto result = test_will_delivery(will_topic, "retained_will", true);
    EXPECT_FALSE(result.empty()) << "Retained will message was not delivered";

    auto retained = capture_cmd(
        "mosquitto_sub -h " PIPEPP_MQTT_TEST_BROKER
        " -p " + std::to_string(PIPEPP_MQTT_TEST_PORT) +
        " -t '" + will_topic + "' -C 1 -W 2", 5000);
    EXPECT_FALSE(retained.empty()) << "Retained will should still be available after disconnect";
}

TEST(MqttWillTest, NoWillWhenNotSet) {
    pipepp_test::start_broker();
    auto topic = pipepp_test::unique_topic("pipepp-will-test/", "will/notset");

    mqtt_source<mqtt_default_config> no_will_src;
    ASSERT_TRUE(pipepp_test::try_connect_with_retry(no_will_src));

    mqtt_source<mqtt_default_config> monitor;
    ASSERT_TRUE(pipepp_test::try_connect_with_retry(monitor));

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
    pipepp_test::start_broker();
    auto will_topic = pipepp_test::unique_topic("pipepp-will-test/", "will/payload");
    std::string will_payload = "PAYLOAD_5_BYTES_ABCDE";

    auto result = test_will_delivery(will_topic, will_payload);
    ASSERT_FALSE(result.empty()) << "Will payload not received";
    EXPECT_NE(result.find(will_payload), std::string::npos)
        << "Will payload content mismatch. Got: '" << result << "'";
}
