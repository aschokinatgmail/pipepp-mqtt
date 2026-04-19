#include <gtest/gtest.h>
#include "test_helpers.hpp"

TEST(MqttReconnectTest, ConnectionLostDetectedOnBrokerStop) {
    pipepp_test::start_broker();
    mqtt_source<mqtt_default_config> src;
    ASSERT_TRUE(pipepp_test::try_connect_with_retry<mqtt_default_config>(src, "rc")) << "Initial connect failed";
    EXPECT_TRUE(src.is_connected());

    pipepp_test::stop_broker();
    pipepp_test::wait_for_disconnect(src);

    EXPECT_FALSE(src.is_connected()) << "is_connected() should return false after broker stops";
}

TEST(MqttReconnectTest, ReconnectAfterBrokerRestart) {
    pipepp_test::start_broker();
    mqtt_source<mqtt_default_config> src;
    ASSERT_TRUE(pipepp_test::try_connect_with_retry<mqtt_default_config>(src, "rc")) << "Initial connect failed";
    EXPECT_TRUE(src.is_connected());

    src.disconnect();
    EXPECT_FALSE(src.is_connected());

    pipepp_test::stop_broker();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    pipepp_test::start_broker();

    mqtt_source<mqtt_default_config> src2;
    ASSERT_TRUE(pipepp_test::try_connect_with_retry<mqtt_default_config>(src2, "rc")) << "Reconnect after broker restart failed";
    EXPECT_TRUE(src2.is_connected());
    src2.disconnect();
}

TEST(MqttReconnectTest, PublishAfterBrokerRestart) {
    pipepp_test::start_broker();
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(pipepp_test::try_connect_with_retry<mqtt_default_config>(pub, "rc-pub")) << "Publisher connect failed";
    ASSERT_TRUE(pipepp_test::try_connect_with_retry<mqtt_default_config>(sub, "rc-sub")) << "Subscriber connect failed";

    auto topic = pipepp_test::unique_topic("pipepp-reconn-test/", "post-restart");
    std::atomic<bool> received{false};
    sub.set_message_callback([&](const message_view&) { received.store(true); });
    sub.subscribe(topic, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte p[] = {std::byte{0x99}};
    auto pr = pub.publish(topic, p, 1);
    ASSERT_TRUE(pr.has_value());
    EXPECT_TRUE(pipepp_test::wait_for_message(received)) << "Publish before restart should work";

    pub.disconnect();
    sub.disconnect();

    pipepp_test::stop_broker();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    pipepp_test::start_broker();

    mqtt_source<mqtt_default_config> pub2;
    mqtt_source<mqtt_default_config> sub2;
    ASSERT_TRUE(pipepp_test::try_connect_with_retry<mqtt_default_config>(pub2, "rc-pub2")) << "Publisher reconnect failed";
    ASSERT_TRUE(pipepp_test::try_connect_with_retry<mqtt_default_config>(sub2, "rc-sub2")) << "Subscriber reconnect failed";

    auto topic2 = pipepp_test::unique_topic("pipepp-reconn-test/", "after-restart");
    std::atomic<bool> received2{false};
    sub2.set_message_callback([&](const message_view&) { received2.store(true); });
    sub2.subscribe(topic2, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte q[] = {std::byte{0xAA}};
    auto pr2 = pub2.publish(topic2, q, 1);
    ASSERT_TRUE(pr2.has_value());
    EXPECT_TRUE(pipepp_test::wait_for_message(received2)) << "Publish after restart should work";

    pub2.disconnect();
    sub2.disconnect();
}

TEST(MqttReconnectTest, ConsumerReconnectAfterBrokerRestart) {
    pipepp_test::start_broker();
    mqtt_source<mqtt_default_consumer_config> src;
    ASSERT_TRUE(pipepp_test::try_connect_with_retry<mqtt_default_consumer_config>(src, "rc-c")) << "Consumer connect failed";
    EXPECT_TRUE(src.is_connected());

    pipepp_test::stop_broker();
    pipepp_test::wait_for_disconnect(src);
    EXPECT_FALSE(src.is_connected());

    pipepp_test::start_broker();
    src.disconnect();
}

TEST(MqttReconnectTest, TlsConnectionLostAndReconnect) {
    pipepp_test::start_broker();
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;
    ASSERT_TRUE(pipepp_test::try_tls_connect(pub, "rc-tls-pub")) << "TLS publisher connect failed";
    ASSERT_TRUE(pipepp_test::try_tls_connect(sub, "rc-tls-sub")) << "TLS subscriber connect failed";

    auto topic = pipepp_test::unique_topic("pipepp-reconn-test/", "tls-restart");
    std::atomic<bool> received{false};
    sub.set_message_callback([&](const message_view&) { received.store(true); });
    sub.subscribe(topic, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte p[] = {std::byte{0xCC}};
    auto pr = pub.publish(topic, p, 1);
    ASSERT_TRUE(pr.has_value());
    EXPECT_TRUE(pipepp_test::wait_for_message(received)) << "TLS publish before restart should work";

    pub.disconnect();
    sub.disconnect();

    pipepp_test::stop_broker();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    pipepp_test::start_broker();

    mqtt_source<mqtt_default_config> pub2;
    mqtt_source<mqtt_default_config> sub2;
    ASSERT_TRUE(pipepp_test::try_tls_connect(pub2, "rc-tls-pub2")) << "TLS publisher reconnect failed";
    ASSERT_TRUE(pipepp_test::try_tls_connect(sub2, "rc-tls-sub2")) << "TLS subscriber reconnect failed";

    auto topic2 = pipepp_test::unique_topic("pipepp-reconn-test/", "tls-after-restart");
    std::atomic<bool> received2{false};
    sub2.set_message_callback([&](const message_view&) { received2.store(true); });
    sub2.subscribe(topic2, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::byte q[] = {std::byte{0xDD}};
    auto pr2 = pub2.publish(topic2, q, 1);
    ASSERT_TRUE(pr2.has_value());
    EXPECT_TRUE(pipepp_test::wait_for_message(received2)) << "TLS publish after restart should work";

    pub2.disconnect();
    sub2.disconnect();
}
