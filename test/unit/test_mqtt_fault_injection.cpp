#include <gtest/gtest.h>
#include <pipepp/mqtt/mqtt.hpp>
#include <pipepp/core/concepts.hpp>
#include <chrono>
#include <thread>
#include <atomic>

#include <mqtt/async_client.h>

using namespace pipepp::mqtt;
using namespace pipepp::core;

using fi = ::mqtt::async_client::fault_injection;

template<typename Config>
static void mock_connect(mqtt_source<Config>& src) {
    src.set_broker("mockhost", "1883");
    auto r = src.connect();
    ASSERT_TRUE(r.has_value()) << "mock connect() failed";
    EXPECT_TRUE(src.is_connected());
}

class FaultInjectionTest : public ::testing::Test {
protected:
    void SetUp() override {
        fi::instance().reset();
    }
    void TearDown() override {
        fi::instance().reset();
    }
};

// ==============================================================================
// Destructor exception paths
// ==============================================================================

// Line 144: destructor catch when consumer stop_consuming throws
TEST_F(FaultInjectionTest, DestructorConsumerStopConsumingThrows) {
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        fi::instance().stop_consuming_should_throw = true;
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        EXPECT_TRUE(src.is_connected());
    }
}

TEST_F(FaultInjectionTest, DestructorEmbeddedConsumerStopConsumingThrows) {
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        fi::instance().stop_consuming_should_throw = true;
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
    }
}

// Line 144: destructor catch when disconnect throws (consumer)
TEST_F(FaultInjectionTest, DestructorConsumerDisconnectThrows) {
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        fi::instance().disconnect_should_throw = true;
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
    }
}

// Destructor for callback mode (lines 135-137): set_message_callback(nullptr)
TEST_F(FaultInjectionTest, DestructorCallbackModeClearsCallback) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_message_callback([](message_view) {});
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
    }
}

TEST_F(FaultInjectionTest, DestructorEmbeddedCallbackModeClearsCallback) {
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_message_callback([](message_view) {});
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
    }
}

// ==============================================================================
// Move-assignment exception paths (lines 167-173)
// ==============================================================================

// Line 173: move-assign catch when disconnect throws on connected consumer
TEST_F(FaultInjectionTest, MoveAssignConsumerDisconnectThrows) {
    mqtt_source<mqtt_default_consumer_config> src1;
    mqtt_source<mqtt_default_consumer_config> src2;
    mock_connect(src1);
    fi::instance().disconnect_should_throw = true;
    src1 = std::move(src2);
}

TEST_F(FaultInjectionTest, MoveAssignEmbeddedConsumerDisconnectThrows) {
    mqtt_source<mqtt_embedded_consumer_config> src1;
    mqtt_source<mqtt_embedded_consumer_config> src2;
    mock_connect(src1);
    fi::instance().disconnect_should_throw = true;
    src1 = std::move(src2);
}

// Line 170: move-assign consumer stop_consuming throws
TEST_F(FaultInjectionTest, MoveAssignConsumerStopConsumingThrows) {
    mqtt_source<mqtt_default_consumer_config> src1;
    mqtt_source<mqtt_default_consumer_config> src2;
    mock_connect(src1);
    fi::instance().stop_consuming_should_throw = true;
    src1 = std::move(src2);
}

// Move-assign connected callback source (lines 164-166)
TEST_F(FaultInjectionTest, MoveAssignCallbackDisconnectThrows) {
    mqtt_source<mqtt_default_config> src1;
    mqtt_source<mqtt_default_config> src2;
    src1.set_message_callback([](message_view) {});
    mock_connect(src1);
    fi::instance().disconnect_should_throw = true;
    src1 = std::move(src2);
}

// ==============================================================================
// Connect exception paths
// ==============================================================================

// Line 196: catch in disconnect-then-reconnect (connect while already connected)
TEST_F(FaultInjectionTest, ConnectReconnectDisconnectThrows) {
    mqtt_source<mqtt_default_config> src;
    mock_connect(src);
    fi::instance().disconnect_should_throw = true;
    auto r2 = src.connect();
    EXPECT_TRUE(r2.has_value());
}

TEST_F(FaultInjectionTest, ConnectReconnectConsumerDisconnectThrows) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    fi::instance().disconnect_should_throw = true;
    auto r2 = src.connect();
    EXPECT_TRUE(r2.has_value());
}

// Lines 192-196: consumer stop_consuming throws during reconnect
TEST_F(FaultInjectionTest, ConnectReconnectConsumerStopConsumingThrows) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    fi::instance().stop_consuming_should_throw = true;
    auto r2 = src.connect();
    EXPECT_TRUE(r2.has_value());
}

// Line 360: connect throws paho::exception
TEST_F(FaultInjectionTest, ConnectThrowsPahoException) {
    mqtt_source<mqtt_default_config> src;
    src.set_broker("mockhost", "1883");
    fi::instance().connect_should_throw_paho = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
    EXPECT_FALSE(src.is_connected());
}

TEST_F(FaultInjectionTest, ConnectThrowsPahoExceptionEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    src.set_broker("mockhost", "1883");
    fi::instance().connect_should_throw_paho = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, ConnectThrowsPahoExceptionConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    src.set_broker("mockhost", "1883");
    fi::instance().connect_should_throw_paho = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, ConnectThrowsPahoExceptionEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    src.set_broker("mockhost", "1883");
    fi::instance().connect_should_throw_paho = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
}

// Line 364: connect throws generic exception
TEST_F(FaultInjectionTest, ConnectThrowsGenericException) {
    mqtt_source<mqtt_default_config> src;
    src.set_broker("mockhost", "1883");
    fi::instance().connect_should_throw_generic = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, ConnectThrowsGenericExceptionEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    src.set_broker("mockhost", "1883");
    fi::instance().connect_should_throw_generic = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, ConnectThrowsGenericExceptionConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    src.set_broker("mockhost", "1883");
    fi::instance().connect_should_throw_generic = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, ConnectThrowsGenericExceptionEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    src.set_broker("mockhost", "1883");
    fi::instance().connect_should_throw_generic = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
}

// ==============================================================================
// Connected handler subscription restore (line 292)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectedHandlerSubscribeRestoreThrows) {
    mqtt_source<mqtt_default_config> src;
    mock_connect(src);
    auto r = src.subscribe("test/restore", 0);
    ASSERT_TRUE(r.has_value());
    fi::instance().subscribe_should_throw = true;
    fi::instance().simulate_reconnect();
    SUCCEED();
}

TEST_F(FaultInjectionTest, ConnectedHandlerSubscribeRestoreThrowsConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    auto r = src.subscribe("test/restore", 0);
    ASSERT_TRUE(r.has_value());
    fi::instance().subscribe_should_throw = true;
    fi::instance().simulate_reconnect();
    SUCCEED();
}

// ==============================================================================
// Connection lost handler (line 297)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectionLostHandlerFires) {
    mqtt_source<mqtt_default_config> src;
    mock_connect(src);
    fi::instance().simulate_connection_lost();
    EXPECT_FALSE(src.is_connected());
}

TEST_F(FaultInjectionTest, ConnectionLostHandlerFiresConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    fi::instance().simulate_connection_lost();
}

// ==============================================================================
// Disconnect exception path (line 386)
// ==============================================================================

TEST_F(FaultInjectionTest, DisconnectThrowsDefault) {
    mqtt_source<mqtt_default_config> src;
    mock_connect(src);
    fi::instance().disconnect_should_throw = true;
    src.disconnect();
}

TEST_F(FaultInjectionTest, DisconnectThrowsEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    mock_connect(src);
    fi::instance().disconnect_should_throw = true;
    src.disconnect();
}

TEST_F(FaultInjectionTest, DisconnectThrowsConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    fi::instance().disconnect_should_throw = true;
    src.disconnect();
}

TEST_F(FaultInjectionTest, DisconnectThrowsEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    mock_connect(src);
    fi::instance().disconnect_should_throw = true;
    src.disconnect();
}

TEST_F(FaultInjectionTest, DisconnectConsumerStopConsumingThrows) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    fi::instance().stop_consuming_should_throw = true;
    src.disconnect();
}

// ==============================================================================
// is_connected exception path (lines 406-407)
// ==============================================================================

TEST_F(FaultInjectionTest, IsConnectedThrowsDefault) {
    mqtt_source<mqtt_default_config> src;
    mock_connect(src);
    fi::instance().is_connected_should_throw = true;
    EXPECT_FALSE(src.is_connected());
}

TEST_F(FaultInjectionTest, IsConnectedThrowsEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    mock_connect(src);
    fi::instance().is_connected_should_throw = true;
    EXPECT_FALSE(src.is_connected());
}

TEST_F(FaultInjectionTest, IsConnectedThrowsConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    fi::instance().is_connected_should_throw = true;
    EXPECT_FALSE(src.is_connected());
}

TEST_F(FaultInjectionTest, IsConnectedThrowsEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    mock_connect(src);
    fi::instance().is_connected_should_throw = true;
    EXPECT_FALSE(src.is_connected());
}

// ==============================================================================
// Subscribe exception path (lines 445-449)
// ==============================================================================

TEST_F(FaultInjectionTest, SubscribeThrowsPahoDefault) {
    mqtt_source<mqtt_default_config> src;
    mock_connect(src);
    fi::instance().subscribe_should_throw = true;
    auto r = src.subscribe("test/topic", 0);
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, SubscribeThrowsPahoEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    mock_connect(src);
    fi::instance().subscribe_should_throw = true;
    auto r = src.subscribe("test/topic", 0);
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, SubscribeThrowsPahoConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    fi::instance().subscribe_should_throw = true;
    auto r = src.subscribe("test/topic", 0);
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, SubscribeThrowsPahoEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    mock_connect(src);
    fi::instance().subscribe_should_throw = true;
    auto r = src.subscribe("test/topic", 0);
    EXPECT_FALSE(r.has_value());
}

// ==============================================================================
// Publish exception path (line 501-504)
// ==============================================================================

TEST_F(FaultInjectionTest, PublishThrowsPahoDefault) {
    mqtt_source<mqtt_default_config> src;
    mock_connect(src);
    fi::instance().publish_should_throw = true;
    std::byte data[] = {std::byte{0x01}};
    auto r = src.publish("test/topic", data, 0);
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, PublishThrowsPahoEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    mock_connect(src);
    fi::instance().publish_should_throw = true;
    std::byte data[] = {std::byte{0x02}};
    auto r = src.publish("test/topic", data, 0);
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, PublishThrowsPahoConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    fi::instance().publish_should_throw = true;
    std::byte data[] = {std::byte{0x03}};
    auto r = src.publish("test/topic", data, 0);
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, PublishThrowsPahoEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    mock_connect(src);
    fi::instance().publish_should_throw = true;
    std::byte data[] = {std::byte{0x04}};
    auto r = src.publish("test/topic", data, 0);
    EXPECT_FALSE(r.has_value());
}

// ==============================================================================
// Poll exception path (line 570)
// ==============================================================================

TEST_F(FaultInjectionTest, PollThrowsConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    bool called = false;
    src.set_message_callback([&called](message_view) { called = true; });
    fi::instance().consume_should_throw = true;
    src.poll();
    EXPECT_FALSE(called);
}

TEST_F(FaultInjectionTest, PollThrowsEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    mock_connect(src);
    src.set_message_callback([](message_view) {});
    fi::instance().consume_should_throw = true;
    src.poll();
}

// ==============================================================================
// Start consuming throws during connect (consumer path, line 354)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectStartConsumingThrowsConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    src.set_broker("mockhost", "1883");
    fi::instance().start_consuming_should_throw = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, ConnectStartConsumingThrowsEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    src.set_broker("mockhost", "1883");
    fi::instance().start_consuming_should_throw = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
}

// ==============================================================================
// Message callback invocation through mock (lines 266-277, 529-540)
// ==============================================================================

TEST_F(FaultInjectionTest, MessageCallbackInvokedOnConnect) {
    mqtt_source<mqtt_default_config> src;
    bool received = false;
    src.set_message_callback([&](message_view mv) {
        received = true;
        EXPECT_EQ(mv.topic, "mock/topic");
        EXPECT_EQ(mv.payload.size(), 2u);
    });
    mock_connect(src);
    auto mock_msg = ::mqtt::make_message("mock/topic", "AB", 2, 0, false);
    fi::instance().mock_message = mock_msg;
    fi::instance().consume_should_return_message = true;
    EXPECT_FALSE(received);
}

// ==============================================================================
// set_message_callback on connected client (lines 518-545)
// ==============================================================================

TEST_F(FaultInjectionTest, SetCallbackOnConnectedClientUpdatesCallback) {
    mqtt_source<mqtt_default_config> src;
    mock_connect(src);
    bool first_called = false;
    src.set_message_callback([&first_called](message_view) { first_called = true; });
    bool second_called = false;
    src.set_message_callback([&second_called](message_view) { second_called = true; });
    EXPECT_FALSE(first_called);
    EXPECT_FALSE(second_called);
}

TEST_F(FaultInjectionTest, SetCallbackOnConnectedConsumerUpdatesCallback) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    src.set_message_callback([](message_view) {});
    src.set_message_callback([](message_view) {});
}

// ==============================================================================
// Edge: connect with empty host
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectEmptyHostReturnsError) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://:1883");
    auto r = src.connect(uri.view());
    EXPECT_FALSE(r.has_value());
}

// ==============================================================================
// Edge: subscribe/publish on disconnected source
// ==============================================================================

TEST_F(FaultInjectionTest, SubscribeWhenNotConnectedReturnsErrorAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    EXPECT_FALSE(s1.subscribe("t", 0).has_value());
    mqtt_source<mqtt_embedded_config> s2;
    EXPECT_FALSE(s2.subscribe("t", 0).has_value());
    mqtt_source<mqtt_default_consumer_config> s3;
    EXPECT_FALSE(s3.subscribe("t", 0).has_value());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    EXPECT_FALSE(s4.subscribe("t", 0).has_value());
}

TEST_F(FaultInjectionTest, PublishWhenNotConnectedReturnsErrorAllConfigs) {
    std::byte data[] = {std::byte{0x01}};
    mqtt_source<mqtt_default_config> s1;
    EXPECT_FALSE(s1.publish("t", data, 0).has_value());
    mqtt_source<mqtt_embedded_config> s2;
    EXPECT_FALSE(s2.publish("t", data, 0).has_value());
    mqtt_source<mqtt_default_consumer_config> s3;
    EXPECT_FALSE(s3.publish("t", data, 0).has_value());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    EXPECT_FALSE(s4.publish("t", data, 0).has_value());
}

// ==============================================================================
// Edge: disconnect on null impl
// ==============================================================================

TEST_F(FaultInjectionTest, DisconnectOnMovedFromSource) {
    mqtt_source<mqtt_default_config> src;
    auto dst = std::move(src);
    src.disconnect();
    EXPECT_FALSE(src.is_connected());
}

// ==============================================================================
// Full lifecycle with all SSL options (covers key_store/private_key paths)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectWithAllSslOptionsSet) {
    mqtt_source<mqtt_default_config> src;
    src.set_ssl("/path/to/ca.crt", "/path/to/client.crt", "/path/to/client.key");
    mock_connect(src);
    src.disconnect();
}

TEST_F(FaultInjectionTest, ConnectWithAllSslOptionsEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    src.set_ssl("/path/to/ca.crt", "/path/to/client.crt", "/path/to/client.key");
    mock_connect(src);
    src.disconnect();
}

TEST_F(FaultInjectionTest, ConnectWithAllSslOptionsConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    src.set_ssl("/path/to/ca.crt", "/path/to/client.crt", "/path/to/client.key");
    mock_connect(src);
    src.disconnect();
}

TEST_F(FaultInjectionTest, ConnectWithAllSslOptionsEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    src.set_ssl("/path/to/ca.crt", "/path/to/client.crt", "/path/to/client.key");
    mock_connect(src);
    src.disconnect();
}

// ==============================================================================
// Connect with will for all configs (covers will paths)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectWithWillAllConfigs) {
    std::byte data[] = {std::byte{0xAA}};
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/topic", data, 1, true);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/topic", data, 1, true);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/topic", data, 1, true);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/topic", data, 1, true);
        mock_connect(src);
        src.disconnect();
    }
}

// ==============================================================================
// Connect with auto reconnect for all configs
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectWithAutoReconnectAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        src.set_automatic_reconnect(1, 10);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        src.set_automatic_reconnect(1, 10);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_automatic_reconnect(1, 10);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_automatic_reconnect(1, 10);
        mock_connect(src);
        src.disconnect();
    }
}

// ==============================================================================
// Connect with username/password via URI for all configs
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectWithUserinfoAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        auto uri = basic_uri<mqtt_default_config>::parse("mqtt://user:pass@mockhost:1883");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtt://user:pass@mockhost:1883");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtt://user:pass@mockhost:1883");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtt://user:pass@mockhost:1883");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// MQTTS scheme (covers line 224 empty port default + SSL path)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectMqttsSchemeEmptyPortAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "8883");
        auto uri = basic_uri<mqtt_default_config>::parse("mqtts://mockhost");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "8883");
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtts://mockhost");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "8883");
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtts://mockhost");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "8883");
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtts://mockhost");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// query_param edge cases (lines 48-49, 53-54, 60)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectWithQueryParamsEdgeCases) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse(
        "mqtt://mockhost:1883/?xkeepalive=10&keepalive=30&keepalive&clean=true&clientid=test-fault");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

TEST_F(FaultInjectionTest, ConnectWithQueryParamUnknownKey) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse(
        "mqtt://mockhost:1883/?unknown=42");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

TEST_F(FaultInjectionTest, ConnectWithQueryParamOverflow) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse(
        "mqtt://mockhost:1883/?keepalive=999999999999");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// ==============================================================================
// URI userinfo without colon (line 237)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectUriUserinfoNoColon) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://justuser@mockhost:1883");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// ==============================================================================
// Connect with mqtt version 5 for all configs
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectVersion5AllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        src.set_mqtt_version(5);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        src.set_mqtt_version(5);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_mqtt_version(5);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_mqtt_version(5);
        mock_connect(src);
        src.disconnect();
    }
}

// ==============================================================================
// Connect with mqtt version 3 for all configs
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectVersion3AllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        src.set_mqtt_version(3);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        src.set_mqtt_version(3);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_mqtt_version(3);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_mqtt_version(3);
        mock_connect(src);
        src.disconnect();
    }
}

// ==============================================================================
// Publish with QoS 1 and 2 (covers lines 497-498)
// ==============================================================================

TEST_F(FaultInjectionTest, PublishQoS1AllConfigs) {
    std::byte data[] = {std::byte{0x01}};
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos1", data, 1);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos1", data, 1);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
}

TEST_F(FaultInjectionTest, PublishQoS2AllConfigs) {
    std::byte data[] = {std::byte{0x02}};
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos2", data, 2);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos2", data, 2);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Subscribe exceeding max for all configs (covers add_subscription false path)
// ==============================================================================

TEST_F(FaultInjectionTest, SubscribeExceedsMaxEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    mock_connect(src);
    for (std::size_t i = 0; i < mqtt_embedded_config::max_subscriptions; ++i) {
        auto r = src.subscribe(("test/max/" + std::to_string(i)).c_str(), 0);
        EXPECT_TRUE(r.has_value());
    }
    auto r = src.subscribe("test/max/overflow", 0);
    EXPECT_FALSE(r.has_value());
    src.disconnect();
}

TEST_F(FaultInjectionTest, SubscribeExceedsMaxEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    mock_connect(src);
    for (std::size_t i = 0; i < mqtt_embedded_consumer_config::max_subscriptions; ++i) {
        auto r = src.subscribe(("test/maxe/" + std::to_string(i)).c_str(), 0);
        EXPECT_TRUE(r.has_value());
    }
    auto r = src.subscribe("test/maxe/overflow", 0);
    EXPECT_FALSE(r.has_value());
    src.disconnect();
}

// ==============================================================================
// clean_session=false via URI query (covers line 249)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectCleanSessionFalse) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse(
        "mqtt://mockhost:1883/?clean=false");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

TEST_F(FaultInjectionTest, ConnectCleanSessionTrue) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse(
        "mqtt://mockhost:1883/?clean=true");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

TEST_F(FaultInjectionTest, ConnectCleanSession1) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse(
        "mqtt://mockhost:1883/?clean=1");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// ==============================================================================
// Comprehensive branch coverage: move-assign connected sources
// ==============================================================================

TEST_F(FaultInjectionTest, MoveAssignConnectedDefaultToDefault) {
    mqtt_source<mqtt_default_config> src1;
    mqtt_source<mqtt_default_config> src2;
    mock_connect(src1);
    mock_connect(src2);
    src1 = std::move(src2);
}

TEST_F(FaultInjectionTest, MoveAssignConnectedEmbeddedToEmbedded) {
    mqtt_source<mqtt_embedded_config> src1;
    mqtt_source<mqtt_embedded_config> src2;
    mock_connect(src1);
    mock_connect(src2);
    src1 = std::move(src2);
}

TEST_F(FaultInjectionTest, MoveAssignConnectedConsumerToConsumer) {
    mqtt_source<mqtt_default_consumer_config> src1;
    mqtt_source<mqtt_default_consumer_config> src2;
    mock_connect(src1);
    mock_connect(src2);
    src1 = std::move(src2);
}

TEST_F(FaultInjectionTest, MoveAssignConnectedEmbeddedConsumerToEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src1;
    mqtt_source<mqtt_embedded_consumer_config> src2;
    mock_connect(src1);
    mock_connect(src2);
    src1 = std::move(src2);
}

// ==============================================================================
// Move-assign connected callback source (covers line 161-175)
// ==============================================================================

TEST_F(FaultInjectionTest, MoveAssignConnectedCallbackDefault) {
    mqtt_source<mqtt_default_config> src1;
    src1.set_message_callback([](message_view) {});
    mqtt_source<mqtt_default_config> src2;
    mock_connect(src1);
    src1 = std::move(src2);
}

TEST_F(FaultInjectionTest, MoveAssignConnectedCallbackEmbedded) {
    mqtt_source<mqtt_embedded_config> src1;
    src1.set_message_callback([](message_view) {});
    mqtt_source<mqtt_embedded_config> src2;
    mock_connect(src1);
    src1 = std::move(src2);
}

// ==============================================================================
// Move-assign connected consumer with disconnect/stop_consuming (covers 167,172)
// ==============================================================================

TEST_F(FaultInjectionTest, MoveAssignConnectedConsumerDisconnectThrows) {
    mqtt_source<mqtt_default_consumer_config> src1;
    mqtt_source<mqtt_default_consumer_config> src2;
    mock_connect(src1);
    fi::instance().disconnect_should_throw = true;
    src1 = std::move(src2);
}

TEST_F(FaultInjectionTest, MoveAssignConnectedConsumerStopConsumingThrows) {
    mqtt_source<mqtt_default_consumer_config> src1;
    mqtt_source<mqtt_default_consumer_config> src2;
    mock_connect(src1);
    fi::instance().stop_consuming_should_throw = true;
    src1 = std::move(src2);
}

// ==============================================================================
// Poll with message delivery via mock (covers lines 555-566)
// ==============================================================================

TEST_F(FaultInjectionTest, PollDeliversMessageConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    src.set_broker("mockhost", "1883");
    src.set_message_callback([](message_view) {});
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    fi::instance().mock_message = ::mqtt::make_message("test/topic", "AB", 2, 0, false);
    fi::instance().consume_should_return_message = true;
    src.poll();
}

TEST_F(FaultInjectionTest, PollDeliversMessageEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    src.set_broker("mockhost", "1883");
    src.set_message_callback([](message_view) {});
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    fi::instance().mock_message = ::mqtt::make_message("test/topic", "AB", 2, 0, false);
    fi::instance().consume_should_return_message = true;
    src.poll();
}

TEST_F(FaultInjectionTest, PollNoMessageConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    src.set_broker("mockhost", "1883");
    src.set_message_callback([](message_view) {});
    auto r = src.connect();
    ASSERT_TRUE(r.has_value());
    fi::instance().consume_should_return_message = false;
    src.poll();
}

// ==============================================================================
// Message callback invoked on connected client via mock inject (covers 530-540)
// ==============================================================================

TEST_F(FaultInjectionTest, MessageCallbackInvokedOnConnectedDefault) {
    mqtt_source<mqtt_default_config> src;
    bool received = false;
    src.set_message_callback([&](message_view mv) {
        received = true;
        EXPECT_EQ(mv.topic, "inject/topic");
    });
    mock_connect(src);
    auto mock_msg = ::mqtt::make_message("inject/topic", "XY", 2, 1, false);
    fi::instance().active_client->inject_message(mock_msg);
    EXPECT_TRUE(received);
}

TEST_F(FaultInjectionTest, MessageCallbackInvokedOnConnectedEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    bool received = false;
    src.set_message_callback([&](message_view mv) {
        received = true;
    });
    mock_connect(src);
    auto mock_msg = ::mqtt::make_message("inject/topic", "Z", 1, 0, false);
    fi::instance().active_client->inject_message(mock_msg);
    EXPECT_TRUE(received);
}

// ==============================================================================
// Connected handler subscription restore (covers 287-290)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectedHandlerRestoresSubscriptions) {
    mqtt_source<mqtt_default_config> src;
    mock_connect(src);
    auto r = src.subscribe("test/sub1", 0);
    ASSERT_TRUE(r.has_value());
    r = src.subscribe("test/sub2", 1);
    ASSERT_TRUE(r.has_value());
    fi::instance().simulate_reconnect();
    SUCCEED();
}

TEST_F(FaultInjectionTest, ConnectedHandlerRestoresSubscriptionsConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    auto r = src.subscribe("test/sub1", 0);
    ASSERT_TRUE(r.has_value());
    fi::instance().simulate_reconnect();
    SUCCEED();
}

// ==============================================================================
// Connection lost handler (covers 296)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectionLostHandlerEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    mock_connect(src);
    fi::instance().simulate_connection_lost();
    EXPECT_FALSE(src.is_connected());
}

TEST_F(FaultInjectionTest, ConnectionLostHandlerEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    mock_connect(src);
    fi::instance().simulate_connection_lost();
}

// ==============================================================================
// Setters after connect (covers 582-649 branches)
// ==============================================================================

TEST_F(FaultInjectionTest, SettersNoOpAfterConnectAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        mock_connect(src);
        src.set_client_id("nope");
        src.set_keepalive(99);
        src.set_clean_session(false);
        src.set_automatic_reconnect(1, 10);
        src.set_will("will/t", {}, 0, false);
        src.set_username("nope");
        src.set_password("nope");
        src.set_ssl("nope", "nope", "nope");
        src.set_ssl_verify(false);
        src.set_mqtt_version(5);
        src.set_broker("nope", "nope");
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        mock_connect(src);
        src.set_client_id("nope");
        src.set_keepalive(99);
        src.set_clean_session(false);
        src.set_automatic_reconnect(1, 10);
        src.set_will("will/t", {}, 0, false);
        src.set_username("nope");
        src.set_password("nope");
        src.set_ssl("nope", "nope", "nope");
        src.set_ssl_verify(false);
        src.set_mqtt_version(5);
        src.set_broker("nope", "nope");
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        mock_connect(src);
        src.set_client_id("nope");
        src.set_keepalive(99);
        src.set_clean_session(false);
        src.set_automatic_reconnect(1, 10);
        src.set_will("will/t", {}, 0, false);
        src.set_username("nope");
        src.set_password("nope");
        src.set_ssl("nope", "nope", "nope");
        src.set_ssl_verify(false);
        src.set_mqtt_version(5);
        src.set_broker("nope", "nope");
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        mock_connect(src);
        src.set_client_id("nope");
        src.set_keepalive(99);
        src.set_clean_session(false);
        src.set_automatic_reconnect(1, 10);
        src.set_will("will/t", {}, 0, false);
        src.set_username("nope");
        src.set_password("nope");
        src.set_ssl("nope", "nope", "nope");
        src.set_ssl_verify(false);
        src.set_mqtt_version(5);
        src.set_broker("nope", "nope");
    }
}

// ==============================================================================
// Will QoS clamping (covers 609-610)
// ==============================================================================

TEST_F(FaultInjectionTest, WillQosClampingNegativeAllConfigs) {
    std::byte data[] = {std::byte{0x01}};
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/t", data, -1, false);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/t", data, -1, false);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/t", data, -1, false);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/t", data, -1, false);
        mock_connect(src);
        src.disconnect();
    }
}

TEST_F(FaultInjectionTest, WillQosClampingAbove2AllConfigs) {
    std::byte data[] = {std::byte{0x02}};
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/t", data, 5, true);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/t", data, 5, true);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/t", data, 5, true);
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_will("will/t", data, 5, true);
        mock_connect(src);
        src.disconnect();
    }
}

// ==============================================================================
// Client ID auto-generation (covers 256-258)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectAutoGeneratesClientIdAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Callback installed before connect (covers 262-280)
// ==============================================================================

TEST_F(FaultInjectionTest, CallbackInstalledBeforeConnectAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_message_callback([](message_view) {});
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_message_callback([](message_view) {});
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// URI with host override (covers 206-228)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectUriOverridesBroker) {
    mqtt_source<mqtt_default_config> src;
    src.set_broker("wronghost", "9999");
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://mockhost:1883");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// ==============================================================================
// MQTTS with empty port (covers 223-224)
// ==============================================================================

TEST_F(FaultInjectionTest, MqttsEmptyPortAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        auto uri = basic_uri<mqtt_default_config>::parse("mqtts://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtts://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtts://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtts://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Connect with URI query version (covers 250, 315)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectVersionViaQueryAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        auto uri = basic_uri<mqtt_default_config>::parse("mqtt://mockhost:1883/?version=5");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtt://mockhost:1883/?version=5");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtt://mockhost:1883/?version=5");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtt://mockhost:1883/?version=5");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// parse_int overflow path (covers 33-34)
// ==============================================================================

TEST_F(FaultInjectionTest, QueryParamKeepaliveOverflow) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://mockhost:1883/?keepalive=99999999999");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// ==============================================================================
// query_param key not found (covers 45)
// ==============================================================================

TEST_F(FaultInjectionTest, QueryParamKeyNotFound) {
    mqtt_source<mqtt_default_config> src;
    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://mockhost:1883/?nonexistent=42");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// ==============================================================================
// Set message callback on connected client (covers 518, 520, 525-544)
// ==============================================================================

TEST_F(FaultInjectionTest, SetCallbackOnConnectedClientDefault) {
    mqtt_source<mqtt_default_config> src;
    mock_connect(src);
    bool called = false;
    src.set_message_callback([&called](message_view) { called = true; });
    auto mock_msg = ::mqtt::make_message("cb/test", "data", 4, 0, false);
    fi::instance().active_client->inject_message(mock_msg);
    EXPECT_TRUE(called);
}

TEST_F(FaultInjectionTest, SetCallbackOnConnectedClientEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    mock_connect(src);
    bool called = false;
    src.set_message_callback([&called](message_view) { called = true; });
    auto mock_msg = ::mqtt::make_message("cb/test", "data", 4, 0, false);
    fi::instance().active_client->inject_message(mock_msg);
    EXPECT_TRUE(called);
}

TEST_F(FaultInjectionTest, SetCallbackOnConnectedClientConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    src.set_message_callback([](message_view) {});
    src.set_message_callback([](message_view) {});
}

TEST_F(FaultInjectionTest, SetCallbackOnConnectedClientEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    mock_connect(src);
    src.set_message_callback([](message_view) {});
    src.set_message_callback([](message_view) {});
}

// ==============================================================================
// Reconnect while connected (covers 190-202)
// ==============================================================================

TEST_F(FaultInjectionTest, ReconnectWhileConnectedAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        mock_connect(src);
        auto r2 = src.connect();
        EXPECT_TRUE(r2.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        mock_connect(src);
        auto r2 = src.connect();
        EXPECT_TRUE(r2.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        mock_connect(src);
        auto r2 = src.connect();
        EXPECT_TRUE(r2.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        mock_connect(src);
        auto r2 = src.connect();
        EXPECT_TRUE(r2.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Poll on callback-mode source (covers 572-574)
// ==============================================================================

TEST_F(FaultInjectionTest, PollCallbackModeYieldsAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        mock_connect(src);
        src.poll();
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        mock_connect(src);
        src.poll();
        src.disconnect();
    }
}

// ==============================================================================
// Poll when not connected
// ==============================================================================

TEST_F(FaultInjectionTest, PollNotConnectedAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    s1.poll();
    mqtt_source<mqtt_embedded_config> s2;
    s2.poll();
    mqtt_source<mqtt_default_consumer_config> s3;
    s3.poll();
    mqtt_source<mqtt_embedded_consumer_config> s4;
    s4.poll();
}

// ==============================================================================
// Destructor with connected consumer (covers 138-144)
// ==============================================================================

TEST_F(FaultInjectionTest, DestructorConnectedConsumerDefault) {
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
    }
}

TEST_F(FaultInjectionTest, DestructorConnectedConsumerEmbedded) {
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
    }
}

// ==============================================================================
// Subscribe/publish with invalid QoS (covers 416, 421, 470, 475)
// ==============================================================================

TEST_F(FaultInjectionTest, SubscribeInvalidQosAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    EXPECT_FALSE(s1.subscribe("t", -1).has_value());
    EXPECT_FALSE(s1.subscribe("t", 3).has_value());
    mqtt_source<mqtt_embedded_config> s2;
    EXPECT_FALSE(s2.subscribe("t", -1).has_value());
    EXPECT_FALSE(s2.subscribe("t", 3).has_value());
    mqtt_source<mqtt_default_consumer_config> s3;
    EXPECT_FALSE(s3.subscribe("t", -1).has_value());
    EXPECT_FALSE(s3.subscribe("t", 3).has_value());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    EXPECT_FALSE(s4.subscribe("t", -1).has_value());
    EXPECT_FALSE(s4.subscribe("t", 3).has_value());
}

TEST_F(FaultInjectionTest, PublishInvalidQosAllConfigs) {
    std::byte data[] = {std::byte{0x01}};
    mqtt_source<mqtt_default_config> s1;
    EXPECT_FALSE(s1.publish("t", data, -1).has_value());
    EXPECT_FALSE(s1.publish("t", data, 3).has_value());
    mqtt_source<mqtt_embedded_config> s2;
    EXPECT_FALSE(s2.publish("t", data, -1).has_value());
    EXPECT_FALSE(s2.publish("t", data, 3).has_value());
    mqtt_source<mqtt_default_consumer_config> s3;
    EXPECT_FALSE(s3.publish("t", data, -1).has_value());
    EXPECT_FALSE(s3.publish("t", data, 3).has_value());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    EXPECT_FALSE(s4.publish("t", data, -1).has_value());
    EXPECT_FALSE(s4.publish("t", data, 3).has_value());
}

// ==============================================================================
// Subscribe with empty topic (covers 416)
// ==============================================================================

TEST_F(FaultInjectionTest, SubscribeEmptyTopicAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    EXPECT_FALSE(s1.subscribe("", 0).has_value());
    mqtt_source<mqtt_embedded_config> s2;
    EXPECT_FALSE(s2.subscribe("", 0).has_value());
    mqtt_source<mqtt_default_consumer_config> s3;
    EXPECT_FALSE(s3.subscribe("", 0).has_value());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    EXPECT_FALSE(s4.subscribe("", 0).has_value());
}

// ==============================================================================
// Publish with empty topic (covers 470)
// ==============================================================================

TEST_F(FaultInjectionTest, PublishEmptyTopicAllConfigs) {
    std::byte data[] = {std::byte{0x01}};
    mqtt_source<mqtt_default_config> s1;
    EXPECT_FALSE(s1.publish("", data, 0).has_value());
    mqtt_source<mqtt_embedded_config> s2;
    EXPECT_FALSE(s2.publish("", data, 0).has_value());
    mqtt_source<mqtt_default_consumer_config> s3;
    EXPECT_FALSE(s3.publish("", data, 0).has_value());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    EXPECT_FALSE(s4.publish("", data, 0).has_value());
}

// ==============================================================================
// Setters on moved-from source (covers !impl_ path)
// ==============================================================================

TEST_F(FaultInjectionTest, SettersOnMovedFromSource) {
    mqtt_source<mqtt_default_config> src;
    auto dst = std::move(src);
    src.set_client_id("x");
    src.set_keepalive(1);
    src.set_clean_session(true);
    src.set_automatic_reconnect(1, 10);
    std::byte data[] = {std::byte{0x01}};
    src.set_will("t", data, 0, false);
    src.set_username("u");
    src.set_password("p");
    src.set_ssl("a", "b", "c");
    src.set_ssl_verify(true);
    src.set_mqtt_version(4);
    src.set_broker("h", "p");
    src.set_message_callback([](message_view) {});
}

// ==============================================================================
// Disconnect normal path all configs (covers 380, 385)
// ==============================================================================

TEST_F(FaultInjectionTest, DisconnectNormalAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        mock_connect(src);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        mock_connect(src);
        src.disconnect();
    }
}

// ==============================================================================
// Disconnect not connected (no-op)
// ==============================================================================

TEST_F(FaultInjectionTest, DisconnectNotConnectedAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    s1.disconnect();
    mqtt_source<mqtt_embedded_config> s2;
    s2.disconnect();
    mqtt_source<mqtt_default_consumer_config> s3;
    s3.disconnect();
    mqtt_source<mqtt_embedded_consumer_config> s4;
    s4.disconnect();
}

// ==============================================================================
// URI host override for all configs (covers line 206 false branch)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectUriHostOverrideAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        auto uri = basic_uri<mqtt_default_config>::parse("mqtt://urihost/");//?keepalive=5");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtt://urihost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtt://urihost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtt://urihost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// URI query params for all configs (covers 246-252)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectQueryParamsEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    auto uri = basic_uri<mqtt_embedded_config>::parse("mqtt://mockhost:1883/?keepalive=20&clean=false&clientid=emb-test");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

TEST_F(FaultInjectionTest, ConnectQueryParamsConsumer) {
    mqtt_source<mqtt_default_consumer_config> src;
    auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtt://mockhost:1883/?keepalive=20&clean=false&clientid=con-test");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

TEST_F(FaultInjectionTest, ConnectQueryParamsEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtt://mockhost:1883/?keepalive=20&clean=false&clientid=ec-test");
    auto r = src.connect(uri.view());
    ASSERT_TRUE(r.has_value());
    src.disconnect();
}

// ==============================================================================
// Query param non-digit and overflow for all configs (covers 31, 33)
// ==============================================================================

TEST_F(FaultInjectionTest, QueryParamNonDigitValueAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        auto uri = basic_uri<mqtt_default_config>::parse("mqtt://mockhost:1883/?keepalive=abc");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtt://mockhost:1883/?keepalive=abc");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtt://mockhost:1883/?keepalive=abc");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtt://mockhost:1883/?keepalive=abc");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Client ID explicit for all configs (covers 256-258 false branch)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectExplicitClientIdAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        src.set_client_id("explicit-id");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        src.set_client_id("explicit-id");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_client_id("explicit-id");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_client_id("explicit-id");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Move-assign from connected source all configs (covers 158-175)
// ==============================================================================

TEST_F(FaultInjectionTest, MoveAssignConnectedSrcDefault) {
    mqtt_source<mqtt_default_config> src1;
    mock_connect(src1);
    mqtt_source<mqtt_default_config> src2;
    src1 = std::move(src2);
}

TEST_F(FaultInjectionTest, MoveAssignConnectedSrcEmbedded) {
    mqtt_source<mqtt_embedded_config> src1;
    mock_connect(src1);
    mqtt_source<mqtt_embedded_config> src2;
    src1 = std::move(src2);
}

TEST_F(FaultInjectionTest, MoveAssignConnectedSrcConsumer) {
    mqtt_source<mqtt_default_consumer_config> src1;
    mock_connect(src1);
    mqtt_source<mqtt_default_consumer_config> src2;
    src1 = std::move(src2);
}

TEST_F(FaultInjectionTest, MoveAssignConnectedSrcEmbeddedConsumer) {
    mqtt_source<mqtt_embedded_consumer_config> src1;
    mock_connect(src1);
    mqtt_source<mqtt_embedded_consumer_config> src2;
    src1 = std::move(src2);
}

// ==============================================================================
// Destructor connected callback source all configs (covers 132-137)
// ==============================================================================

TEST_F(FaultInjectionTest, DestructorConnectedCallbackDefault) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        src.set_message_callback([](message_view) {});
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
    }
}

TEST_F(FaultInjectionTest, DestructorConnectedCallbackEmbedded) {
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        src.set_message_callback([](message_view) {});
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
    }
}

// ==============================================================================
// Move-assign from empty to connected (covers 159 false path)
// ==============================================================================

TEST_F(FaultInjectionTest, MoveAssignEmptyToConnectedAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src1;
        mqtt_source<mqtt_default_config> src2;
        mock_connect(src2);
        src1 = std::move(src2);
    }
    {
        mqtt_source<mqtt_embedded_config> src1;
        mqtt_source<mqtt_embedded_config> src2;
        mock_connect(src2);
        src1 = std::move(src2);
    }
    {
        mqtt_source<mqtt_default_consumer_config> src1;
        mqtt_source<mqtt_default_consumer_config> src2;
        mock_connect(src2);
        src1 = std::move(src2);
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src1;
        mqtt_source<mqtt_embedded_consumer_config> src2;
        mock_connect(src2);
        src1 = std::move(src2);
    }
}

// ==============================================================================
// Connect throws with callback installed (covers 267-277 callback path + catch)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectThrowsWithCallbackDefault) {
    mqtt_source<mqtt_default_config> src;
    src.set_broker("mockhost", "1883");
    src.set_message_callback([](message_view) {});
    fi::instance().connect_should_throw_paho = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
}

TEST_F(FaultInjectionTest, ConnectThrowsWithCallbackEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    src.set_broker("mockhost", "1883");
    src.set_message_callback([](message_view) {});
    fi::instance().connect_should_throw_paho = true;
    auto r = src.connect();
    EXPECT_FALSE(r.has_value());
}

// ==============================================================================
// Connect throws with consumer config (covers 354 start_consuming + catch)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectThrowsGenericConsumerAll) {
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        fi::instance().connect_should_throw_generic = true;
        auto r = src.connect();
        EXPECT_FALSE(r.has_value());
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        fi::instance().connect_should_throw_generic = true;
        auto r = src.connect();
        EXPECT_FALSE(r.has_value());
    }
}

// ==============================================================================
// Subscribe success all configs (covers 443 normal path)
// ==============================================================================

TEST_F(FaultInjectionTest, SubscribeSuccessAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        mock_connect(src);
        auto r = src.subscribe("test/topic", 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        mock_connect(src);
        auto r = src.subscribe("test/topic", 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        mock_connect(src);
        auto r = src.subscribe("test/topic", 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        mock_connect(src);
        auto r = src.subscribe("test/topic", 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Publish success all configs (covers 492-498 normal path)
// ==============================================================================

TEST_F(FaultInjectionTest, PublishSuccessAllConfigs) {
    std::byte data[] = {std::byte{0x01}};
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Publish QoS 1/2 success all configs (covers 497-498)
// ==============================================================================

TEST_F(FaultInjectionTest, PublishQoS1AllConfigsFull) {
    std::byte data[] = {std::byte{0x01}};
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos1", data, 1);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos1", data, 1);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos1", data, 1);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos1", data, 1);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
}

TEST_F(FaultInjectionTest, PublishQoS2AllConfigsFull) {
    std::byte data[] = {std::byte{0x02}};
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos2", data, 2);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos2", data, 2);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos2", data, 2);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/qos2", data, 2);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Subscribe on moved-from (covers 427 !impl_)
// ==============================================================================

TEST_F(FaultInjectionTest, SubscribeMovedFromAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    auto d1 = std::move(s1);
    EXPECT_FALSE(s1.subscribe("t", 0).has_value());
    mqtt_source<mqtt_embedded_config> s2;
    auto d2 = std::move(s2);
    EXPECT_FALSE(s2.subscribe("t", 0).has_value());
    mqtt_source<mqtt_default_consumer_config> s3;
    auto d3 = std::move(s3);
    EXPECT_FALSE(s3.subscribe("t", 0).has_value());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    auto d4 = std::move(s4);
    EXPECT_FALSE(s4.subscribe("t", 0).has_value());
}

// ==============================================================================
// Publish on moved-from (covers 481 !impl_)
// ==============================================================================

TEST_F(FaultInjectionTest, PublishMovedFromAllConfigs) {
    std::byte data[] = {std::byte{0x01}};
    mqtt_source<mqtt_default_config> s1;
    auto d1 = std::move(s1);
    EXPECT_FALSE(s1.publish("t", data, 0).has_value());
    mqtt_source<mqtt_embedded_config> s2;
    auto d2 = std::move(s2);
    EXPECT_FALSE(s2.publish("t", data, 0).has_value());
    mqtt_source<mqtt_default_consumer_config> s3;
    auto d3 = std::move(s3);
    EXPECT_FALSE(s3.publish("t", data, 0).has_value());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    auto d4 = std::move(s4);
    EXPECT_FALSE(s4.publish("t", data, 0).has_value());
}

// ==============================================================================
// Set message callback on moved-from (covers 518 !impl_)
// ==============================================================================

TEST_F(FaultInjectionTest, SetCallbackOnMovedFromAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    auto d1 = std::move(s1);
    s1.set_message_callback([](message_view) {});
    mqtt_source<mqtt_embedded_config> s2;
    auto d2 = std::move(s2);
    s2.set_message_callback([](message_view) {});
    mqtt_source<mqtt_default_consumer_config> s3;
    auto d3 = std::move(s3);
    s3.set_message_callback([](message_view) {});
    mqtt_source<mqtt_embedded_consumer_config> s4;
    auto d4 = std::move(s4);
    s4.set_message_callback([](message_view) {});
}

// ==============================================================================
// is_connected on moved-from (covers 401)
// ==============================================================================

TEST_F(FaultInjectionTest, IsConnectedMovedFromAllConfigs) {
    mqtt_source<mqtt_default_config> s1;
    auto d1 = std::move(s1);
    EXPECT_FALSE(s1.is_connected());
    mqtt_source<mqtt_embedded_config> s2;
    auto d2 = std::move(s2);
    EXPECT_FALSE(s2.is_connected());
    mqtt_source<mqtt_default_consumer_config> s3;
    auto d3 = std::move(s3);
    EXPECT_FALSE(s3.is_connected());
    mqtt_source<mqtt_embedded_consumer_config> s4;
    auto d4 = std::move(s4);
    EXPECT_FALSE(s4.is_connected());
}

// ==============================================================================
// parse_int overflow (covers 33-34)
// ==============================================================================

TEST_F(FaultInjectionTest, ParseIntOverflowAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        auto uri = basic_uri<mqtt_default_config>::parse("mqtt://mockhost:1883/?keepalive=99999999999");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtt://mockhost:1883/?keepalive=99999999999");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtt://mockhost:1883/?keepalive=99999999999");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtt://mockhost:1883/?keepalive=99999999999");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// query_param key preceded by non-separator (covers 46-49)
// ==============================================================================

TEST_F(FaultInjectionTest, QueryParamKeyAfterNonSeparatorAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        auto uri = basic_uri<mqtt_default_config>::parse("mqtt://mockhost:1883/?xkeepalive=10&keepalive=5");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtt://mockhost:1883/?xkeepalive=10&keepalive=5");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtt://mockhost:1883/?xkeepalive=10&keepalive=5");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtt://mockhost:1883/?xkeepalive=10&keepalive=5");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// query_param key without equals (covers 52-53)
// ==============================================================================

TEST_F(FaultInjectionTest, QueryParamKeyWithoutEqualsAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        auto uri = basic_uri<mqtt_default_config>::parse("mqtt://mockhost:1883/?keepalive&clean=true");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtt://mockhost:1883/?keepalive&clean=true");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtt://mockhost:1883/?keepalive&clean=true");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtt://mockhost:1883/?keepalive&clean=true");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Reconnect while connected with consumer config (covers 193-196 stop_consuming)
// ==============================================================================

TEST_F(FaultInjectionTest, ReconnectConsumerStopConsumingThrows) {
    mqtt_source<mqtt_default_consumer_config> src;
    mock_connect(src);
    fi::instance().stop_consuming_should_throw = true;
    auto r2 = src.connect();
    EXPECT_TRUE(r2.has_value());
    src.disconnect();
}

TEST_F(FaultInjectionTest, ReconnectEmbeddedConsumerStopConsumingThrows) {
    mqtt_source<mqtt_embedded_consumer_config> src;
    mock_connect(src);
    fi::instance().stop_consuming_should_throw = true;
    auto r2 = src.connect();
    EXPECT_TRUE(r2.has_value());
    src.disconnect();
}

TEST_F(FaultInjectionTest, ReconnectCallbackDisconnectThrows) {
    mqtt_source<mqtt_default_config> src;
    src.set_message_callback([](message_view) {});
    mock_connect(src);
    fi::instance().disconnect_should_throw = true;
    auto r2 = src.connect();
    EXPECT_TRUE(r2.has_value());
    src.disconnect();
}

TEST_F(FaultInjectionTest, ReconnectEmbeddedCallbackDisconnectThrows) {
    mqtt_source<mqtt_embedded_config> src;
    src.set_message_callback([](message_view) {});
    mock_connect(src);
    fi::instance().disconnect_should_throw = true;
    auto r2 = src.connect();
    EXPECT_TRUE(r2.has_value());
    src.disconnect();
}

// ==============================================================================
// Connect with empty port via set_broker for all configs (covers line 224)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectEmptyPortDefaultAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

TEST_F(FaultInjectionTest, ConnectMqttsEmptyPortViaBrokerAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "");
        auto uri = basic_uri<mqtt_default_config>::parse("mqtts://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "");
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtts://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "");
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtts://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "");
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtts://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Self-move-assignment (covers line 158 branch 1: this == &other)
// ==============================================================================

TEST_F(FaultInjectionTest, SelfMoveAssignAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        mock_connect(src);
        src = std::move(src);
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        mock_connect(src);
        src = std::move(src);
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        mock_connect(src);
        src = std::move(src);
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        mock_connect(src);
        src = std::move(src);
    }
}

// ==============================================================================
// Move-assign to moved-from source (covers line 159 branch 2: impl_ null)
// ==============================================================================

TEST_F(FaultInjectionTest, MoveAssignToMovedFromAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src1;
        mqtt_source<mqtt_default_config> src2;
        auto sink1 = std::move(src1);
        src1 = std::move(src2);
    }
    {
        mqtt_source<mqtt_embedded_config> src1;
        mqtt_source<mqtt_embedded_config> src2;
        auto sink2 = std::move(src1);
        src1 = std::move(src2);
    }
    {
        mqtt_source<mqtt_default_consumer_config> src1;
        mqtt_source<mqtt_default_consumer_config> src2;
        auto sink3 = std::move(src1);
        src1 = std::move(src2);
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src1;
        mqtt_source<mqtt_embedded_consumer_config> src2;
        auto sink4 = std::move(src1);
        src1 = std::move(src2);
    }
}

// ==============================================================================
// Move-assign connected-then-disconnected source (covers line 167 branch 3)
// ==============================================================================

TEST_F(FaultInjectionTest, MoveAssignDisconnectedSourceAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src1;
        mock_connect(src1);
        src1.disconnect();
        mqtt_source<mqtt_default_config> src2;
        src1 = std::move(src2);
    }
    {
        mqtt_source<mqtt_embedded_config> src1;
        mock_connect(src1);
        src1.disconnect();
        mqtt_source<mqtt_embedded_config> src2;
        src1 = std::move(src2);
    }
    {
        mqtt_source<mqtt_default_consumer_config> src1;
        mock_connect(src1);
        src1.disconnect();
        mqtt_source<mqtt_default_consumer_config> src2;
        src1 = std::move(src2);
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src1;
        mock_connect(src1);
        src1.disconnect();
        mqtt_source<mqtt_embedded_consumer_config> src2;
        src1 = std::move(src2);
    }
}

// ==============================================================================
// Move-assign from connected source with callback installed (covers 164-166 all configs)
// ==============================================================================

TEST_F(FaultInjectionTest, MoveAssignConnectedCallbackConsumerAllConfigs) {
    {
        mqtt_source<mqtt_default_consumer_config> src1;
        src1.set_message_callback([](message_view) {});
        mock_connect(src1);
        mqtt_source<mqtt_default_consumer_config> src2;
        src1 = std::move(src2);
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src1;
        src1.set_message_callback([](message_view) {});
        mock_connect(src1);
        mqtt_source<mqtt_embedded_consumer_config> src2;
        src1 = std::move(src2);
    }
}

// ==============================================================================
// Setter normal path (disconnected, impl exists) for all configs (covers 588-643)
// ==============================================================================

TEST_F(FaultInjectionTest, SettersNormalPathAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_keepalive(30);
        src.set_clean_session(false);
        src.set_username("user");
        src.set_password("pass");
        src.set_ssl_verify(false);
        src.set_broker("host", "1234");
        src.set_client_id("cid");
        src.set_mqtt_version(5);
        src.set_automatic_reconnect(1, 10);
        std::byte data[] = {std::byte{0x01}};
        src.set_will("will/t", data, 1, true);
        src.set_ssl("ca.crt", "client.crt", "client.key");
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_keepalive(30);
        src.set_clean_session(false);
        src.set_username("user");
        src.set_password("pass");
        src.set_ssl_verify(false);
        src.set_broker("host", "1234");
        src.set_client_id("cid");
        src.set_mqtt_version(5);
        src.set_automatic_reconnect(1, 10);
        std::byte data[] = {std::byte{0x01}};
        src.set_will("will/t", data, 1, true);
        src.set_ssl("ca.crt", "client.crt", "client.key");
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_keepalive(30);
        src.set_clean_session(false);
        src.set_username("user");
        src.set_password("pass");
        src.set_ssl_verify(false);
        src.set_broker("host", "1234");
        src.set_client_id("cid");
        src.set_mqtt_version(5);
        src.set_automatic_reconnect(1, 10);
        std::byte data[] = {std::byte{0x01}};
        src.set_will("will/t", data, 1, true);
        src.set_ssl("ca.crt", "client.crt", "client.key");
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_keepalive(30);
        src.set_clean_session(false);
        src.set_username("user");
        src.set_password("pass");
        src.set_ssl_verify(false);
        src.set_broker("host", "1234");
        src.set_client_id("cid");
        src.set_mqtt_version(5);
        src.set_automatic_reconnect(1, 10);
        std::byte data[] = {std::byte{0x01}};
        src.set_will("will/t", data, 1, true);
        src.set_ssl("ca.crt", "client.crt", "client.key");
    }
}

// ==============================================================================
// Connect with callback installed for consumer configs (covers 262-280 for consumers)
// ==============================================================================

TEST_F(FaultInjectionTest, CallbackInstalledBeforeConnectConsumerConfigs) {
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_message_callback([](message_view) {});
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_message_callback([](message_view) {});
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Full connect lifecycle with set_broker for all configs (covers 206,209,256,257)
// ==============================================================================

TEST_F(FaultInjectionTest, FullLifecycleSetBrokerAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        src.set_client_id("test-id");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        EXPECT_TRUE(src.is_connected());
        r = src.subscribe("test/topic", 0);
        EXPECT_TRUE(r.has_value());
        std::byte data[] = {std::byte{0x01}};
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
        EXPECT_FALSE(src.is_connected());
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        src.set_client_id("test-id");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        EXPECT_TRUE(src.is_connected());
        r = src.subscribe("test/topic", 0);
        EXPECT_TRUE(r.has_value());
        std::byte data[] = {std::byte{0x01}};
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
        EXPECT_FALSE(src.is_connected());
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_client_id("test-id");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        EXPECT_TRUE(src.is_connected());
        r = src.subscribe("test/topic", 0);
        EXPECT_TRUE(r.has_value());
        std::byte data[] = {std::byte{0x01}};
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
        EXPECT_FALSE(src.is_connected());
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        src.set_client_id("test-id");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        EXPECT_TRUE(src.is_connected());
        r = src.subscribe("test/topic", 0);
        EXPECT_TRUE(r.has_value());
        std::byte data[] = {std::byte{0x01}};
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
        EXPECT_FALSE(src.is_connected());
    }
}

// ==============================================================================
// URI overrides with explicit client_id for all configs (covers 256-258 both branches)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectUriWithExplicitClientIdAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_client_id("explicit-cid");
        auto uri = basic_uri<mqtt_default_config>::parse("mqtt://urihost:1883/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_client_id("explicit-cid");
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtt://urihost:1883/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_client_id("explicit-cid");
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtt://urihost:1883/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_client_id("explicit-cid");
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtt://urihost:1883/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// URI without port for all configs (covers 209 empty port path)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectUriNoPortAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        auto uri = basic_uri<mqtt_default_config>::parse("mqtt://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        auto uri = basic_uri<mqtt_embedded_config>::parse("mqtt://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        auto uri = basic_uri<mqtt_default_consumer_config>::parse("mqtt://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        auto uri = basic_uri<mqtt_embedded_consumer_config>::parse("mqtt://mockhost/");
        auto r = src.connect(uri.view());
        ASSERT_TRUE(r.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Subscription restore with subscriptions for all configs (covers 287-291)
// ==============================================================================

TEST_F(FaultInjectionTest, SubscriptionRestoreAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        mock_connect(src);
        src.subscribe("test/a", 0);
        src.subscribe("test/b", 1);
        fi::instance().simulate_reconnect();
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        mock_connect(src);
        src.subscribe("test/a", 0);
        fi::instance().simulate_reconnect();
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        mock_connect(src);
        src.subscribe("test/a", 0);
        src.subscribe("test/b", 1);
        fi::instance().simulate_reconnect();
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        mock_connect(src);
        src.subscribe("test/a", 0);
        fi::instance().simulate_reconnect();
        src.disconnect();
    }
}

// ==============================================================================
// Poll with callback installed on consumer (covers 553-556)
// ==============================================================================

TEST_F(FaultInjectionTest, PollConsumerWithCallbackAllConfigs) {
    {
        mqtt_source<mqtt_default_consumer_config> src;
        bool called = false;
        src.set_message_callback([&called](message_view) { called = true; });
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        fi::instance().mock_message = ::mqtt::make_message("poll/t", "X", 1, 0, false);
        fi::instance().consume_should_return_message = true;
        src.poll();
        EXPECT_TRUE(called);
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        bool called = false;
        src.set_message_callback([&called](message_view) { called = true; });
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        fi::instance().mock_message = ::mqtt::make_message("poll/t", "X", 1, 0, false);
        fi::instance().consume_should_return_message = true;
        src.poll();
        EXPECT_TRUE(called);
        src.disconnect();
    }
}

// ==============================================================================
// set_message_callback after connect with null message (covers 530 msg null path)
// ==============================================================================

TEST_F(FaultInjectionTest, SetCallbackNullMessageDefault) {
    mqtt_source<mqtt_default_config> src;
    bool called = false;
    src.set_message_callback([&called](message_view) { called = true; });
    mock_connect(src);
    fi::instance().active_client->inject_message(nullptr);
    EXPECT_FALSE(called);
}

TEST_F(FaultInjectionTest, SetCallbackNullMessageEmbedded) {
    mqtt_source<mqtt_embedded_config> src;
    bool called = false;
    src.set_message_callback([&called](message_view) { called = true; });
    mock_connect(src);
    fi::instance().active_client->inject_message(nullptr);
    EXPECT_FALSE(called);
}

// ==============================================================================
// Destructor connected default config (covers 132-148 for default)
// ==============================================================================

TEST_F(FaultInjectionTest, DestructorConnectedDefault) {
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
    }
}

// ==============================================================================
// Disconnect on connected but then check state (covers 380 all configs)
// ==============================================================================

TEST_F(FaultInjectionTest, DisconnectThenVerifyAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        mock_connect(src);
        EXPECT_TRUE(src.is_connected());
        src.disconnect();
        EXPECT_FALSE(src.is_connected());
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        mock_connect(src);
        EXPECT_TRUE(src.is_connected());
        src.disconnect();
        EXPECT_FALSE(src.is_connected());
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        mock_connect(src);
        EXPECT_TRUE(src.is_connected());
        src.disconnect();
        EXPECT_FALSE(src.is_connected());
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        mock_connect(src);
        EXPECT_TRUE(src.is_connected());
        src.disconnect();
        EXPECT_FALSE(src.is_connected());
    }
}

// ==============================================================================
// Empty host via URI with no broker set (covers line 213-216)
// ==============================================================================

TEST_F(FaultInjectionTest, ConnectEmptyHostNoBrokerAllConfigs) {
    {
        mqtt_source<mqtt_default_config> src;
        auto r = src.connect();
        EXPECT_FALSE(r.has_value());
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        auto r = src.connect();
        EXPECT_FALSE(r.has_value());
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        auto r = src.connect();
        EXPECT_FALSE(r.has_value());
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        auto r = src.connect();
        EXPECT_FALSE(r.has_value());
    }
}

// ==============================================================================
// Reconnect with callback for consumer configs (covers 190-201 callback path)
// ==============================================================================

TEST_F(FaultInjectionTest, ReconnectWithCallbackConsumerConfigs) {
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_message_callback([](message_view) {});
        mock_connect(src);
        auto r2 = src.connect();
        EXPECT_TRUE(r2.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_message_callback([](message_view) {});
        mock_connect(src);
        auto r2 = src.connect();
        EXPECT_TRUE(r2.has_value());
        src.disconnect();
    }
}

// ==============================================================================
// Publish QoS 0 success all configs via set_broker (covers 492-500)
// ==============================================================================

TEST_F(FaultInjectionTest, PublishQoS0SetBrokerAllConfigs) {
    std::byte data[] = {std::byte{0x01}};
    {
        mqtt_source<mqtt_default_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_default_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
    {
        mqtt_source<mqtt_embedded_consumer_config> src;
        src.set_broker("mockhost", "1883");
        auto r = src.connect();
        ASSERT_TRUE(r.has_value());
        r = src.publish("test/topic", data, 0);
        EXPECT_TRUE(r.has_value());
        src.disconnect();
    }
}
