#pragma once

#include <cstddef>
#include <memory>
#include <span>
#include <string_view>

#include <pipepp/core/concepts.hpp>
#include <pipepp/core/config.hpp>
#include <pipepp/core/error_code.hpp>
#include <pipepp/core/expected.hpp>
#include <pipepp/core/message.hpp>
#include <pipepp/core/message_callback.hpp>
#include <pipepp/core/uri.hpp>

#include "mqtt_config.hpp"

namespace pipepp::mqtt {


template<typename Config>
struct mqtt_impl;

template<typename Config = mqtt_default_config>
class mqtt_source {
public:
    mqtt_source();
    ~mqtt_source();

    mqtt_source(const mqtt_source&) = delete;
    mqtt_source& operator=(const mqtt_source&) = delete;

    mqtt_source(mqtt_source&& other) noexcept;
    mqtt_source& operator=(mqtt_source&& other) noexcept;

    pipepp::core::result connect(pipepp::core::uri_view uri = {});
    pipepp::core::result disconnect();
    bool is_connected() const;
    pipepp::core::result subscribe(std::string_view topic, int qos);
    pipepp::core::result publish(std::string_view topic, std::span<const std::byte> payload, int qos);
    void set_message_callback(pipepp::core::message_callback<Config> cb);
    void poll();

    void set_client_id(std::string_view id);
    void set_keepalive(int seconds);
    void set_clean_session(bool clean);
    void set_automatic_reconnect(int min_s, int max_s);
    void set_will(std::string_view topic, std::span<const std::byte> payload, int qos, bool retained);
    void set_username(std::string_view user);
    void set_password(std::string_view pass);
    void set_ssl(std::string_view trust_store, std::string_view key_store, std::string_view private_key);
    void set_ssl_verify(bool enable);
    void set_mqtt_version(int version);
    void set_broker(std::string_view host, std::string_view port);

private:
    std::unique_ptr<mqtt_impl<Config>> impl_;
};

static_assert(pipepp::core::BusSource<mqtt_source<mqtt_default_config>, mqtt_default_config>,
              "mqtt_source<mqtt_default_config> must satisfy BusSource concept");
static_assert(pipepp::core::BusSource<mqtt_source<mqtt_embedded_config>, mqtt_embedded_config>,
              "mqtt_source<mqtt_embedded_config> must satisfy BusSource concept");
static_assert(pipepp::core::BusSource<mqtt_source<mqtt_default_consumer_config>, mqtt_default_consumer_config>,
              "mqtt_source<mqtt_default_consumer_config> must satisfy BusSource concept");
static_assert(pipepp::core::BusSource<mqtt_source<mqtt_embedded_consumer_config>, mqtt_embedded_consumer_config>,
              "mqtt_source<mqtt_embedded_consumer_config> must satisfy BusSource concept");

} // namespace pipepp::mqtt
