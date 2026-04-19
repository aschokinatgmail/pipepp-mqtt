#pragma once

#include <cstddef>

#include <pipepp/core/config.hpp>
#include <pipepp/core/fixed_string.hpp>

#include "mqtt_config.hpp"

namespace pipepp::mqtt {

template<typename Config = mqtt_default_config>
struct mqtt_connect_options {
    pipepp::core::fixed_string<Config::max_client_id_len> client_id{};
    int keepalive = Config::default_keepalive;
    bool clean_session = Config::default_clean_session;
    bool auto_reconnect = false;
    int reconnect_min_s = 1;
    int reconnect_max_s = 30;
    int mqtt_version = 4;
    pipepp::core::fixed_string<Config::max_broker_addr_len> username{};
    pipepp::core::fixed_string<Config::max_broker_addr_len> password{};
};

template<typename Config = mqtt_default_config>
struct mqtt_will_options {
    pipepp::core::fixed_string<Config::max_topic_len> topic{};
    std::byte payload[Config::max_payload_len]{};
    std::size_t payload_len = 0;
    int qos = 0;
    bool retained = false;
};

template<typename Config = mqtt_default_config>
struct mqtt_ssl_options {
    pipepp::core::fixed_string<Config::max_broker_addr_len> trust_store{};
    pipepp::core::fixed_string<Config::max_broker_addr_len> key_store{};
    pipepp::core::fixed_string<Config::max_broker_addr_len> private_key{};
    bool verify = true;
};

} // namespace pipepp::mqtt
