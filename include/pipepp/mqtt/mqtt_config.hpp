#pragma once

#include <cstddef>

#include <pipepp/core/config.hpp>

namespace pipepp::mqtt {

struct mqtt_callback_tag {};
struct mqtt_consumer_tag {};

struct mqtt_default_config : pipepp::core::default_config {
    using poll_mode = mqtt_callback_tag;
    static constexpr std::size_t source_size = 256;
    static constexpr int default_keepalive = 60;
    static constexpr bool default_clean_session = true;
    static constexpr std::size_t max_client_id_len = 64;
    static constexpr std::size_t max_broker_addr_len = 256;
};

struct mqtt_embedded_config : pipepp::core::embedded_config {
    using poll_mode = mqtt_callback_tag;
    static constexpr std::size_t source_size = 128;
    static constexpr int default_keepalive = 30;
    static constexpr bool default_clean_session = true;
    static constexpr std::size_t max_client_id_len = 32;
    static constexpr std::size_t max_broker_addr_len = 128;
};

} // namespace pipepp::mqtt
