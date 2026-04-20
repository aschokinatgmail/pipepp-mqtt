#pragma once

#include <cstddef>
#include <cstdio>
#include <string_view>

#include <pipepp/core/pipeline.hpp>

#include "mqtt_config.hpp"
#include "mqtt_source.hpp"

namespace pipepp::mqtt {

struct mqtt_endpoint {
    const char* host = "localhost";
    int port = 1883;
};

template<typename Config = mqtt_default_config>
pipepp::core::basic_pipeline<mqtt_source<Config>, Config>
pipeline(const mqtt_endpoint& endpoint)
{
    mqtt_source<Config> src;
    char uri[Config::max_uri_len + 1];
    auto n = std::snprintf(uri, sizeof(uri), "mqtt://%s:%d",
                           endpoint.host, endpoint.port);
    auto pipe = pipepp::core::pipeline(std::move(src), Config{});
    pipe.connect_uri(std::string_view(uri, static_cast<std::size_t>(n)));
    return pipe;
}

template<typename Config = mqtt_default_config>
mqtt_source<Config>
source(const mqtt_endpoint& endpoint)
{
    mqtt_source<Config> src;
    char port_buf[16];
    std::snprintf(port_buf, sizeof(port_buf), "%d", endpoint.port);
    src.set_broker(endpoint.host, port_buf);
    return src;
}

} // namespace pipepp::mqtt
