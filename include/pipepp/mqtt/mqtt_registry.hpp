#pragma once

#include <pipepp/core/source_registry.hpp>

#include "mqtt_source.hpp"

namespace pipepp::mqtt {

template<typename Config = mqtt_default_config>
void register_mqtt(pipepp::core::source_registry<Config>& registry) {
    registry.template register_scheme<mqtt_source<Config>>("mqtt");
    registry.template register_scheme<mqtt_source<Config>>("mqtts");
}

} // namespace pipepp::mqtt
