#pragma once

#include <cstdint>

namespace pipepp::mqtt {

enum class mqtt_error_code : uint8_t {
    none = 0,
    protocol_error = 1,
    invalid_client_id = 2,
    broker_unavailable = 3,
    auth_failed = 4,
    qos_not_supported = 5,
    payload_too_large = 6,
    will_invalid = 7,
    ssl_handshake_failed = 8,
};

inline constexpr const char* mqtt_error_message(mqtt_error_code ec) noexcept {
    switch (ec) {
    case mqtt_error_code::none:               return "no mqtt error";
    case mqtt_error_code::protocol_error:     return "mqtt protocol error";
    case mqtt_error_code::invalid_client_id:  return "invalid mqtt client id";
    case mqtt_error_code::broker_unavailable: return "mqtt broker unavailable";
    case mqtt_error_code::auth_failed:        return "mqtt authentication failed";
    case mqtt_error_code::qos_not_supported:  return "mqtt qos not supported";
    case mqtt_error_code::payload_too_large:  return "mqtt payload too large";
    case mqtt_error_code::will_invalid:       return "mqtt will message invalid";
    case mqtt_error_code::ssl_handshake_failed: return "mqtt ssl handshake failed";
    default:                                  return "unknown mqtt error";
    }
}

} // namespace pipepp::mqtt
