#pragma once

#include <pipepp/mqtt/mqtt_source.hpp>
#include <pipepp/core/uri.hpp>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>

#ifndef PIPEPP_MQTT_TEST_BROKER
#define PIPEPP_MQTT_TEST_BROKER "localhost"
#endif
#ifndef PIPEPP_MQTT_TEST_PORT
#define PIPEPP_MQTT_TEST_PORT 1883
#endif
#ifndef PIPEPP_MQTT_TEST_TLS_PORT
#define PIPEPP_MQTT_TEST_TLS_PORT 8883
#endif
#ifndef PIPEPP_MQTT_TEST_CA_CERT
#define PIPEPP_MQTT_TEST_CA_CERT "/etc/mosquitto/certs/ca.crt"
#endif

using namespace pipepp::mqtt;
using namespace pipepp::core;

namespace pipepp_test {

inline std::string broker_uri() {
    return "mqtt://" PIPEPP_MQTT_TEST_BROKER ":" +
           std::to_string(PIPEPP_MQTT_TEST_PORT);
}

inline std::string tls_broker_uri() {
    return "mqtts://" PIPEPP_MQTT_TEST_BROKER ":" +
           std::to_string(PIPEPP_MQTT_TEST_TLS_PORT);
}

inline std::string unique_topic(const char* prefix, const char* suffix) {
    return std::string(prefix) +
           std::to_string(::getpid()) + "/" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
           "/" + suffix;
}

inline bool broker_is_running() {
    return system("pgrep -x mosquitto > /dev/null 2>&1") == 0;
}

inline void wait_for_broker(int max_wait_secs = 5) {
    for (int i = 0; i < max_wait_secs * 10 && !broker_is_running(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

inline bool broker_port_open() {
    return system("bash -c 'echo > /dev/tcp/localhost/1883' 2>/dev/null") == 0;
}

inline void start_broker() {
    if (broker_is_running()) {
        if (broker_port_open()) return;
        system("pkill -x mosquitto 2>/dev/null || true");
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    system("mosquitto -c /etc/mosquitto/mosquitto.conf -d 2>/dev/null || true");
    wait_for_broker(5);
    for (int i = 0; i < 30 && !broker_port_open(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

inline void stop_broker() {
    system("pkill -x mosquitto 2>/dev/null || true");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (int i = 0; i < 30 && broker_is_running(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

inline void wait_for_disconnect(mqtt_source<mqtt_default_config>& src, int max_wait_secs = 5) {
    for (int i = 0; i < max_wait_secs * 10 && src.is_connected(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

inline void wait_for_disconnect(mqtt_source<mqtt_default_consumer_config>& src, int max_wait_secs = 5) {
    for (int i = 0; i < max_wait_secs * 10 && src.is_connected(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

inline void wait_for_disconnect(mqtt_source<mqtt_embedded_config>& src, int max_wait_secs = 5) {
    for (int i = 0; i < max_wait_secs * 10 && src.is_connected(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

inline void wait_for_disconnect(mqtt_source<mqtt_embedded_consumer_config>& src, int max_wait_secs = 5) {
    for (int i = 0; i < max_wait_secs * 10 && src.is_connected(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

template<typename Config>
inline bool try_connect(mqtt_source<Config>& src, const char* prefix = "tc") {
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id((std::string(prefix) + "-" + uid).c_str());
    auto uri = basic_uri<Config>::parse(broker_uri());
    auto r = src.connect(uri.view());
    return r.has_value();
}

template<typename Config>
inline bool try_connect_with_retry(mqtt_source<Config>& src, const char* prefix = "tc", int retries = 3) {
    for (int i = 0; i < retries; ++i) {
        start_broker();
        if (try_connect(src, prefix)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return try_connect(src, prefix);
}

template<typename Config>
inline bool try_tls_connect(mqtt_source<Config>& src,
                            const char* prefix = "tls",
                            const char* user = "pipepp_test",
                            const char* pass = "pipepp_test_pw") {
    auto uid = std::to_string(reinterpret_cast<uintptr_t>(&src));
    src.set_client_id((std::string(prefix) + "-" + uid).c_str());
    src.set_username(user);
    src.set_password(pass);
    src.set_ssl(PIPEPP_MQTT_TEST_CA_CERT, "", "");
    auto uri = basic_uri<Config>::parse(tls_broker_uri());
    auto r = src.connect(uri.view());
    return r.has_value();
}

inline bool wait_for_message(std::atomic<bool>& flag, int max_wait_ms = 5000) {
    for (int i = 0; i < max_wait_ms / 100 && !flag.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return flag.load();
}

inline bool wait_for_count(std::atomic<int>& count, int target, int max_wait_ms = 5000) {
    for (int i = 0; i < max_wait_ms / 100 && count.load() < target; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return count.load() >= target;
}

} // namespace pipepp_test
