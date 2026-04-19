# pipepp-mqtt

MQTT transport module for [pipepp](https://github.com/aschokinatgmail/pipepp-core), a C++20 publish-subscribe pipeline library. It wraps [Eclipse Paho MQTT C++](https://github.com/eclipse/paho.mqtt.cpp) to provide `mqtt_source<Config>` -- a type satisfying pipepp-core's `BusSource` concept, enabling MQTT as a first-class message bus in any pipepp pipeline.

## Quick Start

### CMake Integration

```cmake
# Option A: find_package (recommended for installed dependencies)
find_package(pipepp-core REQUIRED)
find_package(pipepp-mqtt REQUIRED)

target_link_libraries(my_app PRIVATE pipepp::mqtt)

# Option B: add_subdirectory with pipepp-core path
set(PIPEPP_CORE_DIR /path/to/pipepp-core)
add_subdirectory(/path/to/pipepp-mqtt)

target_link_libraries(my_app PRIVATE pipepp::mqtt)
```

### Minimal Publish/Subscribe

```cpp
#include <pipepp/mqtt/mqtt.hpp>
#include <pipepp/core/uri.hpp>

using namespace pipepp::mqtt;
using namespace pipepp::core;

int main() {
    mqtt_source<mqtt_default_config> pub;
    mqtt_source<mqtt_default_config> sub;

    sub.set_message_callback([](const message_view& mv) {
        // mv.topic    - std::string_view
        // mv.payload  - std::span<const std::byte>
        // mv.qos      - uint8_t
    });

    auto uri = basic_uri<mqtt_default_config>::parse("mqtt://broker.example.com:1883");
    pub.connect(uri.view());
    sub.connect(uri.view());

    sub.subscribe("sensors/temperature", 0);

    const char* msg = "22.5C";
    auto payload = std::as_bytes(std::span{msg, 5});
    pub.publish("sensors/temperature", payload, 0);

    // ... later
    pub.disconnect();
    sub.disconnect();
}
```

## API Reference

All public symbols are in the `pipepp::mqtt` namespace. The single convenience header is:

```cpp
#include <pipepp/mqtt/mqtt.hpp>
```

Individual headers: `mqtt_source.hpp`, `mqtt_config.hpp`, `mqtt_registry.hpp`, `mqtt_error.hpp`.

### mqtt_source\<Config\>

A template class satisfying `pipepp::core::BusSource<Config>`. The default template argument is `mqtt_default_config`.

```cpp
template<typename Config = mqtt_default_config>
class mqtt_source;
```

#### Construction and Lifetime

| Expression | Description |
|---|---|
| `mqtt_source<Config> src;` | Default construct. Not connected. |
| `src.~mqtt_source()` | Disconnects if connected, releases Paho client. |
| `mqtt_source<Config> other(std::move(src));` | Move construct. Source is left in a valid-but-empty state. |
| `src = std::move(other)` | Move assign. Disconnects current client first. |

Copy construction and copy assignment are deleted.

#### Connection

| Method | Signature | Description |
|---|---|---|
| `connect` | `result connect(uri_view uri = {});` | Connect to broker. Parses scheme (`mqtt`/`mqtts`), host, port, userinfo, and query parameters. Returns error on failure. |
| `disconnect` | `result disconnect();` | Disconnect from broker with 5-second timeout. |
| `is_connected` | `bool is_connected() const;` | Returns current connection state. |

#### Messaging

| Method | Signature | Description |
|---|---|---|
| `subscribe` | `result subscribe(std::string_view topic, int qos);` | Subscribe to topic. QoS must be 0--2. Returns error if not connected or capacity exceeded. |
| `publish` | `result publish(std::string_view topic, std::span<const std::byte> payload, int qos);` | Publish to topic. QoS must be 0--2. QoS 0 is fire-and-forget; QoS 1/2 block until ack. |
| `set_message_callback` | `void set_message_callback(message_callback<Config> cb);` | Set the message handler. Can be called before or after connect. |
| `poll` | `void poll();` | Drains queued messages in consumer mode. No-op (yields thread) in callback mode. |

#### Pre-connect Configuration (setters)

All setters are no-ops once connected (`is_connected() == true`).

| Method | Signature | Description |
|---|---|---|
| `set_client_id` | `void set_client_id(std::string_view id);` | MQTT client identifier. Auto-generated if empty. |
| `set_keepalive` | `void set_keepalive(int seconds);` | Keep-alive interval. Default: 60s (default) or 30s (embedded). |
| `set_clean_session` | `void set_clean_session(bool clean);` | Clean session flag. Default: `true`. |
| `set_automatic_reconnect` | `void set_automatic_reconnect(int min_s, int max_s);` | Enable auto-reconnect with backoff range. |
| `set_will` | `void set_will(std::string_view topic, std::span<const std::byte> payload, int qos, bool retained);` | Set last-will message. QoS clamped to 0--2. |
| `set_username` | `void set_username(std::string_view user);` | Authentication username. |
| `set_password` | `void set_password(std::string_view pass);` | Authentication password. |
| `set_ssl` | `void set_ssl(std::string_view trust_store, std::string_view key_store, std::string_view private_key);` | Configure TLS. Enables SSL. Pass empty strings for unused parameters. |
| `set_ssl_verify` | `void set_ssl_verify(bool enable);` | Enable/disable server certificate verification. Default: `true`. |
| `set_mqtt_version` | `void set_mqtt_version(int version);` | MQTT protocol version: 3 (3.1), 4 (3.1.1, default), or 5. |
| `set_broker` | `void set_broker(std::string_view host, std::string_view port);` | Set default broker address, used when URI has no host/port. |

### Configuration Types

All configs are defined in `<pipepp/mqtt/mqtt_config.hpp>` and inherit from pipepp-core base configs.

#### mqtt_default_config

Full-featured configuration. Inherits from `pipepp::core::default_config`.

| Field | Type | Value | Description |
|---|---|---|---|
| `poll_mode` | type alias | `mqtt_callback_tag` | Callback-based message delivery |
| `source_size` | `size_t` | 256 | Max `sizeof(mqtt_source)` for static allocation |
| `default_keepalive` | `int` | 60 | Default keep-alive in seconds |
| `default_clean_session` | `bool` | `true` | Default clean session flag |
| `max_client_id_len` | `size_t` | 64 | Max client ID string length |
| `max_broker_addr_len` | `size_t` | 256 | Max broker address string length |

Also inherits from `default_config`: `max_topic_len` (256), `max_payload_len` (4096), `max_subscriptions` (8), etc.

#### mqtt_embedded_config

Resource-constrained configuration. Inherits from `pipepp::core::embedded_config`.

| Field | Type | Value |
|---|---|---|
| `poll_mode` | type alias | `mqtt_callback_tag` |
| `source_size` | `size_t` | 128 |
| `default_keepalive` | `int` | 30 |
| `default_clean_session` | `bool` | `true` |
| `max_client_id_len` | `size_t` | 32 |
| `max_broker_addr_len` | `size_t` | 128 |

Inherits from `embedded_config`: `max_topic_len` (64), `max_payload_len` (512), `max_subscriptions` (4), etc.

#### mqtt_default_consumer_config

Same as `mqtt_default_config` but with consumer poll mode.

```cpp
struct mqtt_default_consumer_config : mqtt_default_config {
    using poll_mode = mqtt_consumer_tag;
};
```

#### mqtt_embedded_consumer_config

Same as `mqtt_embedded_config` but with consumer poll mode.

```cpp
struct mqtt_embedded_consumer_config : mqtt_embedded_config {
    using poll_mode = mqtt_consumer_tag;
};
```

### Poll Mode Selection

The `Config::poll_mode` type alias determines how messages are delivered:

| Mode | Tag | Delivery Mechanism |
|---|---|---|
| **Callback** | `mqtt_callback_tag` | Paho calls the message callback directly from its internal thread. `poll()` is a no-op (yields). |
| **Consumer** | `mqtt_consumer_tag` | Messages are queued internally. You must call `poll()` from your own thread to drain them and invoke the callback. |

See the [Comparison Table](#comparison-callback-mode-vs-consumer-mode) below for details.

### URI Format

The `connect()` method accepts a `pipepp::core::uri_view` with the following structure:

```
mqtt://[user[:password]@]host[:port][/?query-params]
mqtts://[user[:password]@]host[:port][/?query-params]
```

| Component | Description |
|---|---|
| `mqtt://` | Plaintext connection (default port 1883) |
| `mqtts://` | TLS connection (default port 8883, enables SSL) |
| `user:pass@` | Optional authentication. Overrides `set_username`/`set_password`. |
| `host:port` | Broker address. Overrides `set_broker` values. |
| Query params | See below |

**Query Parameters:**

| Parameter | Values | Description |
|---|---|---|
| `keepalive` | integer | Override keep-alive interval (seconds) |
| `clean` | `0`, `1`, `true`, `false` | Override clean session flag |
| `version` | `3`, `4`, `5` | Override MQTT protocol version |
| `clientid` | string | Override client identifier |

Examples:

```cpp
// Plaintext with query params
"mqtt://broker.example.com:1883/?keepalive=30&clean=0&version=5"

// TLS with inline credentials
"mqtts://user:pass@broker.example.com:8883"

// Host-only (port defaults to 1883 for mqtt, 8883 for mqtts)
"mqtt://broker.local"
```

### Source Registry Integration

`register_mqtt()` registers the `mqtt` and `mqtts` URI schemes with a pipepp source registry, allowing the pipeline to create MQTT sources from URI strings.

```cpp
#include <pipepp/mqtt/mqtt_registry.hpp>
#include <pipepp/core/source_registry.hpp>

pipepp::core::source_registry<pipepp::mqtt::mqtt_default_config> registry;
pipepp::mqtt::register_mqtt(registry);

// Creates an mqtt_source<mqtt_default_config>
auto src = registry.create("mqtt://broker:1883");
```

### Error Codes

Defined in `<pipepp/mqtt/mqtt_error.hpp>` as `enum class mqtt_error_code : uint8_t`:

| Value | Name | Description |
|---|---|---|
| 0 | `none` | No error |
| 1 | `protocol_error` | MQTT protocol violation |
| 2 | `invalid_client_id` | Client ID rejected by broker |
| 3 | `broker_unavailable` | Cannot reach broker |
| 4 | `auth_failed` | Authentication failed |
| 5 | `qos_not_supported` | Requested QoS not supported |
| 6 | `payload_too_large` | Payload exceeds limit |
| 7 | `will_invalid` | Last-will message is invalid |
| 8 | `ssl_handshake_failed` | TLS handshake failed |

Use `mqtt_error_message(mqtt_error_code)` to get a human-readable string.

## Usage Examples

### SSL/TLS Connection

```cpp
#include <pipepp/mqtt/mqtt.hpp>
#include <pipepp/core/uri.hpp>

using namespace pipepp::mqtt;
using namespace pipepp::core;

mqtt_source<mqtt_default_config> src;

// Configure TLS
src.set_ssl("/etc/ssl/certs/ca.crt", "", "");
src.set_ssl_verify(false);

// Authenticate
src.set_username("myuser");
src.set_password("mypassword");

auto uri = basic_uri<mqtt_default_config>::parse("mqtts://broker.example.com:8883");
auto r = src.connect(uri.view());
if (!r.has_value()) {
    // handle error
}

src.subscribe("secure/topic", 1);

// ... later
src.disconnect();
```

### Authentication via URI

Credentials can be embedded directly in the URI:

```cpp
src.set_ssl("/etc/ssl/certs/ca.crt", "", "");
src.set_ssl_verify(false);

auto uri = basic_uri<mqtt_default_config>::parse(
    "mqtts://myuser:mypassword@broker.example.com:8883");
src.connect(uri.view());
```

### Consumer Mode (Manual Polling)

Consumer mode gives you full control over which thread processes messages:

```cpp
#include <pipepp/mqtt/mqtt.hpp>

using namespace pipepp::mqtt;

mqtt_source<mqtt_default_consumer_config> src;

src.set_message_callback([](const pipepp::core::message_view& mv) {
    // Called from the thread that calls poll()
});

auto uri = pipepp::core::basic_uri<mqtt_default_consumer_config>::parse(
    "mqtt://broker:1883");
src.connect(uri.view());
src.subscribe("topic", 1);

// Your own event loop
while (running) {
    src.poll();  // Drains all queued messages, invokes callback
    // ... do other work
}

src.disconnect();
```

### Custom Config with Last Will and Auto-Reconnect

```cpp
#include <pipepp/mqtt/mqtt.hpp>

using namespace pipepp::mqtt;
using namespace pipepp::core;

mqtt_source<mqtt_default_config> src;

src.set_client_id("sensor-node-42");
src.set_keepalive(15);
src.set_clean_session(false);
src.set_automatic_reconnect(1, 60);

// Last-will message: published if client disconnects ungracefully
std::byte offline[] = {std::byte{0x00}};
src.set_will("status/sensor-42", offline, 1, true);

auto uri = basic_uri<mqtt_default_config>::parse("mqtt://broker.local");
src.connect(uri.view());
```

### Registry-based Source Creation

```cpp
#include <pipepp/mqtt/mqtt_registry.hpp>
#include <pipepp/core/source_registry.hpp>

using namespace pipepp::mqtt;
using namespace pipepp::core;

source_registry<mqtt_default_config> registry;
register_mqtt(registry);

// Create sources by URI
auto src1 = registry.create("mqtt://broker1:1883");
auto src2 = registry.create("mqtts://broker2:8883");

if (src1) {
    src1->set_message_callback([](const message_view& mv) { /* ... */ });
    src1->connect();
}
```

## Build Instructions

### Dependencies

| Dependency | Required | Notes |
|---|---|---|
| C++20 compiler | Yes | GCC 12+, Clang 15+, MSVC 2022+ |
| CMake 3.20+ | Yes | Build system |
| pipepp-core | Yes | Sibling library |
| Eclipse Paho MQTT C++ | Optional | If not found, stub implementations are compiled |

### CMake Options

| Variable | Type | Default | Description |
|---|---|---|---|
| `PIPEPP_CORE_DIR` | Path | -- | Path to pipepp-core source tree (for `add_subdirectory`) |
| `PIPEPP_MQTT_BUILD_TESTS` | Bool | `OFF` | Build unit and integration tests |
| `PIPEPP_MQTT_ENABLE_COVERAGE` | Bool | `OFF` | Enable code coverage (requires `PIPEPP_MQTT_BUILD_TESTS=ON`) |
| `PIPEPP_MQTT_TEST_BROKER` | String | `bms-logging-server.local` | Broker hostname for integration tests |
| `PIPEPP_MQTT_TEST_PORT` | String | `1883` | Broker plaintext port for tests |
| `PIPEPP_MQTT_TEST_TLS_PORT` | String | `8883` | Broker TLS port for tests |
| `PIPEPP_MQTT_TEST_CA_CERT` | String | `/etc/mosquitto/certs/ca.crt` | CA cert path for TLS tests |

### Build Example

```bash
mkdir build && cd build
cmake .. \
    -DPIPEPP_CORE_DIR=../pipepp-core \
    -DCMAKE_PREFIX_PATH=/usr/local \
    -DPIPEPP_MQTT_BUILD_TESTS=ON
cmake --build .
ctest --output-on-failure
```

### Stub Mode

When Eclipse Paho MQTT C++ is not found, the library compiles in **stub mode** (`PIPEPP_MQTT_STUB=1`). In stub mode, `connect()` always succeeds, `publish()` is a no-op, and `subscribe()` records the subscription but does nothing. This allows building and testing pipepp pipelines without a live MQTT stack.

### Compiler Flags

The library builds with `-fno-rtti` (or `/GR-` on MSVC) and `-fno-exceptions` globally, with exceptions enabled only for `mqtt_source.cpp` (required by Paho). Link with `-fno-rtti` compatible code.

## Thread Safety

- `mqtt_source` is **not** shared-thread-safe for concurrent calls. Each instance should be accessed from one thread at a time.
- In **callback mode**, the message callback is invoked from Paho's internal thread. The callback itself is protected by an internal mutex. You must synchronize access to any shared state in your callback.
- In **consumer mode**, `poll()` drains messages and invokes the callback from the calling thread. No cross-thread callback invocation occurs.
- `is_connected()` is thread-safe (uses atomic operations).
- `set_message_callback()` is thread-safe (uses an internal mutex).
- Move operations disconnect the old client, which joins Paho's background threads. Do not move while another thread is calling methods on the same instance.
- All configuration setters (`set_client_id`, `set_keepalive`, etc.) are no-ops once connected, so there is no race between configuration and connection.

## Comparison: Callback Mode vs Consumer Mode

| Aspect | Callback Mode (`mqtt_callback_tag`) | Consumer Mode (`mqtt_consumer_tag`) |
|---|---|---|
| Config types | `mqtt_default_config`, `mqtt_embedded_config` | `mqtt_default_consumer_config`, `mqtt_embedded_consumer_config` |
| Message delivery | Paho background thread invokes callback directly | Messages queued by Paho; `poll()` drains and invokes callback |
| `poll()` behavior | No-op (`std::this_thread::yield()`) | Drains all queued messages from Paho's consumer queue |
| Thread ownership | Callback runs on Paho's thread | Callback runs on your thread (the one calling `poll()`) |
| Latency | Lower (immediate delivery) | Depends on poll frequency |
| Synchronization | Must protect shared state in callback | No cross-thread callback; simpler synchronization |
| Use case | Event-driven, low-latency applications | Game loops, embedded main loops, controlled threading |
| Paho internals | `set_message_callback` on client | `start_consuming()`/`stop_consuming()` on client |

## Project Structure

```
pipepp-mqtt/
  include/pipepp/mqtt/
    mqtt.hpp              # Convenience header (includes all below)
    mqtt_source.hpp       # mqtt_source<Config> class
    mqtt_config.hpp       # Config types and poll mode tags
    mqtt_registry.hpp     # register_mqtt() free function
    mqtt_error.hpp        # mqtt_error_code enum
  src/
    mqtt_source.cpp       # Implementation (Paho-backed or stub)
  test/
    unit/                 # Unit tests (no broker required)
    integration/          # Integration tests (require live broker)
  docker/                 # Docker files for test broker with TLS/auth
```

## License

MIT License. See [LICENSE](LICENSE).
