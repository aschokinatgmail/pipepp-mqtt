#include <pipepp/mqtt/mqtt_source.hpp>

#include <cstring>
#include <cstdlib>
#include <thread>
#include <utility>

#ifdef PIPEPP_MQTT_HAS_PAHO_CPP
#include <mqtt/async_client.h>
#include <mqtt/connect_options.h>
#include <mqtt/message.h>
#include <mqtt/ssl_options.h>
#include <mqtt/will_options.h>

namespace paho = ::mqtt;
#endif

namespace pipepp::mqtt {

namespace {

std::string_view query_param(std::string_view query, std::string_view key) {
    auto pos = query.find(key);
    if (pos == std::string_view::npos) return {};
    auto val_start = pos + key.size();
    if (val_start >= query.size() || query[val_start] != '=') return {};
    ++val_start;
    auto val_end = query.find('&', val_start);
    return query.substr(val_start, val_end == std::string_view::npos ? std::string_view::npos : val_end - val_start);
}

}

template<typename Config>
struct mqtt_impl {
    mqtt_impl() = default;
    ~mqtt_impl() = default;
    mqtt_impl(const mqtt_impl&) = delete;
    mqtt_impl& operator=(const mqtt_impl&) = delete;
    mqtt_impl(mqtt_impl&&) = default;
    mqtt_impl& operator=(mqtt_impl&&) = default;

#ifdef PIPEPP_MQTT_HAS_PAHO_CPP
    paho::async_client* client = nullptr;
#endif
    message_callback<Config> callback;
    fixed_string<Config::max_client_id_len> client_id{};
    fixed_string<Config::max_broker_addr_len> broker_host{};
    fixed_string<Config::max_broker_addr_len> broker_port{"1883"};
    int keepalive = Config::default_keepalive;
    bool clean_session = Config::default_clean_session;
    bool auto_reconnect = false;
    int reconnect_min_s = 1;
    int reconnect_max_s = 30;
    int mqtt_version = 4;
    fixed_string<Config::max_broker_addr_len> username{};
    fixed_string<Config::max_broker_addr_len> password{};
    bool ssl_enabled = false;
    fixed_string<Config::max_broker_addr_len> ssl_trust_store{};
    fixed_string<Config::max_broker_addr_len> ssl_key_store{};
    fixed_string<Config::max_broker_addr_len> ssl_private_key{};
    bool ssl_verify = true;
    fixed_string<Config::max_topic_len> will_topic{};
    std::byte will_payload[Config::max_payload_len]{};
    std::size_t will_payload_len = 0;
    int will_qos = 0;
    bool will_retained = false;
    bool has_will = false;

    static constexpr std::size_t max_subscriptions = Config::max_subscriptions;
    struct sub_entry {
        fixed_string<Config::max_topic_len> topic{};
        int qos = 0;
    };
    sub_entry subscriptions[max_subscriptions]{};
    std::size_t sub_count = 0;

    void add_subscription(std::string_view topic, int qos) {
        if (sub_count < max_subscriptions) {
            subscriptions[sub_count].topic.checked_from(topic);
            subscriptions[sub_count].qos = qos;
            ++sub_count;
        }
    }
};

template<typename Config>
mqtt_source<Config>::mqtt_source()
    : impl_(new mqtt_impl<Config>{}) {}

template<typename Config>
mqtt_source<Config>::~mqtt_source() {
    if (connected_) {
        disconnect();
    }
    auto* p = static_cast<mqtt_impl<Config>*>(impl_);
#ifdef PIPEPP_MQTT_HAS_PAHO_CPP
    if (p && p->client) {
        delete p->client;
    }
#endif
    delete p;
    impl_ = nullptr;
}

template<typename Config>
mqtt_source<Config>::mqtt_source(mqtt_source&& other) noexcept
    : impl_(other.impl_)
    , connected_(other.connected_) {
    other.impl_ = nullptr;
    other.connected_ = false;
}

template<typename Config>
mqtt_source<Config>& mqtt_source<Config>::operator=(mqtt_source&& other) noexcept {
    if (this != &other) {
        if (connected_) disconnect();
        auto* p = static_cast<mqtt_impl<Config>*>(impl_);
#ifdef PIPEPP_MQTT_HAS_PAHO_CPP
        if (p && p->client) delete p->client;
#endif
        delete p;
        impl_ = other.impl_;
        connected_ = other.connected_;
        other.impl_ = nullptr;
        other.connected_ = false;
    }
    return *this;
}

template<typename Config>
pipepp::core::result mqtt_source<Config>::connect(pipepp::core::uri_view uri) {
#ifdef PIPEPP_MQTT_HAS_PAHO_CPP
    auto* s = static_cast<mqtt_impl<Config>*>(impl_);
    try {
        std::string host = uri.host.empty()
            ? std::string(static_cast<std::string_view>(s->broker_host))
            : std::string(uri.host);
        std::string port = uri.port.empty()
            ? std::string(static_cast<std::string_view>(s->broker_port))
            : std::string(uri.port);

        if (uri.scheme == "mqtts" || uri.scheme == "ssl" || uri.scheme == "tls") {
            s->ssl_enabled = true;
        }

        if (port.empty()) {
            port = s->ssl_enabled ? "8883" : "1883";
        }

        std::string proto = s->ssl_enabled ? "ssl://" : "tcp://";
        std::string server_uri = proto + host + ":" + port;

        if (!uri.userinfo().empty()) {
            auto info = uri.userinfo();
            auto colon = info.find(':');
            if (colon != std::string_view::npos) {
                s->username.checked_from(info.substr(0, colon));
                s->password.checked_from(info.substr(colon + 1));
            } else {
                s->username.checked_from(info);
            }
        }

        int keepalive = s->keepalive;
        bool clean_session = s->clean_session;
        int mqtt_version = s->mqtt_version;

        if (!uri.query.empty()) {
            if (auto v = query_param(uri.query, "keepalive"); !v.empty())
                keepalive = std::atoi(v.data());
            if (auto v = query_param(uri.query, "clean"); !v.empty())
                clean_session = (v == "1" || v == "true");
            if (auto v = query_param(uri.query, "version"); !v.empty())
                mqtt_version = std::atoi(v.data());
            if (auto v = query_param(uri.query, "clientid"); !v.empty())
                s->client_id.checked_from(v);
        }

        std::string cid = s->client_id.empty()
            ? std::string("pipepp-") + std::to_string(reinterpret_cast<std::uintptr_t>(this))
            : std::string(static_cast<std::string_view>(s->client_id));

        auto* cli = new paho::async_client(server_uri, cid);
        s->client = cli;

        auto conn_opts = paho::connect_options_builder()
            .keep_alive_interval(std::chrono::seconds(keepalive))
            .clean_session(clean_session)
            .automatic_reconnect(std::chrono::seconds(s->reconnect_min_s),
                                 std::chrono::seconds(s->reconnect_max_s))
            .finalize();

        if (mqtt_version == 5) {
            conn_opts.set_mqtt_version(5);
        } else if (mqtt_version == 3) {
            conn_opts.set_mqtt_version(3);
        } else {
            conn_opts.set_mqtt_version(4);
        }

        if (!s->username.empty()) {
            auto u = std::string(static_cast<std::string_view>(s->username));
            conn_opts.set_user_name(paho::string_ref(u.data(), u.size()));
        }
        if (!s->password.empty()) {
            auto pw = std::string(static_cast<std::string_view>(s->password));
            conn_opts.set_password(paho::binary_ref(pw.data(), pw.size()));
        }

        if (s->has_will) {
            auto will_msg = paho::make_message(
                std::string(static_cast<std::string_view>(s->will_topic)),
                s->will_payload, s->will_payload_len,
                s->will_qos, s->will_retained);
            conn_opts.set_will_message(will_msg);
        }

        if (s->ssl_enabled) {
            paho::ssl_options ssl_opts;
            if (!s->ssl_trust_store.empty()) {
                auto ts = std::string(static_cast<std::string_view>(s->ssl_trust_store));
                ssl_opts.set_trust_store(ts);
            }
            if (!s->ssl_key_store.empty()) {
                auto ks = std::string(static_cast<std::string_view>(s->ssl_key_store));
                ssl_opts.set_key_store(ks);
            }
            if (!s->ssl_private_key.empty()) {
                auto pk = std::string(static_cast<std::string_view>(s->ssl_private_key));
                ssl_opts.set_private_key(pk);
            }
            ssl_opts.set_enable_server_cert_auth(s->ssl_verify);
            conn_opts.set_ssl(std::move(ssl_opts));
        }

        auto* s_ptr = s;
        auto* connected_flag = &connected_;
        cli->set_connected_handler([s_ptr, connected_flag](const std::string&) {
            *connected_flag = true;
            for (std::size_t i = 0; i < s_ptr->sub_count; ++i) {
                try {
                    s_ptr->client->subscribe(
                        std::string(static_cast<std::string_view>(s_ptr->subscriptions[i].topic)),
                        s_ptr->subscriptions[i].qos);
                } catch (...) {}
            }
        });

        if constexpr (std::is_same_v<typename Config::poll_mode, mqtt_consumer_tag>) {
            cli->start_consuming();
        }

        cli->connect(conn_opts)->wait_for(std::chrono::seconds(10));
        connected_ = true;
        return {};
    } catch (const paho::exception&) {
        return pipepp::core::result{
            pipepp::core::unexpect,
            pipepp::core::make_unexpected(pipepp::core::error_code::connection_failed)};
    } catch (...) {
        return pipepp::core::result{
            pipepp::core::unexpect,
            pipepp::core::make_unexpected(pipepp::core::error_code::connection_failed)};
    }
#else
    connected_ = true;
    return {};
#endif
}

template<typename Config>
pipepp::core::result mqtt_source<Config>::disconnect() {
#ifdef PIPEPP_MQTT_HAS_PAHO_CPP
    auto* s = static_cast<mqtt_impl<Config>*>(impl_);
    if (s && s->client && connected_) {
        try {
            if constexpr (std::is_same_v<typename Config::poll_mode, mqtt_consumer_tag>) {
                s->client->stop_consuming();
            }
            s->client->disconnect(std::chrono::seconds(5))->wait_for(std::chrono::seconds(10));
        } catch (...) {}
    }
#endif
    connected_ = false;
    return {};
}

template<typename Config>
bool mqtt_source<Config>::is_connected() const {
#ifdef PIPEPP_MQTT_HAS_PAHO_CPP
    auto* s = static_cast<mqtt_impl<Config>*>(impl_);
    if (s && s->client) {
        try {
            return s->client->is_connected();
        } catch (...) {
            return false;
        }
    }
#endif
    return connected_;
}

template<typename Config>
pipepp::core::result mqtt_source<Config>::subscribe(std::string_view topic, int qos) {
    if (qos < 0 || qos > 2) {
        return pipepp::core::result{
            pipepp::core::unexpect,
            pipepp::core::make_unexpected(pipepp::core::error_code::invalid_argument)};
    }
#ifdef PIPEPP_MQTT_HAS_PAHO_CPP
    if (!connected_) {
        return pipepp::core::result{
            pipepp::core::unexpect,
            pipepp::core::make_unexpected(pipepp::core::error_code::not_connected)};
    }
    auto* s = static_cast<mqtt_impl<Config>*>(impl_);
    try {
        s->client->subscribe(std::string(topic), qos)->wait_for(std::chrono::seconds(5));
        s->add_subscription(topic, qos);
        return {};
    } catch (const paho::exception&) {
        return pipepp::core::result{
            pipepp::core::unexpect,
            pipepp::core::make_unexpected(pipepp::core::error_code::connection_failed)};
    }
#else
    return {};
#endif
}

template<typename Config>
pipepp::core::result mqtt_source<Config>::publish(std::string_view topic,
                                                    std::span<const std::byte> payload,
                                                    int qos) {
    if (qos < 0 || qos > 2) {
        return pipepp::core::result{
            pipepp::core::unexpect,
            pipepp::core::make_unexpected(pipepp::core::error_code::invalid_argument)};
    }
#ifdef PIPEPP_MQTT_HAS_PAHO_CPP
    if (!connected_) {
        return pipepp::core::result{
            pipepp::core::unexpect,
            pipepp::core::make_unexpected(pipepp::core::error_code::not_connected)};
    }
    auto* s = static_cast<mqtt_impl<Config>*>(impl_);
    try {
        auto msg = paho::make_message(
            std::string(topic),
            payload.data(), payload.size(),
            qos, false);
        s->client->publish(msg);
        return {};
    } catch (const paho::exception&) {
        return pipepp::core::result{
            pipepp::core::unexpect,
            pipepp::core::make_unexpected(pipepp::core::error_code::connection_failed)};
    }
#else
    return {};
#endif
}

template<typename Config>
void mqtt_source<Config>::set_message_callback(pipepp::core::message_callback<Config> cb) {
    auto* s = static_cast<mqtt_impl<Config>*>(impl_);
    s->callback = std::move(cb);
#ifdef PIPEPP_MQTT_HAS_PAHO_CPP
    if (s->client) {
        auto* cb_ptr = &s->callback;
        if constexpr (std::is_same_v<typename Config::poll_mode, mqtt_callback_tag>) {
            s->client->set_message_callback([cb_ptr](paho::const_message_ptr msg) {
                if (msg && *cb_ptr) {
                    auto topic = std::string_view(msg->get_topic());
                    auto& payload_ref = msg->get_payload_ref();
                    auto payload = std::span<const std::byte>(
                        reinterpret_cast<const std::byte*>(payload_ref.data()),
                        payload_ref.size());
                    auto qos = msg->get_qos();
                    pipepp::core::message_view mv(topic, payload, static_cast<uint8_t>(qos));
                    (*cb_ptr)(mv);
                }
            });
        }
    }
#endif
}

template<typename Config>
void mqtt_source<Config>::poll() {
#ifdef PIPEPP_MQTT_HAS_PAHO_CPP
    auto* s = static_cast<mqtt_impl<Config>*>(impl_);
    if constexpr (std::is_same_v<typename Config::poll_mode, mqtt_consumer_tag>) {
        if (s && s->client && s->callback) {
            try {
                while (auto msg = s->client->try_consume_message_for(std::chrono::milliseconds(0))) {
                    if (msg) {
                        auto topic = std::string_view(msg->get_topic());
                        auto& payload_ref = msg->get_payload_ref();
                        auto payload = std::span<const std::byte>(
                            reinterpret_cast<const std::byte*>(payload_ref.data()),
                            payload_ref.size());
                        auto qos = msg->get_qos();
                        pipepp::core::message_view mv(topic, payload, static_cast<uint8_t>(qos));
                        s->callback(mv);
                    }
                }
            } catch (...) {}
        }
    } else {
        std::this_thread::yield();
    }
#endif
}

template<typename Config>
void mqtt_source<Config>::set_client_id(std::string_view id) {
    static_cast<mqtt_impl<Config>*>(impl_)->client_id.checked_from(id);
}

template<typename Config>
void mqtt_source<Config>::set_keepalive(int seconds) {
    static_cast<mqtt_impl<Config>*>(impl_)->keepalive = seconds;
}

template<typename Config>
void mqtt_source<Config>::set_clean_session(bool clean) {
    static_cast<mqtt_impl<Config>*>(impl_)->clean_session = clean;
}

template<typename Config>
void mqtt_source<Config>::set_automatic_reconnect(int min_s, int max_s) {
    auto* s = static_cast<mqtt_impl<Config>*>(impl_);
    s->auto_reconnect = true;
    s->reconnect_min_s = min_s;
    s->reconnect_max_s = max_s;
}

template<typename Config>
void mqtt_source<Config>::set_will(std::string_view topic, std::span<const std::byte> payload, int qos, bool retained) {
    auto* s = static_cast<mqtt_impl<Config>*>(impl_);
    s->will_topic.checked_from(topic);
    auto copy_len = payload.size() < Config::max_payload_len ? payload.size() : Config::max_payload_len;
    std::memcpy(s->will_payload, payload.data(), copy_len);
    s->will_payload_len = copy_len;
    s->will_qos = qos;
    s->will_retained = retained;
    s->has_will = true;
}

template<typename Config>
void mqtt_source<Config>::set_username(std::string_view user) {
    static_cast<mqtt_impl<Config>*>(impl_)->username.checked_from(user);
}

template<typename Config>
void mqtt_source<Config>::set_password(std::string_view pass) {
    static_cast<mqtt_impl<Config>*>(impl_)->password.checked_from(pass);
}

template<typename Config>
void mqtt_source<Config>::set_ssl(std::string_view trust_store, std::string_view key_store, std::string_view private_key) {
    auto* s = static_cast<mqtt_impl<Config>*>(impl_);
    s->ssl_enabled = true;
    s->ssl_trust_store.checked_from(trust_store);
    s->ssl_key_store.checked_from(key_store);
    s->ssl_private_key.checked_from(private_key);
}

template<typename Config>
void mqtt_source<Config>::set_mqtt_version(int version) {
    static_cast<mqtt_impl<Config>*>(impl_)->mqtt_version = version;
}

template<typename Config>
void mqtt_source<Config>::set_broker(std::string_view host, std::string_view port) {
    auto* s = static_cast<mqtt_impl<Config>*>(impl_);
    s->broker_host.checked_from(host);
    s->broker_port.checked_from(port);
}

template class mqtt_source<mqtt_default_config>;
template class mqtt_source<mqtt_embedded_config>;
template class mqtt_source<mqtt_default_consumer_config>;
template class mqtt_source<mqtt_embedded_consumer_config>;

} // namespace pipepp::mqtt
