#pragma once
#include <string>
#include <chrono>
#include <functional>
#include <memory>
#include <vector>
#include <mutex>
#include "exception.h"

namespace mqtt {

class string_ref {
public:
    string_ref() = default;
    string_ref(const char* d, size_t s) : data_(d, s) {}
    string_ref(const std::string& s) : data_(s) {}
    std::string str() const { return data_; }
private:
    std::string data_;
};

class binary_ref {
public:
    binary_ref() = default;
    binary_ref(const char* d, size_t s) : data_(d, d + s) {}
    const char* data() const { return data_.data(); }
    size_t size() const { return data_.size(); }
private:
    std::vector<char> data_;
};

class message {
public:
    message(std::string topic, const void* payload, size_t len, int qos, bool retained)
        : topic_(std::move(topic)), payload_(static_cast<const char*>(payload),
                                               static_cast<const char*>(payload) + len),
          qos_(qos), retained_(retained) {}

    const std::string& get_topic() const { return topic_; }
    const std::string& get_payload_ref() const { return payload_; }
    int get_qos() const { return qos_; }

private:
    std::string topic_;
    std::string payload_;
    int qos_;
    bool retained_;
};

using const_message_ptr = std::shared_ptr<message>;

inline const_message_ptr make_message(std::string topic, const void* payload, size_t len, int qos, bool retained) {
    return std::make_shared<message>(std::move(topic), payload, len, qos, retained);
}

class token {
public:
    virtual ~token() = default;
    virtual void wait_for(std::chrono::milliseconds) {}
    virtual void wait_for(std::chrono::seconds) {}
};
using token_ptr = std::shared_ptr<token>;

struct ssl_options {
    std::string trust_store_;
    std::string key_store_;
    std::string private_key_;
    bool enable_server_cert_auth_ = true;

    void set_trust_store(const std::string& s) { trust_store_ = s; }
    void set_key_store(const std::string& s) { key_store_ = s; }
    void set_private_key(const std::string& s) { private_key_ = s; }
    void set_enable_server_cert_auth(bool v) { enable_server_cert_auth_ = v; }
};

class connect_options {
public:
    int mqtt_version_ = 4;
    int keepalive_ = 60;
    bool clean_session_ = true;
    bool auto_reconnect_ = false;
    int reconnect_min_ = 1;
    int reconnect_max_ = 30;
    std::string username_;
    std::string password_;
    const_message_ptr will_;
    ssl_options ssl_;
    bool has_ssl_ = false;

    void set_automatic_reconnect(std::chrono::seconds min_s, std::chrono::seconds max_s) {
        auto_reconnect_ = true;
        reconnect_min_ = static_cast<int>(min_s.count());
        reconnect_max_ = static_cast<int>(max_s.count());
    }
    void set_mqtt_version(int v) { mqtt_version_ = v; }
    void set_user_name(string_ref u) { username_ = u.str(); }
    void set_password(binary_ref p) { password_ = std::string(p.data(), p.size()); }
    void set_will_message(const_message_ptr m) { will_ = m; }
    void set_ssl(ssl_options s) { ssl_ = std::move(s); has_ssl_ = true; }
};

class connect_options_builder {
public:
    connect_options_builder& keep_alive_interval(std::chrono::seconds s) {
        opts_.keepalive_ = static_cast<int>(s.count());
        return *this;
    }
    connect_options_builder& clean_session(bool v) {
        opts_.clean_session_ = v;
        return *this;
    }
    connect_options finalize() { return std::move(opts_); }
private:
    connect_options opts_;
};

class async_client {
public:
    using message_callback = std::function<void(const_message_ptr)>;
    using handler_callback = std::function<void(const std::string&)>;

    async_client(const std::string& server_uri, const std::string& client_id)
        : server_uri_(server_uri), client_id_(client_id) {
        fault_injection::instance().active_client = this;
    }

    virtual ~async_client() {
        if (fault_injection::instance().active_client == this) {
            fault_injection::instance().active_client = nullptr;
        }
    }

    token_ptr connect(const connect_options&) {
        auto& fi = fault_injection::instance();
        fi.record_call("connect");
        if (fi.connect_should_throw_paho) {
            throw exception("mock connect failure");
        }
        if (fi.connect_should_throw_generic) {
            throw std::runtime_error("mock generic connect failure");
        }
        connected_ = true;
        if (connected_handler_) connected_handler_("");
        return std::make_shared<token>();
    }

    token_ptr disconnect(std::chrono::seconds timeout) {
        auto& fi = fault_injection::instance();
        fi.record_call("disconnect");
        if (fi.disconnect_should_throw) {
            throw exception("mock disconnect failure");
        }
        connected_ = false;
        return std::make_shared<token>();
    }

    token_ptr subscribe(const std::string& topic, int qos) {
        auto& fi = fault_injection::instance();
        fi.record_call("subscribe");
        if (fi.subscribe_should_throw) {
            throw exception("mock subscribe failure");
        }
        return std::make_shared<token>();
    }

    token_ptr publish(const_message_ptr msg) {
        auto& fi = fault_injection::instance();
        fi.record_call("publish");
        if (fi.publish_should_throw) {
            throw exception("mock publish failure");
        }
        return std::make_shared<token>();
    }

    void start_consuming() {
        auto& fi = fault_injection::instance();
        fi.record_call("start_consuming");
        if (fi.start_consuming_should_throw) {
            throw exception("mock start_consuming failure");
        }
        consuming_ = true;
    }

    void stop_consuming() {
        auto& fi = fault_injection::instance();
        fi.record_call("stop_consuming");
        if (fi.stop_consuming_should_throw) {
            throw exception("mock stop_consuming failure");
        }
        consuming_ = false;
    }

    const_message_ptr try_consume_message_for(std::chrono::milliseconds dur) {
        auto& fi = fault_injection::instance();
        fi.record_call("try_consume_message_for");
        if (fi.consume_should_throw) {
            throw exception("mock consume failure");
        }
        if (fi.consume_should_return_message && fi.mock_message) {
            auto msg = fi.mock_message;
            fi.consume_should_return_message = false;
            return msg;
        }
        return nullptr;
    }

    bool is_connected() const {
        auto& fi = fault_injection::instance();
        fi.record_call("is_connected");
        if (fi.is_connected_should_throw) {
            throw exception("mock is_connected failure");
        }
        return connected_;
    }

    void set_message_callback(message_callback cb) {
        message_cb_ = std::move(cb);
    }

    void set_connected_handler(handler_callback cb) {
        connected_handler_ = std::move(cb);
    }

    void set_connection_lost_handler(handler_callback cb) {
        connection_lost_handler_ = std::move(cb);
    }

    void inject_message(const_message_ptr msg) {
        if (message_cb_) message_cb_(msg);
    }

    void simulate_connection_lost() {
        connected_ = false;
        if (connection_lost_handler_) connection_lost_handler_("");
    }

    void simulate_reconnect() {
        connected_ = true;
        if (connected_handler_) connected_handler_("");
    }

    bool connected_ = false;
    bool consuming_ = false;
    std::string server_uri_;
    std::string client_id_;
    message_callback message_cb_;
    handler_callback connected_handler_;
    handler_callback connection_lost_handler_;

    struct fault_injection {
        static fault_injection& instance() {
            static fault_injection fi;
            return fi;
        }

        async_client* active_client = nullptr;

        void reset() {
            active_client = nullptr;
            connect_should_throw_paho = false;
            connect_should_throw_generic = false;
            disconnect_should_throw = false;
            subscribe_should_throw = false;
            publish_should_throw = false;
            consume_should_throw = false;
            start_consuming_should_throw = false;
            stop_consuming_should_throw = false;
            is_connected_should_throw = false;
            consume_should_return_message = false;
            mock_message = nullptr;
            calls_.clear();
        }

        void record_call(const std::string& method) {
            std::lock_guard<std::mutex> lock(mutex_);
            calls_.push_back(method);
        }

        bool was_called(const std::string& method) const {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& c : calls_) if (c == method) return true;
            return false;
        }

        void simulate_connection_lost() {
            if (active_client) active_client->simulate_connection_lost();
        }

        void simulate_reconnect() {
            if (active_client) active_client->simulate_reconnect();
        }

        bool connect_should_throw_paho = false;
        bool connect_should_throw_generic = false;
        bool disconnect_should_throw = false;
        bool subscribe_should_throw = false;
        bool publish_should_throw = false;
        bool consume_should_throw = false;
        bool start_consuming_should_throw = false;
        bool stop_consuming_should_throw = false;
        bool is_connected_should_throw = false;
        bool consume_should_return_message = false;
        const_message_ptr mock_message;

    private:
        mutable std::mutex mutex_;
        std::vector<std::string> calls_;
    };
};

}
