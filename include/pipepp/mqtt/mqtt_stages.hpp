#pragma once

#include <algorithm>
#include <cstring>
#include <string>
#include <string_view>

#include <pipepp/core/message.hpp>
#include <pipepp/core/pipeline.hpp>

#include "mqtt_config.hpp"

namespace pipepp::mqtt {

inline bool mqtt_topic_matches(std::string_view filter, std::string_view topic)
{
    std::size_t fi = 0, ti = 0;
    while (fi < filter.size() && ti < topic.size()) {
        if (filter[fi] == '#') {
            return true;
        }
        if (filter[fi] == '+') {
            ++fi;
            while (ti < topic.size() && topic[ti] != '/') {
                ++ti;
            }
            if (fi < filter.size() && filter[fi] == '/') {
                ++fi;
            }
            if (ti < topic.size() && topic[ti] == '/') {
                ++ti;
            }
            continue;
        }
        if (filter[fi] != topic[ti]) {
            return false;
        }
        ++fi;
        ++ti;
    }
    if (fi < filter.size() && filter[fi] == '#') {
        return true;
    }
    return fi == filter.size() && ti == topic.size();
}

template<typename Config = mqtt_default_config>
struct subscribe_stage {
    std::string topic;
    int qos = 0;

    template<typename Source>
    pipepp::core::basic_pipeline<Source, Config>&
    apply(pipepp::core::basic_pipeline<Source, Config>& pipe) const
    {
        pipe.subscribe(topic, qos);
        auto filter_copy = topic;
        pipe.add_stage([f = std::move(filter_copy)](pipepp::core::basic_message<Config>& msg) -> bool {
            return mqtt_topic_matches(f, msg.topic());
        });
        return pipe;
    }
};

template<typename Config = mqtt_default_config>
struct publish_stage {
    std::string topic_prefix;

    template<typename Source>
    pipepp::core::basic_pipeline<Source, Config>&
    apply(pipepp::core::basic_pipeline<Source, Config>& pipe) const
    {
        auto* src = &pipe.source();
        auto prefix = topic_prefix;
        pipe.set_sink([src, p = std::move(prefix)](const pipepp::core::message_view& msg) {
            auto full_len = p.size() + msg.topic.size();
            if (full_len > Config::max_topic_len) {
                full_len = Config::max_topic_len;
            }
            char buf[Config::max_topic_len + 1];
            auto prefix_part = std::min(p.size(), full_len);
            std::memcpy(buf, p.data(), prefix_part);
            auto remaining = full_len - prefix_part;
            std::memcpy(buf + prefix_part, msg.topic.data(), remaining);
            src->publish(std::string_view(buf, full_len), msg.payload, msg.qos);
        });
        return pipe;
    }
};

template<typename Config = mqtt_default_config>
subscribe_stage<Config> subscribe(std::string_view topic, int qos = 0)
{
    return subscribe_stage<Config>{std::string(topic), qos};
}

template<typename Config = mqtt_default_config>
publish_stage<Config> publish(std::string_view topic_prefix = {})
{
    return publish_stage<Config>{std::string(topic_prefix)};
}

template<typename Source, typename Config>
pipepp::core::basic_pipeline<Source, Config>&
operator|(pipepp::core::basic_pipeline<Source, Config>& pipe,
          const subscribe_stage<Config>& stage)
{
    return stage.apply(pipe);
}

template<typename Source, typename Config>
pipepp::core::basic_pipeline<Source, Config>&
operator|(pipepp::core::basic_pipeline<Source, Config>& pipe,
          const publish_stage<Config>& stage)
{
    return stage.apply(pipe);
}

} // namespace pipepp::mqtt
