#pragma once
#include <stdexcept>
#include <string>

namespace mqtt {

class exception : public std::runtime_error {
public:
    explicit exception(const std::string& msg) : std::runtime_error(msg) {}
};

}
