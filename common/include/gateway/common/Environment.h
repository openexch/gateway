#ifndef GATEWAY_COMMON_ENVIRONMENT_H
#define GATEWAY_COMMON_ENVIRONMENT_H

#include <map>
#include <string>
#include <vector>

// Environment - where the settings come from. The parsers read through this interface only, so the tests run them
// on a map with no process environment involved.

namespace openexchange { namespace gateway {

class Environment
{
public:
    virtual ~Environment() = default;
    // nullptr = unset (an empty string is a value, and the parsers refuse it where a value is needed)
    virtual const char *get(const std::string &name) const = 0;
    // every variable name present, for the unknown-setting check
    virtual std::vector<std::string> names() const = 0;
};

class SystemEnvironment final : public Environment
{
public:
    const char *get(const std::string &name) const override;
    std::vector<std::string> names() const override;
};

class MapEnvironment final : public Environment
{
public:
    MapEnvironment() = default;
    explicit MapEnvironment(std::map<std::string, std::string> values) : m_values(std::move(values)) {}

    void set(const std::string &name, const std::string &value) { m_values[name] = value; }
    void unset(const std::string &name) { m_values.erase(name); }

    const char *get(const std::string &name) const override;
    std::vector<std::string> names() const override;

private:
    std::map<std::string, std::string> m_values;
};

}}

#endif
