#include "gateway/common/Environment.h"

#include <cstdlib>
#include <cstring>

extern char **environ;

namespace openexchange { namespace gateway {

const char *SystemEnvironment::get(const std::string &name) const
{
    return std::getenv(name.c_str());
}

std::vector<std::string> SystemEnvironment::names() const
{
    std::vector<std::string> out;
    for (char **entry = environ; nullptr != entry && nullptr != *entry; ++entry)
    {
        const char *eq = std::strchr(*entry, '=');
        out.emplace_back(*entry, nullptr == eq ? std::strlen(*entry) : static_cast<std::size_t>(eq - *entry));
    }
    return out;
}

const char *MapEnvironment::get(const std::string &name) const
{
    const auto it = m_values.find(name);
    return m_values.end() == it ? nullptr : it->second.c_str();
}

std::vector<std::string> MapEnvironment::names() const
{
    std::vector<std::string> out;
    out.reserve(m_values.size());
    for (const auto &entry : m_values)
    {
        out.push_back(entry.first);
    }
    return out;
}

}}
