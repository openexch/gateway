#include "gateway/common/SettingsReader.h"

#include <charconv>

namespace openexchange { namespace gateway {

namespace {

const char *const SET = "set";
const char *const DEFAULT = "default";
const char *const UNSET = "unset";
const char *const REDACTED = "(redacted)";

}

bool parsePositiveDecimal(const std::string &text, std::int64_t &out)
{
    if (text.empty() || '+' == text.front() || '-' == text.front())
    {
        return false;
    }
    std::int64_t v = 0;
    const char *first = text.data();
    const char *last = text.data() + text.size();
    const auto r = std::from_chars(first, last, v);
    if (r.ec != std::errc() || r.ptr != last || v < 1)
    {
        return false;
    }
    out = v;
    return true;
}

std::optional<Rate> parseRate(const std::string &text)
{
    const std::size_t slash = text.find('/');
    if (std::string::npos == slash)
    {
        return std::nullopt;
    }
    Rate r;
    if (!parsePositiveDecimal(text.substr(0, slash), r.count))
    {
        return std::nullopt;
    }
    const std::string unit = text.substr(slash + 1);
    if ("s" == unit)
    {
        r.periodMs = 1000;
    }
    else if ("min" == unit)
    {
        r.periodMs = 60'000;
    }
    else
    {
        return std::nullopt;
    }
    r.text = text;
    return r;
}

std::optional<std::vector<std::string>> splitList(const std::string &text)
{
    std::vector<std::string> out;
    std::size_t start = 0;
    for (;;)
    {
        const std::size_t comma = text.find(',', start);
        std::string item = text.substr(start, std::string::npos == comma ? std::string::npos : comma - start);
        if (item.empty())
        {
            return std::nullopt;
        }
        out.push_back(std::move(item));
        if (std::string::npos == comma)
        {
            return out;
        }
        start = comma + 1;
    }
}

std::optional<std::string> SettingsReader::raw(const std::string &name)
{
    m_known.insert(name);
    const char *value = m_env.get(name);
    if (nullptr == value)
    {
        return std::nullopt;
    }
    return std::string(value);
}

bool SettingsReader::isSet(const std::string &name)
{
    m_known.insert(name);
    return nullptr != m_env.get(name);
}

void SettingsReader::record(const std::string &name, const std::string &value, const std::string &source)
{
    m_records.push_back(SettingRecord{ name, value, source });
}

std::string SettingsReader::requiredString(const std::string &name, const std::string &form, bool secret)
{
    const std::optional<std::string> value = raw(name);
    if (!value || value->empty())
    {
        problem(name + " is required (no default): " + form);
        return {};
    }
    record(name, secret ? REDACTED : *value, SET);
    return *value;
}

std::optional<std::string> SettingsReader::optionalString(const std::string &name, const std::string &form, bool secret)
{
    const std::optional<std::string> value = raw(name);
    if (!value)
    {
        record(name, "(unset)", UNSET);
        return std::nullopt;
    }
    if (value->empty())
    {
        problem(name + " is set but empty (unset it, or give " + form + ")");
        return std::nullopt;
    }
    record(name, secret ? REDACTED : *value, SET);
    return value;
}

std::int64_t SettingsReader::positive(const std::string &name, std::int64_t defaultValue)
{
    const std::optional<std::string> value = raw(name);
    if (!value)
    {
        record(name, std::to_string(defaultValue), DEFAULT);
        return defaultValue;
    }
    std::int64_t v = 0;
    if (!parsePositiveDecimal(*value, v))
    {
        problem(name + " must be a positive integer (decimal, no sign, no unit), got '" + *value + "'");
        return defaultValue;
    }
    record(name, *value, SET);
    return v;
}

std::int64_t SettingsReader::requiredPositive(const std::string &name)
{
    const std::optional<std::string> value = raw(name);
    if (!value || value->empty())
    {
        problem(name + " is required (no default): a positive integer");
        return 0;
    }
    std::int64_t v = 0;
    if (!parsePositiveDecimal(*value, v))
    {
        problem(name + " must be a positive integer in 1..9223372036854775807 (decimal, no sign), got '" + *value + "'");
        return 0;
    }
    record(name, *value, SET);
    return v;
}

bool SettingsReader::flag01(const std::string &name, bool defaultValue)
{
    const std::optional<std::string> value = raw(name);
    if (!value)
    {
        record(name, defaultValue ? "1" : "0", DEFAULT);
        return defaultValue;
    }
    if ("0" != *value && "1" != *value)
    {
        problem(name + " must be exactly 0 or 1, got '" + *value + "'");
        return defaultValue;
    }
    record(name, *value, SET);
    return "1" == *value;
}

int SettingsReader::choice(const std::string &name, const std::vector<std::string> &choices, int defaultIndex)
{
    std::string listed;
    for (const std::string &c : choices)
    {
        listed += (listed.empty() ? "" : " or ") + c;
    }
    const std::optional<std::string> value = raw(name);
    if (!value || value->empty())
    {
        if (defaultIndex < 0)
        {
            problem(name + " is required (no default): exactly " + listed);
            return -1;
        }
        if (value)
        {
            problem(name + " is set but empty (unset it, or give exactly " + listed + ")");
            return defaultIndex;
        }
        record(name, choices[static_cast<std::size_t>(defaultIndex)], DEFAULT);
        return defaultIndex;
    }
    for (std::size_t i = 0; i < choices.size(); ++i)
    {
        if (choices[i] == *value)
        {
            record(name, *value, SET);
            return static_cast<int>(i);
        }
    }
    problem(name + " must be exactly " + listed + ", got '" + *value + "'");
    return defaultIndex < 0 ? -1 : defaultIndex;
}

Rate SettingsReader::rate(const std::string &name, const std::string &defaultValue)
{
    const std::optional<std::string> value = raw(name);
    if (!value)
    {
        record(name, defaultValue, DEFAULT);
        return *parseRate(defaultValue);
    }
    const std::optional<Rate> parsed = parseRate(*value);
    if (!parsed)
    {
        problem(name + " must be <count>/s or <count>/min with a positive integer count, got '" + *value + "'");
        return *parseRate(defaultValue);
    }
    record(name, *value, SET);
    return *parsed;
}

void SettingsReader::refuseUnknown(const std::vector<std::string> &prefixes)
{
    for (const std::string &name : m_env.names())
    {
        for (const std::string &prefix : prefixes)
        {
            if (0 == name.compare(0, prefix.size(), prefix) && 0 == m_known.count(name))
            {
                problem(name + " is not a setting of this process (misspelt? it would be ignored)");
                break;
            }
        }
    }
}

}}
