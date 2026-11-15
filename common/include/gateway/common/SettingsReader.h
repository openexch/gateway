#ifndef GATEWAY_COMMON_SETTINGS_READER_H
#define GATEWAY_COMMON_SETTINGS_READER_H

#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "gateway/common/Environment.h"

// SettingsReader - the strict reading rules every setting of both binaries goes through, one copy:
//   - a value is taken exactly as given: no trimming, no case folding, no unit guessing; a value that does not parse
//     is a problem that names the setting, the expected form and the value received;
//   - a setting that is set but empty is a problem (unset it to take the default), never "the default";
//   - a required setting has no default; missing is a problem;
//   - every problem is collected: the caller reports all of them in one pass and exits 78;
//   - every setting read is recorded with its value in force and where it came from ("set" or "default"), so the
//     boot log and /health show every setting, defaults included; a secret is recorded redacted;
//   - a variable carrying one of the binary's own prefixes that no reader asked for is a problem (a misspelt name
//     would otherwise leave the default silently in force).
// Numbers are validity-checked (a count is positive, a duration is positive), never capped: sizes, bounds, rates and
// timeouts are settings with defaults, not limits in code.

namespace openexchange { namespace gateway {

struct SettingRecord
{
    std::string name;
    std::string value;  // the value in force as text ("(redacted)" parts for secrets, "(unset)" for an absent optional)
    std::string source; // "set", "default" or "unset"
};

struct Rate
{
    std::int64_t count = 0;    // tokens
    std::int64_t periodMs = 0; // per this many ms (1000 for "/s", 60000 for "/min")
    std::string text;          // as configured, e.g. "50/s"
};

class SettingsReader
{
public:
    explicit SettingsReader(const Environment &env) : m_env(env) {}

    // The raw value; nullopt when unset. Marks the name as known (for the unknown-setting check).
    std::optional<std::string> raw(const std::string &name);
    bool isSet(const std::string &name);

    // A required non-empty string ("" when missing; the problem is recorded). `form` describes the expected value.
    std::string requiredString(const std::string &name, const std::string &form, bool secret = false);
    // An optional non-empty string: nullopt when unset; set but empty is a problem.
    std::optional<std::string> optionalString(const std::string &name, const std::string &form, bool secret = false);

    // Integers in decimal, no sign, no unit.
    std::int64_t positive(const std::string &name, std::int64_t defaultValue);          // >= 1
    std::int64_t requiredPositive(const std::string &name);                             // >= 1, no default
    // Exactly "0" or "1".
    bool flag01(const std::string &name, bool defaultValue);
    // Exactly one of `choices`; returns the index (defaultIndex when unset; -1 when required and missing or invalid).
    int choice(const std::string &name, const std::vector<std::string> &choices, int defaultIndex);
    // "<count>/s" or "<count>/min", count >= 1.
    Rate rate(const std::string &name, const std::string &defaultValue);

    void problem(const std::string &text) { m_errors.push_back(text); }
    void record(const std::string &name, const std::string &value, const std::string &source);

    // Every variable starting with one of `prefixes` that no reader asked for is a problem.
    void refuseUnknown(const std::vector<std::string> &prefixes);

    const std::vector<std::string> &errors() const noexcept { return m_errors; }
    const std::vector<SettingRecord> &records() const noexcept { return m_records; }

private:
    const Environment &m_env;
    std::set<std::string> m_known;
    std::vector<std::string> m_errors;
    std::vector<SettingRecord> m_records;
};

// Helpers shared by the parsers and the tests.
bool parsePositiveDecimal(const std::string &text, std::int64_t &out);
std::optional<Rate> parseRate(const std::string &text);
// Splits on ',' (no trimming); an empty item makes the result nullopt.
std::optional<std::vector<std::string>> splitList(const std::string &text);

}}

#endif
