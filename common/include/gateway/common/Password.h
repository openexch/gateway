#ifndef GATEWAY_COMMON_PASSWORD_H
#define GATEWAY_COMMON_PASSWORD_H

#include <cstdint>
#include <string>
#include <string_view>

// Password - the stored password form of the demo users table, the format the previous order service wrote and the
// gateway keeps: pbkdf2$<iterations>$<salt>$<hash>, PBKDF2-HMAC-SHA256 over the password's UTF-8 bytes, a 16-byte
// random salt and a 32-byte hash, both unpadded base64url. The iteration count is part of each stored value, so a
// stored hash keeps verifying after GATEWAY_PBKDF2_ITERATIONS changes; new hashes use the setting.
// Both functions are slow on purpose and run only on the gw-hash threads (never an I/O thread, never the duty thread).

namespace openexchange { namespace gateway { namespace password {

std::string hash(std::string_view password, std::uint32_t iterations);
// Constant-time on the hash bytes. False for a malformed stored value (and `malformed` set).
bool verify(std::string_view password, std::string_view stored, bool *malformed = nullptr);
// Parses the stored form only (no hashing).
bool wellFormed(std::string_view stored, std::uint32_t *iterations = nullptr);

// Test observation only: when set, called on the hashing thread right before every PBKDF2 run.
void setHashObserver(void (*observer)());

}}}

#endif
