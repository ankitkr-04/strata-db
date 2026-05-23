#pragma once

// Single inclusion point for XXH3 in StrataDB.
//
// XXH_INLINE_ALL must be defined BEFORE <xxhash.h> and must be defined the
// same way in every translation unit that uses XXH3 symbols.  Including this
// header is the ONLY correct way to use XXH3 in StrataDB; pulling <xxhash.h>
// directly elsewhere risks ODR violations or missing inlines.
#ifndef XXH_INLINE_ALL
#define XXH_INLINE_ALL
#endif
#include <cstddef>
#include <xxhash.h>

namespace stratadb::utils {

// One-shot XXH3-128 over a contiguous buffer.
// Returns a (low64, high64) pair treated as an opaque 128-bit fingerprint.
[[nodiscard]] inline auto xxhash3_128(const void* data, std::size_t length) noexcept -> XXH128_hash_t {
    return XXH3_128bits(data, length);
}

// One-shot XXH3-64 over a contiguous buffer.
[[nodiscard]] inline auto xxhash3_64(const void* data, std::size_t length) noexcept -> XXH64_hash_t {
    return XXH3_64bits(data, length);
}

// Thin wrappers so call sites stay consistent without repeating the
// XXH3_128bits_* prefix.

inline void xxhash3_128_reset(XXH3_state_t& state) noexcept {
    XXH3_128bits_reset(&state);
}

inline void xxhash3_128_update(XXH3_state_t& state, const void* data, std::size_t length) noexcept {
    XXH3_128bits_update(&state, data, length);
}

[[nodiscard]] inline auto xxhash3_128_digest(XXH3_state_t& state) noexcept -> XXH128_hash_t {
    return XXH3_128bits_digest(&state);
}

} // namespace stratadb::utils