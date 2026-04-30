#pragma once

#include <cstddef>
#include <cstdint>
#include <nmmintrin.h>

namespace stratadb::utils {
[[nodiscard]] inline auto crc32c(const void* data, size_t length) noexcept -> uint32_t {
    const auto* ptr = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;

    // Process 8 bytes (64 bits) at a time
    while (length >= 8) {
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(ptr)));
        ptr += 8;
        length -= 8;
    }

    // Process remaining 4 bytes
    if (length >= 4) {
        crc = _mm_crc32_u32(crc, *reinterpret_cast<const uint32_t*>(ptr));
        ptr += 4;
        length -= 4;
    }

    // Process remaining 2 bytes
    if (length >= 2) {
        crc = _mm_crc32_u16(crc, *reinterpret_cast<const uint16_t*>(ptr));
        ptr += 2;
        length -= 2;
    }

    // Process remaining 1 byte
    if (length > 0) {
        crc = _mm_crc32_u8(crc, *ptr);
    }

    return ~crc;
}

} // namespace stratadb::utils