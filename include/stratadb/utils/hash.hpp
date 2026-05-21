#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(__x86_64__) || defined(_M_X64)
#include <nmmintrin.h>
#define STRATADB_CRC32_HW 1
#elif defined(__aarch64__)
#include <arm_acle.h>
#define STRATADB_CRC32_HW 1
#endif

namespace stratadb::utils {
[[nodiscard]]
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("sse4.2,crc32")))
#endif
inline auto crc32c(const void* data, size_t length) noexcept -> uint32_t {
    const auto* ptr = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;

    // Process 8 bytes (64 bits) at a time
    while (length >= 8) {
        std::uint64_t word;
        std::memcpy(&word, ptr, sizeof(word));
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, word));
        ptr += 8;
        length -= 8;
    }

    // Process remaining 4 bytes
    if (length >= 4) {
        std::uint32_t dword;
        std::memcpy(&dword, ptr, sizeof(dword));
        crc = _mm_crc32_u32(crc, dword);
        ptr += 4;
        length -= 4;
    }

    // Process remaining 2 bytes
    if (length >= 2) {
        std::uint16_t word;
        std::memcpy(&word, ptr, sizeof(word));
        crc = _mm_crc32_u16(crc, word);
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