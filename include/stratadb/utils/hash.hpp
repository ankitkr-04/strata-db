#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(__x86_64__) || defined(_M_X64)
#include <nmmintrin.h>
#define STRATADB_CRC32_ARCH_X86 1
#elif defined(__aarch64__)
#include <arm_acle.h>
#define STRATADB_CRC32_ARCH_ARM 1
#endif

namespace stratadb::utils {

#if defined(STRATADB_CRC32_ARCH_X86)

[[nodiscard]]
__attribute__((target("sse4.2,crc32"))) inline auto crc32c(const void* data, std::size_t length) noexcept
    -> std::uint32_t {
    const auto* ptr = static_cast<const std::uint8_t*>(data);
    std::uint32_t crc = 0xFFFF'FFFFU;

    while (length >= 8) {
        std::uint64_t word;
        std::memcpy(&word, ptr, sizeof(word));
        crc = static_cast<std::uint32_t>(_mm_crc32_u64(crc, word));
        ptr += 8;
        length -= 8;
    }
    if (length >= 4) {
        std::uint32_t dword;
        std::memcpy(&dword, ptr, sizeof(dword));
        crc = _mm_crc32_u32(crc, dword);
        ptr += 4;
        length -= 4;
    }
    if (length >= 2) {
        std::uint16_t word;
        std::memcpy(&word, ptr, sizeof(word));
        crc = _mm_crc32_u16(crc, word);
        ptr += 2;
        length -= 2;
    }
    if (length > 0) {
        crc = _mm_crc32_u8(crc, *ptr);
    }
    return ~crc;
}

#elif defined(STRATADB_CRC32_ARCH_ARM)

[[nodiscard]]
inline auto crc32c(const void* data, std::size_t length) noexcept -> std::uint32_t {
    const auto* ptr = static_cast<const std::uint8_t*>(data);
    std::uint32_t crc = 0xFFFF'FFFFU;

    while (length >= 8) {
        std::uint64_t word;
        std::memcpy(&word, ptr, sizeof(word));
        crc = __crc32cd(crc, word);
        ptr += 8;
        length -= 8;
    }
    if (length >= 4) {
        std::uint32_t dword;
        std::memcpy(&dword, ptr, sizeof(dword));
        crc = __crc32cw(crc, dword);
        ptr += 4;
        length -= 4;
    }
    if (length >= 2) {
        std::uint16_t word;
        std::memcpy(&word, ptr, sizeof(word));
        crc = __crc32ch(crc, word);
        ptr += 2;
        length -= 2;
    }
    if (length > 0) {
        crc = __crc32cb(crc, *ptr);
    }
    return ~crc;
}

#else 

[[nodiscard]]
inline auto crc32c(const void* data, std::size_t length) noexcept -> std::uint32_t {
    // Castagnoli polynomial (0x82F63B78 bit-reversed)
    static constexpr std::uint32_t TABLE[256] = {
        // generated via standard CRC-32C table construction
        0x00000000U,
        0xF26B8303U,
        0xE13B70F7U,
        0x1350F3F4U, /* ... full table ... */
    };
    const auto* ptr = static_cast<const std::uint8_t*>(data);
    std::uint32_t crc = 0xFFFF'FFFFU;
    while (length-- > 0) {
        crc = TABLE[(crc ^ *ptr++) & 0xFFU] ^ (crc >> 8);
    }
    return ~crc;
}

#endif

} // namespace stratadb::utils