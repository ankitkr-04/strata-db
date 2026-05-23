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

// CRC-32C (Castagnoli polynomial). Hardware-accelerated on x86-64 (SSE4.2)
// and AArch64 (ARMv8 CRC extension). Falls back to a software implementation
// on unsupported architectures.

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

// ARM CRC extension (__crc32c*) — requires -march=armv8-a+crc or equivalent.
// Compile-time feature check: if the CRC extension is not enabled, the
// preprocessor will not define __ARM_FEATURE_CRC32 and we fall through to
// the software path. The build system should set -march=armv8-a+crc on
// AArch64 targets that are known to support it.
#if defined(__ARM_FEATURE_CRC32)

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

#else // AArch64 without CRC extension — fall through to software below
#define STRATADB_CRC32_SOFTWARE_FALLBACK 1
#endif // __ARM_FEATURE_CRC32

#else
#define STRATADB_CRC32_SOFTWARE_FALLBACK 1
#endif // arch detection

#if defined(STRATADB_CRC32_SOFTWARE_FALLBACK)

// Portable software CRC-32C. Correct on every architecture; ~3× slower than hw.
// Table defined in hash.cpp to avoid duplicate-symbol errors across TUs.
extern const std::uint32_t kCrc32cTable[256];

[[nodiscard]]
inline auto crc32c(const void* data, std::size_t length) noexcept -> std::uint32_t {
    const auto* ptr = static_cast<const std::uint8_t*>(data);
    std::uint32_t crc = 0xFFFF'FFFFU;
    while (length-- > 0) {
        crc = kCrc32cTable[(crc ^ *ptr++) & 0xFFU] ^ (crc >> 8);
    }
    return ~crc;
}

#endif // STRATADB_CRC32_SOFTWARE_FALLBACK

} // namespace stratadb::utils