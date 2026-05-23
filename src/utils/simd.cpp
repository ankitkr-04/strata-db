// src/utils/simd_utils.cpp
//
// Domain-agnostic SIMD primitives. No WAL, no compaction, no bloom.
// See include/stratadb/utils/simd.hpp for the public API.

#include "stratadb/utils/simd.hpp"

#include <cstring>
#include <mutex>

#define XXH_INLINE_ALL
#include <xxhash.h>

namespace stratadb::utils::simd {

// Dispatch table — written once in init(), read-only forever after

namespace {

using ScanFn = MarkerScanResult (*)(const uint8_t*, size_t, uint32_t) noexcept;

struct DispatchTable {
    ScanFn scan_u32{nullptr};
    SimdLevel level{SimdLevel::Scalar};
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DispatchTable g_dispatch{};
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::once_flag g_init_flag{};

#if defined(STRATADB_ARCH_X86_64)

struct CpuFeatures {
    bool avx2{false};
    bool bmi{false};
    bool avx512bw{false};
    bool avx512vl{false};
};

[[nodiscard]] auto probe_cpu_features() noexcept -> CpuFeatures {
    CpuFeatures f{};
    uint32_t eax = 0, ebx = 0, ecx = 0, edx = 0;
    __cpuid_count(7, 0, eax, ebx, ecx, edx); // NOLINT
    f.avx2 = (ebx >> 5) & 1U;
    f.bmi = (ebx >> 3) & 1U;
    f.avx512bw = (ebx >> 30) & 1U;
    f.avx512vl = (ebx >> 31) & 1U;
    return f;
}

#endif

} // anonymous namespace

void init() noexcept {
    std::call_once(g_init_flag, []() noexcept {

#if defined(STRATADB_ARCH_X86_64)
        const auto f = probe_cpu_features();
        if (f.avx512bw && f.avx512vl) {
            g_dispatch.scan_u32 = detail::scan_marker_u32_avx512;
            g_dispatch.level = SimdLevel::Avx512;
        } else if (f.avx2 && f.bmi) {
            g_dispatch.scan_u32 = detail::scan_marker_u32_avx2;
            g_dispatch.level = SimdLevel::Avx2;
        } else {
            g_dispatch.scan_u32 = detail::scan_marker_u32_scalar;
            g_dispatch.level = SimdLevel::Scalar;
        }

#elif defined(STRATADB_ARCH_AARCH64)
        g_dispatch.scan_u32 = detail::scan_marker_u32_neon;
        g_dispatch.level    = SimdLevel::Neon;
#else
        g_dispatch.scan_u32 = detail::scan_marker_u32_scalar;
        g_dispatch.level    = SimdLevel::Scalar;
#endif
    });
}

[[nodiscard]] auto detected_level() noexcept -> SimdLevel {
    return g_dispatch.level;
}

[[nodiscard]] auto level_name(SimdLevel level) noexcept -> const char* {
    switch (level) {
        case SimdLevel::Scalar:
            return "Scalar";
        case SimdLevel::Neon:
            return "ARM NEON";
        case SimdLevel::Avx2:
            return "x86 AVX2+BMI";
        case SimdLevel::Avx512:
            return "x86 AVX-512BW/VL";
    }
    return "Unknown";
}

[[nodiscard]] auto scan_marker_u32(const uint8_t* buffer, size_t length, uint32_t marker) noexcept -> MarkerScanResult {

    assert(reinterpret_cast<uintptr_t>(buffer) % SIMD_BUFFER_ALIGNMENT == 0);

    const ScanFn fn = g_dispatch.scan_u32 ? g_dispatch.scan_u32 : detail::scan_marker_u32_scalar;
    return fn(buffer, length, marker);
}

[[nodiscard]] auto validate_sector_checksums(const uint8_t* sector_data,
                                             uint8_t sector_count,
                                             const uint32_t* expected_checksums) noexcept -> SectorValidationResult {

    assert(sector_count <= 64); 

    SectorValidationResult result{};
    result.all_valid = true;

    for (uint8_t i = 0; i < sector_count; ++i) {
        const uint8_t* ptr = sector_data + (static_cast<size_t>(i) * 512U);

        // xxhash3-64 truncated to 32 bits. Matches the write-side primitive.
        // Algorithm selection (xxhash3 vs crc32) is the caller's concern:
        // the caller passes the expected_checksums array pre-computed with
        // whichever algorithm the block was written with.
        const auto computed = static_cast<uint32_t>(XXH3_64bits(ptr, 512U) & 0xFFFF'FFFFU);

        if (computed == expected_checksums[i]) {
            result.valid_sector_mask |= (uint64_t{1} << i);
        } else {
            result.all_valid = false;
            if (result.first_failed_sector == UINT8_MAX) {
                result.first_failed_sector = i;
            }
        }
    }

    return result;
}

// Scalar — correct on every architecture, auto-vectorizer friendly

namespace detail {

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] auto scan_marker_u32_scalar(const uint8_t* buffer, size_t length, uint32_t marker) noexcept
    -> MarkerScanResult {

    MarkerScanResult result{};
    if (length < 4)
        return result;

    const size_t end = length - 3U;
    for (size_t i = 0; i < end && result.count < MAX_SCAN_RESULTS; ++i) {
        uint32_t word = 0;
        std::memcpy(&word, buffer + i, sizeof(word)); // avoids aliasing UB
        if (word == marker) {
            result.offsets[result.count++] = static_cast<uint32_t>(i);
        }
    }
    return result;
}

// marker is decomposed into its 4 individual bytes at runtime so the
// broadcast targets are data-driven, not hardcoded. The compiler constant-folds
// the byte extraction at the call site when marker is a compile-time constant
// (e.g. WALR_MARKER from wal_simd.hpp), giving zero runtime overhead.

#if defined(STRATADB_ARCH_X86_64)

__attribute__((target("avx2,bmi")))
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto scan_marker_u32_avx2(const uint8_t* buffer, size_t length, uint32_t marker) noexcept -> MarkerScanResult {

    MarkerScanResult result{};

    // Decompose the 4-byte marker into individual byte lanes.
    const __m256i v0 = _mm256_set1_epi8(static_cast<char>((marker >> 0) & 0xFFU));
    const __m256i v1 = _mm256_set1_epi8(static_cast<char>((marker >> 8) & 0xFFU));
    const __m256i v2 = _mm256_set1_epi8(static_cast<char>((marker >> 16) & 0xFFU));
    const __m256i v3 = _mm256_set1_epi8(static_cast<char>((marker >> 24) & 0xFFU));

    const size_t vec_end = (length >= 35U) ? (length - 35U) : 0U;
    size_t i = 0;

    for (; i <= vec_end && result.count < MAX_SCAN_RESULTS; i += 32) {
        // Four overlapping 32-byte loads catch markers that straddle a
        // 32-byte register boundary.
        const __m256i b0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(buffer + i));
        const __m256i b1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(buffer + i + 1));
        const __m256i b2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(buffer + i + 2));
        const __m256i b3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(buffer + i + 3));

        const __m256i match = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(b0, v0), _mm256_cmpeq_epi8(b1, v1)),
                                               _mm256_and_si256(_mm256_cmpeq_epi8(b2, v2), _mm256_cmpeq_epi8(b3, v3)));

        uint32_t bitmask = static_cast<uint32_t>(_mm256_movemask_epi8(match));

        // BLSR + TZCNT: 2 cycles per match on Haswell+
        while (bitmask != 0 && result.count < MAX_SCAN_RESULTS) {
            const int bit = __builtin_ctz(bitmask);
            result.offsets[result.count++] = static_cast<uint32_t>(i) + static_cast<uint32_t>(bit);
            bitmask = bitmask & (bitmask - 1U); // BLSR
        }
    }

    // Scalar tail
    for (; i + 3 < length && result.count < MAX_SCAN_RESULTS; ++i) {
        uint32_t word = 0;
        std::memcpy(&word, buffer + i, sizeof(word));
        if (word == marker) {
            result.offsets[result.count++] = static_cast<uint32_t>(i);
        }
    }

    return result;
}

// k-mask registers return uint64_t directly from _mm512_cmpeq_epi8_mask,
// eliminating the movemask + vector AND from the AVX2 path.

__attribute__((target("avx512bw,avx512vl"))) auto
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
scan_marker_u32_avx512(const uint8_t* buffer, size_t length, uint32_t marker) noexcept -> MarkerScanResult {

    MarkerScanResult result{};

    const __m512i v0 = _mm512_set1_epi8(static_cast<char>((marker >> 0) & 0xFFU));
    const __m512i v1 = _mm512_set1_epi8(static_cast<char>((marker >> 8) & 0xFFU));
    const __m512i v2 = _mm512_set1_epi8(static_cast<char>((marker >> 16) & 0xFFU));
    const __m512i v3 = _mm512_set1_epi8(static_cast<char>((marker >> 24) & 0xFFU));

    const size_t vec_end = (length >= 67U) ? (length - 67U) : 0U;
    size_t i = 0;

    for (; i <= vec_end && result.count < MAX_SCAN_RESULTS; i += 64) {
        const __m512i b0 = _mm512_loadu_si512(buffer + i);
        const __m512i b1 = _mm512_loadu_si512(buffer + i + 1);
        const __m512i b2 = _mm512_loadu_si512(buffer + i + 2);
        const __m512i b3 = _mm512_loadu_si512(buffer + i + 3);

        // k-mask AND: no vector registers consumed for the result
        const uint64_t bitmask = _mm512_cmpeq_epi8_mask(b0, v0) & _mm512_cmpeq_epi8_mask(b1, v1)
                                 & _mm512_cmpeq_epi8_mask(b2, v2) & _mm512_cmpeq_epi8_mask(b3, v3);

        uint64_t bits = bitmask;
        while (bits != 0 && result.count < MAX_SCAN_RESULTS) {
            const int bit = __builtin_ctzll(bits);
            result.offsets[result.count++] = static_cast<uint32_t>(i) + static_cast<uint32_t>(bit);
            bits = bits & (bits - 1ULL); // BLSR
        }
    }

    // Scalar tail
    for (; i + 3 < length && result.count < MAX_SCAN_RESULTS; ++i) {
        uint32_t word = 0;
        std::memcpy(&word, buffer + i, sizeof(word));
        if (word == marker) {
            result.offsets[result.count++] = static_cast<uint32_t>(i);
        }
    }

    return result;
}

#endif // STRATADB_ARCH_X86_64

#if defined(STRATADB_ARCH_AARCH64)

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto scan_marker_u32_neon(const uint8_t* buffer, size_t length, uint32_t marker) noexcept -> MarkerScanResult {

    MarkerScanResult result{};

    // Decompose marker into 4 broadcast vectors
    const uint8x16_t v0 = vdupq_n_u8(static_cast<uint8_t>((marker >> 0) & 0xFFU));
    const uint8x16_t v1 = vdupq_n_u8(static_cast<uint8_t>((marker >> 8) & 0xFFU));
    const uint8x16_t v2 = vdupq_n_u8(static_cast<uint8_t>((marker >> 16) & 0xFFU));
    const uint8x16_t v3 = vdupq_n_u8(static_cast<uint8_t>((marker >> 24) & 0xFFU));

    // Shift table: lane N contributes bit N to the 16-bit condensed mask.
    static constexpr uint8_t kShift[16] = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    const uint8x16_t shift_tbl = vld1q_u8(kShift);

    const size_t vec_end = (length >= 19U) ? (length - 19U) : 0U;
    size_t i = 0;

    for (; i <= vec_end && result.count < MAX_SCAN_RESULTS; i += 16) {
        const uint8x16_t b0 = vld1q_u8(buffer + i);
        const uint8x16_t b1 = vld1q_u8(buffer + i + 1);
        const uint8x16_t b2 = vld1q_u8(buffer + i + 2);
        const uint8x16_t b3 = vld1q_u8(buffer + i + 3);

        const uint8x16_t match =
            vandq_u8(vandq_u8(vceqq_u8(b0, v0), vceqq_u8(b1, v1)), vandq_u8(vceqq_u8(b2, v2), vceqq_u8(b3, v3)));

        // Condense 16 lanes → 16-bit bitmask via vpadd
        const uint8x16_t bits = vandq_u8(match, shift_tbl);
        const uint8x8_t lo = vpadd_u8(vget_low_u8(bits), vget_high_u8(bits));
        const uint64_t raw = vget_lane_u64(vreinterpret_u64_u8(lo), 0);

        // Re-pack 8 bytes into a 16-bit bitmask (each byte holds 2 bits)
        uint32_t bitmask = 0;
        for (int lane = 0; lane < 8; ++lane) {
            const uint8_t byte = static_cast<uint8_t>(raw >> (lane * 8));
            bitmask |= static_cast<uint32_t>(byte) << (lane * 2);
        }

        while (bitmask != 0 && result.count < MAX_SCAN_RESULTS) {
            const int bit = __builtin_ctz(bitmask);
            result.offsets[result.count++] = static_cast<uint32_t>(i) + static_cast<uint32_t>(bit);
            bitmask = bitmask & (bitmask - 1U);
        }
    }

    // Scalar tail
    for (; i + 3 < length && result.count < MAX_SCAN_RESULTS; ++i) {
        uint32_t word = 0;
        std::memcpy(&word, buffer + i, sizeof(word));
        if (word == marker) {
            result.offsets[result.count++] = static_cast<uint32_t>(i);
        }
    }

    return result;
}

#endif // STRATADB_ARCH_AARCH64

} // namespace detail
} // namespace stratadb::utils::simd