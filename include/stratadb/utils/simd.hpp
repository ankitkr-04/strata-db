#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

#if defined(__x86_64__) || defined(_M_X64)
#define STRATADB_ARCH_X86_64 1
#include <cpuid.h>
#include <immintrin.h>
#elif defined(__aarch64__)
#define STRATADB_ARCH_AARCH64 1
#include <arm_neon.h>
#endif

namespace stratadb::utils::simd {
inline constexpr size_t SIMD_BUFFER_ALIGNMENT = 64;
inline constexpr size_t MAX_SCAN_RESULTS = 64;

struct MarkerScanResult {
    std::uint32_t offsets[MAX_SCAN_RESULTS]{};
    std::uint32_t count{0};
};

struct SectorValidationResult {
    std::uint64_t valid_sector_mask{0};
    std::uint8_t first_failed_sector{UINT8_MAX};
    bool all_valid{false};
};

enum class SimdLevel : uint8_t {
    Scalar = 0,
    Neon = 1,  // ARM Neon on AArch64
    Avx2 = 2,  // AVX2 on x86_64
    Avx512 = 3 // AVX-512 on x86_64 BW/VL (512-bit wide vectors)
};

void init() noexcept;

/// Returns the level detected. Returns Scalar if init() not yet called.
[[nodiscard]] auto detected_level() noexcept -> SimdLevel;

[[nodiscard]] auto level_name(SimdLevel level) noexcept -> const char*;

/// Scans [buffer, buffer + length) for every occurrence of the 4-byte little-
/// endian pattern `marker`. Returns the relative byte offset of each match.
///
/// This is the primitive that WAL recovery, SSTable block scanning, and any
/// other subsystem uses. The caller owns the semantic meaning of the marker.
///
/// Preconditions (debug-asserted):
///   - buffer is SIMD_BUFFER_ALIGNMENT-aligned.
///   - length >= 4 (returns empty result otherwise, no crash).

[[nodiscard]] auto scan_marker_u32(const uint8_t* buffer, size_t length, uint32_t marker) noexcept -> MarkerScanResult;

/// Validates `sector_count` contiguous 512-byte sectors starting at
/// `sector_data` against the `expected_checksums` array.
///
/// The checksum algorithm (xxhash3 truncated to 32-bit) is fixed at the
/// primitive layer. Algorithm selection (xxhash3 vs crc32) is the caller's
/// responsibility — the caller passes the correct expected_checksums array
/// pre-computed with the matching algorithm.
///
/// Returns a bitmask of which sectors passed, plus the index of the first
/// failure for fast torn-write boundary detection.

[[nodiscard]] auto validate_sector_checksums(const uint8_t* sector_data,
                                             uint8_t sector_count,
                                             const uint32_t* expected_checksums) noexcept -> SectorValidationResult;

/// Returns true if current > previous. Used for LSN regression detection,
/// sequence number validation, and any other monotonic stream check.

[[nodiscard]] inline constexpr auto is_monotonically_increasing(uint64_t previous, uint64_t current) noexcept -> bool {
    return current > previous;
}

// ONLY for Benchmarking and Testing. Not for production use.
namespace detail {

[[nodiscard]] auto scan_marker_u32_scalar(const uint8_t* buffer, size_t length, uint32_t marker) noexcept
    -> MarkerScanResult;

#if defined(STRATADB_ARCH_X86_64)

[[nodiscard]]
__attribute__((target("avx2,bmi"))) auto
scan_marker_u32_avx2(const uint8_t* buffer, size_t length, uint32_t marker) noexcept -> MarkerScanResult;

[[nodiscard]]
__attribute__((target("avx512bw,avx512vl"))) auto
scan_marker_u32_avx512(const uint8_t* buffer, size_t length, uint32_t marker) noexcept -> MarkerScanResult;

#elif defined(STRATADB_ARCH_AARCH64)

[[nodiscard]] auto scan_marker_u32_neon(const uint8_t* buffer, size_t length, uint32_t marker) noexcept
    -> MarkerScanResult;

#endif
} // namespace detail
} // namespace stratadb::utils::simd
