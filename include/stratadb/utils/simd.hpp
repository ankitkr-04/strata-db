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

inline constexpr std::size_t SIMD_BUFFER_ALIGNMENT = 64;
inline constexpr std::size_t MAX_SCAN_RESULTS = 64;

// Maximum sectors validate_sector_checksums() can handle in one call.
// Bound by valid_sector_mask width (uint64_t = 64 bits).
inline constexpr std::uint8_t MAX_SECTOR_COUNT = 64;

struct MarkerScanResult {
    std::uint32_t offsets[MAX_SCAN_RESULTS]{};
    std::uint32_t count{0};
};

struct SectorValidationResult {
    // Bit N is set iff sector N passed its checksum.
    std::uint64_t valid_sector_mask{0};
    // Index of the first sector that failed. UINT8_MAX = no failure.
    std::uint8_t first_failed_sector{UINT8_MAX};
    bool all_valid{false};
};

enum class SimdLevel : std::uint8_t {
    Scalar = 0,
    Neon = 1,   // ARM NEON (AArch64)
    Avx2 = 2,   // x86-64 AVX2 + BMI
    Avx512 = 3, // x86-64 AVX-512BW/VL
};

// Must be called once at process start before any scan / validate calls.
void init() noexcept;

// Returns the level chosen by init(). Returns Scalar if init() not yet called.
[[nodiscard]] auto detected_level() noexcept -> SimdLevel;

[[nodiscard]] auto level_name(SimdLevel level) noexcept -> const char*;

// Scans [buffer, buffer + length) for every occurrence of the 4-byte
// little-endian `marker`. Returns relative byte offsets of each match.
//
// Preconditions (debug-asserted):
//   - buffer is SIMD_BUFFER_ALIGNMENT-aligned.
//   - length >= 4 (returns empty result otherwise — no crash).
[[nodiscard]] auto scan_marker_u32(const std::uint8_t* buffer, std::size_t length, std::uint32_t marker) noexcept
    -> MarkerScanResult;

// Validates sector_count contiguous 512-byte sectors starting at sector_data
// against expected_checksums (xxhash3-64 truncated to 32 bits, write-side must match).
//
// sector_count must be <= MAX_SECTOR_COUNT (64). Debug-asserted.
[[nodiscard]] auto validate_sector_checksums(const std::uint8_t* sector_data,
                                             std::uint8_t sector_count,
                                             const std::uint32_t* expected_checksums) noexcept
    -> SectorValidationResult;

[[nodiscard]] inline constexpr auto is_monotonically_increasing(std::uint64_t previous, std::uint64_t current) noexcept
    -> bool {
    return current > previous;
}

// ── Benchmark / test internals ────────────────────────────────────────────────
// NOT for production use.
namespace detail {

[[nodiscard]] auto scan_marker_u32_scalar(const std::uint8_t* buffer, std::size_t length, std::uint32_t marker) noexcept
    -> MarkerScanResult;

#if defined(STRATADB_ARCH_X86_64)

[[nodiscard]]
__attribute__((target("avx2,bmi"))) auto
scan_marker_u32_avx2(const std::uint8_t* buffer, std::size_t length, std::uint32_t marker) noexcept -> MarkerScanResult;

[[nodiscard]]
__attribute__((target("avx512bw,avx512vl"))) auto
scan_marker_u32_avx512(const std::uint8_t* buffer, std::size_t length, std::uint32_t marker) noexcept
    -> MarkerScanResult;

#elif defined(STRATADB_ARCH_AARCH64)

[[nodiscard]]
auto scan_marker_u32_neon(const std::uint8_t* buffer, std::size_t length, std::uint32_t marker) noexcept
    -> MarkerScanResult;

#endif

} // namespace detail
} // namespace stratadb::utils::simd