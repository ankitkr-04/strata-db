#pragma once

#include "stratadb/utils/cache.hpp"
#include "stratadb/utils/hash.hpp"
#include "stratadb/utils/simd.hpp"
#include "stratadb/utils/xxhash.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace stratadb::wal::reader {

// Block layout tags — tell the validator which checksum scheme to apply.
enum class BlockLayout : std::uint8_t {
    Gamma4K = 0,  // GammaBlock<4096>  — XXH3-128 whole-block
    Gamma16K = 1, // GammaBlock<16384> — XXH3-128 whole-block
    Delta4K = 2,  // DeltaBlock<4096>  — CRC32c per 4 KiB sector
};

struct BlockValidationResult {
    bool checksum_ok{false};
    std::uint8_t first_failed_sector{UINT8_MAX}; // UINT8_MAX = no failure
    std::uint32_t valid_data_end_offset{0};
};

// Torn-write defense layers — ordered by detection cost (cheapest first).
enum class TearDefense : std::uint8_t {
    None = 0,           // block is valid
    MarkerMissing = 1,  // WALR frame marker absent
    ChecksumFailed = 2, // CRC32c or XXH3-128 mismatch
    LsnRegression = 3,  // sequence number did not advance monotonically
};

struct TearCheckResult {
    TearDefense defense{TearDefense::None};
    std::uint8_t failed_sector_index{UINT8_MAX};
    std::uint32_t valid_data_end_offset{0};
    bool is_valid{true};
};

// 4-byte little-endian frame marker: "WALR"
inline constexpr std::uint32_t WALR_MARKER = 0x57414C52U;

// DeltaBlock sector geometry constants.
inline constexpr std::size_t DELTA_SECTOR_SIZE = 4096;
inline constexpr std::size_t DELTA_CRC_SIZE = sizeof(std::uint32_t);
inline constexpr std::size_t DELTA_CRC_OFFSET = DELTA_SECTOR_SIZE - DELTA_CRC_SIZE;

// scan_wal_record_boundaries
//
// SIMD scan for WALR_MARKER byte offsets inside a raw WAL buffer.
// Delegates to utils::simd::scan_marker_u32 which dispatches to AVX-512,
// AVX2, NEON, or scalar at runtime based on utils::simd::init() detection.
[[nodiscard]] inline auto scan_wal_record_boundaries(const std::uint8_t* buffer, std::size_t length) noexcept
    -> utils::simd::MarkerScanResult {
    return utils::simd::scan_marker_u32(buffer, length, WALR_MARKER);
}

// validate_gamma_block
//
// Re-computes the XXH3-128 digest over [0, valid_end) with the stored hash
// field zeroed, then compares against the stored value.
[[nodiscard]] inline auto validate_gamma_block(const std::uint8_t* block_data,
                                               std::size_t block_size,
                                               std::size_t valid_end) noexcept -> BlockValidationResult {
    BlockValidationResult result{};

    if (valid_end == 0 || valid_end > block_size) {
        return result; // checksum_ok = false by default
    }

    // Extract the stored hash (bytes 8..24 of the 32-byte GammaBlock header).
    XXH128_hash_t stored_hash{};
    std::memcpy(&stored_hash, block_data + sizeof(std::uint64_t), sizeof(XXH128_hash_t));

    // Re-hash with the hash field zeroed so the digest is reproducible.
    alignas(8) std::uint8_t header_copy[32];
    std::memcpy(header_copy, block_data, sizeof(header_copy));
    std::memset(header_copy + sizeof(std::uint64_t), 0, sizeof(XXH128_hash_t));

    // Stack-allocate the streaming state; align to a cache line so XXH3's
    // internal SIMD loads don't straddle a boundary.
    alignas(utils::CACHE_LINE_SIZE) XXH3_state_t state;
    utils::xxhash3_128_reset(state);
    utils::xxhash3_128_update(state, header_copy, sizeof(header_copy));
    if (valid_end > sizeof(header_copy)) {
        utils::xxhash3_128_update(state, block_data + sizeof(header_copy), valid_end - sizeof(header_copy));
    }
    const XXH128_hash_t computed = utils::xxhash3_128_digest(state);

    const bool ok = (computed.low64 == stored_hash.low64) && (computed.high64 == stored_hash.high64);

    result.checksum_ok = ok;
    result.first_failed_sector = UINT8_MAX;
    result.valid_data_end_offset = ok ? static_cast<std::uint32_t>(valid_end) : 0U;
    return result;
}

// validate_delta_block
//
// Verifies CRC32c for each written 4 KiB sector.  Stops at the first failure
// and reports which sector failed and the last good byte offset.
[[nodiscard]] inline auto validate_delta_block(const std::uint8_t* block_data,
                                               // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                                               std::size_t block_size,
                                               std::size_t written_end) noexcept -> BlockValidationResult {
    BlockValidationResult result{};

    const std::size_t total_sectors = block_size / DELTA_SECTOR_SIZE;
    const std::size_t sectors_written = (written_end + DELTA_SECTOR_SIZE - 1U) / DELTA_SECTOR_SIZE;
    const std::size_t sectors_to_check = sectors_written < total_sectors ? sectors_written : total_sectors;

    for (std::size_t s = 0; s < sectors_to_check; ++s) {
        const std::uint8_t* sector = block_data + (s * DELTA_SECTOR_SIZE);

        std::uint32_t stored_crc = 0;
        std::memcpy(&stored_crc, sector + DELTA_CRC_OFFSET, sizeof(stored_crc));

        const std::uint32_t computed_crc = utils::crc32c(sector, DELTA_CRC_OFFSET);

        if (computed_crc != stored_crc) {
            result.checksum_ok = false;
            result.first_failed_sector = static_cast<std::uint8_t>(s);
            result.valid_data_end_offset = static_cast<std::uint32_t>(s * DELTA_SECTOR_SIZE);
            return result;
        }
    }

    result.checksum_ok = true;
    result.first_failed_sector = UINT8_MAX;
    result.valid_data_end_offset = static_cast<std::uint32_t>(written_end);
    return result;
}

// validate_block — dispatcher
[[nodiscard]] inline auto validate_block(BlockLayout layout,
                                         const std::uint8_t* block_data,
                                         std::size_t block_size,
                                         std::size_t written_end) noexcept -> BlockValidationResult {
    switch (layout) {
        case BlockLayout::Gamma4K:
        case BlockLayout::Gamma16K:
            return validate_gamma_block(block_data, block_size, written_end);
        case BlockLayout::Delta4K:
            return validate_delta_block(block_data, block_size, written_end);
    }
    return BlockValidationResult{
        .checksum_ok = false,
        .first_failed_sector = 0,
        .valid_data_end_offset = 0,
    };
}

// check_for_tear — three-layer torn-write defense
//
// Layers (cheapest to most expensive):
//   1. WALR frame marker must be present.
//   2. Block checksum must pass.
//   3. LSN must be strictly monotonically increasing.
[[nodiscard]] inline auto check_for_tear(bool marker_found,
                                         const BlockValidationResult& validation,
                                         std::uint64_t lsn_previous,
                                         std::uint64_t lsn_current) noexcept -> TearCheckResult {
    if (!marker_found) {
        return TearCheckResult{
            .defense = TearDefense::MarkerMissing,
            .failed_sector_index = UINT8_MAX,
            .valid_data_end_offset = 0,
            .is_valid = false,
        };
    }

    if (!validation.checksum_ok) {
        return TearCheckResult{
            .defense = TearDefense::ChecksumFailed,
            .failed_sector_index = validation.first_failed_sector,
            .valid_data_end_offset = validation.valid_data_end_offset,
            .is_valid = false,
        };
    }

    if (!utils::simd::is_monotonically_increasing(lsn_previous, lsn_current)) {
        return TearCheckResult{
            .defense = TearDefense::LsnRegression,
            .failed_sector_index = UINT8_MAX,
            .valid_data_end_offset = 0,
            .is_valid = false,
        };
    }

    return TearCheckResult{
        .defense = TearDefense::None,
        .failed_sector_index = UINT8_MAX,
        .valid_data_end_offset = validation.valid_data_end_offset,
        .is_valid = true,
    };
}

} // namespace stratadb::wal::reader