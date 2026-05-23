#pragma once

#include "stratadb/utils/hash.hpp"
#include "stratadb/utils/simd.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>

#define XXH_INLINE_ALL
#include <xxhash.h>

namespace stratadb::wal::reader {

enum class BlockLayout : uint8_t {
    Gamma4K = 0,
    Gamma16K = 1,
    Delta4K = 2,
};

struct BlockValidationResult {
    bool checksum_ok{false};

    uint8_t first_failed_sector{UINT8_MAX};

    uint32_t valid_data_end_offset{0};
};

enum class TearDefense : uint8_t {
    None = 0,
    MarkerMissing = 1,
    ChecksumFailed = 2,
    LsnRegression = 3,
};

struct TearCheckResult {
    TearDefense defense{TearDefense::None};
    uint8_t failed_sector_index{UINT8_MAX};
    uint32_t valid_data_end_offset{0};
    bool is_valid{true};
};

inline constexpr uint32_t WALR_MARKER = 0x57414C52U;

inline constexpr size_t DELTA_SECTOR_SIZE = 4096;
inline constexpr size_t DELTA_CRC_SIZE = sizeof(uint32_t);
inline constexpr size_t DELTA_CRC_OFFSET = DELTA_SECTOR_SIZE - DELTA_CRC_SIZE;
inline constexpr size_t DELTA_DATA_BYTES = DELTA_CRC_OFFSET;

[[nodiscard]] inline auto scan_wal_record_boundaries(const uint8_t* buffer, size_t length) noexcept
    -> utils::simd::MarkerScanResult {
    return utils::simd::scan_marker_u32(buffer, length, WALR_MARKER);
}

[[nodiscard]] inline auto validate_gamma_block(const uint8_t* block_data, size_t block_size, size_t valid_end) noexcept
    -> BlockValidationResult {

    BlockValidationResult result{};

    if (valid_end == 0 || valid_end > block_size) {
        result.checksum_ok = false;
        result.valid_data_end_offset = 0;
        return result;
    }

    XXH128_hash_t stored_hash{};
    std::memcpy(&stored_hash, block_data + sizeof(uint64_t), sizeof(XXH128_hash_t));

    alignas(8) uint8_t header_copy[32];
    std::memcpy(header_copy, block_data, sizeof(header_copy));
    std::memset(header_copy + sizeof(uint64_t), 0, sizeof(XXH128_hash_t));


    XXH3_state_t* state = XXH3_createState();
    XXH3_128bits_reset(state);
    XXH3_128bits_update(state, header_copy, sizeof(header_copy));
    if (valid_end > sizeof(header_copy)) {
        XXH3_128bits_update(state, block_data + sizeof(header_copy), valid_end - sizeof(header_copy));
    }
    const XXH128_hash_t computed = XXH3_128bits_digest(state);
    XXH3_freeState(state);

    const bool ok = (computed.low64 == stored_hash.low64) && (computed.high64 == stored_hash.high64);

    result.checksum_ok = ok;
    result.first_failed_sector = UINT8_MAX;
    result.valid_data_end_offset = ok ? static_cast<uint32_t>(valid_end) : 0U;

    return result;
}

[[nodiscard]] inline auto validate_delta_block(const uint8_t* block_data,
    //NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                                               size_t block_size,
                                               size_t written_end) noexcept -> BlockValidationResult {

    BlockValidationResult result{};

    const size_t sector_count = block_size / DELTA_SECTOR_SIZE;
    const size_t sectors_written = (written_end + DELTA_SECTOR_SIZE - 1U) / DELTA_SECTOR_SIZE;
    const size_t sectors_to_check = (sectors_written < sector_count) ? sectors_written : sector_count;

    for (size_t s = 0; s < sectors_to_check; ++s) {
        const uint8_t* sector_ptr = block_data + (s * DELTA_SECTOR_SIZE);

        uint32_t stored_crc = 0;
        std::memcpy(&stored_crc, sector_ptr + DELTA_CRC_OFFSET, sizeof(stored_crc));

        const uint32_t computed_crc = utils::crc32c(sector_ptr, DELTA_CRC_OFFSET);

        if (computed_crc != stored_crc) {
            result.checksum_ok = false;
            result.first_failed_sector = static_cast<uint8_t>(s);
            result.valid_data_end_offset = static_cast<uint32_t>(s * DELTA_SECTOR_SIZE);
            return result;
        }
    }

    result.checksum_ok = true;
    result.first_failed_sector = UINT8_MAX;
    result.valid_data_end_offset = static_cast<uint32_t>(written_end);
    return result;
}

[[nodiscard]] inline auto
validate_block(BlockLayout layout, const uint8_t* block_data, size_t block_size, size_t written_end) noexcept
    -> BlockValidationResult {

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

[[nodiscard]] inline auto check_for_tear(bool marker_found,
                                         const BlockValidationResult& validation,
                                         uint64_t lsn_previous,
                                         uint64_t lsn_current) noexcept -> TearCheckResult {

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