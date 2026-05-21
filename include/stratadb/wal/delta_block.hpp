#pragma once

#include "stratadb/utils/hash.hpp"
#include "wal_concept.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

namespace stratadb::wal {

template <size_t BlockSize = 4096>
struct alignas(4096) DeltaBlock {
    static_assert(BlockSize % 4096 == 0, "Block size must be a multiple of 4Kn sectors");
    static constexpr size_t SECTOR_SIZE = 4096;
    static constexpr size_t SECTOR_MASK = SECTOR_SIZE - 1;
    static constexpr std::size_t CRC_FIELD_SIZE = sizeof(std::uint32_t);
    static constexpr std::size_t CRC_OFFSET = SECTOR_SIZE - CRC_FIELD_SIZE;

    struct Header {
        uint64_t sequence_number{0};
        uint16_t record_count{0};
        uint16_t flags{0};
        uint32_t _padding{0};
    } header;

    static_assert(sizeof(Header) == 16, "Header must be exactly 16 bytes");

    alignas(8) std::array<std::byte, BlockSize - sizeof(Header)> arena{};

    size_t append_offset_{sizeof(Header)};
    size_t flush_offset_{0};

    void init(uint64_t seq) noexcept {
        header.sequence_number = seq;
        header.record_count = 0;
        header.flags = 0;
        append_offset_ = sizeof(Header);
        flush_offset_ = 0;
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] auto append(std::span<const std::byte> key, std::span<const std::byte> value) noexcept -> bool {
        const auto k_len = static_cast<uint32_t>(key.size());
        const auto v_len = static_cast<uint32_t>(value.size());
        const size_t total_required = sizeof(k_len) + sizeof(v_len) + k_len + v_len;

        size_t simulated_abs = append_offset_;
        size_t remaining_payload = total_required;

        while (remaining_payload > 0) {
            size_t intra = simulated_abs & SECTOR_MASK;
            if (intra == CRC_OFFSET) {
                simulated_abs += 4; // Skip CRC space
                intra = 0;
            }
            size_t chunk = std::min(remaining_payload, CRC_OFFSET - intra);
            simulated_abs += chunk;
            remaining_payload -= chunk;
        }

        if (simulated_abs > BlockSize)
            return false;

        const std::array<std::span<const std::byte>, 4> parts =
            {std::span<const std::byte>(reinterpret_cast<const std::byte*>(&k_len), sizeof(k_len)),
             std::span<const std::byte>(reinterpret_cast<const std::byte*>(&v_len), sizeof(v_len)),
             key,
             value};

        auto* block_start = reinterpret_cast<std::byte*>(this);

        for (auto part : parts) {
            while (!part.empty()) {
                size_t intra = append_offset_ & SECTOR_MASK;
                if (intra == CRC_OFFSET) {
                    uint32_t crc = utils::crc32c(block_start + (append_offset_ & ~SECTOR_MASK), CRC_OFFSET);
                    std::memcpy(block_start + append_offset_, &crc, sizeof(crc));
                    append_offset_ += 4;
                    intra = 0;
                }

                size_t chunk = std::min(part.size(), CRC_OFFSET - intra);
                std::memcpy(block_start + append_offset_, part.data(), chunk);
                append_offset_ += chunk;
                part = part.subspan(chunk);
            }
        }

        header.record_count++;
        return true;
    }

    [[nodiscard]] auto partial_flush() noexcept -> FlushResult {
        size_t start_offset = flush_offset_;
        auto* block_start = reinterpret_cast<std::byte*>(this);

        if ((append_offset_ & SECTOR_MASK) != 0) {
            size_t sector_start = append_offset_ & ~SECTOR_MASK;
            size_t crc_abs_offset = sector_start + CRC_OFFSET;

            if (append_offset_ < crc_abs_offset) {
                std::memset(block_start + append_offset_, 0, crc_abs_offset - append_offset_);
            }

            uint32_t tail_crc = utils::crc32c(block_start + sector_start, CRC_OFFSET);
            std::memcpy(block_start + crc_abs_offset, &tail_crc, sizeof(tail_crc));

            // HDD PHYSICS: Mechanical seek penalty avoidance!
            // We seal this sector completely. The next append goes to the next sector boundary.
            append_offset_ = sector_start + SECTOR_SIZE;
        }

        auto span = std::span<const std::byte>(reinterpret_cast<const std::byte*>(this) + start_offset,
                                               append_offset_ - start_offset);

        flush_offset_ = append_offset_;

        return FlushResult{.memory_to_write = span,
                           .block_internal_offset = start_offset,
                           .max_lsn = header.sequence_number};
    }

    [[nodiscard]] auto finalize(std::uint64_t /*seq*/) noexcept -> FlushResult {

        // Because DeltaBlock uses per-sector checksums, sealing the block is identical
        // to doing a partial flush of the remaining unwritten data.
        return partial_flush();
    }
};

static_assert(WALBlockLayout<DeltaBlock<4096>>);

} // namespace stratadb::wal