#pragma once

#include "stratadb/utils/hash.hpp"
#include "stratadb/wal/wal_concept.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

namespace stratadb::wal {

// Architecture Delta: 4Kn Interleaved Checksums (HDD Optimized)
template <size_t BlockSize = 4096>
struct alignas(4096) DeltaBlock {
    static_assert(BlockSize % 4096 == 0, "Block size must be a multiple of 4Kn sectors");
    static constexpr size_t SECTOR_SIZE = 4096;
    static constexpr size_t SECTOR_MASK = SECTOR_SIZE - 1;
    static constexpr size_t CRC_OFFSET = 4092;

    struct Header {
        uint64_t sequence_number{0};
        uint16_t record_count{0};
        uint16_t flags{0};
        uint32_t _padding{0};
    } header;

    static_assert(sizeof(Header) == 16, "Header must be exactly 16 bytes");

    alignas(8) std::array<std::byte, BlockSize - sizeof(Header)> arena{};

    size_t current_offset{0};

    [[nodiscard]] inline auto absolute_offset() const noexcept -> size_t {
        return sizeof(Header) + current_offset;
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] auto append(std::span<const std::byte> key, std::span<const std::byte> value) -> bool {
        const auto k_len = static_cast<uint32_t>(key.size());
        const auto v_len = static_cast<uint32_t>(value.size());
        const size_t total_required = sizeof(k_len) + sizeof(v_len) + k_len + v_len;

        size_t simulated_abs = absolute_offset();
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

        if (simulated_abs > BlockSize) {
            return false; // Not enough space
        }

        const std::array<std::span<const std::byte>, 4> parts =
            {std::span<const std::byte>(reinterpret_cast<const std::byte*>(&k_len), sizeof(k_len)),
             std::span<const std::byte>(reinterpret_cast<const std::byte*>(&v_len), sizeof(v_len)),
             key,
             value};

        for (auto part : parts) {
            while (!part.empty()) {
                size_t abs_offset = absolute_offset();
                size_t intra = abs_offset & SECTOR_MASK;
                if (intra == CRC_OFFSET) {
                    const auto* block_start = reinterpret_cast<const std::byte*>(this);
                    uint32_t crc = utils::crc32c(block_start, (abs_offset - CRC_OFFSET));

                    std::memcpy(arena.data() + current_offset, &crc, sizeof(crc));
                    current_offset += 4;
                    abs_offset += 4;
                    intra = 0;
                }

                size_t chunk = std::min(part.size(), CRC_OFFSET - intra);
                std::memcpy(arena.data() + current_offset, part.data(), chunk);
                current_offset += chunk;
                part = part.subspan(chunk);
            }
        }

        header.record_count++;
        return true;
    }

    auto finalize(uint64_t seq) -> std::span<const std::byte> {
        header.sequence_number = seq;

        const size_t abs_offset = absolute_offset();

        if ((abs_offset & SECTOR_MASK) != 0) {
            const size_t sector_start = abs_offset & ~SECTOR_MASK;
            const size_t crc_abs_offset = sector_start + CRC_OFFSET;

            const auto* block_start = reinterpret_cast<const std::byte*>(this);

            uint32_t tail_crc = utils::crc32c(block_start + sector_start, CRC_OFFSET);

            std::memcpy(arena.data() + (crc_abs_offset - sizeof(Header)), &tail_crc, sizeof(tail_crc));
        }

        const size_t final_size = (abs_offset + SECTOR_MASK) & ~SECTOR_MASK;

        return {reinterpret_cast<const std::byte*>(this), final_size};
    }
};

static_assert(WALBlockLayout<DeltaBlock<4096>>);

} // namespace stratadb::wal