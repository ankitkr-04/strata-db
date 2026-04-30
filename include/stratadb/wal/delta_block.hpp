#pragma once

#include "stratadb/wal/wal_concept.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace stratadb::wal {

// Architecture Delta: 4Kn Interleaved Checksums (HDD Optimized)
template <size_t BlockSize = 4096>
struct alignas(4096) DeltaBlock {
    static_assert(BlockSize % 4096 == 0, "Block size must be a multiple of 4Kn sectors");

    struct Header {
        uint64_t sequence_number{0};
        uint16_t record_count{0};
        uint16_t flags{0};
    } header;

    alignas(8) std::array<std::byte, BlockSize - sizeof(Header)> arena{};

    size_t current_offset{0};

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] auto append(std::span<const std::byte> key, std::span<const std::byte> value) -> bool {
        // TODO: THE MATH CHALLENGE
        return false;
    }

    auto finalize(uint64_t seq) -> std::span<const std::byte> {
        header.sequence_number = seq;
        const auto* block_start = reinterpret_cast<const std::byte*>(this);
        return {block_start, sizeof(Header) + current_offset};
    }
};

static_assert(WALBlockLayout<DeltaBlock<4096>>);

} // namespace stratadb::wal