#pragma once
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>

namespace stratadb::wal {
// Physical Wal block layout must be able to append KV pairs
//  and seal itself for I/O handoff
template <typename T>
concept WALBlockLayout =
    requires(T layout, std::span<const std::byte> key, std::span<const std::byte> value, uint64_t seq) {
        // Attempt to append, returning false if the block is full and needs to be sealed.
        { layout.append(key, value) } -> std::same_as<bool>;

        // Seal the block, compute necessary CRCs, stamp sequence.
        // Returns the exact memory view to be handed to the I/O engine.
        { layout.finalize(seq) } -> std::same_as<std::span<const std::byte>>;
    };

} // namespace stratadb::wal