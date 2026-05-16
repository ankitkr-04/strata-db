#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>

namespace stratadb::wal {

// Represents the state of a partial flush for O_DIRECT RMW
struct FlushResult {
    std::span<const std::byte> memory_to_write;
    size_t new_flush_offset;
};

// Physical Wal block layout must be able to append KV pairs,
// execute sector-aligned partial flushes, and seal itself.
template <typename T>
concept WALBlockLayout =
    requires(T layout, std::span<const std::byte> key, std::span<const std::byte> value, uint64_t seq) {
        // Attempt to append, returning false if the block is out of space.
        { layout.append(key, value) } -> std::same_as<bool>;

        // Aligns the unwritten data to the next hardware sector boundary,
        // zero-pads the gap, and returns the exact span to pass to writev().
        // Does NOT seal the block.
        { layout.partial_flush() } -> std::same_as<FlushResult>;

        // Seals the block, computes necessary whole-block CRCs (like XXH3), stamps sequence.
        // Returns the final memory view to be handed to the I/O engine.
        { layout.finalize(seq) } -> std::same_as<std::span<const std::byte>>;
    };

} // namespace stratadb::wal