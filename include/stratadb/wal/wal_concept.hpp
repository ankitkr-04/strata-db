#pragma once

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>

namespace stratadb::wal {

// The intrusive base class. Any payload (like FlushResult or GammaBlock)
// that needs to be passed through the queue must inherit from this or wrap it.
struct MpscNode {
    std::atomic<MpscNode*> next{nullptr};
};

template <typename T>
concept ConcurrencyQueue = requires(T q, MpscNode* node) {
    { q.push(node) } -> std::same_as<void>;
    { q.pop() } -> std::same_as<MpscNode*>;
    { q.wait_for_work() } -> std::same_as<void>;
};

// Represents the state of a partial flush for O_DIRECT RMW
struct FlushResult : public MpscNode {
    std::span<const std::byte> memory_to_write;
    std::size_t block_internal_offset;
};

// Physical Wal block layout must be able to append KV pairs,
// execute sector-aligned partial flushes, and seal itself.
template <typename T>
concept WALBlockLayout =
    requires(T layout, std::span<const std::byte> key, std::span<const std::byte> value, uint64_t seq) {
        // Initialize the block with its base sequence number before any appends.
        { layout.init(seq) } -> std::same_as<void>;

        // Attempt to append, returning false if the block is out of space.
        { layout.append(key, value) } -> std::same_as<bool>;

        // Aligns the unwritten data to the next hardware sector boundary,
        // zero-pads the gap, and returns the exact span to pass to writev().
        // Does NOT seal the block.
        { layout.partial_flush() } -> std::same_as<FlushResult>;

        // Seals the block, computes necessary whole-block CRCs (like XXH3), stamps sequence.
        // Returns the final memory view to be handed to the I/O engine.
        { layout.finalize(seq) } -> std::same_as<FlushResult>;
    };

} // namespace stratadb::wal