#pragma once

#include "stratadb/wal/types.hpp"

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>

namespace stratadb::wal {
// ConcurrencyQueue
//
// Contract for the lock-free queues that bridge N writer threads to the single
// flusher thread.  Two implementations ship with StrataDB:
//   VyukovMpscQueue   — MPSC, wait-free producers (XCHG), lock-free consumer.
//   SpscMailboxQueue  — per-thread SPSC ring buffers; zero push contention,
//                     O(MAX_THREADS) consumer sweep
template <typename Q>
concept ConcurrencyQueue = requires(Q q, MpscNode* node, std::atomic<bool>& stop) {
    { q.push(node) } -> std::same_as<void>;
    { q.pop() } -> std::same_as<PopResultData>;
    { q.wait_for_work(stop) } -> std::same_as<void>;
    { q.force_wakeup() } -> std::same_as<void>;
};
// WALBlockLayout
//
// Contract for physical write-buffer formats (GammaBlock, DeltaBlock).
// WalPipeline is templated on Layout so every dispatch resolves at compile
// time — no virtual calls on the hot write path
template <typename L>
concept WALBlockLayout =
    requires(L layout, std::span<const std::byte> key, std::span<const std::byte> value, std::uint64_t seq) {
        // Reset internal write cursor and stamp the initial sequence number.
        { layout.init(seq) } -> std::same_as<void>;

        // Attempt to append a KV record.  Returns false when the block is full.
        { layout.append(key, value) } -> std::same_as<bool>;

        // Advance the write cursor to the next sector boundary, zero-pad the gap,
        // and return the exact span to submit via writev().  Does NOT seal the block.
        { layout.partial_flush() } -> std::same_as<FlushResult>;

        // Seal the block: compute whole-block checksums, return the final memory
        // view for the last I/O submission.
        { layout.finalize(seq) } -> std::same_as<FlushResult>;
    };

} // namespace stratadb::wal