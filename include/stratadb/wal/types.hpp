#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>

namespace stratadb::wal {

// MpscNode — intrusive linked-list node; the base currency of WAL queues.
//
// Every object passed through a WAL queue (FlushResult, shutdown sentinels)
// embeds MpscNode so queue linkage requires zero extra allocation.
struct MpscNode {
    std::atomic<MpscNode*> next{nullptr};

    // true  → this node was placement-new'd inside a BlockPool block and must
    //         be returned to the pool by the flusher after processing.
    // false → lives in static / stack storage (e.g. per-thread sentinel array);
    //         the flusher must NOT attempt pool release.
    bool pool_managed{true};
};

// PopResultData — value returned by queue::pop().
//
// payload_node : carries the flusher's work item (cast to FlushResult*).
//               nullptr → queue was empty or a producer is mid-push.
// node_to_free : memory now safe to recycle back to the BlockPool.
//               Vyukov MPSC : the old dummy head (differs from payload_node).
//               SPSC mailbox: equals payload_node.
struct PopResultData {
    MpscNode* payload_node{nullptr};
    MpscNode* node_to_free{nullptr};
};

// FlushResult — produced by WalPipeline, consumed by the flusher thread.
//
// Placement-new'd at offset 0 of each BlockPool block so that no separate
// heap allocation is needed for queue linkage.  The WAL Layout object starts
// at WalPipeline::LAYOUT_OFFSET bytes into the same block.
//
// Inherits MpscNode so it can be pushed into any queue without a wrapper.
struct FlushResult : MpscNode {
    // Exact byte span to hand to the I/O engine.
    // May be a partial-sector slice (partial_flush) or the full sealed block
    // (finalize), depending on how the pipeline triggered the flush.
    std::span<const std::byte> memory_to_write{};

    // Byte offset within the pool block where memory_to_write begins.
    // The flusher adds this to current_file_offset_ to compute the physical
    // pwritev offset.
    std::size_t block_internal_offset{0};

    // Highest LSN contained in memory_to_write.
    // After a successful fdatasync the flusher advances durable_lsn_ to this
    // value and wakes all sleeping writers via atomic::notify_all.
    std::uint64_t max_lsn{0};
};

} // namespace stratadb::wal