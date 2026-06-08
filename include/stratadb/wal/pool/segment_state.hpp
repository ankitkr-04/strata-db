#pragma once

#include "stratadb/io/unique_file_descriptor.hpp"

#include <atomic>
#include <cstdint>
#include <filesystem>

namespace stratadb::wal::pool {

// Lifecycle: Empty → Creating → Ready → Active → Sealed → (unlink + TRIM) → Empty
//
// Recyclable is gone. Sealed slots are deleted via std::filesystem::remove(),
// which triggers an SSD TRIM on the freed LBAs. The in-memory slot transitions
// directly to Empty, making it immediately available to the Vanguard for the
// next pre-allocation — no file overwrite, no FTL GC pressure.
enum class SegmentState : uint8_t {
    Empty = 0,    // No file on disk; ready for pre-allocation.
    Creating = 1, // Vanguard BG thread is fallocating/syncing header.
    Ready = 2,    // Pre-allocated and fdatasync'd; waiting for Flusher.
    Active = 3,   // Being written by Flusher.
    Sealed = 4,   // Full, footer synced, FD closed; waiting for GC to Unlink.
};

struct Segment {
    std::atomic<SegmentState> state{SegmentState::Empty};
    io::UniqueFd fd{};        // valid only during Creating, Ready, Active
    uint64_t sequence{0};     // monotonic; encoded in the filename
    uint64_t write_offset{0}; // absolute file offset; Flusher-owned after activation
    uint64_t min_lsn{0};      // LSN of first block; 0 = not yet written
    uint64_t max_lsn{0};      // LSN of last block; stamped into footer at seal time
    uint8_t pool_index{0};    // index into WalSegmentPool::segments_[]

    Segment() = default;
    ~Segment() = default;
    Segment(const Segment&) = delete;
    auto operator=(const Segment&) -> Segment& = delete;
    Segment(Segment&&) = delete;
    auto operator=(Segment&&) -> Segment& = delete;
};

} // namespace stratadb::wal::pool