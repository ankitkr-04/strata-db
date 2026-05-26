#pragma once

#include "stratadb/io/unique_file_descriptor.hpp"

#include <atomic>
#include <cstdint>
#include <filesystem>

namespace stratadb::wal::ring {
enum class WalSlotState : uint8_t {
    Empty = 0,      // No file on disk, or awaiting BG recycle
    Creating = 1,   // BG: fallocate + header write in progress
    Ready = 2,      // Pre-allocated, fdatasync'd; awaiting Flusher activation
    Active = 3,     // Flusher is writing blocks here (single owner)
    Sealed = 4,     // data on disk; NOT safe to recycle until checkpoint confirms
    Recyclable = 5, // engine confirmed all LSNs flushed to SSTables; safe to overwrite
};

struct WalSlot {
    std::atomic<WalSlotState> state{WalSlotState::Empty};
    io::UniqueFd fd{};
    uint64_t sequence{0};         // slot_sequence from on-disk header
    uint64_t write_offset{0};     // absolute file offset; Flusher-only
    uint64_t min_lsn{0};          // LSN of first block; 0 = none yet
    uint64_t max_lsn{0};          // LSN of last block written
    std::filesystem::path path{}; // wal_slot_NNN.wal; for logging / re-open
    uint8_t ring_index{0};        // index into WalRing::slots_[]

    WalSlot() = default;
    ~WalSlot() = default;
    WalSlot(const WalSlot&) = delete;
    auto operator=(const WalSlot&) -> WalSlot& = delete;
    WalSlot(WalSlot&&) = delete;
    auto operator=(WalSlot&&) -> WalSlot& = delete;
};

} // namespace stratadb::wal::ring