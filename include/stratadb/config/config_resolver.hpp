#pragma once

#include "stratadb/config/immutable_config.hpp"
#include "stratadb/config/mutable_config.hpp"

#include <cstdint>
#include <expected>

namespace stratadb::config {

enum class ResolveError : std::uint8_t {
    // ── Immutable ─────────────────────────────────────────────────────────────
    InvalidBlockSize,            // block_size_bytes: zero or not a power of two
    InvalidMemoryBudget,         // total_budget_bytes <= tlab_size_bytes
    InvalidLargeAllocFraction,   // large_alloc_tlab_fraction == 0
    InvalidUringQueueDepth,      // uring_queue_depth: not a power of two
    InvalidBlockPoolCapacity,    // capacity: not a power of two, or > 65535
    InvalidBlockPoolBlockSize,   // block_pool.block_size_bytes: not a power of two
    InvalidSkipListHeight,       // max_height: 0 or > MAX_SKIPLIST_HEIGHT
    InvalidSkipListBranching,    // branching_factor: not a power of two
    InvalidEpochReclaimInterval, // reclaim_interval: not a power of two
    InvalidSpscConfig,           // ManualOverride: core_id absent or >= logical_core_count

    // ── Mutable ───────────────────────────────────────────────────────────────
    InvalidFlushThresholds,   // flush_trigger >= stall_trigger, or stall_trigger >= max_size
    InvalidPrecreateRatio,    // precreate_trigger_ratio not in (0.0, 1.0)
    InvalidMaxStagingBytes,   // max_staging_bytes == 0
    InvalidCompactionThreads, // background_compaction_threads == 0
};

class ConfigResolver {
  public:
    // Probe hardware once and produce a fully trusted ImmutableConfig.
    //
    // What this does:
    //   - Fills memory.block_alignment_bytes from system_page_size() when 0.
    //   - Fills block_pool.payload_alignment_bytes from the resolved alignment when 0.
    //   - Resolves wal.spsc: AutoDiscover → probes sysfs for an isolated core,
    //     falls back to Disabled if none found.
    //   - Validates all power-of-two and range invariants.
    //
    // Post-condition on success: every sentinel-zero / optional field is populated;
    // downstream components receive the returned config without further validation.
    [[nodiscard]] static auto resolve_immutable(ImmutableConfig intent) noexcept
        -> std::expected<ImmutableConfig, ResolveError>;

    // Validate a proposed mutable config update against logical invariants.
    //
    // resolved_imm must be the result of a prior resolve_immutable() call.
    // Cross-config checks (e.g. memtable budget vs. Arena budget) live here.
    //
    // The ConfigManager's update path calls this before atomically swapping
    // the live MutableConfig, so no invalid state can ever be observed.
    [[nodiscard]] static auto resolve_mutable(MutableConfig intent) noexcept
        -> std::expected<MutableConfig, ResolveError>;
};

} // namespace stratadb::config