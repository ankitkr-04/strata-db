#include "stratadb/config/config_resolver.hpp"

#include "stratadb/config/immutable/wal_config.hpp"
#include "stratadb/utils/limits.hpp"
#include "stratadb/utils/os.hpp"
#include "stratadb/utils/probe.hpp"

#include <bit>
#include <cstdint>
#include <limits>
#include <optional>

namespace stratadb::config {

namespace {

[[nodiscard]] constexpr auto is_pow2(std::size_t v) noexcept -> bool {
    return v != 0 && std::has_single_bit(v);
}

[[nodiscard]] constexpr auto is_pow2_u8(std::uint8_t v) noexcept -> bool {
    return v != 0 && std::has_single_bit(static_cast<unsigned>(v));
}

[[nodiscard]] auto resolve_memory(MemoryConfig& mem) noexcept -> std::optional<ResolveError> {
    if (mem.block_alignment_bytes == 0) {
        mem.block_alignment_bytes = utils::system_page_size();
    }
    if (mem.total_budget_bytes <= mem.tlab_size_bytes) {
        return ResolveError::InvalidMemoryBudget;
    }
    if (mem.large_alloc_tlab_fraction == 0) {
        return ResolveError::InvalidLargeAllocFraction;
    }
    return std::nullopt;
}

[[nodiscard]] auto resolve_block_pool(BlockPoolConfig& pool, const MemoryConfig& mem) noexcept
    -> std::optional<ResolveError> {
    if (!is_pow2(pool.capacity) || pool.capacity > std::numeric_limits<std::uint16_t>::max()) {
        return ResolveError::InvalidBlockPoolCapacity;
    }
    if (!is_pow2(pool.block_size_bytes)) {
        return ResolveError::InvalidBlockPoolBlockSize;
    }
    if (pool.payload_alignment_bytes == 0) {
        // Depends on MemoryConfig being resolved first
        pool.payload_alignment_bytes = mem.block_alignment_bytes;
    }
    return std::nullopt;
}

[[nodiscard]] auto validate_skiplist(const SkipListConfig& skiplist) noexcept -> std::optional<ResolveError> {
    if (skiplist.max_height == 0 || skiplist.max_height > static_cast<std::uint8_t>(utils::MAX_SKIPLIST_HEIGHT)) {
        return ResolveError::InvalidSkipListHeight;
    }
    if (!is_pow2_u8(skiplist.branching_factor)) {
        return ResolveError::InvalidSkipListBranching;
    }
    return std::nullopt;
}

[[nodiscard]] auto validate_epoch(const EpochConfig& epoch) noexcept -> std::optional<ResolveError> {
    if (!is_pow2(epoch.reclaim_interval)) {
        return ResolveError::InvalidEpochReclaimInterval;
    }
    return std::nullopt;
}

[[nodiscard]] auto resolve_wal(WalConfig& wal) noexcept -> std::optional<ResolveError> {
    if (!is_pow2(wal.io_config.uring_queue_depth)) {
        return ResolveError::InvalidUringQueueDepth;
    }

    if (wal.spsc.mode == SpscMode::AutoDiscover) {
        if (auto core = utils::os::auto_discover_isolated_core(); core.has_value()) {
            wal.spsc.mode = SpscMode::ManualOverride;
            wal.spsc.core_id = core;
        } else {
            wal.spsc.mode = SpscMode::Disabled;
            wal.spsc.core_id = std::nullopt;
        }
    }

    if (wal.spsc.mode == SpscMode::ManualOverride) {
        if (!wal.spsc.core_id.has_value()) {
            return ResolveError::InvalidSpscConfig;
        }
        const std::uint32_t total_cores = utils::logical_core_count();
        if (wal.spsc.core_id.value() >= total_cores) {
            return ResolveError::InvalidSpscConfig;
        }
    }

    return std::nullopt;
}

[[nodiscard]] auto validate_memtable(const MemTableConfig& memtable) noexcept -> std::optional<ResolveError> {
    if (memtable.flush_trigger_bytes >= memtable.stall_trigger_bytes
        || memtable.stall_trigger_bytes >= memtable.max_size_bytes) {
        return ResolveError::InvalidFlushThresholds;
    }
    return std::nullopt;
}

[[nodiscard]] auto validate_wal_tuning(const WalTuning& tuning) noexcept -> std::optional<ResolveError> {
    const float ratio = tuning.precreate_trigger_ratio;
    if (ratio <= 0.0f || ratio >= 1.0f) {
        return ResolveError::InvalidPrecreateRatio;
    }
    if (tuning.max_staging_bytes == 0) {
        return ResolveError::InvalidMaxStagingBytes;
    }
    return std::nullopt;
}

} // namespace

auto ConfigResolver::resolve_immutable(ImmutableConfig cfg) noexcept -> std::expected<ImmutableConfig, ResolveError> {

    if (!is_pow2(cfg.block_size_bytes)) {
        return std::unexpected(ResolveError::InvalidBlockSize);
    }

    if (auto err = resolve_memory(cfg.memory)) {
        return std::unexpected(*err);
    }
    if (auto err = resolve_block_pool(cfg.block_pool, cfg.memory)) {
        return std::unexpected(*err);
    }
    if (auto err = validate_skiplist(cfg.skiplist)) {
        return std::unexpected(*err);
    }
    if (auto err = validate_epoch(cfg.epoch)) {
        return std::unexpected(*err);
    }
    if (auto err = resolve_wal(cfg.wal)) {
        return std::unexpected(*err);
    }

    return cfg;
}

auto ConfigResolver::resolve_mutable(MutableConfig cfg) noexcept -> std::expected<MutableConfig, ResolveError> {

    if (auto err = validate_memtable(cfg.memtable)) {
        return std::unexpected(*err);
    }
    if (auto err = validate_wal_tuning(cfg.wal_tuning)) {
        return std::unexpected(*err);
    }

    if (cfg.background_compaction_threads == 0) {
        return std::unexpected(ResolveError::InvalidCompactionThreads);
    }

    return cfg;
}

} // namespace stratadb::config