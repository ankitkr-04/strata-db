#include "stratadb/config/config_resolver.hpp"

#include "stratadb/config/immutable/wal_config.hpp"
#include "stratadb/utils/limits.hpp"
#include "stratadb/utils/os.hpp"
#include "stratadb/utils/probe.hpp"

#include <bit>
#include <cstdint>
#include <limits>

namespace stratadb::config {

namespace {

[[nodiscard]] constexpr auto is_pow2(std::size_t v) noexcept -> bool {
    return v != 0 && std::has_single_bit(v);
}

[[nodiscard]] constexpr auto is_pow2_u8(std::uint8_t v) noexcept -> bool {
    return v != 0 && std::has_single_bit(static_cast<unsigned>(v));
}

} // namespace

auto ConfigResolver::resolve_immutable(ImmutableConfig cfg) noexcept -> std::expected<ImmutableConfig, ResolveError> {

    if (!is_pow2(cfg.block_size_bytes)) {
        return std::unexpected(ResolveError::InvalidBlockSize);
    }

    if (cfg.memory.block_alignment_bytes == 0) {
        cfg.memory.block_alignment_bytes = utils::system_page_size();
    }
    if (cfg.memory.total_budget_bytes <= cfg.memory.tlab_size_bytes) {
        return std::unexpected(ResolveError::InvalidMemoryBudget);
    }
    if (cfg.memory.large_alloc_tlab_fraction == 0) {
        return std::unexpected(ResolveError::InvalidLargeAllocFraction);
    }

    if (!is_pow2(cfg.block_pool.capacity) || cfg.block_pool.capacity > std::numeric_limits<std::uint16_t>::max()) {
        return std::unexpected(ResolveError::InvalidBlockPoolCapacity);
    }
    if (!is_pow2(cfg.block_pool.block_size_bytes)) {
        return std::unexpected(ResolveError::InvalidBlockPoolBlockSize);
    }
    if (cfg.block_pool.payload_alignment_bytes == 0) {
        cfg.block_pool.payload_alignment_bytes = cfg.memory.block_alignment_bytes;
    }

    if (cfg.skiplist.max_height == 0
        || cfg.skiplist.max_height > static_cast<std::uint8_t>(utils::MAX_SKIPLIST_HEIGHT)) {
        return std::unexpected(ResolveError::InvalidSkipListHeight);
    }
    if (!is_pow2_u8(cfg.skiplist.branching_factor)) {
        return std::unexpected(ResolveError::InvalidSkipListBranching);
    }

    if (!is_pow2(cfg.epoch.reclaim_interval)) {
        return std::unexpected(ResolveError::InvalidEpochReclaimInterval);
    }

    if (!is_pow2(cfg.wal.io_config.uring_queue_depth)) {
        return std::unexpected(ResolveError::InvalidUringQueueDepth);
    }

    if (cfg.wal.spsc.mode == SpscMode::AutoDiscover) {
        if (auto core = utils::os::auto_discover_isolated_core(); core.has_value()) {
            cfg.wal.spsc.mode = SpscMode::ManualOverride;
            cfg.wal.spsc.core_id = core;
        } else {
            cfg.wal.spsc.mode = SpscMode::Disabled;
            cfg.wal.spsc.core_id = std::nullopt;
        }
    }
    if (cfg.wal.spsc.mode == SpscMode::ManualOverride) {
        if (!cfg.wal.spsc.core_id.has_value()) {
            return std::unexpected(ResolveError::InvalidSpscConfig);
        }
        const std::uint32_t total_cores = utils::logical_core_count();
        if (cfg.wal.spsc.core_id.value() >= total_cores) {
            return std::unexpected(ResolveError::InvalidSpscConfig);
        }
    }

    return cfg;
}

auto ConfigResolver::resolve_mutable(MutableConfig cfg) noexcept -> std::expected<MutableConfig, ResolveError> {

    if (cfg.memtable.flush_trigger_bytes >= cfg.memtable.stall_trigger_bytes
        || cfg.memtable.stall_trigger_bytes >= cfg.memtable.max_size_bytes) {
        return std::unexpected(ResolveError::InvalidFlushThresholds);
    }

    const float ratio = cfg.wal_tuning.precreate_trigger_ratio;
    if (ratio <= 0.0f || ratio >= 1.0f) {
        return std::unexpected(ResolveError::InvalidPrecreateRatio);
    }
    if (cfg.wal_tuning.max_staging_bytes == 0) {
        return std::unexpected(ResolveError::InvalidMaxStagingBytes);
    }

    if (cfg.background_compaction_threads == 0) {
        return std::unexpected(ResolveError::InvalidCompactionThreads);
    }

    return cfg;
}

} // namespace stratadb::config