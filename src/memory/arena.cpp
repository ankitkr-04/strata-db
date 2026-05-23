#include "stratadb/memory/arena.hpp"

#include "stratadb/utils/align.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <limits>

namespace stratadb::memory {

Arena::Arena(VirtualRegion region, const config::MemoryConfig& cfg) noexcept
    : region_(std::move(region))
    , config_(cfg)
    , large_alloc_threshold_bytes_(cfg.tlab_size_bytes / cfg.large_alloc_tlab_fraction) {}

auto Arena::create(const config::MemoryConfig& cfg) noexcept -> std::expected<Arena, ArenaError> {
    config::MemoryConfig effective_config = cfg;
    assert(effective_config.block_alignment_bytes != 0);
    assert(effective_config.large_alloc_tlab_fraction != 0);

    auto region_expected = VirtualRegion::allocate(effective_config);
    if (!region_expected) {
        return std::unexpected(region_expected.error());
    }

    return Arena(std::move(*region_expected), effective_config);
}

Arena::Arena(Arena&& other) noexcept
    : region_(std::move(other.region_))
    , config_(other.config_)
    , large_alloc_threshold_bytes_(other.large_alloc_threshold_bytes_) {
    offset_.store(other.offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);

    other.offset_.store(0, std::memory_order_relaxed);
    other.large_alloc_threshold_bytes_ = 0;
    other.config_.total_budget_bytes = 0;
}

auto Arena::operator=(Arena&& other) noexcept -> Arena& {
    if (this != &other) {
        region_ = std::move(other.region_);
        config_ = other.config_;
        large_alloc_threshold_bytes_ = other.large_alloc_threshold_bytes_;
        offset_.store(other.offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);

        other.offset_.store(0, std::memory_order_relaxed);
        other.large_alloc_threshold_bytes_ = 0;
        other.config_.total_budget_bytes = 0;
    }
    return *this;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto Arena::bump_allocate(std::size_t size, std::size_t alignment) noexcept -> std::size_t {
    if (!region_.data() || !std::has_single_bit(alignment)) [[unlikely]] {
        return std::numeric_limits<std::size_t>::max();
    }

    std::size_t old = offset_.load(std::memory_order_relaxed);

    while (true) {
        std::size_t aligned_offset = 0;
        if (!utils::try_align_up(old, alignment, aligned_offset)) [[unlikely]] {
            return std::numeric_limits<std::size_t>::max();
        }

        if (config_.total_budget_bytes < size || aligned_offset > config_.total_budget_bytes - size) [[unlikely]] {
            return std::numeric_limits<std::size_t>::max();
        }

        const std::size_t next = aligned_offset + size;
        if (offset_.compare_exchange_weak(old, next, std::memory_order_release, std::memory_order_relaxed)) {
            return aligned_offset;
        }
    }
}

auto Arena::allocate_block(std::size_t min_size) noexcept -> std::span<std::byte> {
    std::size_t size = (min_size > config_.tlab_size_bytes) ? min_size : config_.tlab_size_bytes;

    if (!utils::try_align_up(size, config_.block_alignment_bytes, size)) [[unlikely]] {
        return {};
    }

    const std::size_t aligned_offset = bump_allocate(size, config_.block_alignment_bytes);
    if (aligned_offset == std::numeric_limits<std::size_t>::max()) [[unlikely]] {
        return {};
    }

    return {region_.data() + aligned_offset, size};
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto Arena::allocate_aligned(std::size_t size, std::size_t alignment) noexcept -> void* {
    assert(std::has_single_bit(alignment));
    if (!std::has_single_bit(alignment)) [[unlikely]] {
        return nullptr;
    }

    const std::size_t aligned_offset = bump_allocate(size, alignment);
    if (aligned_offset == std::numeric_limits<std::size_t>::max()) [[unlikely]] {
        return nullptr;
    }

    return region_.data() + aligned_offset;
}

auto Arena::reset() noexcept -> void {
    offset_.store(0, std::memory_order_relaxed);
}

} // namespace stratadb::memory