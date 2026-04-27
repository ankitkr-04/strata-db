#include "stratadb/memory/arena.hpp"

#include "stratadb/utils/math.hpp"

#include <bit>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <limits>
#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>
namespace stratadb::memory {
namespace {

auto try_mmap(std::size_t size, int flags) noexcept -> void* {
    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, flags, -1, 0);
    return (ptr == MAP_FAILED) ? nullptr : ptr;
}

// fallback chain executor
auto try_sequence(std::size_t total, int base_flags, std::initializer_list<int> flags_list) noexcept -> void* {
    for (int flags : flags_list) {
        if (void* p = try_mmap(total, base_flags | flags)) {
            return p;
        }
    }
    return nullptr;
}

// NUMA policy application
auto apply_numa_policy(void* ptr, std::size_t total, config::NumaPolicy policy) noexcept -> bool {
    constexpr unsigned long kFlags = MPOL_MF_STRICT | MPOL_MF_MOVE;

    switch (policy) {
        case config::NumaPolicy::UMA:
            return true;

        case config::NumaPolicy::Interleaved: {
            // fix sparse topology
            struct bitmask* nodes = numa_get_mems_allowed();
            if (!nodes) {
                return false;
            }

            const long result = mbind(ptr, total, MPOL_INTERLEAVE, nodes->maskp, nodes->size + 1, kFlags);
            numa_bitmask_free(nodes);
            return result == 0;
        }

        case config::NumaPolicy::StrictLocal:
            return mbind(ptr, total, MPOL_LOCAL, nullptr, 0, kFlags) == 0;
    }
    return false;
}

// optional prefault
inline void prefault_memory(void* ptr, std::size_t size) noexcept {
    std::memset(ptr, 0, size);
}

} // namespace

Arena::Arena(std::byte* base, const config::MemoryConfig& config) noexcept
    : base_(base)
    , config_(config) {}

Arena::~Arena() noexcept {
    if (base_) {
        munmap(base_, config_.total_budget_bytes);
    }
}

Arena::Arena(Arena&& other) noexcept
    : base_(other.base_)
    , config_(other.config_) {
    offset_.store(other.offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    other.base_ = nullptr;
    other.offset_.store(0, std::memory_order_relaxed);
    other.config_.total_budget_bytes = 0;
}

Arena& Arena::operator=(Arena&& other) noexcept {
    if (this != &other) {
        if (base_) {
            munmap(base_, config_.total_budget_bytes);
        }
        base_ = other.base_;
        config_ = other.config_;
        offset_.store(other.offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);

        other.base_ = nullptr;
        other.offset_.store(0, std::memory_order_relaxed);
        other.config_.total_budget_bytes = 0;
    }
    return *this;
}

auto Arena::create(const config::MemoryConfig& config) noexcept -> std::expected<Arena, ArenaError> {

    // effective config (runtime truth)
    config::MemoryConfig effective_config = config;

    const std::size_t total = effective_config.total_budget_bytes;
    const auto page_strategy = effective_config.page_strategy;

    constexpr int base_flags = MAP_PRIVATE | MAP_ANONYMOUS;

    void* ptr = nullptr;

    switch (page_strategy) {

        case config::PageStrategy::Standard4K:
            ptr = try_sequence(total, base_flags, {0});
            break;

        case config::PageStrategy::Huge2M_Opportunistic:
            ptr = try_sequence(total,
                               base_flags,
                               {
#ifdef MAP_HUGETLB
                                   MAP_HUGETLB | MAP_HUGE_2MB,
#endif
                                   0});
            break;

        case config::PageStrategy::Huge2M_Strict:
            ptr = try_sequence(total,
                               base_flags,
                               {
#ifdef MAP_HUGETLB
                                   MAP_HUGETLB | MAP_HUGE_2MB
#endif
                               });
            if (!ptr)
                return std::unexpected(ArenaError::MmapFailed);
            break;

        case config::PageStrategy::Huge1G_Opportunistic:
            ptr = try_sequence(total,
                               base_flags,
                               {
#ifdef MAP_HUGETLB
                                   MAP_HUGETLB | MAP_HUGE_1GB,
                                   MAP_HUGETLB | MAP_HUGE_2MB,
#endif
                                   0});
            break;

        case config::PageStrategy::Huge1G_Strict:
            ptr = try_sequence(total,
                               base_flags,
                               {
#ifdef MAP_HUGETLB
                                   MAP_HUGETLB | MAP_HUGE_1GB
#endif
                               });
            if (!ptr)
                return std::unexpected(ArenaError::MmapFailed);
            break;
    }

    if (!ptr) {
        if (errno == ENOMEM)
            return std::unexpected(ArenaError::OutOfMemory);
        return std::unexpected(ArenaError::MmapFailed);
    }

    // apply NUMA policy (truthful + fallback)
    if (!apply_numa_policy(ptr, total, effective_config.numa_policy)) {
        if (effective_config.numa_policy == config::NumaPolicy::StrictLocal) {
            munmap(ptr, total);
            return std::unexpected(ArenaError::MbindFailed);
        }
        // fallback → record actual behavior
        effective_config.numa_policy = config::NumaPolicy::UMA;
    }

    // optional prefault
    if (effective_config.prefault_on_init) {
        prefault_memory(ptr, total);
    }

    return Arena(reinterpret_cast<std::byte*>(ptr), effective_config);
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto Arena::bump_allocate(std::size_t size, std::size_t alignment) noexcept -> std::size_t {
    if (!base_ || !std::has_single_bit(alignment)) [[unlikely]] {
        return std::numeric_limits<std::size_t>::max();
    }

    std::size_t old = offset_.load(std::memory_order_relaxed);

    while (true) {
        std::size_t aligned_offset = 0;
        if (!utils::align_up_checked(old, alignment, aligned_offset)) [[unlikely]] {
            return std::numeric_limits<std::size_t>::max();
        }

        if (config_.total_budget_bytes < size || aligned_offset > config_.total_budget_bytes - size) [[unlikely]] {
            return std::numeric_limits<std::size_t>::max();
        }

        const std::size_t next = aligned_offset + size;
        if (offset_.compare_exchange_weak(old, next, std::memory_order_acq_rel, std::memory_order_relaxed)) {
            return aligned_offset;
        }
    }
}

auto Arena::allocate_block(std::size_t min_size) noexcept -> std::span<std::byte> {
    std::size_t size = (min_size > config_.tlab_size_bytes) ? min_size : config_.tlab_size_bytes;

    if (!utils::align_up_checked(size, config_.block_alignment_bytes, size)) [[unlikely]] {
        return {};
    }

    const std::size_t aligned_offset = bump_allocate(size, config_.block_alignment_bytes);
    if (aligned_offset == std::numeric_limits<std::size_t>::max()) [[unlikely]] {
        return {};
    }

    return {base_ + aligned_offset, size};
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

    return base_ + aligned_offset;
}

auto Arena::reset() noexcept -> void {
    offset_.store(0, std::memory_order_relaxed);
}

} // namespace stratadb::memory
