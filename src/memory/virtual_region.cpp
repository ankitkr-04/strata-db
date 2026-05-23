#include "stratadb/memory/virtual_region.hpp"

#include <cerrno>
#include <cstring>
#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>

namespace stratadb::memory {
namespace {

#if defined(MAP_HUGETLB)
constexpr int OS_MAP_HUGE_2MB = MAP_HUGETLB | MAP_HUGE_2MB;
constexpr int OS_MAP_HUGE_1GB = MAP_HUGETLB | MAP_HUGE_1GB;
#else
constexpr int OS_MAP_HUGE_2MB = 0;
constexpr int OS_MAP_HUGE_1GB = 0;
#endif

[[nodiscard]] auto try_mmap(std::size_t size, int flags) noexcept -> void* {
    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, flags, -1, 0);
    return (ptr == MAP_FAILED) ? nullptr : ptr;
}

[[nodiscard]] auto apply_numa_policy(void* ptr, std::size_t total, config::NumaPolicy policy) noexcept -> bool {
    constexpr unsigned long kFlags = MPOL_MF_STRICT | MPOL_MF_MOVE;

    switch (policy) {
        case config::NumaPolicy::UMA:
            return true;

        case config::NumaPolicy::Interleaved: {
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

inline void prefault_memory(void* ptr, std::size_t size) noexcept {
    std::memset(ptr, 0, size);
}

} // namespace

VirtualRegion::VirtualRegion(std::byte* base, std::size_t size, config::PageStrategy strategy) noexcept
    : base_(base)
    , size_(size)
    , actual_strategy_(strategy) {}

VirtualRegion::~VirtualRegion() noexcept {
    if (base_) {
        munmap(base_, size_);
    }
}

VirtualRegion::VirtualRegion(VirtualRegion&& other) noexcept
    : base_(other.base_)
    , size_(other.size_)
    , actual_strategy_(other.actual_strategy_) {
    other.base_ = nullptr;
    other.size_ = 0;
}

auto VirtualRegion::operator=(VirtualRegion&& other) noexcept -> VirtualRegion& {
    if (this != &other) {
        if (base_) {
            munmap(base_, size_);
        }
        base_ = other.base_;
        size_ = other.size_;
        actual_strategy_ = other.actual_strategy_;

        other.base_ = nullptr;
        other.size_ = 0;
    }
    return *this;
}

auto VirtualRegion::allocate(const config::MemoryConfig& cfg) noexcept -> std::expected<VirtualRegion, ArenaError> {
    const std::size_t total = cfg.total_budget_bytes;
    constexpr int base_flags = MAP_PRIVATE | MAP_ANONYMOUS;

    void* ptr = nullptr;
    config::PageStrategy actual_strategy = cfg.page_strategy;

    // Direct mapping with explicit downgrades so we capture the true final state
    switch (cfg.page_strategy) {
        case config::PageStrategy::Standard4K:
            ptr = try_mmap(total, base_flags);
            break;

        case config::PageStrategy::Huge2M_Opportunistic:
            ptr = try_mmap(total, base_flags | OS_MAP_HUGE_2MB);
            if (ptr) {
                actual_strategy = config::PageStrategy::Huge2M_Strict;
            } else {
                ptr = try_mmap(total, base_flags);
                actual_strategy = config::PageStrategy::Standard4K;
            }
            break;

        case config::PageStrategy::Huge2M_Strict:
            ptr = try_mmap(total, base_flags | OS_MAP_HUGE_2MB);
            break;

        case config::PageStrategy::Huge1G_Opportunistic:
            ptr = try_mmap(total, base_flags | OS_MAP_HUGE_1GB);
            if (ptr) {
                actual_strategy = config::PageStrategy::Huge1G_Strict;
            } else {
                ptr = try_mmap(total, base_flags | OS_MAP_HUGE_2MB);
                if (ptr) {
                    actual_strategy = config::PageStrategy::Huge2M_Strict;
                } else {
                    ptr = try_mmap(total, base_flags);
                    actual_strategy = config::PageStrategy::Standard4K;
                }
            }
            break;

        case config::PageStrategy::Huge1G_Strict:
            ptr = try_mmap(total, base_flags | OS_MAP_HUGE_1GB);
            break;
    }

    if (!ptr) {
        if (errno == ENOMEM) {
            return std::unexpected(ArenaError::OutOfMemory);
        }
        return std::unexpected(ArenaError::MmapFailed);
    }

    config::NumaPolicy active_numa_policy = cfg.numa_policy;
    if (!apply_numa_policy(ptr, total, active_numa_policy)) {
        if (active_numa_policy == config::NumaPolicy::StrictLocal) {
            munmap(ptr, total);
            return std::unexpected(ArenaError::MbindFailed);
        }
        // Graceful downgrade handled implicitly
    }

    if (cfg.prefault_on_init) {
        prefault_memory(ptr, total);
    }

    return VirtualRegion(static_cast<std::byte*>(ptr), total, actual_strategy);
}

} // namespace stratadb::memory