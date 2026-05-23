#pragma once

#include "stratadb/config/immutable/memory_config.hpp"
#include "stratadb/config/immutable/page_config.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>

namespace stratadb::memory {

enum class ArenaError : std::uint8_t {
    MmapFailed,
    OutOfMemory,
    MbindFailed,
    MunmapFailed,
};

class VirtualRegion {
  public:
    // Takes the intended configuration, attempts allocation, gracefully degrades
    // if huge pages are fragmented, and returns the successfully mapped region.
    [[nodiscard]] static auto allocate(const config::MemoryConfig& cfg) noexcept
        -> std::expected<VirtualRegion, ArenaError>;

    VirtualRegion() noexcept = default;
    ~VirtualRegion() noexcept;

    VirtualRegion(const VirtualRegion&) = delete;
    auto operator=(const VirtualRegion&) -> VirtualRegion& = delete;

    VirtualRegion(VirtualRegion&& other) noexcept;
    auto operator=(VirtualRegion&& other) noexcept -> VirtualRegion&;

    [[nodiscard]] auto data() const noexcept -> std::byte* {
        return base_;
    }
    [[nodiscard]] auto size() const noexcept -> std::size_t {
        return size_;
    }

    // Returns the concrete strategy that was actually successfully mapped,
    // reflecting reality after any OS-level fragmentation fallbacks.
    [[nodiscard]] auto actual_page_strategy() const noexcept -> config::PageStrategy {
        return actual_strategy_;
    }

  private:
    VirtualRegion(std::byte* base, std::size_t size, config::PageStrategy strategy) noexcept;

    std::byte* base_{nullptr};
    std::size_t size_{0};
    config::PageStrategy actual_strategy_{config::PageStrategy::Standard4K};
};

} // namespace stratadb::memory