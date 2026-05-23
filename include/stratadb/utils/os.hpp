#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

namespace stratadb::utils::os {

// Cross-platform data sync.
// Linux: fdatasync. macOS: F_FULLFSYNC (forces hardware write-cache flush).
[[nodiscard]] auto sync_data(int fd) noexcept -> bool;

// Safely probes the OS maximum scatter-gather vector limit (IOV_MAX).
[[nodiscard]] auto get_iov_max() noexcept -> std::size_t;

// Safely closes a file descriptor; ignores errors (e.g. EBADF, EINTR).
void close_fd(int fd) noexcept;

// Attempts to pin the current thread to a specific logical core.
// Gracefully returns false on macOS / Windows where affinity APIs differ.
[[nodiscard]] auto pin_current_thread_to_core(std::uint32_t core_id) noexcept -> bool;

// Attempts to elevate the current thread to SCHED_FIFO real-time priority.
// Requires CAP_SYS_NICE or configured ulimits on Linux.
[[nodiscard]] auto elevate_to_realtime_priority() noexcept -> bool;

// Returns true if core_id appears in /sys/devices/system/cpu/isolated,
// meaning the kernel scheduler will not schedule ordinary tasks onto it.
[[nodiscard]] auto is_core_isolated(std::uint32_t core_id) noexcept -> bool;

// Parses /sys/devices/system/cpu/isolated and returns the highest isolated
// core id, which is the best candidate for SPSC busy-polling.
// Returns nullopt if no isolated cores are configured.
[[nodiscard]] auto auto_discover_isolated_core() noexcept -> std::optional<std::uint32_t>;

} // namespace stratadb::utils::os