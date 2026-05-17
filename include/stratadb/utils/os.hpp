#pragma once

#include <cstddef>
#include <cstdint>

namespace stratadb::utils::os {

// Cross-platform data sync (safely maps to fdatasync or F_FULLFSYNC)
[[nodiscard]] auto sync_data(int fd) noexcept -> bool;

// Safely probes the OS for the maximum allowed scatter-gather vector limit
[[nodiscard]] auto get_iov_max() noexcept -> std::size_t;

// Safely closes a file descriptor, ignoring any errors (e.g. EBADF)
void close_fd(int fd) noexcept;

// Attempts to pin the currently executing thread to a specific physical/logical core.
// Falls back gracefully on macOS/Windows.
[[nodiscard]] auto pin_current_thread_to_core(std::uint32_t core_id) noexcept -> bool;

// Attempts to elevate the current thread to SCHED_FIFO real-time priority.
// Requires CAP_SYS_NICE or properly configured ulimits on Linux.
[[nodiscard]] auto elevate_to_realtime_priority() noexcept -> bool;

// Parses Linux /sys/devices/system/cpu/isolated to check if a core is completely
// hidden from the standard kernel scheduler (required for zero-latency SPSC busy-polling).
[[nodiscard]] auto is_core_isolated(std::uint32_t core_id) noexcept -> bool;

} // namespace stratadb::utils::os