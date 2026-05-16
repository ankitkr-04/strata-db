#pragma once

#include <cstddef>

namespace stratadb::utils::os {

// Cross-platform data sync (safely maps to fdatasync or F_FULLFSYNC)
[[nodiscard]] auto sync_data(int fd) noexcept -> bool;

// Safely probes the OS for the maximum allowed scatter-gather vector limit
[[nodiscard]] auto get_iov_max() noexcept -> std::size_t;

// Safely closes a file descriptor, ignoring any errors (e.g. EBADF)
void close_fd(int fd) noexcept;

} // namespace stratadb::utils::os