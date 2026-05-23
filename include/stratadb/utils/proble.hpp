#pragma once

#include "stratadb/io/io_capabilities.hpp"

#include <cstddef>
#include <cstdint>

namespace stratadb::utils {

// Probes the disk's physical sector size, FUA support, and atomicity guarantees
// via BLKPBSZGET / BLKSSZGET ioctl and STATX_WRITE_ATOMIC (Linux 6.11+).
[[nodiscard]] auto probe_io_capabilities(int fd) noexcept -> io::IOCapabilities;

// Queries the OS memory-management page size (4 KiB, 16 KiB, or 64 KiB).
[[nodiscard]] auto system_page_size() noexcept -> std::size_t;

// Queries available logical CPU cores, respecting cgroup / cpuset constraints.
[[nodiscard]] auto logical_core_count() noexcept -> std::uint32_t;

// Queries total installed physical memory in bytes.
// Returns 0 if the platform does not expose a reliable value.
[[nodiscard]] auto total_physical_memory_bytes() noexcept -> std::size_t;

} // namespace stratadb::utils