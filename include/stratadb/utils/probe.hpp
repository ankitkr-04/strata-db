#pragma once

#include <cstddef>
#include <cstdint>

namespace stratadb::utils {

[[nodiscard]] auto system_page_size() noexcept -> std::size_t;

// Respects cgroup / cpuset constraints.
[[nodiscard]] auto logical_core_count() noexcept -> std::uint32_t;

// Returns 0 if the platform doesn't expose a reliable value.
[[nodiscard]] auto total_physical_memory_bytes() noexcept -> std::size_t;

[[nodiscard]] auto probe_logical_sector_size(int fd) noexcept -> std::size_t;   // default: 512
[[nodiscard]] auto probe_physical_sector_size(int fd) noexcept -> std::size_t;  // default: 4096
[[nodiscard]] auto probe_atomic_write_unit_min(int fd) noexcept -> std::size_t; // default: 512
[[nodiscard]] auto probe_atomic_write_unit_max(int fd) noexcept -> std::size_t; // default: 4096
[[nodiscard]] auto probe_is_rotational(int fd) noexcept -> bool;                // default: false
[[nodiscard]] auto probe_supports_fua(int fd) noexcept -> bool;                 // default: false
[[nodiscard]] auto probe_supports_rwf_atomic(int fd) noexcept -> bool;          // default: false
[[nodiscard]] auto probe_supports_fallocate(int fd) noexcept -> bool;           // default: false

} // namespace stratadb::utils