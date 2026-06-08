#pragma once

#include "stratadb/io/io_types.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
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

// Returns a raw FileHandle; wrap in io::UniqueFd at the call site.
// create=true  → O_RDWR | O_CREAT | O_TRUNC
// create=false → O_RDWR
[[nodiscard]] auto open_buffered(const std::filesystem::path& path, bool create = false) noexcept
    -> std::expected<io::FileHandle, io::IOError>;

// Linux: O_RDWR | O_DIRECT.  macOS: O_RDWR + F_NOCACHE.
[[nodiscard]] auto open_direct(const std::filesystem::path& path) noexcept
    -> std::expected<io::FileHandle, io::IOError>;

// posix_fallocate — extends EOF, must be called on a buffered fd.
[[nodiscard]] auto allocate_file_space(int fd, std::uint64_t bytes) noexcept -> std::expected<void, io::IOError>;

// pread loop — retries on EINTR, errors on short read.
[[nodiscard]] auto read_exact(int fd, void* buf, std::size_t size, std::uint64_t offset) noexcept
    -> std::expected<void, io::IOError>;

// pwrite loop — retries on EINTR, errors on short write.
[[nodiscard]] auto write_exact(int fd, const void* buf, std::size_t size, std::uint64_t offset) noexcept
    -> std::expected<void, io::IOError>;
// Syncs the directory's metadata/journal to persistent storage.
// Crucial after creating or deleting files to guarantee visibility after a crash.
[[nodiscard]] auto sync_dir_entries(int fd) noexcept -> bool;

// Returns a file descriptor for a directory.
// Must be closed via close_fd() when the manager shuts down.
[[nodiscard]] auto open_directory(const std::filesystem::path& path) noexcept -> std::expected<int, io::IOError>;

} // namespace stratadb::utils::os