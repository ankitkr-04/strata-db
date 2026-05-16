#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <sys/uio.h>

namespace stratadb::io {
using FileHandle = int; // Alias for file descriptor type

enum class IOError : std::uint8_t { AlignmentViolation, HardwareError, DeviceFull, UnknownError };

struct IOCapabilities {
    size_t logical_sector_size;  // Minimum I/O unit size (e.g., 512 bytes)
    size_t physical_sector_size; // Actual physical sector size (e.g., 4096 bytes)

    // PHYSICS
    bool is_rotational; // True if the device is a spinning disk, false if it's an SSD
    bool supports_fua;  // True if the device supports Force Unit Access (FUA) for durability guarantees

    // ATOMICITY
    size_t atomic_write_unit_min; // Minimum boundary for torn write protection (e.g., 512 bytes)
    size_t atomic_write_unit_max; // Maximum boundary for torn write protection (e.g., 4096 bytes)
    // KERNEL/OS Support
    bool supports_rwf_atomic; // true = Linux 6.11+ STATX_WRITE_ATOMIC available
    bool supports_fallocate;  // true = Filesystem supports extent pre-allocation (Critical for HDD performance)
};

template <typename T>
concept IoEngineConcept = requires(T engine,
                                   FileHandle fd,
                                   uint64_t offset,
                                   std::span<const struct iovec> iovs,
                                   std::span<std::byte> buffer) {
    // --- CONTROL PLANE ---
    // Exposes the hardware limits. The WalManager reads this ONCE at startup.
    { engine.capabilities() } -> std::same_as<const IOCapabilities&>;

    // --- DATA PLANE (The Hot Path) ---
    { engine.writev(fd, iovs, offset) } -> std::same_as<std::expected<size_t, IOError>>;

    { engine.read(fd, buffer, offset) } -> std::same_as<std::expected<size_t, IOError>>;

    { engine.sync(fd) } -> std::same_as<std::expected<void, IOError>>;
};
} // namespace stratadb::io