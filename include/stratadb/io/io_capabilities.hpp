#pragma once
#include "stratadb/io/io_types.hpp"

#include <cstddef>
#include <cstdint>

namespace stratadb::io {

struct IOCapabilities {
    std::size_t logical_sector_size;  // Minimum I/O unit size (e.g., 512 bytes)
    std::size_t physical_sector_size; // Actual physical sector size (e.g., 4096 bytes)

    // PHYSICS
    bool is_rotational; // True if the device is a spinning disk, false if it's an SSD
    bool supports_fua;  // True if the device supports Force Unit Access (FUA) for durability guarantees

    // ATOMICITY
    std::size_t atomic_write_unit_min; // Minimum boundary for torn write protection (e.g., 512 bytes)
    std::size_t atomic_write_unit_max; // Maximum boundary for torn write protection (e.g., 4096 bytes)
    // KERNEL/OS Support
    bool supports_rwf_atomic; // true = Linux 6.11+ STATX_WRITE_ATOMIC available
    bool supports_fallocate;  // true = Filesystem supports extent pre-allocation (Critical for HDD performance)
};

} // namespace stratadb::io