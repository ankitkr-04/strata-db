#pragma once

#include "stratadb/platform/hardware_model.hpp"

#include <filesystem>

namespace stratadb::platform {

// Probes OS and hardware for machine capabilities, then applies any overrides.
//
// data_dir is the DB root directory. A file descriptor is opened against it
// to run the IO capability ioctls (BLKPBSZGET, BLKSSZGET, STATX_WRITE_ATOMIC).
//
// On any individual probe failure the field falls back to a safe industry default:
//   - physical_sector_size / atomic_write_unit_max : 4096
//   - logical_sector_size / atomic_write_unit_min  : 512
//   - is_rotational                                : false  (assume SSD)
//   - supports_fua / supports_rwf_atomic           : false
//   - supports_fallocate                           : false
//
// If an override is present for a field, the OS value is discarded entirely.
// This function never throws; errors are absorbed into the safe defaults.
[[nodiscard]] auto probe_hardware(const std::filesystem::path& data_dir, const HardwareOverrides& overrides) noexcept
    -> HardwareInfo;

} // namespace stratadb::platform