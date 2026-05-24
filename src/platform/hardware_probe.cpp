#include "stratadb/platform/hardware_probe.hpp"

#include "stratadb/utils/probe.hpp"

#include <fcntl.h>
#include <unistd.h>

namespace stratadb::platform {

namespace {

template <typename T>
constexpr auto apply(std::optional<T> override_val, T probed_val) noexcept -> T {
    return override_val.has_value() ? *override_val : probed_val;
}

} // namespace

auto probe_hardware(const std::filesystem::path& data_dir, const HardwareOverrides& overrides) noexcept
    -> HardwareInfo {
    HardwareInfo info{};

    info.cpu.logical_count = apply(overrides.cpu.logical_count, utils::logical_core_count());

    info.memory.total_bytes = apply(overrides.memory.total_bytes, utils::total_physical_memory_bytes());
    info.memory.page_size_bytes = apply(overrides.memory.page_size_bytes, utils::system_page_size());

    // Open the data directory to run per-property ioctls against the block device.
    // If open fails (directory not yet created) every probe falls back to its
    // safe default internally — fd = -1 is the signal.
    const int dir_fd = ::open(data_dir.c_str(), O_RDONLY | O_DIRECTORY); // NOLINT(cppcoreguidelines-pro-type-vararg)

    info.io = {
        .logical_sector_size = apply(overrides.io.logical_sector_size, utils::probe_logical_sector_size(dir_fd)),
        .physical_sector_size = apply(overrides.io.physical_sector_size, utils::probe_physical_sector_size(dir_fd)),
        .atomic_write_unit_min = apply(overrides.io.atomic_write_unit_min, utils::probe_atomic_write_unit_min(dir_fd)),
        .atomic_write_unit_max = apply(overrides.io.atomic_write_unit_max, utils::probe_atomic_write_unit_max(dir_fd)),
        .is_rotational = apply(overrides.io.is_rotational, utils::probe_is_rotational(dir_fd)),
        .supports_fua = apply(overrides.io.supports_fua, utils::probe_supports_fua(dir_fd)),
        .supports_rwf_atomic = apply(overrides.io.supports_rwf_atomic, utils::probe_supports_rwf_atomic(dir_fd)),
        .supports_fallocate = apply(overrides.io.supports_fallocate, utils::probe_supports_fallocate(dir_fd)),
    };

    if (dir_fd >= 0)
        ::close(dir_fd);

    return info;
}

} // namespace stratadb::platform