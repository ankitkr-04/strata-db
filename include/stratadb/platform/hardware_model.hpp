#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>

namespace stratadb::platform {
template <bool IsOverride, typename T>
using Field = std::conditional_t<IsOverride, std::optional<T>, T>;

template <bool IsOverride>
struct HardwareModel {
    struct Cpu {
        Field<IsOverride, std::uint32_t> logical_count;
    } cpu;

    struct Memory {
        Field<IsOverride, std::size_t> total_bytes;
        Field<IsOverride, std::size_t> page_size_bytes;
    } memory;

    struct Io {
        Field<IsOverride, std::size_t> logical_sector_size;
        Field<IsOverride, std::size_t> physical_sector_size;
        Field<IsOverride, std::size_t> atomic_write_unit_min;
        Field<IsOverride, std::size_t> atomic_write_unit_max;
        Field<IsOverride, bool> is_rotational;
        Field<IsOverride, bool> supports_fua;
        Field<IsOverride, bool> supports_rwf_atomic;
        Field<IsOverride, bool> supports_fallocate;
    } io;
};

// Produced once at startup by probe_hardware(). Passed by const-ref to every
// subsystem. All fields are guaranteed to hold concrete values.
using HardwareInfo = HardwareModel<false>;

// User-supplied corrections. All fields are optional.
// Absent fields defer to the OS probe result.
using HardwareOverrides = HardwareModel<true>;

} // namespace stratadb::platform