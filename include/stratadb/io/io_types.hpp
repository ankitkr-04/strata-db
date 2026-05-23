
#pragma once

#include <cstdint>

namespace stratadb::io {

// Alias for a POSIX file descriptor. Negative values are invalid.
using FileHandle = int;

enum class IOError : std::uint8_t {
    AlignmentViolation,
    HardwareError,
    DeviceFull,
    UnknownError,
};

} // namespace stratadb::io