#pragma once

#include "io_types.hpp"
#include "stratadb/platform/hardware_info.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <sys/uio.h>

namespace stratadb::io {

template <typename T>
concept IoEngineConcept = requires(T engine,
                                   FileHandle fd,
                                   uint64_t offset,
                                   std::span<const struct iovec> iovs,
                                   std::span<std::byte> buffer) {
    { engine.capabilities() } -> std::same_as<const platform::HardwareInfo::Io&>;
    { engine.writev(fd, iovs, offset) } -> std::same_as<std::expected<size_t, IOError>>;
    { engine.read(fd, buffer, offset) } -> std::same_as<std::expected<size_t, IOError>>;
    { engine.sync(fd) } -> std::same_as<std::expected<void, IOError>>;
};

} // namespace stratadb::io