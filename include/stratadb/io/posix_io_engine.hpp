#pragma once

#include "io_concept.hpp"
#include "io_types.hpp"
#include "stratadb/platform/hardware_model.hpp"

#include <cstddef>
#include <expected>
#include <span>
#include <sys/types.h>

struct iovec;

namespace stratadb::io {

class PosixIoEngine {
  public:
    explicit PosixIoEngine(const platform::HardwareInfo::Io& io_info) noexcept
        : io_info_(io_info) {}

    [[nodiscard]] auto capabilities() const noexcept -> const platform::HardwareInfo::Io& {
        return io_info_;
    }

    [[nodiscard]] auto writev(FileHandle fd, std::span<const struct iovec> iovs, uint64_t offset) const noexcept
        -> std::expected<size_t, IOError>;

    [[nodiscard]] auto read(FileHandle fd, std::span<std::byte> buffer, uint64_t offset) const noexcept
        -> std::expected<size_t, IOError>;

    [[nodiscard]] auto sync(FileHandle fd) const noexcept -> std::expected<void, IOError>;

  private:
    platform::HardwareInfo::Io io_info_;
};

static_assert(sizeof(off_t) >= sizeof(uint64_t),
              "off_t is too narrow for 64-bit offsets; compile with -D_FILE_OFFSET_BITS=64");
static_assert(IoEngineConcept<PosixIoEngine>, "PosixIoEngine does not satisfy IoEngineConcept");

} // namespace stratadb::io