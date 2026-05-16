#pragma once
#include "io_concept.hpp"

#include <cstddef>
#include <span>
#include <sys/types.h>

struct iovec;

namespace stratadb::io {
class PosixIoEngine {
  public:
    explicit PosixIoEngine(const io::IOCapabilities caps) noexcept
        : caps_(caps) {}

    [[nodiscard]] auto capabilities() const noexcept -> const io::IOCapabilities& {
        return caps_;
    }

    [[nodiscard]] auto writev(FileHandle fd, std::span<const struct iovec> iovs, uint64_t offset) const noexcept
        -> std::expected<size_t, IOError>;

    [[nodiscard]] auto read(FileHandle fd, std::span<std::byte> buffer, uint64_t offset) const noexcept
        -> std::expected<size_t, IOError>;

    [[nodiscard]] auto sync(FileHandle fd) const noexcept -> std::expected<void, IOError>;

  private:
    io::IOCapabilities caps_;
};

static_assert(io::IoEngineConcept<PosixIoEngine>, "PosixIoEngine does not satisfy IoEngineConcept");
}; // namespace stratadb::io
