#pragma once
#include "stratadb/io/io_concept.hpp"

#include <cstddef>
#include <span>
#include <sys/types.h>

struct iovec;

namespace stratadb::io {
class PosixIoEngine {
  public:
    [[nodiscard]] auto write_vectored(int fd, std::span<const struct iovec> iov) noexcept -> bool;
    [[nodiscard]] auto sync(int fd) noexcept -> bool;
    [[nodiscard]] auto probe_optimal_queue_depth(int fd) const noexcept -> std::size_t;

  private:
    off_t offset_{0}; // Positional tracker for pwritev
};

static_assert(IsIoEngine<PosixIoEngine>, "PosixIoEngine does not satisfy the IsIoEngine concept");
} // namespace stratadb::io