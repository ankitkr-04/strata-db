#pragma once

#include <concepts>
#include <cstddef>
#include <span>
#include <sys/uio.h>

namespace stratadb::io {

template <typename T>
concept IsIoEngine = requires(T engine, int fd, std::span<const struct iovec> iov) {
    // Scatter-gather write. Returns true if all vectors were fully written, false otherwise.
    { engine.write_vectored(fd, iov) } -> std::same_as<bool>;

    // Force durability: flushes OS buffers to disk hardware (e.g., fdatasync).
    { engine.sync(fd) } -> std::same_as<bool>;

    // Hardware sympathy: dynamically probe the OS/Drive for its optimal submission queue depth.
    { engine.probe_optimal_queue_depth(fd) } -> std::same_as<std::size_t>;
};
} // namespace stratadb::io