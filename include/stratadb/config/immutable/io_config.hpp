#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace stratadb::config {

enum class IOStrategy : std::uint8_t {
    // Probes hardware via sysfs / statx and selects the best strategy below.
    AutoDetect,

    // LEVEL 0: pwritev + O_DIRECT. Uses fallocate for HDD pre-allocation.
    PosixDirect,

    // LEVEL 1/2: io_uring + O_DIRECT.
    // Batches multiple thread-local blocks into a single SQE chain.
    // Uses hardware FUA (Force Unit Access) if supported to skip fdatasync.
    UringGroupCommit,

    // LEVEL 3: io_uring + pwritev2 + RWF_ATOMIC.
    // Requires Linux 6.11+ and NVMe AWUPF; eliminates torn writes and
    // space waste by safely overwriting 4 KiB / 16 KiB sectors in-place.
    UringAtomic,
};

struct IOConfig {
    IOStrategy strategy{IOStrategy::AutoDetect};

    // io_uring submission / completion queue depth.
    // Must be a power of 2; validated at IoEngine construction.
    std::size_t uring_queue_depth{512};

    // When set, the kernel spawns a dedicated SQPOLL thread that polls the
    // submission queue without requiring a syscall from the application.
    // nullopt = SQPOLL disabled (standard eventfd-based submission).
    std::optional<std::chrono::milliseconds> uring_sqpoll_idle{std::nullopt};

    // Pre-register fixed buffer / file tables with the kernel to eliminate
    // per-operation virtual-memory mapping overhead on the hot I/O path.
    bool pre_register_buffers{true};
    bool pre_register_files{true};
};

} // namespace stratadb::config