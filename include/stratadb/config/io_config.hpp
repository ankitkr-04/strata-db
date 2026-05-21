#pragma once

#include <chrono>
#include <cstdint>

namespace stratadb::config {

enum class IOStrategy : std::uint8_t {
    // Probes hardware capabilities via sysfs/statx and picks the best strategy below.
    AutoDetect,

    // LEVEL 0: Posix pwritev + O_DIRECT. Uses `fallocate` for HDDs.
    PosixDirect,

    // LEVEL 1/2: io_uring + O_DIRECT.
    // Batches multiple thread-local blocks into a single SQE chain.
    // Uses hardware FUA (Force Unit Access) if supported to skip fdatasync.
    UringGroupCommit,

    // LEVEL 3: io_uring + pwritev2 + RWF_ATOMIC.
    // Relies on Linux 6.11+ and NVMe AWUPF to safely overwrite 4KiB/16KiB
    // sectors in-place, eliminating space waste and torn writes.
    UringAtomic
};

struct IOConfig {
    IOStrategy strategy{IOStrategy::AutoDetect};

    // Size of the io_uring submission/completion queues (must be power of 2).
    // Both WAL and SSTable instances of the IoEngine need this.
    std::size_t uring_queue_depth{512};

    // If > 0, tells the Linux kernel to spawn a dedicated SQPOLL thread.
    // This allows the database to submit I/O with ZERO syscall context switches.
    std::chrono::milliseconds uring_sqpoll_idle{
        0}; // 0 = disabled, otherwise the idle timeout before the kernel thread goes to sleep.

    // Bypasses kernel virtual memory mapping overhead during hot-path I/O.
    bool pre_register_buffers{true};
    bool pre_register_files{true};
};

} // namespace stratadb::config