#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <limits>
#include <new>
#include <vector>

namespace stratadb::memory {

namespace defaults {
constexpr std::size_t MAX_THREADS = 128;
constexpr std::size_t RECLAIM_BATCH = 64;
constexpr std::size_t RECLAIM_MASK = RECLAIM_BATCH - 1;
constexpr std::size_t RETIRE_LIST_THRESHOLD = 10000;

} // namespace defaults

class EpochManager {
  public:
    class ReadGuard {
      public:
        explicit ReadGuard(EpochManager& mgr)
            : mgr_(mgr) {
            mgr_.enter();
        }

        ~ReadGuard() {
            mgr_.leave();
        }

      private:
        EpochManager& mgr_;
    };

    void register_thread();
    void unregister_thread();

    // TODO: enter and leave method will be private, and Readguad will be friend class. after we finalize the config
    // manager

    void enter() noexcept;
    void leave() noexcept;

    template <typename T>
    void retire(T* ptr) noexcept {
        if (ptr == nullptr)
            return;
        retire_node(static_cast<void*>(ptr), [](void* p) noexcept { delete static_cast<T*>(p); });
    }
    void advance_epoch() noexcept;
    void reclaim() noexcept;

  private:
    static constexpr std::size_t INVALID_THREAD = std::numeric_limits<std::size_t>::max();
    struct RetireNode {
        void* ptr{nullptr};
        void (*deleter)(void*){nullptr};
        uint64_t retire_epoch{0};
    };

    struct alignas(std::hardware_destructive_interference_size) ThreadState {
        std::atomic<uint64_t> state{UINT64_MAX};
        std::vector<RetireNode> retire_list_;
        std::atomic<bool> in_use{false};
    };

  private:
    std::atomic<uint64_t> global_epoch_{0};

    std::array<ThreadState, defaults::MAX_THREADS> thread_states_{};

    inline static thread_local std::size_t thread_index_{INVALID_THREAD};

  private:
    void retire_node(void* ptr, void (*deleter)(void*)) noexcept;
};

} // namespace stratadb::memory