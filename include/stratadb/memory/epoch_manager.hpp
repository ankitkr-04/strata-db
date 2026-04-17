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
        explicit ReadGuard(EpochManager& mgr) noexcept
            : mgr_(&mgr) {
            mgr_->enter();
        }

        ~ReadGuard() noexcept {
            if (mgr_ != nullptr) {
                mgr_->leave();
            }
        }

        ReadGuard(const ReadGuard&) = delete;
        ReadGuard& operator=(const ReadGuard&) = delete;

        // Safe move semantics: steal the pointer and hollow out the source
        ReadGuard(ReadGuard&& other) noexcept
            : mgr_(other.mgr_) {
            other.mgr_ = nullptr;
        }

        // Move assignment (optional, but good practice if you implement move construction)
        ReadGuard& operator=(ReadGuard&& other) noexcept {
            if (this != &other) {
                if (mgr_ != nullptr) {
                    mgr_->leave();
                }
                mgr_ = other.mgr_;
                other.mgr_ = nullptr;
            }
            return *this;
        }

      private:
        EpochManager* mgr_;
    };

    class ThreadGuard {
      public:
        explicit ThreadGuard(EpochManager& mgr)
            : mgr_(mgr) {
            mgr_.register_thread();
        }

        ~ThreadGuard() noexcept {
            mgr_.unregister_thread();
        }

        ThreadGuard(const ThreadGuard&) = delete;
        ThreadGuard& operator=(const ThreadGuard&) = delete;

      private:
        EpochManager& mgr_;
    };

    friend class ReadGuard;   // Allow ReadGuard to call private enter/leave methods
    friend class ThreadGuard; // Allow ThreadGuard to call private register/unregister methods

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
    void enter() noexcept;
    void leave() noexcept;
    void register_thread();
    void unregister_thread();
};

} // namespace stratadb::memory