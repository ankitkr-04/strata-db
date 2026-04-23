#pragma once

#include "stratadb/utils/hardware.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <vector>

namespace stratadb::memory {
enum class EpochError : std::uint8_t { ThreadLimitExceeded };
class EpochManager {
  public:
    class [[nodiscard]] ReadGuard {
      public:
        [[nodiscard]] explicit ReadGuard(EpochManager& mgr) noexcept
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
        ReadGuard(ReadGuard&&) = delete;
        ReadGuard& operator=(ReadGuard&&) = delete;

      private:
        EpochManager* mgr_;
    };

    class [[nodiscard]] ThreadRegistrationGuard {
      public:
        [[nodiscard]] explicit ThreadRegistrationGuard(EpochManager& mgr) noexcept
            : mgr_(mgr)
            , result_(mgr_.register_thread()) {}

        ~ThreadRegistrationGuard() noexcept {
            if (result_.has_value())
                mgr_.unregister_thread();
        }

        ThreadRegistrationGuard(const ThreadRegistrationGuard&) = delete;
        auto operator=(const ThreadRegistrationGuard&) -> ThreadRegistrationGuard& = delete;
        ThreadRegistrationGuard(ThreadRegistrationGuard&&) = delete;
        auto operator=(ThreadRegistrationGuard&&) -> ThreadRegistrationGuard& = delete;

        [[nodiscard]] auto is_registered() const noexcept -> bool {
            return result_.has_value();
        }

        [[nodiscard]] auto result() const noexcept -> std::expected<void, EpochError> {
            return result_;
        }

      private:
        EpochManager& mgr_;
        std::expected<void, EpochError> result_;
    };

    friend class ReadGuard;
    friend class ThreadRegistrationGuard;

    template <typename T>
    void retire(T* ptr) noexcept {
        if (ptr == nullptr)
            return;
        retire_node(static_cast<void*>(ptr), [](void* p) noexcept -> auto { delete static_cast<T*>(p); });
    }

    [[nodiscard]] static auto is_registered() noexcept -> bool {
        return thread_index_ != INVALID_THREAD;
    }

    // Internal maintenance hook for external components/tests that need deterministic progress.
    // Requires current thread to be registered.
    void quiescent_reclaim() noexcept;

    // Single-threaded teardown escape hatch: bypasses epoch checks and drains every retire list.
    void force_reclaim_all() noexcept;

  private:
    // 128 threads x 64-byte padded ThreadState ~= 8KB bookkeeping.
    static constexpr std::size_t MAX_THREADS = 128;
    static constexpr std::size_t MASK_WORD_BITS = std::numeric_limits<std::uint64_t>::digits;
    static constexpr std::size_t ACTIVE_THREAD_MASK_WORDS = MAX_THREADS / MASK_WORD_BITS;
    // Throughput tuning knob: trigger epoch advance/reclaim every N retirements.
    static constexpr std::size_t RECLAIM_INTERVAL = 64;
    static constexpr std::size_t RECLAIM_INTERVAL_MASK = RECLAIM_INTERVAL - 1;
    // Yield if a single thread accumulates too many unreclaimed pointers.
    static constexpr std::size_t RETIRE_LIST_THRESHOLD = 10000;
    static constexpr std::size_t INVALID_THREAD = std::numeric_limits<std::size_t>::max();
    static constexpr std::uint64_t INACTIVE_EPOCH = std::numeric_limits<std::uint64_t>::max();
    static_assert(MAX_THREADS % MASK_WORD_BITS == 0);

    struct RetireNode {
        void* ptr{nullptr};
        void (*deleter)(void*){nullptr};
        std::uint64_t retire_epoch{0};
    };

    struct alignas(stratadb::utils::CACHE_LINE_SIZE) ThreadState {
        std::atomic<std::uint64_t> state{INACTIVE_EPOCH};
        std::vector<RetireNode> retire_list_;
    };

    struct alignas(stratadb::utils::CACHE_LINE_SIZE) ActiveThreadMaskWord {
        std::atomic<std::uint64_t> bits{0};
    };

  private:
    std::atomic<std::uint64_t> global_epoch_{0};

    std::array<ThreadState, MAX_THREADS> thread_states_{};
    std::array<ActiveThreadMaskWord, ACTIVE_THREAD_MASK_WORDS> active_thread_masks_{};

    inline static thread_local std::size_t thread_index_{INVALID_THREAD};

  private:
    void retire_node(void* ptr, void (*deleter)(void*)) noexcept;
    void enter() noexcept;
    void leave() noexcept;
    void advance_epoch() noexcept;
    void reclaim() noexcept;
    [[nodiscard]] auto register_thread() noexcept -> std::expected<void, EpochError>;
    void unregister_thread();
};

} // namespace stratadb::memory
