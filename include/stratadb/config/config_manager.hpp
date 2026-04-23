#pragma once

#include "immutable_config.hpp"
#include "mutable_config.hpp"
#include "stratadb/memory/epoch_manager.hpp"
#include "stratadb/utils/hardware.hpp"

#include <atomic>
#include <mutex>

namespace stratadb::config {

class ConfigManager {
  public:
    ConfigManager(ImmutableConfig imm, MutableConfig mut, memory::EpochManager& epoch_mgr) noexcept;

    ~ConfigManager() noexcept;

    ConfigManager(const ConfigManager&) = delete;
    ConfigManager& operator=(const ConfigManager&) = delete;
    ConfigManager(ConfigManager&&) = delete;
    ConfigManager& operator=(ConfigManager&&) = delete;

    class [[nodiscard]] ReadGuard {
      public:
        ReadGuard(const ReadGuard&) = delete;
        ReadGuard& operator=(const ReadGuard&) = delete;

        ReadGuard(ReadGuard&&) = delete;
        ReadGuard& operator=(ReadGuard&&) = delete;

        [[nodiscard]] const MutableConfig* operator->() const noexcept {
            return config_;
        }

        [[nodiscard]] const MutableConfig& get() const noexcept {
            return *config_;
        }

      private:
        friend class ConfigManager;

        [[nodiscard]] explicit ReadGuard(const ConfigManager& manager) noexcept
            : epoch_guard_(manager.epoch_mgr_)
            , config_(manager.current_mutable_config_.load(std::memory_order_acquire)) {}

        memory::EpochManager::ReadGuard epoch_guard_;
        const MutableConfig* config_;
    };

    // Relies on guaranteed copy elision (C++17) because ReadGuard is non-copyable and non-movable.
    [[nodiscard]] ReadGuard get_mutable() const noexcept;

    void update_mutable(MutableConfig new_cfg) noexcept;

  private:
    ImmutableConfig immutable_config_;

    alignas(stratadb::utils::CACHE_LINE_SIZE) std::atomic<MutableConfig*> current_mutable_config_{nullptr};

    memory::EpochManager& epoch_mgr_;

    std::mutex update_mutex_;
};

} // namespace stratadb::config
