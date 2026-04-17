#pragma once

#include "immutable_config.hpp"
#include "mutable_config.hpp"
#include "stratadb/memory/epoch_manager.hpp"

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

    class ReadGuard {
      public:
        ReadGuard(memory::EpochManager::ReadGuard&& guard, const MutableConfig* cfg) noexcept
            : epoch_guard_(std::move(guard))
            , config_(cfg) {}

        ReadGuard(const ReadGuard&) = delete;
        ReadGuard& operator=(const ReadGuard&) = delete;

        ReadGuard(ReadGuard&&) noexcept = default;
        ReadGuard& operator=(ReadGuard&&) = delete;

        [[nodiscard]] const MutableConfig* operator->() const noexcept {
            return config_;
        }

        [[nodiscard]] const MutableConfig& get() const noexcept {
            return *config_;
        }

      private:
        memory::EpochManager::ReadGuard epoch_guard_;
        const MutableConfig* config_;
    };

    [[nodiscard]] ReadGuard get_mutable() const noexcept;

    void update_mutable(MutableConfig new_cfg) noexcept;

  private:
    ImmutableConfig immutable_config_;

    alignas(std::hardware_destructive_interference_size) std::atomic<MutableConfig*> current_mutable_config_{nullptr};

    memory::EpochManager& epoch_mgr_;

    std::mutex update_mutex_;
};

} // namespace stratadb::config