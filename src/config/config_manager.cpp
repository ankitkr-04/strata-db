#include "stratadb/config/config_manager.hpp"

#include <exception>
#include <new>
#include <utility>

namespace stratadb::config {

ConfigManager::ConfigManager(ImmutableConfig imm, MutableConfig mut, memory::EpochManager& epoch_mgr) noexcept
    : immutable_config_(std::move(imm))
    , epoch_mgr_(epoch_mgr) {

    auto* ptr = new (std::nothrow) MutableConfig{std::move(mut)};
    if (!ptr) {
        std::terminate();
    }

    current_mutable_config_.store(ptr, std::memory_order_release);
}

ConfigManager::~ConfigManager() noexcept {
    auto* ptr = current_mutable_config_.load(std::memory_order_acquire);

    if (ptr) {
        current_mutable_config_.store(nullptr, std::memory_order_release);
        delete ptr;
    }

    epoch_mgr_.force_reclaim_all();
}

ConfigManager::ReadGuard ConfigManager::get_mutable() const noexcept {
    return ConfigManager::ReadGuard(*this);
}

void ConfigManager::update_mutable(MutableConfig new_cfg) noexcept {
    auto* new_ptr = new (std::nothrow) MutableConfig{std::move(new_cfg)};
    if (!new_ptr) {
        std::terminate();
    }

    std::lock_guard lock(update_mutex_);

    MutableConfig* old_ptr = current_mutable_config_.exchange(new_ptr, std::memory_order_acq_rel);

    if (old_ptr) {
        epoch_mgr_.retire(old_ptr);
    }
}

} // namespace stratadb::config
