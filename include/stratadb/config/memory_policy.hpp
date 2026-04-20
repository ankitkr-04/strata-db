#pragma once

#include <cstddef>
#include <cstdint>

namespace stratadb::config {

enum class PageStrategy : uint8_t {
    Standard4K,           // Safe, works everywhere.
    Huge2M_Opportunistic, // Try MAP_HUGE_2MB, fallback to 4K without crashing.
    Huge2M_Strict,        // Require MAP_HUGE_2MB, abort if unavailable.
    Huge1G_Opportunistic, // Try MAP_HUGE_1GB, fallback to 2MB then 4K without crashing.
    Huge1G_Strict         // Bleeding-edge bare-metal only.
};

enum class NumaPolicy : uint8_t {
    UMA,         // Ignore NUMA, rely on OS scheduler.
    Interleaved, // Striped allocation across nodes (predictable latency).
    StrictLocal  // Pin threads & allocate strictly local to the node.
};

enum class MemoryConfigError : uint8_t {
    TlabExceedsBudget,    // TLAB size cannot be larger than the total budget.
    BudgetNotPageAligned, // Budget is not a multiple of the chosen page strategy size.
    TlabNotPageAligned,   // TLAB size is not a multiple of the chosen page strategy size.
    ZeroMemoryBudget      // User allocated 0 bytes.
};

struct MemoryConfig {
    static constexpr std::size_t DEFAULT_TOTAL_BUDGET = 1024ULL * 1024 * 1024; // 1GB
    static constexpr std::size_t DEFAULT_TLAB_SIZE = 2ULL * 1024 * 1024;       // 2MB

    PageStrategy page_strategy{PageStrategy::Huge2M_Opportunistic};
    NumaPolicy numa_policy{NumaPolicy::UMA}; // Default to UMA for embedded safety

    std::size_t total_budget_bytes{DEFAULT_TOTAL_BUDGET};
    std::size_t tlab_size_bytes{DEFAULT_TLAB_SIZE};

    bool prefault{false};
};

} // namespace stratadb::config