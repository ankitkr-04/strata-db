#pragma once

#include "stratadb/memtable/memtable_result.hpp"
#include "stratadb/memory/tlab.hpp"

#include <concepts>
#include <optional>
#include <string_view>

namespace stratadb::memtable {

template <typename T>
concept IsMemTable =
    requires(T t, std::string_view key, std::string_view value, memory::TLAB& tlab) {
        // Inserts a key-value pair.
        { t.put(key, value, tlab) } -> std::convertible_to<PutResult>;

        // Inserts a tombstone for a key.
        { t.remove(key, tlab) } -> std::convertible_to<PutResult>;

        // Gets a value. Returns a zero-copy view into the Arena, or nullopt.
        { t.get(key) } -> std::same_as<std::optional<std::string_view>>;

        // Exposes current memory footprint for flush triggers.
        { t.memory_usage() } -> std::same_as<std::size_t>;

        // true when memory_usage() exceeds internal flush thresholds, false otherwise.
        { t.should_flush() } -> std::convertible_to<bool>;

        // Sorted forward scan over all in-memory entries.
        { t.scan([](const typename T::EntryView&) {}) } -> std::same_as<void>;
    };

} // namespace stratadb::memtable