#pragma once

#include <concepts>
#include <optional>
#include <string_view>

namespace stratadb::memtable {

template <typename T>
concept IsMemTable = requires(T t, std::string_view key, std::string_view value) {
    // Inserts a key-value pair. Returns true on success (e.g., not OOM).
    { t.put(key, value) } -> std::same_as<bool>;

    // Inserts a tombstone for a key. Returns true on success.
    { t.remove(key) } -> std::same_as<bool>;

    // Gets a value. Returns a zero-copy view into the Arena, or nullopt.
    { t.get(key) } -> std::same_as<std::optional<std::string_view>>;

    // Exposes current memory footprint for flush triggers.
    { t.memory_usage() } -> std::same_as<std::size_t>;
};

} // namespace stratadb::memtable