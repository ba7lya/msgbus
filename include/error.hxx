///
/// @file error.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The error vocabulary the core shares: one code per failure mode, plus the offending text.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <cstddef>
#include <expected>
#include <string>
#include <string_view>

namespace ba7lya::msgbus {

///
/// @brief Either a value of `T` or an error of `E`.
/// @details Pattern parsing and trie lookups fail in ways the caller is expected to handle, so they
/// report by value instead of throwing; exceptions stay reserved for genuine programming errors —
/// an empty handler, an out-of-range access. No `void` specialisation is needed: the standard
/// `std::expected<void, E>` already means "succeeded, carrying nothing".
///
/// A bare alias, with no wrapper: the member names (`has_value`, `value`, `error`, `value_or`,
/// `operator bool`) and the conversion from `std::unexpected` are the standard ones. Reading the
/// branch that is not held throws `std::bad_expected_access<E>`.
/// @tparam T the value produced on success
/// @tparam E the error produced on failure
///
template<typename T, typename E>
using result = std::expected<T, E>;

///
/// @brief How a topic went wrong. Every failure the core reports is one of these.
/// @details An enum rather than an exception hierarchy: the core reports failure by value through
/// `result`, so the code has to be data. `%_error` names keep the numeric contract stable while a
/// message describes the instance.
///
enum class error_code {
    EMPTY_INPUT,                ///< The text was empty where a pattern or topic is required.
    EMPTY_LEVEL,                ///< A level between separators was empty, as in `device//temp`.
    INVALID_CHARACTER,          ///< A level contained a character the grammar does not allow.
    INVALID_WILDCARD,           ///< `+` or `#` appeared somewhere the grammar does not allow it.
    TRAILING_WILDCARD_NOT_LAST, ///< `#` was not the last level, as in `device/#/temp`.
    WILDCARD_IN_TOPIC,          ///< A wildcard was used where a concrete topic is required, as in a publish target.
    NO_SUCH_LEVEL,              ///< A level of a pattern has no node in the trie, so nothing was stored under it.
    DUPLICATE_PATTERN,          ///< The pattern was already subscribed by this subscriber.
};

/// @brief The code's name, for a message.
/// @details Spelled out rather than reflected, because the vocabulary is small and closed: a `switch`
/// that names every code makes adding one a compile-time event — a new enumerator with no arm here is
/// what `-Wswitch` reports, and no `default` is written precisely so that it can.
[[nodiscard]]
constexpr std::string_view to_string(error_code code) noexcept {
    switch (code) {
    case error_code::EMPTY_INPUT:                return "the input is empty";
    case error_code::EMPTY_LEVEL:                return "a level between separators is empty";
    case error_code::INVALID_CHARACTER:          return "a level holds a character the grammar does not allow";
    case error_code::INVALID_WILDCARD:           return "a wildcard is somewhere the grammar does not allow it";
    case error_code::TRAILING_WILDCARD_NOT_LAST: return "the multi-level wildcard is not the last level";
    case error_code::WILDCARD_IN_TOPIC:          return "a wildcard is in a place that requires a concrete topic";
    case error_code::NO_SUCH_LEVEL:              return "a pattern level has no node, so nothing is stored under it";
    case error_code::DUPLICATE_PATTERN:          return "the pattern is already subscribed by this subscriber";
    }
    return "unknown error code";
}

///
/// @brief One failure: what went wrong, the text it went wrong on, and where.
/// @details `where_` is the byte offset into `text_` where the problem starts, or `npos` when the
/// failure is about the text as a whole. Carrying the offset lets a caller point at the offending
/// level instead of restating the whole topic.
///
struct error {
    /// @brief The offset used when a failure has no single position.
    static constexpr std::size_t npos = static_cast<std::size_t>(-1);

    /// @brief Builds an error with no position.
    [[nodiscard]]
    static error make(error_code code, std::string_view text) {
        return error { code, std::string(text), npos };
    }

    /// @brief Builds an error pointing at `where` within `text`.
    [[nodiscard]]
    static error make(error_code code, std::string_view text, std::size_t where) {
        return error { code, std::string(text), where };
    }

    /// @brief Whether two errors describe the same failure of the same text at the same place.
    /// @details A hidden friend so the comparison is found by argument-dependent lookup only, which keeps
    /// it out of the overload set of every translation unit that includes this header.
    [[nodiscard]]
    friend bool operator==(const error&, const error&) = default;

    error_code code_ = error_code::EMPTY_INPUT;
    std::string text_;
    std::size_t where_ = npos;
};

} // namespace ba7lya::msgbus
