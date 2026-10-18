///
/// @file topic.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The topic vocabulary: a level, a path, the id set a node holds, and the one place the
/// grammar is written down.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <cctype>
#include <cstddef>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "error.hxx"

namespace ba7lya::msgbus {

///
/// @brief A topic, as in `device/room1/temp`: the concrete text a publisher names.
/// @details Borrowed, because naming a topic is not owning it. An entry point takes the alias by value
/// and the `message` it builds keeps its own copy, so the caller may name a topic with a literal, a
/// `std::string`, or a view of either, and the text need only outlive the call — a queued message in
/// particular is routed on the worker thread after its publisher has returned and owns its own text.
/// Everything that reads a topic — parsing, matching, logging — takes the alias too.
///
using topic = std::string_view;

///
/// @brief The role a level plays in the sequence it belongs to.
/// @details The tree treats each role differently: a literal level gets a child of its own, a single-level
/// wildcard gets one shared child, and a multi-level wildcard gets a payload and no child — being the last
/// level by definition. The vocabulary lives beside `topic_level` rather than with the tree, so that the
/// tree is what depends on the topic types and not the other way round.
///
enum class level_kind {
    LITERAL,      ///< stands for itself
    SINGLE_LEVEL, ///< stands for exactly one level
    MULTI_LEVEL,  ///< stands for the remaining levels, and only ever ends a sequence
};

///
/// @brief One level of a topic or of a pattern: the text it stands for, and which of the three roles it plays.
/// @details A literal level stands for itself; `+` stands for any one level; `#` stands for the remaining
/// levels and only ever ends a pattern. This is the vocabulary the trie is indexed by, which is all the trie
/// needs to know — nothing here tells it what a topic is.
///
struct topic_level {
    level_kind kind_ = level_kind::LITERAL;
    std::string text_;

    [[nodiscard]]
    level_kind kind() const noexcept {
        return kind_;
    }

    /// @brief The level's own spelling: the literal text, or `+` / `#` for a wildcard.
    /// @note Satisfies the trie's `trie_level` concept.
    [[nodiscard]]
    std::string_view text() const noexcept {
        return text_;
    }

    [[nodiscard]]
    bool is_literal() const noexcept {
        return kind_ == level_kind::LITERAL;
    }

    [[nodiscard]]
    bool is_single_level() const noexcept {
        return kind_ == level_kind::SINGLE_LEVEL;
    }

    [[nodiscard]]
    bool is_multi_level() const noexcept {
        return kind_ == level_kind::MULTI_LEVEL;
    }

    [[nodiscard]]
    static topic_level literal(std::string_view text) {
        return topic_level { level_kind::LITERAL, std::string(text) };
    }

    [[nodiscard]]
    static topic_level single_level() {
        return topic_level { level_kind::SINGLE_LEVEL, "+" };
    }

    [[nodiscard]]
    static topic_level multi_level() {
        return topic_level { level_kind::MULTI_LEVEL, "#" };
    }

    /// @brief Whether two levels play the same role and spell the same text.
    /// @details A hidden friend, and defaulted: found by argument-dependent lookup alone, so it never joins
    /// the overload set of a translation unit that includes this header, and `= default` compares both
    /// members with `!=` synthesized from it.
    [[nodiscard]]
    friend bool operator==(const topic_level&, const topic_level&) = default;
};

///
/// @brief A topic or a pattern, as the list of levels it was split into.
/// @details Owning, because a pattern outlives the text it was parsed from: a subscription keeps its pattern for as
/// long as it is registered. This is the key the trie is indexed by.
///
struct topic_path {
    [[nodiscard]]
    std::span<const topic_level> levels() const noexcept {
        return levels_;
    }

    /// @brief Rebuilds a path from its levels; what the trie calls when it walks itself.
    /// @note Satisfies the trie's `trie_key` concept. Carries no error channel: every path already in the
    /// trie was validated before it was inserted, so rebuilding one cannot fail.
    [[nodiscard]]
    static topic_path from_levels(std::span<const topic_level> levels) {
        return topic_path { std::vector<topic_level>(levels.begin(), levels.end()) };
    }

    [[nodiscard]]
    bool empty() const noexcept {
        return levels_.empty();
    }

    [[nodiscard]]
    std::size_t size() const noexcept {
        return levels_.size();
    }

    /// @brief The levels' spellings, for a consumer that only reads them.
    [[nodiscard]]
    std::vector<std::string_view> texts() const {
        std::vector<std::string_view> spellings;
        spellings.reserve(levels_.size());
        for (const auto& level : levels_) { spellings.push_back(level.text()); }
        return spellings;
    }

    /// @brief Whether every level is a literal, which a published topic requires and a pattern need not be.
    [[nodiscard]]
    bool all_literal() const noexcept {
        for (const auto& level : levels_) {
            if (!level.is_literal()) { return false; }
        }
        return true;
    }

    /// @brief The path spelled back out, `/` separated.
    [[nodiscard]]
    std::string spell() const {
        std::string out;
        for (const auto& level : levels_) {
            if (!out.empty()) { out += '/'; }
            out += level.text();
        }
        return out;
    }

    /// @brief Whether two paths hold the same levels in the same order.
    /// @details A hidden friend for the same reason as 	opic_level: defaulted, and found by ADL alone.
    [[nodiscard]]
    friend bool operator==(const topic_path&, const topic_path&) = default;

    std::vector<topic_level> levels_;
};

///
/// @brief Splits `text` into levels, reporting the first thing wrong with it.
/// @details The one place the topic grammar is written down. A pattern may hold wildcards, a published topic
/// may not, and the only difference between parsing the two is that flag — so `+` in a topic and `+` in a
/// pattern are rejected for different reasons but by the same code path.
/// @param text the topic or pattern to split
/// @param allow_wildcards whether `+` and `#` are grammar here
/// @return the levels, or why the text is not a topic
/// @note An empty level is rejected wherever it appears, so a leading, trailing or doubled `/` all fail.
///
[[nodiscard]]
inline result<topic_path, error> parse_topic_levels(std::string_view text, bool allow_wildcards) {
    if (text.empty()) { return std::unexpected(error::make(error_code::EMPTY_INPUT, text)); }

    topic_path path;
    std::size_t start = 0;
    while (true) {
        const auto separator = text.find('/', start);
        const auto end = separator == std::string_view::npos ? text.size() : separator;
        const auto level = text.substr(start, end - start);

        if (level.empty()) { return std::unexpected(error::make(error_code::EMPTY_LEVEL, text, start)); }

        if (level == "+") {
            if (!allow_wildcards) { return std::unexpected(error::make(error_code::WILDCARD_IN_TOPIC, text, start)); }
            path.levels_.push_back(topic_level::single_level());
        }
        else if (level == "#") {
            if (!allow_wildcards) { return std::unexpected(error::make(error_code::WILDCARD_IN_TOPIC, text, start)); }
            if (end != text.size()) {
                return std::unexpected(error::make(error_code::TRAILING_WILDCARD_NOT_LAST, text, start));
            }
            path.levels_.push_back(topic_level::multi_level());
        }
        else {
            for (std::size_t at = start; at < end; ++at) {
                const auto character = text[at];
                if (character == '+' || character == '#') {
                    return std::unexpected(error::make(error_code::INVALID_WILDCARD, text, at));
                }
                const auto code = static_cast<unsigned char>(character);
                if (std::isspace(code) != 0 || std::iscntrl(code) != 0) {
                    return std::unexpected(error::make(error_code::INVALID_CHARACTER, text, at));
                }
            }
            path.levels_.push_back(topic_level::literal(level));
        }

        if (separator == std::string_view::npos) { break; }
        start = separator + 1;
    }

    return path;
}

/// @brief Splits a published topic, where no wildcard is allowed.
[[nodiscard]]
inline result<topic_path, error> parse_topic(std::string_view topic) {
    return parse_topic_levels(topic, false);
}

} // namespace ba7lya::msgbus
