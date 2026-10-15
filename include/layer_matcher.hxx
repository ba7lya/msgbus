///
/// @file layer_matcher.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The topic vocabulary and the matcher that resolves a published topic against the subscribed
/// patterns.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026
///
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstddef>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common.hxx"
#include "error.hxx"
#include "trie.hxx"

namespace ba7lya::msgbus {

///
/// @brief One level of a topic or of a pattern: the text it stands for, and which of the three roles it
/// plays.
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

    bool operator==(const topic_level&) const = default;
};

/// @brief The id set a trie node holds. A set rather than a list, so a subscriber registered under two
/// patterns that both match one topic is still reached once.
using subscription_set = std::unordered_set<subscription_id>;

///
/// @brief A topic or a pattern, as the list of levels it was split into.
/// @details Owning, because a pattern outlives the text it was parsed from: a subscription keeps its
/// pattern for as long as it is registered. This is the key the trie is indexed by.
///
struct topic_path {
    std::vector<topic_level> levels_;

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

    /// @brief Whether every level is a literal, which a published topic requires and a pattern need not be.
    [[nodiscard]]
    bool all_literal() const noexcept {
        for (const topic_level& level : levels_) {
            if (!level.is_literal()) { return false; }
        }
        return true;
    }

    /// @brief The path spelled back out, `/` separated.
    [[nodiscard]]
    std::string spell() const {
        std::string out;
        for (const topic_level& level : levels_) {
            if (!out.empty()) { out += '/'; }
            out += level.text();
        }
        return out;
    }

    bool operator==(const topic_path&) const = default;
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
        const std::size_t separator = text.find('/', start);
        const std::size_t end = separator == std::string_view::npos ? text.size() : separator;
        const std::string_view level = text.substr(start, end - start);

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
                const char character = text[at];
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

/// @brief Splits a subscription pattern, where `+` and `#` are grammar.
[[nodiscard]]
inline result<topic_path, error> parse_pattern(std::string_view pattern) {
    return parse_topic_levels(pattern, true);
}

/// @brief Splits a published topic, where no wildcard is allowed.
[[nodiscard]]
inline result<topic_path, error> parse_topic(std::string_view topic) {
    return parse_topic_levels(topic, false);
}

///
/// @brief Resolves published topics against subscribed patterns.
/// @details One trie of patterns, with the subscription ids registered under each pattern as that node's
/// payload. Matching is a single walk: at every level a literal takes the exact branch, a `+` takes the
/// wildcard branch, and a `#` harvested on the way down contributes its ids wherever the walk ends — which is
/// what makes `device/#` match `device` itself as well as everything below it.
/// @details The payload is a set of ids, so the walk collects each subscriber once no matter how many of its
/// patterns matched. That is the whole of the cross-pattern deduplication: there is no second pass to run,
/// because a set cannot hold an id twice.
/// @invariant A pattern the trie does not hold is a pattern no subscription was registered under.
/// @note Not thread-safe, and deliberately so: the router owns the locking, and one lock over both the
/// pattern index and the handler table is what keeps subscribe-then-publish coherent.
///
class layer_matcher {
public:
    layer_matcher() = default;

    /// @brief Parses `pattern` and registers `id` under it.
    /// @param pattern a topic pattern, with `+` matching one level and `#` matching the rest
    /// @param id the subscription to register; the caller issues the id
    /// @return `id`, or why the pattern was rejected; nothing is registered on failure
    /// @note Adding a pattern the same `id` already holds is `duplicate_pattern`, not a second registration.
    [[nodiscard]]
    result<subscription_id, error> add(std::string_view pattern, subscription_id id) {
        const auto parsed = parse_pattern(pattern);
        if (!parsed) { return std::unexpected(parsed.error()); }
        return add(parsed.value(), id);
    }

    /// @brief Registers `id` under an already-parsed `path`.
    /// @return `id`, or `duplicate_pattern` when this `id` already holds `path`
    [[nodiscard]]
    result<subscription_id, error> add(const topic_path& path, subscription_id id) {
        const auto held = by_subscription_.find(id);
        if (held != by_subscription_.end()) {
            const auto duplicate = std::find(held->second.begin(), held->second.end(), path);
            if (duplicate != held->second.end()) {
                return std::unexpected(error::make(error_code::DUPLICATE_PATTERN, path.spell()));
            }
            held->second.push_back(path);
            tree_.table_for(path).insert(id);
            return id;
        }

        tree_.table_for(path).insert(id);
        by_subscription_.emplace(id, std::vector<topic_path> { path });
        return id;
    }

    /// @brief Drops `id` from every pattern it holds, pruning what that leaves empty.
    /// @return true if `id` was registered; false if it was unknown or already removed
    /// @details Idempotent: a second call returns false and changes nothing. Answers whether it removed
    /// anything so that a caller holding a lock can skip a second pass when there was nothing to remove.
    ///
    [[nodiscard]]
    bool remove(subscription_id id) {
        const auto held = by_subscription_.find(id);
        if (held == by_subscription_.end()) { return false; }

        for (const topic_path& path : held->second) {
            const auto pruned = tree_.update_node(path.levels(), [id](subscription_set& ids) { ids.erase(id); });
            // A miss means the pattern is not in the trie, which would be an invariant broken rather than a
            // case to handle: the only way out of the trie is this loop, which also drops the pattern from
            // `by_subscription_`. Carrying on leaves the two consistent either way.
            assert(pruned.has_value() && "a registered pattern is missing from the trie");
            static_cast<void>(pruned);
        }
        by_subscription_.erase(held);
        return true;
    }

    /// @brief How many patterns `id` holds, or 0 when nothing is registered under it.
    [[nodiscard]]
    std::size_t pattern_count(subscription_id id) const {
        const auto held = by_subscription_.find(id);
        return held == by_subscription_.end() ? 0 : held->second.size();
    }

    /// @brief Parses `topic` and collects the subscriptions that match it.
    [[nodiscard]]
    result<subscription_set, error> match_topic(std::string_view topic) const {
        const auto parsed = parse_topic(topic);
        if (!parsed) { return std::unexpected(parsed.error()); }
        subscription_set matched;
        match(parsed.value().levels(), matched);
        return matched;
    }

    /// @brief Collects the subscriptions matching an already-parsed topic into `matched`.
    /// @param topic the published topic's levels; every one of them must be a literal
    /// @param matched appended to, not cleared: a caller reusing one set across several topics decides when
    /// to empty it
    /// @note A set holds each id once, so a subscriber whose patterns both match is collected once.
    void match(std::span<const topic_level> topic, subscription_set& matched) const {
        match_into(&tree_, topic, matched);
    }

    /// @brief Whether anything is registered under the level list `path`, reached through literal levels only.
    /// @return the ids stored at that node, or null when the walk cannot get there
    /// @note A diagnostic: the router tracks subscriptions by id and never asks by path. Wildcards are not
    /// traversed, so a path spelling one finds nothing.
    [[nodiscard]]
    const subscription_set* payload_at(std::span<const topic_level> path) const {
        const trie<subscription_set, topic_level>* node = &tree_;
        for (const topic_level& level : path) {
            if (!level.is_literal()) { return nullptr; }
            node = node->exact_child(level.text());
            if (node == nullptr) { return nullptr; }
        }
        return node->exact_match_payload();
    }

    /// @brief The same, for a path spelled as text.
    [[nodiscard]]
    const subscription_set* payload_at(std::string_view path) const {
        const auto parsed = parse_pattern(path);
        if (!parsed) { return nullptr; }
        return payload_at(parsed.value().levels());
    }

    /// @brief Every `(pattern, ids)` pair the matcher holds, for asserting what a sequence of calls left behind.
    /// @note The order is unspecified; compare the result as a set.
    [[nodiscard]]
    std::vector<std::pair<topic_path, subscription_set>> collect() const {
        std::vector<std::pair<topic_path, subscription_set>> collected;
        for (const auto& [path, payload] : tree_.collect_payloads<topic_path>()) {
            collected.emplace_back(path, *payload);
        }
        return collected;
    }

    /// @brief How many subscriptions are registered, distinct ids rather than patterns.
    [[nodiscard]]
    std::size_t subscription_count() const noexcept {
        return by_subscription_.size();
    }

    [[nodiscard]]
    bool empty() const noexcept {
        return by_subscription_.empty();
    }

private:
    /// @brief The walk: descended once per level, collecting the `#` payloads it passes and the terminal
    /// payload it arrives at.
    /// @note Only literal levels are traversed by name; a pattern's wildcards are the branches, not the input.
    void match_into(
        const trie<subscription_set, topic_level>* node, std::span<const topic_level> topic, subscription_set& matched
    ) const {
        // A `#` at this node covers this level and every level below it, so it contributes wherever the
        // walk is, including where it stops.
        if (const subscription_set* multi = node->multi_level_wildcard_payload()) {
            matched.insert(multi->begin(), multi->end());
        }

        if (topic.empty()) {
            if (const subscription_set* exact = node->exact_match_payload()) {
                matched.insert(exact->begin(), exact->end());
            }
            return;
        }

        // A published topic is all literals by the grammar, so a level names at most one branch.
        const topic_level& level = topic.front();
        assert(level.is_literal() && "a published topic level is never a wildcard");

        if (const trie<subscription_set, topic_level>* exact = node->exact_child(level.text())) {
            match_into(exact, topic.subspan(1), matched);
        }
        if (const trie<subscription_set, topic_level>* wildcard = node->single_level_wildcard_child()) {
            match_into(wildcard, topic.subspan(1), matched);
        }
    }

    /// @brief The patterns, indexed by level, each node holding the ids subscribed to the pattern ending there.
    trie<subscription_set, topic_level> tree_;
    /// @brief The reverse index: what each subscription holds, so removing it costs one walk per pattern
    /// rather than one walk of the whole tree.
    std::unordered_map<subscription_id, std::vector<topic_path>> by_subscription_;
};

} // namespace ba7lya::msgbus
