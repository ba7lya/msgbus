///
/// @file layer_matcher.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The matcher that resolves a published topic against the subscribed patterns, one step per
/// level of a trie.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <expected>
#include <span>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "error.hxx"
#include "matcher.hxx"
#include "subscription.hxx"
#include "topic.hxx"
#include "trie.hxx"

namespace ba7lya::msgbus {

/// @brief Splits a subscription pattern, where `+` and `#` are grammar.
[[nodiscard]]
inline result<topic_path, error> parse_pattern(std::string_view pattern) {
    return parse_topic_levels(pattern, true);
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
/// @details An implementation of `matcher`, and the one that reads `+` and `#`. A `prefix_matcher` is what a
/// caller reaches for when the pattern vocabulary is too much: it knows no wildcards at all, so every level of
/// a pattern it accepts is a literal.
/// @invariant A pattern the trie does not hold is a pattern no subscription was registered under.
/// @note Not thread-safe, and deliberately so: the router owns the locking, and one lock over both the
/// pattern index and the handler table is what keeps subscribe-then-publish coherent.
///
class layer_matcher final : public matcher {
public:
    layer_matcher() = default;

    /// @brief Parses `pattern` and registers `id` under it.
    /// @param pattern a topic pattern, with `+` matching one level and `#` matching the rest
    /// @param id the subscription to register; the caller issues the id
    /// @return `id`, or why the pattern was rejected; nothing is registered on failure
    /// @note Adding a pattern the same `id` already holds is `DUPLICATE_PATTERN`, not a second registration.
    [[nodiscard]]
    result<subscription_id, error> add(std::string_view pattern, subscription_id id) override {
        const auto parsed = parse_pattern(pattern);
        if (!parsed) { return std::unexpected(parsed.error()); }
        return add(parsed.value(), id);
    }

    /// @brief Registers `id` under an already-parsed `path`.
    /// @return `id`, or `DUPLICATE_PATTERN` when this `id` already holds `path`
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
    bool remove(subscription_id id) override {
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
        return match(parsed.value().texts());
    }

    /// @brief The subscriptions matching an already-parsed topic.
    /// @param levels the published topic's levels, each one a literal spelling
    /// @note A set holds each id once, so a subscriber whose patterns both match is collected once.
    [[nodiscard]]
    subscription_set match(std::span<const std::string_view> levels) const override {
        subscription_set matched;
        match_into(&tree_, levels, matched);
        return matched;
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
        const trie<subscription_set, topic_level>* node, std::span<const std::string_view> levels,
        subscription_set& matched
    ) const {
        // A `#` at this node covers this level and every level below it, so it contributes wherever the
        // walk is, including where it stops.
        if (const subscription_set* multi = node->multi_level_wildcard_payload()) {
            matched.insert(multi->begin(), multi->end());
        }

        if (levels.empty()) {
            if (const subscription_set* exact = node->exact_match_payload()) {
                matched.insert(exact->begin(), exact->end());
            }
            return;
        }

        // A published topic is all literals by the grammar, so a level names at most one branch.
        if (const trie<subscription_set, topic_level>* exact = node->exact_child(levels.front())) {
            match_into(exact, levels.subspan(1), matched);
        }
        if (const trie<subscription_set, topic_level>* wildcard = node->single_level_wildcard_child()) {
            match_into(wildcard, levels.subspan(1), matched);
        }
    }

    /// @brief The patterns, indexed by level, each node holding the ids subscribed to the pattern ending there.
    trie<subscription_set, topic_level> tree_;
    /// @brief The reverse index: what each subscription holds, so removing it costs one walk per pattern
    /// rather than one walk of the whole tree.
    std::unordered_map<subscription_id, std::vector<topic_path>> by_subscription_;
};

} // namespace ba7lya::msgbus
