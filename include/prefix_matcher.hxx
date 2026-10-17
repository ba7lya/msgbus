///
/// @file prefix_matcher.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The matcher without a wildcard vocabulary: a subscription hears every topic that begins with
/// its literal path.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common.hxx"
#include "error.hxx"
#include "matcher.hxx"
#include "topic.hxx"
#include "trie.hxx"

namespace ba7lya::msgbus {

///
/// @brief Resolves published topics against subscribed literal prefixes.
/// @details The answer `layer_matcher` gives after its wildcards have been removed: a subscription at
/// `site/line1` hears `site/line1`, `site/line1/device7`, `site/line1/device7/temp` and so on, and
/// nothing that merely shares a spelling without sharing the levels. There is no `+` and no `#`, so a
/// pattern is a `topic_path` whose every level is a literal, and a topic matches when its first `n`
/// levels equal the pattern's.
/// @details The structure is the same trie as `layer_matcher`'s, used with two of its four slots: the
/// literal child and the exact-match payload. `table_for`, which is what the trie offers for inserting a
/// path, would create the wildcard branches too; that is harmless, since nothing ever walks them, and it
/// keeps the shared container unmodified.
/// @details The pattern `site` matches the topic `site` itself as well as everything under it. That is
/// the whole of the difference from `layer_matcher`: `#` was only ever the way to say "this level and
/// everything below it", and here that is the default.
/// @details An implementation of `matcher`, so the router can be built over either this or
/// `layer_matcher` without knowing which.
/// @note Not thread-safe, and deliberately so: the router owns the locking.
///
class prefix_matcher final : public matcher {
public:
    prefix_matcher() = default;

    /// @brief Parses `pattern` and registers `id` under it.
    /// @param pattern a literal topic path; a wildcard is rejected rather than interpreted
    /// @param id the subscription to register; the caller issues the id
    /// @return `id`, or why the pattern was rejected; nothing is registered on failure
    /// @note Adding a pattern the same `id` already holds is `duplicate_pattern`.
    [[nodiscard]]
    result<subscription_id, error> add(std::string_view pattern, subscription_id id) override {
        // The topic grammar, not the pattern grammar: this matcher has no wildcards, so `+` and `#` are
        // rejected here for the same reason they are rejected in a published topic.
        const auto parsed = parse_topic(pattern);
        if (!parsed) { return std::unexpected(parsed.error()); }
        return add(parsed.value(), id);
    }

    /// @brief Registers `id` under an already-parsed `path`.
    /// @return `id`, or `duplicate_pattern` when this `id` already holds `path`
    [[nodiscard]]
    result<subscription_id, error> add(const topic_path& path, subscription_id id) {
        if (!path.all_literal()) { return std::unexpected(error::make(error_code::INVALID_WILDCARD, path.spell())); }

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

    /// @brief Drops `id` from every prefix it holds, pruning what that leaves empty.
    /// @return true if `id` was registered; false if it was unknown or already removed
    [[nodiscard]]
    bool remove(subscription_id id) override {
        const auto held = by_subscription_.find(id);
        if (held == by_subscription_.end()) { return false; }

        for (const topic_path& path : held->second) {
            const auto pruned = tree_.update_node(path.levels(), [id](subscription_set& ids) { ids.erase(id); });
            assert(pruned.has_value() && "a registered prefix is missing from the trie");
            static_cast<void>(pruned);
        }
        by_subscription_.erase(held);
        return true;
    }

    /// @brief How many prefixes `id` holds, or 0 when nothing is registered under it.
    [[nodiscard]]
    std::size_t pattern_count(subscription_id id) const {
        const auto held = by_subscription_.find(id);
        return held == by_subscription_.end() ? 0 : held->second.size();
    }

    /// @brief Parses `topic` and collects the subscriptions whose prefix it begins with.
    [[nodiscard]]
    result<subscription_set, error> match_topic(std::string_view topic) const {
        const auto parsed = parse_topic(topic);
        if (!parsed) { return std::unexpected(parsed.error()); }
        return match(parsed.value().texts());
    }

    /// @brief The subscriptions whose prefix `topic` begins with.
    /// @param levels the published topic's levels, each one a literal spelling
    /// @note A set holds each id once, so a subscriber whose prefixes both cover the topic is collected once.
    [[nodiscard]]
    subscription_set match(std::span<const std::string_view> levels) const override {
        subscription_set matched;
        const trie<subscription_set, topic_level>* node = &tree_;
        for (const std::string_view level : levels) {
            if (const subscription_set* prefix = node->exact_match_payload()) {
                matched.insert(prefix->begin(), prefix->end());
            }
            node = node->exact_child(level);
            if (node == nullptr) { return matched; }
        }
        // The walk has consumed every level, so a prefix ending exactly here still covers the topic.
        if (const subscription_set* prefix = node->exact_match_payload()) {
            matched.insert(prefix->begin(), prefix->end());
        }
        return matched;
    }

    /// @brief Whether anything is registered under the level list `path`.
    /// @return the ids stored at that node, or null when the walk cannot get there
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
        const auto parsed = parse_topic(path);
        if (!parsed) { return nullptr; }
        return payload_at(parsed.value().levels());
    }

    /// @brief Every `(prefix, ids)` pair the matcher holds.
    /// @note The order is unspecified; compare the result as a set.
    [[nodiscard]]
    std::vector<std::pair<topic_path, subscription_set>> collect() const {
        std::vector<std::pair<topic_path, subscription_set>> collected;
        for (const auto& [path, payload] : tree_.collect_payloads<topic_path>()) {
            collected.emplace_back(path, *payload);
        }
        return collected;
    }

    /// @brief How many subscriptions are registered, distinct ids rather than prefixes.
    [[nodiscard]]
    std::size_t subscription_count() const noexcept {
        return by_subscription_.size();
    }

    [[nodiscard]]
    bool empty() const noexcept {
        return by_subscription_.empty();
    }

private:
    /// @brief The prefixes, indexed by level, each node holding the ids subscribed to the path ending there.
    trie<subscription_set, topic_level> tree_;
    /// @brief The reverse index: what each subscription holds, for removal and duplicate detection.
    std::unordered_map<subscription_id, std::vector<topic_path>> by_subscription_;
};

} // namespace ba7lya::msgbus
