///
/// @file matcher.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The interface a pattern index implements, which is what the router registers subscriptions
/// through.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <expected>
#include <span>
#include <string_view>

#include "common.hxx"
#include "error.hxx"

namespace ba7lya::msgbus {

///
/// @brief What a pattern index has to provide: register an id under a pattern, drop it again, and
/// collect the ids a published topic reaches.
/// @details Deliberately the three operations the router calls and nothing else. The diagnostics — what
/// is stored under one path, what the index holds in total — are shaped by the structure underneath, so
/// they belong to the concrete matchers, not here.
/// @note A matcher is stateful and neither copyable nor movable in practice; a trie is move-only at its
/// root, so an implementation built over one is too. Nothing in the interface requires either, so a
/// future implementation over a flat table is still expressible.
/// @note Not thread-safe: the router owns the lock, and one lock over both the index and the handler
/// table is what keeps subscribe-then-publish coherent.
///
class matcher {
public:
    /// @brief Virtual so that a matcher is safe to destroy through a `matcher&`.
    virtual ~matcher() = default;

    /// @brief Registers `id` under `pattern`.
    /// @param pattern a pattern in whatever spelling this index accepts
    /// @param id the subscription to register; the caller issues the id
    /// @return `id`, or why the pattern was rejected; nothing is registered on failure
    [[nodiscard]]
    virtual result<subscription_id, error> add(std::string_view pattern, subscription_id id) = 0;

    /// @brief Drops `id` from every pattern it holds.
    /// @return true if `id` was registered; false if it was unknown or already removed
    [[nodiscard]]
    virtual bool remove(subscription_id id) = 0;

    /// @brief The ids matching an already-parsed, all-literal topic.
    /// @param levels the published topic's levels, each one a literal spelling
    /// @return every matching subscription, each id once
    /// @details Takes spellings rather than `topic_level`s: a published topic is all literals by the
    /// grammar, so the level kind a pattern carries would be dead weight on this path. Accepting views
    /// also lets a caller keep the levels it already owns instead of rebuilding them for this call.
    /// @details Returned by value rather than appended into a caller's set: the result belongs to this
    /// one call, and a caller that wants to accumulate across several topics can do so on its side.
    [[nodiscard]]
    virtual subscription_set match(std::span<const std::string_view> levels) const = 0;
};

} // namespace ba7lya::msgbus
