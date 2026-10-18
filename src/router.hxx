///
/// @file router.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The routing table: subscription registration under one lock, and the snapshot a publisher
/// resolves a topic into.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <cassert>
#include <cstddef>
#include <exception>
#include <expected>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common.hxx"
#include "error.hxx"
#include "handler.hxx"
#include "layer_matcher.hxx"
#include "matcher.hxx"
#include "message.hxx"
#include "route.hxx"
#include "subscription.hxx"
#include "topic.hxx"

namespace ba7lya::msgbus {

///
/// @brief Owns the subscriptions and turns a topic into the list of handlers that match it.
/// @details Two tables under one lock: the pattern index (a `matcher`, `layer_matcher` unless the
/// caller names another) and the handler table. One lock over both is what makes
/// subscribe-then-publish coherent — a publisher either sees a subscription in both tables or in
/// neither, never in one.
/// @details Nothing user-supplied ever runs under the lock. `resolve` copies the handlers it matches
/// and releases the lock before returning, so a handler is free to subscribe or unsubscribe from
/// inside a delivery without deadlocking on its own router.
/// @note Callbacks are held by copy, not by reference count. A `std::function` copy duplicates its
/// closure state, so `unsubscribe` destroying the table's copy cannot disturb a snapshot already in
/// flight. What no ownership scheme here can repair is a closure capturing a reference or a raw
/// pointer to something that dies first — that is the subscriber's contract to keep.
/// @note It issues the id and owns the registration; it does not mint the handle. The bus wraps the id in
/// a `subscription`, because the handle cancels by calling the bus, and the router is not what a caller
/// holds.
/// @note Not copyable or movable: the lock and the tables it guards have to travel together, and a
/// lock is not movable anyway.
///
class router {
public:
    /// @brief Builds a router over the default pattern index, a `layer_matcher`.
    router()
        : matcher_(std::make_unique<layer_matcher>()) {}

    /// @brief Builds a router over the matcher the caller supplies.
    /// @param patterns the pattern index; must not be null, and must outlive nothing but this router
    /// @throws std::invalid_argument if `patterns` is null
    /// @details The way to build the bus over a different matcher — `prefix_matcher`, for instance —
    /// without the router naming every implementation.
    explicit router(std::unique_ptr<matcher> patterns)
        : matcher_(std::move(patterns)) {
        if (!matcher_) { throw std::invalid_argument("the matcher must not be null"); }
    }

    router(const router&) = delete;
    router& operator=(const router&) = delete;
    router(router&&) = delete;
    router& operator=(router&&) = delete;

    ~router() = default;

    /// @brief Registers `callback` for every topic matching `pattern`.
    /// @param pattern a topic pattern, with `+` matching one level and `#` matching the rest
    /// @param callback called once per matching message; must not be empty
    /// @param options which executor should run the handler, and what to do when it falls behind
    /// @param on_error where a throw from `callback` goes; empty leaves it to `deliver`'s caller
    /// @return the new subscription's id, or why the pattern was rejected; nothing is registered on failure
    /// @throws std::invalid_argument if `callback` is empty. An empty handler is a programming error
    /// rather than a value the caller is expected to handle, so it is not an `error_code`.
    /// @note A rejected pattern does not consume an id: the counter advances only on success.
    /// @note The id is issued here and nowhere else, which is why `matcher::add` takes it as a
    /// parameter. Ids start at 1; 0 is reserved for "never issued".
    /// @note The id is the registration's whole name: `unsubscribe` takes it, and the bus wraps it in the
    /// `subscription` handle a caller keeps. Dropping the id itself cancels nothing.
    [[nodiscard]]
    result<subscription_id, error> subscribe(
        std::string_view pattern, handler callback, dispatch_options options = {}, error_handler on_error = {}
    ) {
        if (!callback) { throw std::invalid_argument("the handler must not be empty"); }

        const std::unique_lock<std::shared_mutex> lock(mutex_);

        const auto id = next_id_;
        const auto added = matcher_->add(pattern, id);
        if (!added) { return std::unexpected(added.error()); }

        next_id_ = id + 1;
        // Wrap-around would need 2^64 subscriptions; the assertion is here so that the day it is
        // reachable, it says so rather than silently reissuing a live id.
        assert(next_id_ != 0 && "the subscription id counter wrapped around");

        handlers_.emplace(id, registration { std::move(callback), std::move(on_error), options });
        return id;
    }

    /// @brief Cancels a subscription, dropping it from every pattern it holds.
    /// @return true if `id` was live; false if it was unknown or already cancelled
    /// @details Idempotent, and a miss has no side effect at all. The handler table is the authority
    /// on liveness: an id the matcher knows but the table does not is already cancelled.
    /// @note An id is never reissued, so a stale id can only fail to match.
    [[nodiscard]]
    bool unsubscribe(subscription_id id) {
        const std::unique_lock<std::shared_mutex> lock(mutex_);

        const auto live = handlers_.find(id);
        if (live == handlers_.end()) { return false; }
        handlers_.erase(live);

        const auto pruned = matcher_->remove(id);
        // Both tables move under the one lock, so a live handler without patterns is a broken
        // invariant rather than a case to handle.
        assert(pruned && "a live handler without a pattern is an invariant broken");
        static_cast<void>(pruned);
        return true;
    }

    /// @brief The subscriptions matching an already-parsed topic, as a snapshot to deliver.
    /// @param topic the published topic's levels; every one of them must be a literal
    /// @details Callers parse, not the router: a publish must report a malformed topic once, and
    /// parsing here would either duplicate that or grow this method an error channel. Use
    /// `parse_topic` from `topic.hxx`.
    /// @details The snapshot is taken under the shared lock and the lock is released before
    /// returning, which is what lets a handler register or cancel from inside a delivery.
    /// @note A subscription cancelled after this returns is still delivered: the `route` holds its
    /// own copy of the callback. The window is deliberate — re-checking liveness per delivery would
    /// cost a second lock acquisition on the hot path to close a race the contract does not name.
    /// @note The order is unspecified — the matches arrive in a `std::unordered_set` — so a caller
    /// must not read meaning into it.
    [[nodiscard]]
    std::vector<route> resolve(std::span<const topic_level> topic) const {
        std::vector<std::string_view> levels;
        levels.reserve(topic.size());
        for (const auto& level : topic) {
            // A published topic is all literals by the grammar, so the level kind carries no meaning here.
            assert(level.is_literal() && "a published topic level is never a wildcard");
            levels.push_back(level.text());
        }

        const std::shared_lock<std::shared_mutex> lock(mutex_);

        const auto matched = matcher_->match(levels);

        std::vector<route> routes;
        routes.reserve(matched.size());
        for (const auto id : matched) {
            const auto found = handlers_.find(id);
            // Both tables are read under the one lock, so they cannot disagree.
            assert(found != handlers_.end() && "the matcher and the handler table disagree");
            if (found == handlers_.end()) { continue; }
            routes.push_back(route { id, found->second.callback_, found->second.on_error_, found->second.options_ });
        }
        return routes;
    }

    /// @brief Runs one route, isolating whatever it throws.
    /// @param edge the subscription to run
    /// @param msg the message it is handed
    /// @return the exception the callback threw and no error handler consumed, or null when the
    /// delivery completed — or when `edge.on_error_` took the failure
    /// @details A throw with an error handler set is fully consumed here: it neither skips the
    /// sibling subscriptions nor reaches the caller. A throw without one is returned, and the
    /// siblings are still delivered either way.
    /// @note An error handler that throws is swallowed: there is nowhere left to report it, and
    /// letting it out would cost the remaining subscriptions their delivery.
    /// @note Static because it touches no router state, and must not: a worker thread running this
    /// holds no lock, which is the whole point of resolving to a snapshot first.
    [[nodiscard]]
    static std::exception_ptr deliver(const route& route, const message& msg) {
        try {
            route.callback_(msg);
        }
        catch (...) {
            const auto failure = std::current_exception();
            if (!route.on_error_) { return failure; }
            try {
                route.on_error_(msg, failure);
            }
            catch (...) {
                // Nowhere to report this. Swallowed so it cannot cost the remaining subscriptions.
            }
        }
        return nullptr;
    }

    /// @brief Runs every route, isolating each one.
    /// @return the first unhandled exception, or null when every route completed or handed its
    /// failure to an error handler
    /// @details Every route runs regardless of what the previous one threw.
    /// @note It reports only the deliveries that ran on the calling thread. A route whose executor is
    /// another thread is that thread's to run, and its failures are that thread's to report.
    [[nodiscard]]
    static std::exception_ptr deliver_all(std::span<const route> routes, const message& msg) {
        std::exception_ptr first;
        for (const auto& edge : routes) {
            if (const auto failure = deliver(edge, msg); failure && !first) { first = failure; }
        }
        return first;
    }

    /// @brief How many subscriptions are live.
    [[nodiscard]]
    std::size_t subscription_count() const noexcept {
        const std::shared_lock<std::shared_mutex> lock(mutex_);
        return handlers_.size();
    }

    /// @brief Whether any subscription is live.
    [[nodiscard]]
    bool empty() const noexcept {
        const std::shared_lock<std::shared_mutex> lock(mutex_);
        return handlers_.empty();
    }

private:
    /// @brief What a live subscription holds: its callback, its error sink, where it runs.
    /// @details One struct rather than three parallel maps, so a lookup for one subscriber touches
    /// one bucket and the three can never fall out of step.
    struct registration {
        handler callback_;
        error_handler on_error_;
        dispatch_options options_;
    };

    /// @brief Guards both tables below.
    /// Exclusive for a registration or a cancellation, shared for a resolution.
    mutable std::shared_mutex mutex_;
    /// @brief The matcher, and which subscriptions each pattern holds.
    std::unique_ptr<matcher> matcher_;
    /// @brief The subscriptions by id. The authority on liveness.
    std::unordered_map<subscription_id, registration> handlers_;
    /// @brief The next id to issue. 0 is reserved for "never issued".
    subscription_id next_id_ = 1;
};

} // namespace ba7lya::msgbus
