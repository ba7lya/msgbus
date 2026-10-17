///
/// @file dispatcher.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The seam between the routing table and the executors: it resolves a topic, runs what belongs on
/// the calling thread, and hands the rest to the executor each subscriber named.
/// @version 0.1
/// @date 2026-10-16
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common.hxx"
#include "executor.hxx"
#include "handler.hxx"
#include "message.hxx"
#include "router.hxx"

namespace ba7lya::msgbus {

///
/// @brief Owns the executors a subscriber may name, and fans a resolved snapshot out over them.
/// @details Two responsibilities that only make sense together: the id-to-executor table, and the dispatch
/// that consults it. The split from `router` is ownership. The router answers *who* a topic reaches and
/// holds the subscription table; the dispatcher answers *where each of them runs* and holds the threads.
/// Keeping the threads out of the router is what lets the router be tested with no worker at all, and what
/// keeps `router::resolve` free of any lock ordering against an executor's queue.
/// @details The hot path is one `router::resolve` followed, per delivery, by either an inline `deliver` or a
/// `submit`. Nothing user-supplied runs under the dispatcher's own lock: the registry is consulted under it
/// and the lock released before a `submit` — which may park under `BLOCK` — is reached.
/// @note Not copyable or movable: it owns the executors, and an executor owns a thread.
///
class dispatcher {
public:
    /// @brief The id meaning "run on the calling thread".
    static constexpr executor_id INLINE = 0;

    dispatcher() = default;

    dispatcher(const dispatcher&) = delete;
    dispatcher& operator=(const dispatcher&) = delete;
    dispatcher(dispatcher&&) = delete;
    dispatcher& operator=(dispatcher&&) = delete;

    /// @brief Shuts every executor down. See `release`.
    ~dispatcher() { release(); }

    /// @brief Registers `exec` under `id`.
    /// @param id an id from 1 upward; `INLINE` is refused
    /// @param exec the executor; must not be null
    /// @return true if registered; false if `id` was already taken, in which case nothing changes
    /// @throws std::invalid_argument if `exec` is null or `id` is `INLINE` — both programming errors, and
    /// there is no caller that could act on the difference, so neither is a value the caller handles.
    /// @details `false` rather than an error payload because a taken id is the only failure there is and it
    /// has no detail to carry; it matches `remove` and `router::unsubscribe`, which return `bool` for the
    /// same reason. Re-registering is deliberately not idempotent: it would either leave the previous
    /// executor's thread alive with nothing able to reach it, or destroy work already queued in it, and a
    /// refused call is the only outcome that is honest about both.
    [[nodiscard]]
    bool add(executor_id id, std::shared_ptr<executor> exec) {
        if (!exec) { throw std::invalid_argument("the executor must not be null"); }
        if (id == INLINE) {
            throw std::invalid_argument("executor id 0 names the caller's thread and cannot be registered");
        }

        const std::unique_lock<std::shared_mutex> lock(mutex_);
        return executors_.emplace(id, std::move(exec)).second;
    }

    /// @brief Convenience: builds an executor of its own and registers it.
    /// @return true if registered; false if `id` was already taken — in which case the executor built here
    /// is destroyed unused, so a caller that must not pay for that asks `contains` first
    /// @throws std::invalid_argument if `id` is `INLINE`; std::system_error if the worker cannot start
    /// @details The two-step overload covers the caller that already owns an executor — one it shares with
    /// its own work, or one a test drives directly through `executor::drain`.
    [[nodiscard]]
    bool add(executor_id id, std::size_t capacity, exception_sink on_exception = {}) {
        return add(id, std::make_shared<executor>(capacity, std::move(on_exception)));
    }

    /// @brief Stops `id`'s executor and forgets it.
    /// @return true if `id` was registered; false if it was unknown or already removed
    /// @details Idempotent, a miss changing nothing. The executor is closed but not destroyed: whatever else
    /// holds a reference — a message already in its queue, a `dispatch` that looked it up — keeps it alive
    /// until that last reference goes, which is exactly how long its thread has to finish.
    [[nodiscard]]
    bool remove(executor_id id) {
        std::shared_ptr<executor> retired;
        {
            const std::unique_lock<std::shared_mutex> lock(mutex_);
            const auto found = executors_.find(id);
            if (found == executors_.end()) { return false; }
            retired = std::move(found->second);
            executors_.erase(found);
        }
        retired->close();
        return true;
    }

    /// @brief The executor registered under `id`, or null when there is none.
    /// @note The returned `shared_ptr` is what makes `remove` racing a `dispatch` safe: a thread that has
    /// looked the executor up holds it alive for the whole `submit`, whether or not the map still names it.
    [[nodiscard]]
    std::shared_ptr<executor> find(executor_id id) const {
        const std::shared_lock<std::shared_mutex> lock(mutex_);
        const auto found = executors_.find(id);
        return found == executors_.end() ? nullptr : found->second;
    }

    /// @brief Whether `id` names a registered executor.
    [[nodiscard]]
    bool contains(executor_id id) const {
        const std::shared_lock<std::shared_mutex> lock(mutex_);
        return executors_.contains(id);
    }

    /// @brief How many executors are registered.
    [[nodiscard]]
    std::size_t size() const noexcept {
        const std::shared_lock<std::shared_mutex> lock(mutex_);
        return executors_.size();
    }

    /// @brief Routes `routes`, running what belongs on this thread and submitting what belongs elsewhere.
    /// @param routes the snapshot `router::resolve` returned
    /// @param msg the message, shared so that N submissions carry one payload rather than N copies
    /// @param on_failure where a delivery that could not happen is reported: a handler's unhandled throw —
    /// inline or on an executor — and a delivery addressed to an executor that is not registered. Empty
    /// means an inline one is returned instead, while one already handed to an executor is swallowed,
    /// because the dispatcher then has no caller left to return it to and the alternative to swallowing is
    /// terminating the process.
    /// @return the first unhandled exception from a delivery that ran **on this thread**, or null; every
    /// sibling is still attempted either way
    /// @details The reason this class exists rather than the router doing the fan-out itself is the third
    /// argument to `submit`: each route carries its own `dispatch_options`, so one topic can reach a
    /// telemetry subscriber that wants `DROP_OLDEST` and a command subscriber that wants `DROP_NEW` in the
    /// same fan-out, and each policy is honoured.
    /// @note A delivery naming an unregistered executor is **not** run inline: putting a handler on a thread
    /// `dispatch_options` did not name would be a worse lie than a lost delivery, so it is reported and
    /// counted instead.
    /// @note An evicted submit — `DROPPED_OLDEST` — is not a failure here: the executor did accept the
    /// closure, and the message it displaced was displaced by the policy its own subscriber chose. Only
    /// `REJECTED` and `CLOSED` mean the delivery will not happen, and even those are counted rather than
    /// reported, because under `DROP_NEW` a rejection is the designed answer and throwing on it would turn
    /// normal back-pressure into an exception per message.
    [[nodiscard]]
    std::exception_ptr dispatch(
        std::span<const route> routes, const std::shared_ptr<const message>& msg, error_handler on_failure = {}
    ) const;

    /// @brief Convenience: hands `msg` over as the shared payload a submitted closure captures.
    [[nodiscard]]
    std::exception_ptr dispatch(std::span<const route> routes, message msg, error_handler on_failure = {}) const {
        return dispatch(routes, std::make_shared<const message>(std::move(msg)), std::move(on_failure));
    }

    /// @brief How many deliveries were refused, for diagnosis: unregistered executor, or a queue that
    /// answered `REJECTED` / `CLOSED`. Loaded relaxed — it is a counter, not a synchronisation point.
    [[nodiscard]]
    std::uint64_t dropped() const noexcept {
        return dropped_.load(std::memory_order_relaxed);
    }

    /// @brief Closes every executor, then waits for each.
    /// @details Two passes for a reason: the close pass stops all intake before any join begins, so an
    /// executor further down the list cannot be fed by one whose worker is still running. Each `release`
    /// then waits for the work that executor had already accepted. Neither pass holds `mutex_`.
    /// @note `close` then `release` rather than `release` alone, because `release` per executor would join
    /// executor 1 before executor 2 had been told to stop, which is exactly the window the two passes
    /// remove. Idempotent: a second call finds every executor already closed and already joined.
    void release() noexcept {
        std::vector<std::shared_ptr<executor>> all;
        {
            const std::shared_lock<std::shared_mutex> lock(mutex_);
            all.reserve(executors_.size());
            for (const auto& [id, exec] : executors_) {
                static_cast<void>(id);
                all.push_back(exec);
            }
        }
        for (const auto& exec : all) { exec->close(); }
        for (const auto& exec : all) { exec->release(); }
    }

private:
    /// @brief Guards `executors_`. Shared for a lookup on the hot path, exclusive for a registration or a
    /// removal.
    mutable std::shared_mutex mutex_;
    /// @brief The executors by id. `INLINE` is never a key, so this table and the inline path never collide.
    std::unordered_map<executor_id, std::shared_ptr<executor>> executors_;
    /// @brief `dropped()`; atomic so the increment needs no lock on a hot path that already holds none.
    mutable std::atomic<std::uint64_t> dropped_ = 0;
};

inline std::exception_ptr dispatcher::dispatch(
    std::span<const route> routes, const std::shared_ptr<const message>& msg, error_handler on_failure
) const {
    std::exception_ptr first;

    for (const auto& route : routes) {
        const auto target = route.options_.executor_;
        if (target == INLINE) {
            if (const auto failure = router::deliver(route, *msg); failure && !first) { first = failure; }
            continue;
        }

        const auto exec = find(target);
        if (!exec) {
            const auto failure = std::make_exception_ptr(
                std::runtime_error("no executor is registered under the id this subscription named")
            );
            dropped_.fetch_add(1, std::memory_order_relaxed);
            if (on_failure) { on_failure(*msg, failure); }
            else if (!first) { first = failure; }
            continue;
        }

        // A delivery addressed to the executor **this thread is already running on** — a handler on
        // executor X that publishes into a topic routed back to X, of which `msgbus::ROUTER` is the
        // standing case — is run here rather than submitted. `submit` would run it inline anyway, but
        // only after a second admission decision: it records the closure twice, once as this submit and
        // once as the inline run, and once `close()` has cleared intake it refuses the closure with
        // `CLOSED` — the executor's own worker judging work it was asked to hand itself while it is
        // draining. Neither is what a hand-over should cost. Nothing is checked under the registry's
        // lock beyond what `find` already did, and any handler's own exception stays on this thread,
        // where the failure recovery below can still reach a caller.
        if (exec->is_on_worker_thread()) {
            const auto failure = router::deliver(route, *msg);
            if (failure && !first) { first = failure; }
            continue;
        }

        // The failure recovery lives in the closure, not here: by the time the worker runs it, `dispatch`
        // has long returned, and `on_failure` is the only channel left.
        const auto outcome = exec->submit(
            [route, msg, on_failure] {
                const auto failure = router::deliver(route, *msg);
                if (!failure) { return; }
                if (on_failure) {
                    try {
                        on_failure(*msg, failure);
                    }
                    catch (...) {
                        // An error handler that throws has nowhere left to report it; swallowing keeps the
                        // worker alive, matching `router::deliver`'s own branch.
                    }
                }
            },
            route.options_.policy_
        );

        if (outcome == submit_status::REJECTED || outcome == submit_status::CLOSED) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    return first;
}

} // namespace ba7lya::msgbus
