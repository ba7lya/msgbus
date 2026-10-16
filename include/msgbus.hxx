///
/// @file msgbus.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The bus itself: a routing table, a set of named executors, and one worker that turns queued
/// publications into deliveries.
/// @version 0.1
/// @date 2026-10-16
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <any>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common.hxx"
#include "dispatcher.hxx"
#include "error.hxx"
#include "executor.hxx"
#include "msgbus_base.hxx"
#include "router.hxx"

namespace ba7lya::msgbus {

///
/// @brief A message bus that runs in the caller's process: hierarchical topics, a routing table, and
/// executors a subscriber names to decide which thread its handler runs on.
/// @details Three parts, each with one job. The `router` owns the subscription table and answers which
/// subscriptions a topic reaches. The `dispatcher` owns the executors and answers where each delivery
/// runs. This class owns the **publishing path**: it is the piece that knows a topic must be parsed
/// once, that a synchronous publish routes on the calling thread while an asynchronous one is queued,
/// and that a handler failure on the worker's own thread has no caller left to throw at.
/// @details The synchronous and the asynchronous publish share everything after the parse, which is
/// why they are two entry points on one class rather than two classes: `publish_async` is `publish_sync`
/// handed to a thread. Splitting them would duplicate the routing, the failure policy and the executor
/// lookup, and would make a bus that supports both impossible to name.
/// @note Not copyable or movable: it owns a thread and two mutexes.
///
class msgbus final : public msgbus_base {
public:
    /// @brief The id of the worker an asynchronous publish is routed on.
    /// @details `dispatcher::INLINE` is 0, so an unregistered id and "the caller's thread" cannot be
    /// confused: `publish_async` names this one, and `publish_sync` names `INLINE`.
    static constexpr executor_id ROUTER = 1;

    ///
    /// @brief What the bus is built with, all of it optional.
    /// @details A struct rather than an argument list so that a caller naming only the third setting
    /// does not have to restate the first two, and so adding one stays source-compatible.
    ///
    /// The constructor is **user-provided** rather than defaulted, and the three settings are set in its
    /// member-initialiser list rather than by default member initialisers. Neither is a style choice,
    /// and neither is about `std::move_only_function` being `explicit`: `msgbus` is nested inside its own
    /// class, and `explicit msgbus(config settings = {})` needs `config` to be complete — members and all
    /// — at that point in the definition, which is *outside* every member function body. A default member
    /// initialiser is only parsed as part of the constructor's definition, so it is not complete there,
    /// and GCC reports the default argument as unconstructible. A user-provided constructor carries its
    /// defaults in its own signature, which **is** complete at that point, and clang's `-fsyntax-only`
    /// accepts both the pristine spelling (which fails with `default member initializer for
    /// 'queue_capacity_' needed within definition of enclosing class 'msgbus'`) and this one.
    /// @note Not an aggregate, and deliberately so: the members are named, never brace-initialised by
    /// position, so nothing that used to work depends on the aggregate-ness this gives up.
    ///
    struct config {
        config() noexcept
            : queue_capacity_(4096)
            , policy_(back_pressure::DROP_NEW) {}

        /// @brief How many published messages may wait to be routed. `executor::UNBOUNDED` for no bound.
        std::size_t queue_capacity_;
        /// @brief What `publish_async` does when that queue is full: `DROP_NEW` answers `REJECTED`,
        /// `DROP_OLDEST` evicts the oldest queued message, `BLOCK` waits for room. `DROP_NEW` by default
        /// because it is the only one of the three that tells the publisher what happened — a dropped
        /// message under `BLOCK` shows up as a slow publisher, and one under `DROP_OLDEST` as a
        /// subscriber that has to notice a gap in the sequence numbers.
        back_pressure policy_;
        /// @brief Where a handler's failure goes when no caller is left to throw it at — that is, every
        /// failure on the routing worker, and any failure on an executor thread. Empty drops it.
        /// @details Left out of the initialiser list on purpose: an empty `std::move_only_function` is
        /// already its default state, and `on_exception_ = {}` in a default member initialiser would be
        /// copy-initialisation, which libstdc++ rejects against that type's `explicit` default
        /// constructor. A member the initialiser list omits is default-initialised instead, which is a
        /// direct-initialisation and does not go through it.
        exception_sink on_exception_;
    };

    /// @brief Builds the bus and starts its routing worker.
    /// @throws std::system_error if the worker thread cannot be created
    /// @details The worker starts here rather than on the first publish, so a bus a caller holds is a
    /// bus that can be published to; deferring it would only move the creation failure onto a path that
    /// has no error channel.
    explicit msgbus(config settings = {})
        : routes_()
        , dispatch_()
        , on_exception_(std::move(settings.on_exception_))
        , policy_(settings.policy_)
        , worker_(std::make_shared<executor>(settings.queue_capacity_, [this](std::exception_ptr failure) {
            report(std::move(failure));
        })) {
        const bool registered = dispatch_.add(ROUTER, worker_);
        // Nothing else has run yet, so the only way this fails is a bug in the class itself.
        if (!registered) { throw std::logic_error("the routing worker's id is already taken"); }
    }

    msgbus(const msgbus&) = delete;
    msgbus& operator=(const msgbus&) = delete;
    msgbus(msgbus&&) = delete;
    msgbus& operator=(msgbus&&) = delete;

    /// @brief Shuts the bus down: `release()`.
    ~msgbus() override { release(); }

    /// @brief Convenience: a payload-carrying message from a topic string and a value.
    /// @details Re-declared here because a class that overrides `publish_sync(message)` hides the base's
    /// convenience overload; without this a caller holding a `msgbus` — rather than a `msgbus_base&` —
    /// could only build the `message` itself. Same for `publish_async` below.
    void publish_sync(std::string_view topic, std::any payload) {
        msgbus_base::publish_sync(topic, std::move(payload));
    }

    [[nodiscard]]
    publish_status publish_async(std::string_view topic, std::any payload) {
        return msgbus_base::publish_async(topic, std::move(payload));
    }

    /// @brief Convenience: waits without limit for the routing queue to drain.
    [[nodiscard]]
    bool flush() {
        return msgbus_base::flush();
    }

    /// @brief Registers `handler` for every topic matching `pattern`.
    /// @details No lock beyond the router's: the subscription table is the router's alone, so a
    /// subscribe racing a publish is coherent for the same reason a subscribe racing a resolve is.
    /// @note The handle is bound to the router, not to this bus, because the router is what issued the
    /// id and what a cancellation has to reach. The bus is neither copyable nor movable, and the
    /// router it owns is a member, so both outlive any handle a caller keeps.
    [[nodiscard]]
    result<subscription, error> subscribe(
        std::string_view pattern, handler handler, dispatch_options options = {}, error_handler on_error = {}
    ) override {
        return routes_.subscribe(pattern, std::move(handler), options, std::move(on_error));
    }

    /// @brief Cancels a subscription by id.
    /// @details A message already queued for the routing worker is resolved **when the worker reaches
    /// it**, not when it was published, so a subscription cancelled first does not see it.
    /// @note The by-id form, kept because the router issues ids and a caller may hold one. A handle
    /// has `subscription::cancel`, which is the form a subscriber normally wants.
    bool unsubscribe(subscription_id id) override { return routes_.unsubscribe(id); }

    /// @brief Parses `msg`'s topic and routes it on the calling thread.
    /// @throws std::invalid_argument if the topic is malformed — a publish target is concrete, so a
    /// wildcard in it is as much a mistake as an empty level. Also carries out a handler's own unhandled
    /// exception when that handler ran on this thread and named no `error_handler`.
    /// @details A malformed topic is thrown rather than reported because `msgbus_base::publish_sync` returns
    /// `void` and has no channel to report through, and because the callers of a publish that name a
    /// topic are the ones who can fix it. A *rejected subscription pattern* is a different thing: that
    /// travels by value on `subscribe`, where the caller is already handling a `result`.
    /// @note Handlers that named an executor are only handed over, not finished: `publish_sync` waits
    /// for the calling thread's work alone, as a `publish_sync` must if it is to stay usable from inside
    /// a handler.
    void publish_sync(message msg) override {
        std::shared_ptr<const message> shared = std::make_shared<const message>(std::move(msg));
        deliver_now(shared);
    }

    /// @brief Queues `msg` for the routing worker.
    /// @return `ACCEPTED` when the bus took it; `REJECTED` when the queue was full under the configured
    /// policy or the bus has been released
    /// @details Nothing is parsed here: a topic error found now would have no caller left to report to
    /// by the time it was found, and the parse would then have to happen twice or the error be dropped.
    /// The worker parses, resolves and dispatches, so a publish costs the publisher one move and one
    /// queue insert.
    [[nodiscard]]
    publish_status publish_async(message msg) override {
        // The closure is built **by the executor**, on the far side of its admission decision: nothing is
        // captured here, so a `BLOCK` submit waits — on a message it has not yet had to allocate, and on
        // a closure it has not yet had to build — and there is no path where a closure is built and then
        // has to be unwound because the submit was refused.
        const submit_status outcome = worker_->submit_deferred(
            [this, msg = std::move(msg)]() -> work_item {
                const std::shared_ptr<const message> shared = std::make_shared<const message>(std::move(msg));
                return [this, shared] {
                    // Parsing on the worker keeps the whole delivery — parse, resolve, dispatch — on one
                    // thread, and it is where `flush`'s promise ends: once this returns, the message has
                    // been handed to the executors each subscription named.
                    deliver_now(shared);
                };
            },
            policy_
        );
        return outcome == submit_status::REJECTED || outcome == submit_status::CLOSED ? publish_status::REJECTED
                                                                                      : publish_status::ACCEPTED;
    }

    /// @brief Waits until every message accepted so far has been routed.
    /// @param timeout how long to wait; the maximum value waits without limit
    /// @return true if the queue emptied within `timeout`
    /// @details Waits on the routing worker alone, and only until it is idle — not until each executor
    /// has finished, which is the promise `msgbus_base::flush` makes. `publish_async` never parks a
    /// publisher on a full queue, so a thread waiting here is not holding one up.
    /// @throws std::invalid_argument if called from the worker's own thread — that is, from inside a
    /// handler that named no executor and was reached through an asynchronous publish. The wait could
    /// only miss its deadline there, and a programming error should say so rather than time out.
    [[nodiscard]]
    bool flush(std::chrono::milliseconds timeout) override {
        return worker_->drain(timeout);
    }

    /// @brief The bus's own error channel: where a failure with no caller left to throw at goes.
    /// @details Empty by default, and then such a failure is dropped. It is a sink rather than a queue
    /// because the bus has no thread of its own to drain one: whoever built the bus already owns a
    /// thread to log or signal from.
    [[nodiscard]]
    const exception_sink& on_exception() const noexcept {
        return on_exception_;
    }

    /// @brief The executors a subscriber may name, for registration and removal.
    /// @details `add(id, ...)` refuses `dispatcher::INLINE` and the routing worker's own id, so a
    /// subscription can never be moved off them by accident.
    [[nodiscard]]
    dispatcher& executors() noexcept {
        return dispatch_;
    }

    /// @brief Registers an executor of the bus's own making under `id`.
    /// @return false if `id` is already taken, including `ROUTER`
    /// @details Convenience for the common case; the same thing is reachable through `executors()`.
    [[nodiscard]]
    bool add_executor(executor_id id, std::size_t capacity = executor::UNBOUNDED) {
        return dispatch_.add(id, capacity, [this](std::exception_ptr failure) { report(std::move(failure)); });
    }

    /// @brief Stops and forgets the executor registered under `id`.
    /// @return false if `id` was unknown; `ROUTER` is refused, because the bus cannot route without it
    [[nodiscard]]
    bool remove_executor(executor_id id) {
        if (id == ROUTER) { return false; }
        return dispatch_.remove(id);
    }

    /// @brief How many subscriptions are live.
    [[nodiscard]]
    std::size_t subscription_count() const noexcept {
        return routes_.subscription_count();
    }

    /// @brief Whether any subscription is live.
    [[nodiscard]]
    bool empty() const noexcept {
        return routes_.empty();
    }

    /// @brief How many messages the routing worker has finished routing.
    /// @details Acceptance can be counted by the publisher; this is the other half, and the difference
    /// is the queue depth. Sampled without the worker's lock, so it is a counter and not a barrier —
    /// `stats()` is where a consistent snapshot lives.
    [[nodiscard]]
    std::uint64_t routed() const noexcept {
        return routed_.load(std::memory_order_relaxed);
    }

    /// @brief How many deliveries went nowhere: a subscription naming an executor that is not
    /// registered, or a queue that answered `REJECTED` / `CLOSED`.
    [[nodiscard]]
    std::uint64_t dropped() const noexcept {
        return dispatch_.dropped();
    }

    /// @brief The routing worker's counters, read as one snapshot.
    [[nodiscard]]
    executor_stats worker_stats() const noexcept {
        return worker_->stats();
    }

    /// @brief Stops the worker and every executor, then waits for each.
    /// @details Close then join, in that order and in two passes: the first pass stops all intake, so
    /// nothing accepted after it can be queued behind a worker that has already been joined. A message
    /// already accepted is **routed** rather than discarded — it was promised a delivery when the bus
    /// said `ACCEPTED`. Handlers still queued in an executor are run as well, since an executor drains
    /// what it accepted before its worker exits. Idempotent, and the destructor calls it.
    void release() noexcept {
        // Two calls, not one: the first stops the worker accepting and the second waits for it, and
        // between them nothing can be submitted — a publisher that raced the first would otherwise be
        // parked on a queue whose worker has already been told to stop.
        worker_->close();
        const bool quiescent = worker_->drain();
        static_cast<void>(quiescent);
        worker_->release();
        dispatch_.release();
    }

private:
    /// @brief Parses `msg->topic_`, resolves it, and hands the deliveries to the dispatcher.
    /// @details One body shared by `publish_sync` and the worker, so a synchronous and an asynchronous
    /// publish cannot drift apart in what they deliver or in what they do about failure.
    void deliver_now(const std::shared_ptr<const message>& msg) {
        const auto parsed = parse_topic(msg->topic_);
        if (!parsed) {
            throw std::invalid_argument(
                std::string("cannot publish to \"") + msg->topic_
                + "\": " + std::string(to_string(parsed.error().code_))
            );
        }

        const std::vector<delivery> routes = routes_.resolve(parsed->levels());
        // The dispatcher's own error handler is this bus's channel: by the time it is called, every
        // remaining caller has returned.
        const std::exception_ptr failure
            = dispatch_.dispatch(routes, msg, [this](const message&, std::exception_ptr cause) {
                  report(std::move(cause));
              });
        if (failure) { std::rethrow_exception(failure); }
        static_cast<void>(routed_.fetch_add(1, std::memory_order_relaxed));
    }

    /// @brief Hands a failure to the configured sink, if there is one, without ever throwing.
    /// @details The sink is user code called from a worker, so it is isolated the same way a handler is:
    /// a throwing sink would otherwise unwind into a thread function and terminate the process.
    void report(std::exception_ptr failure) noexcept {
        if (!on_exception_) { return; }
        try {
            on_exception_(std::move(failure));
        }
        catch (...) {
            // Nowhere above this to report it; dropping it keeps the worker alive.
        }
    }

    /// @brief The subscription table and the topic-to-subscriber resolution.
    router routes_;
    /// @brief The executors, including the routing worker placed under `ROUTER`.
    dispatcher dispatch_;
    /// @brief The bus-level failure sink, shared with `worker_` from construction.
    exception_sink on_exception_;
    /// @brief What `publish_async` does when the routing queue is full. Fixed at construction.
    back_pressure policy_ = back_pressure::DROP_NEW;
    /// @brief The routing worker, and the executor asynchronous publishes queue on.
    /// @details Held by `shared_ptr` because the dispatcher's registry owns its executors; this is the
    /// bus's own reference, used for `submit`, `drain` and shutdown.
    std::shared_ptr<executor> worker_;
    /// @brief Messages the worker has finished routing.
    std::atomic<std::uint64_t> routed_ = 0;
};

} // namespace ba7lya::msgbus
