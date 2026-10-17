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
#include "handler.hxx"
#include "layer_matcher.hxx"
#include "matcher.hxx"
#include "message.hxx"
#include "msgbus_base.hxx"
#include "prefix_matcher.hxx"
#include "router.hxx"
#include "subscription.hxx"
#include "topic.hxx"

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

    /// @brief Builds the bus with every setting at its default.
    /// @details `LAYER` matching, a 4096-message routing queue, `DROP_NEW`, and no failure sink — the
    /// bus a caller who has not decided anything yet wants.
    /// @throws std::system_error if the worker thread cannot be created
    msgbus()
        : msgbus(matcher_type::LAYER) {}

    /// @brief Builds the bus and starts its routing worker.
    /// @param type the pattern language subscriptions are resolved with
    /// @param queue_capacity how many published messages may wait to be routed;
    /// `executor::UNBOUNDED` for no bound
    /// @param policy what `publish_async` does when that queue is full: `DROP_NEW` answers `REJECTED`,
    /// `DROP_OLDEST` evicts the oldest queued message, `BLOCK` waits for room. `DROP_NEW` by default
    /// because it is the only one of the three that tells the publisher what happened — a dropped
    /// message under `BLOCK` shows up as a slow publisher, and one under `DROP_OLDEST` as a subscriber
    /// that has to notice a gap in the sequence numbers.
    /// @param on_exception where a handler's failure goes when no caller is left to throw it at — that
    /// is, every failure on the routing worker, and any failure on an executor thread. Empty drops it.
    /// @throws std::system_error if the worker thread cannot be created
    /// @details The worker starts here rather than on the first publish, so a bus a caller holds is a
    /// bus that can be published to; deferring it would only move the creation failure onto a path that
    /// has no error channel.
    /// @details `type` comes first because it decides what a pattern means, and the other three are
    /// tuning knobs a caller can leave alone.
    explicit msgbus(
        matcher_type type, std::size_t queue_capacity = 4096, back_pressure policy = back_pressure::DROP_NEW,
        exception_sink on_exception = exception_sink {}
    )
        : router_(make_matcher(type))
        , dispatcher_()
        , on_exception_(std::move(on_exception))
        , policy_(policy)
        , worker_(std::make_shared<executor>(queue_capacity, [this](std::exception_ptr failure) {
            report(std::move(failure));
        })) {
        const auto registered = dispatcher_.add(ROUTER, worker_);
        // Nothing else has run yet, so the only way this fails is a bug in the class itself.
        if (!registered) { throw std::logic_error("the routing worker's id is already taken"); }
    }

    /// @brief Builds the bus over the matcher the caller supplies.
    /// @details The way to drive the bus with an index this class does not name — a prefix matcher
    /// with a different spelling, or a third-party implementation of `matcher` — without the bus
    /// knowing every kind.
    /// @param patterns the pattern index; must not be null
    /// @param queue_capacity how many published messages may wait to be routed
    /// @param policy what `publish_async` does when that queue is full
    /// @param on_exception where a handler's failure goes when no caller is left to throw it at
    /// @throws std::invalid_argument if `patterns` is null
    /// @throws std::system_error if the worker thread cannot be created
    explicit msgbus(
        std::unique_ptr<matcher> patterns, std::size_t queue_capacity = 4096,
        back_pressure policy = back_pressure::DROP_NEW, exception_sink on_exception = exception_sink {}
    )
        : router_(std::move(patterns))
        , dispatcher_()
        , on_exception_(std::move(on_exception))
        , policy_(policy)
        , worker_(std::make_shared<executor>(queue_capacity, [this](std::exception_ptr failure) {
            report(std::move(failure));
        })) {
        const auto registered = dispatcher_.add(ROUTER, worker_);
        if (!registered) { throw std::logic_error("the routing worker's id is already taken"); }
    }

    /// @brief Builds the bus with a failure sink and the default pattern language, queue bound and
    /// policy.
    /// @details The settings are positional, so a caller naming only the sink would otherwise have
    /// to restate the others; this overload is that call.
    explicit msgbus(exception_sink on_exception)
        : msgbus(matcher_type::LAYER, 4096, back_pressure::DROP_NEW, std::move(on_exception)) {}

    msgbus(const msgbus&) = delete;
    msgbus& operator=(const msgbus&) = delete;
    msgbus(msgbus&&) = delete;
    msgbus& operator=(msgbus&&) = delete;

    /// @brief Shuts the bus down: `release()`.
    ~msgbus() override { release(); }

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
        return router_.subscribe(pattern, std::move(handler), options, std::move(on_error));
    }

    /// @brief Cancels a subscription by id.
    /// @details A message already queued for the routing worker is resolved **when the worker reaches
    /// it**, not when it was published, so a subscription cancelled first does not see it.
    /// @note The by-id form, kept because the router issues ids and a caller may hold one. A handle
    /// has `subscription::cancel`, which is the form a subscriber normally wants.
    bool unsubscribe(subscription_id id) override { return router_.unsubscribe(id); }

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
        auto shared = std::make_shared<const message>(std::move(msg));
        deliver_now(shared);
    }

    /// @brief Convenience: a payload-carrying message from a topic string and a value.
    /// @details Re-declared here because a class that overrides `publish_sync(message)` hides the base's
    /// convenience overload; without this a caller holding a `msgbus` — rather than a `msgbus_base&` —
    /// could only build the `message` itself. Same for `publish_async` below.
    void publish_sync(topic topic, std::any payload) {
        msgbus_base::publish_sync(std::move(topic), std::move(payload));
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
        const auto outcome = worker_->submit_deferred(
            [this, msg = std::move(msg)]() -> work_item {
                const auto shared = std::make_shared<const message>(std::move(msg));
                return [this, shared] { deliver_now(shared); };
            },
            policy_
        );
        return outcome == submit_status::REJECTED || outcome == submit_status::CLOSED ? publish_status::REJECTED
                                                                                      : publish_status::ACCEPTED;
    }

    [[nodiscard]]
    publish_status publish_async(topic topic, std::any payload) {
        return msgbus_base::publish_async(std::move(topic), std::move(payload));
    }

    /// @brief Convenience: waits without limit for the routing queue to drain.
    [[nodiscard]]
    bool flush() {
        return msgbus_base::flush();
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
        return dispatcher_;
    }

    /// @brief Registers an executor of the bus's own making under `id`.
    /// @return false if `id` is already taken, including `ROUTER`
    /// @details Convenience for the common case; the same thing is reachable through `executors()`.
    [[nodiscard]]
    bool add_executor(executor_id id, std::size_t capacity = executor::UNBOUNDED) {
        return dispatcher_.add(id, capacity, [this](std::exception_ptr failure) { report(std::move(failure)); });
    }

    /// @brief Stops and forgets the executor registered under `id`.
    /// @return false if `id` was unknown; `ROUTER` is refused, because the bus cannot route without it
    [[nodiscard]]
    bool remove_executor(executor_id id) {
        if (id == ROUTER) { return false; }
        return dispatcher_.remove(id);
    }

    /// @brief How many subscriptions are live.
    [[nodiscard]]
    std::size_t subscription_count() const noexcept {
        return router_.subscription_count();
    }

    /// @brief Whether any subscription is live.
    [[nodiscard]]
    bool empty() const noexcept {
        return router_.empty();
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
        return dispatcher_.dropped();
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
        const auto quiescent = worker_->drain();
        static_cast<void>(quiescent);
        worker_->release();
        dispatcher_.release();
    }

private:
    /// @brief Builds the concrete matcher `type` names.
    /// @details The one place the enum and the classes are tied together, so the constructor does not
    /// name an implementation and a third kind is one line here.
    [[nodiscard]]
    static std::unique_ptr<matcher> make_matcher(matcher_type type) {
        switch (type) {
        case matcher_type::PREFIX: return std::make_unique<prefix_matcher>();
        case matcher_type::LAYER:
        default:                   return std::make_unique<layer_matcher>();
        }
    }

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

        const auto routes = router_.resolve(parsed->levels());
        // The dispatcher's own error handler is this bus's channel: by the time it is called, every
        // remaining caller has returned.
        const auto failure = dispatcher_.dispatch(routes, msg, [this](const message&, std::exception_ptr cause) {
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
    router router_;
    /// @brief The executors, including the routing worker placed under `ROUTER`.
    dispatcher dispatcher_;
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
