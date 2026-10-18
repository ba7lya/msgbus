///
/// @file msgbus.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The bus: subscribing to topic patterns, publishing to concrete topics, and the executors a
/// subscription can name.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <any>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

#include "common.hxx"
#include "error.hxx"
#include "handler.hxx"
#include "message.hxx"
#include "subscription.hxx"
#include "topic.hxx"

// The bus's signatures name no component: everything it owns is behind impl, so an application gets the
// declaration here and the compiled implementation from the library, and never a component header.
namespace ba7lya::msgbus {

///
/// @brief The bus a program constructs: hierarchical topics, a routing table, and executors a subscription
/// can name to decide which thread its handler runs on.
/// @details Subscribers register a pattern, publishers name a concrete topic; the bus resolves the first
/// against the second. Two publishing entry points share one routing path and differ only in whether the
/// publisher waits: `publish_sync` routes on the calling thread, `publish_async` queues and returns.
///
/// The machinery behind that -- the router, the executors, the routing worker, the matchers -- is
/// `msgbus::impl`, defined in `src/msgbus.cxx` and owned by this class. A program never names it: it
/// is constructed and destroyed inside the library, so a bus is one object with one lifetime, and
/// everything it owns goes away with it.
///
/// @note Not copyable or movable: it owns a thread and two mutexes, and the thing it owns is unique.
/// @note The entry points are virtual so that a test double can stand in for a bus: derive from it and
/// override what the test drives. An application never does that.
///
class msgbus {
public:
    /// @brief The id of the worker an asynchronous publish is routed on.
    /// @details `dispatcher::INLINE` is 0, so an unregistered id and "the caller's thread" cannot
    /// be confused: `publish_async` names this one, and `publish_sync` names `INLINE`.
    static constexpr executor_id ROUTER = 1;

    /// @brief Builds the bus with every setting at its default.
    /// @details `LAYER` matching, a 4096-message routing queue, `DROP_NEW`, and no failure sink -- the bus
    /// a caller who has not decided anything yet wants.
    /// @throws std::system_error if the worker thread cannot be created
    msgbus();

    /// @brief Builds the bus and starts its routing worker.
    /// @param type the pattern language subscriptions are resolved with
    /// @param queue_capacity how many published messages may wait to be routed;
    /// `UNBOUNDED` for no bound
    /// @param policy what `publish_async` does when that queue is full: `DROP_NEW` answers `REJECTED`,
    /// `DROP_OLDEST` evicts the oldest queued message, `BLOCK` waits for room. `DROP_NEW` by default
    /// because it is the only one of the three that tells the publisher what happened -- a dropped message
    /// under `BLOCK` shows up as a slow publisher, and one under `DROP_OLDEST` as a subscriber that has to
    /// notice a gap in the sequence numbers.
    /// @param on_exception where a handler's failure goes when no caller is left to throw it at -- that is,
    /// every failure on the routing worker, and any failure on an executor thread. Empty drops it.
    /// @throws std::system_error if the worker thread cannot be created
    /// @details The worker starts here rather than on the first publish, so a bus a caller holds is a bus
    /// that can be published to; deferring it would only move the creation failure onto a path that has no
    /// error channel.
    /// @details `type` comes first because it decides what a pattern means, and the other three are tuning
    /// knobs a caller can leave alone.
    explicit msgbus(
        matcher_type type, std::size_t queue_capacity = 4096, back_pressure policy = back_pressure::DROP_NEW,
        exception_sink on_exception = exception_sink {}
    );

    /// @brief Builds the bus with a failure sink and the default pattern language, queue bound and policy.
    /// @details The settings are positional, so a caller naming only the sink would otherwise have to
    /// restate the others; this overload is that call.
    explicit msgbus(exception_sink on_exception);

    msgbus(const msgbus&) = delete;
    msgbus& operator=(const msgbus&) = delete;
    msgbus(msgbus&&) = delete;
    msgbus& operator=(msgbus&&) = delete;

    /// @brief Shuts the bus down: `release()`.
    virtual ~msgbus();

    /// @brief Registers `handler` for every topic matching `pattern`.
    /// @param pattern a topic pattern, with `+` matching one level and `#` matching the rest
    /// @param handler called once per matching message; must not be empty
    /// @param options which executor runs the handler, and what to do when it falls behind
    /// @param on_error where the handler's exception goes; empty hands it back to the caller of a
    /// `publish_sync` -- a publish that has returned has no caller left, and the bus's own error channel
    /// takes it instead
    /// @return the new subscription, or why the pattern was rejected; nothing is registered on failure
    /// @note The returned handle owns the registration: dropping it cancels the subscription. A caller that
    /// wants the subscription to outlive the statement that armed it must keep the handle.
    [[nodiscard]]
    virtual result<subscription, error> subscribe(
        std::string_view pattern, handler handler, dispatch_options options = {}, error_handler on_error = {}
    );

    /// @brief Cancels a subscription by id.
    /// @return true if `id` was live; false if it was unknown, already cancelled, or never issued.
    /// @details Idempotent: a second call on the same id returns false and changes nothing. A message
    /// already queued for a cancelled subscription is not delivered to it.
    /// @note Prefer `subscription::cancel`, which cannot be handed an id that was never live. This is the
    /// by-id form, and the one an adapter that only holds ids -- a config table, a wire protocol -- has to
    /// reach for.
    virtual bool unsubscribe(subscription_id id);

    /// @brief Routes `msg` on the calling thread.
    /// @throws std::invalid_argument if the topic is malformed -- a publish target is concrete, so a
    /// wildcard in it is as much a mistake as an empty level. Also carries out a handler's own unhandled
    /// exception when that handler ran on this thread and named no `error_handler`.
    /// @details The publisher is not queued behind other work. This does *not* promise that every handler
    /// has finished when this returns: a handler whose executor is another thread -- a serial-port adapter,
    /// say -- is handed to that thread, as it must be. Only handlers on the calling thread are complete on
    /// return.
    /// @note Handlers that named an executor are only handed over, not finished: `publish_sync` waits for
    /// the calling thread's work alone, as a `publish_sync` must if it is to stay usable from inside a
    /// handler.
    virtual void publish_sync(message msg);

    /// @brief Convenience: a concrete topic and a payload, with default provenance.
    /// @details The topic is taken by value and moved into the message, which owns it: the caller's text
    /// may be a temporary, and a message outlives the call that built it.
    void publish_sync(topic topic, std::any payload) {
        publish_sync(message { std::move(topic), std::move(payload), {} });
    }

    /// @brief Queues `msg` for the worker thread to route.
    /// @return whether the bus took ownership; `REJECTED` means it did not.
    /// @details Returns as soon as the message is queued, so no handler has run yet. Acceptance is not a
    /// promise of delivery: a subscription cancelled before the worker reaches the message does not see it.
    [[nodiscard]]
    virtual publish_status publish_async(message msg);

    /// @brief Convenience: a concrete topic and a payload, with default provenance.
    [[nodiscard]]
    publish_status publish_async(topic topic, std::any payload) {
        return publish_async(message { std::move(topic), std::move(payload), {} });
    }

    /// @brief Waits until every message accepted so far has been routed.
    /// @param timeout how long to wait; the maximum value waits without limit
    /// @return true if the queue drained within `timeout`, false if it did not
    /// @details Waits on the routing worker alone, and only until it is idle -- not until each executor has
    /// finished, which is the promise `flush` makes. `publish_async` never parks a publisher on a full
    /// queue, so a thread waiting here is not holding one up.
    /// @throws std::invalid_argument if called from the worker's own thread -- that is, from inside a
    /// handler that named no executor and was reached through an asynchronous publish. The wait could only
    /// miss its deadline there, and a programming error should say so rather than time out.
    [[nodiscard]]
    virtual bool flush(std::chrono::milliseconds timeout);

    /// @brief Waits without limit for the queue to drain.
    [[nodiscard]]
    bool flush() {
        return flush(std::chrono::milliseconds::max());
    }

    /// @brief The bus's own error channel: where a failure with no caller left to throw at goes.
    /// @details Empty by default, and then such a failure is dropped. It is a sink rather than a queue
    /// because the bus has no thread of its own to drain one: whoever built the bus already owns a thread to
    /// log or signal from.
    [[nodiscard]]
    const exception_sink& on_exception() const noexcept;

    /// @brief Waits until the executor registered under `id` has run everything it accepted.
    /// @param id the executor to wait on; `ROUTER` is allowed, the inline id is not an executor at all
    /// @param timeout how long to wait; the maximum value waits without limit
    /// @return true if `id` names an executor and its queue drained within `timeout`; false if the id is
    /// unknown or the deadline passed
    /// @details The per-executor counterpart of `flush`, which waits on the routing queue alone. A handler
    /// that named an executor is only *handed over* by `flush`; this is how a caller waits for that executor
    /// to have run it.
    [[nodiscard]]
    bool drain_executor(executor_id id, std::chrono::milliseconds timeout = std::chrono::milliseconds::max());

    /// @brief Registers an executor of the bus's own making under `id`.
    /// @return false if `id` is already taken, including `ROUTER`
    /// @details Convenience for the common case; the id is what a subscription names, so nothing else about
    /// the executor has to be exposed to a caller.
    [[nodiscard]]
    bool add_executor(executor_id id, std::size_t capacity = UNBOUNDED);

    /// @brief Stops and forgets the executor registered under `id`.
    /// @return false if `id` was unknown; `ROUTER` is refused, because the bus cannot route without it
    [[nodiscard]]
    bool remove_executor(executor_id id);

    /// @brief How many subscriptions are live.
    [[nodiscard]]
    std::size_t subscription_count() const noexcept;

    /// @brief Whether any subscription is live.
    [[nodiscard]]
    bool empty() const noexcept;

    /// @brief How many messages the routing worker has finished routing.
    /// @details Acceptance can be counted by the publisher; this is the other half, and the difference is
    /// the queue depth. Sampled without the worker's lock, so it is a counter and not a barrier --
    /// `worker_stats()` is where a consistent snapshot lives.
    [[nodiscard]]
    std::uint64_t routed() const noexcept;

    /// @brief How many deliveries went nowhere: a subscription naming an executor that is not registered, or
    /// a queue that answered `REJECTED` / `CLOSED`.
    [[nodiscard]]
    std::uint64_t dropped() const noexcept;

    /// @brief The routing worker's counters, read as one snapshot.
    [[nodiscard]]
    executor_stats worker_stats() const noexcept;

    /// @brief Stops the worker and every executor, then waits for each.
    /// @details Close then join, in that order and in two passes: the first pass stops all intake, so
    /// nothing accepted after it can be queued behind a worker that has already been joined. A message
    /// already accepted is **routed** rather than discarded -- it was promised a delivery when the bus said
    /// `ACCEPTED`. Handlers still queued in an executor are run as well, since an executor drains what it
    /// accepted before its worker exits. Idempotent, and the destructor calls it.
    void release() noexcept;

private:
    /// @brief The routing table, the executors and the worker this bus owns.
    /// @details Complete only inside the library, where `src/msgbus.cxx` defines it. A program gets
    /// the bus and never names this type.
    class impl;
    /// @brief The routing table, the executors and the worker, owned for the bus's lifetime.
    std::unique_ptr<impl> impl_;
};

} // namespace ba7lya::msgbus
