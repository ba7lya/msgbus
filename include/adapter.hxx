///
/// @file adapter.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Both halves of a component in one type: the `publisher` it sends through and the `subscriber`
/// it receives through, over the one bus.
/// @version 0.1
/// @date 2026-10-18
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <any>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <utility>

#include "common.hxx"
#include "error.hxx"
#include "handler.hxx"
#include "msgbus.hxx"
#include "publisher.hxx"
#include "subscriber.hxx"
#include "subscription.hxx"
#include "topic.hxx"

namespace ba7lya::msgbus {

///
/// @brief A component that does both: a `publisher` and a `subscriber` composed over the same bus, under
/// one name.
/// @details A gateway's components are rarely one-way. A serial bridge receives commands and publishes
/// telemetry; a rule reads an alarm and writes an interlock. Such a component holds a `publisher` and a
/// `subscriber` as two members anyway -- this type is that pair, named once, so the rest of the code
/// takes one parameter rather than two, and cannot accidentally hold a publish half bound to a different
/// bus than its subscribe half.
///
/// It is a facade and nothing more. Every forward below is the half's own call with the same arguments
/// and the same guarantees, so `publisher.hxx` and `subscriber.hxx` stay the reference; nothing here adds
/// a policy, a queue or a thread. The two halves are private and no accessor hands them out, because
/// every call they offer is forwarded: an accessor could only be a second way to name the same two
/// objects. What the composition does fix is identity: `source()` is the name in every message the
/// adapter builds, and `sent()` is that one counter. Two publishers in one component would issue two
/// sequence streams under one name, which is why this one holds a single publisher.
///
/// @note Non-owning: it holds the bus by reference, so the bus must outlive it, and the adapter must be
/// destroyed before the bus.
/// @note Neither copyable nor movable. The publisher half refuses to move -- two objects named the same
/// source would fork one sequence -- and a subscriber's handles are bound to the bus that issued them, so
/// the adapter refuses both with them. Copies would additionally give one component two registration
/// lists cancelling the same ids.
/// @note As thread-safe as its halves, which differ. Publishing is safe from several threads: the
/// counter is atomic and the source is fixed at construction. Subscribing and cancelling are not, so one
/// adapter's registration list belongs to one thread's scope -- while a handler on a worker thread may
/// still publish through the same adapter, which is the point of the type.
/// @note An adapter's own subscription hears its own publishes when the pattern matches: the bus has no
/// notion of a sender, so the loop is legal. It is usually a mistake -- a component feeding itself
/// through the table -- and `message_meta::source_` against the subscription that logged the delivery is
/// what makes it visible in the trace.
///
class adapter {
public:
    /// @brief Binds to `bus` as one component: the name it stamps, and the defaults its registrations
    /// inherit.
    /// @param bus the bus to publish and subscribe on; must outlive this adapter
    /// @param source the component name stamped into every message it builds; empty means "unnamed"
    /// @param options where its handlers run and what to do when that executor falls behind
    /// @param on_error where its handlers' exceptions go; empty hands them to the bus's error channel
    explicit adapter(msgbus& bus, std::string source = {}, dispatch_options options = {}, error_handler on_error = {})
        : out_(bus, std::move(source))
        , in_(bus, options, std::move(on_error)) {}

    adapter(const adapter&) = delete;
    adapter& operator=(const adapter&) = delete;
    adapter(adapter&&) = delete;
    adapter& operator=(adapter&&) = delete;

    /// @brief Cancels every registration the subscribe half still owns, then destroys the publisher.
    /// @details What the two halves do on their own: the subscriber's destructor is the cancellation, and
    /// the publisher has nothing to release. No explicit teardown is written, here or by the caller.
    ~adapter() = default;

    /// @brief The name this adapter stamps on its messages.
    [[nodiscard]]
    std::string_view source() const noexcept {
        return out_.source();
    }

    /// @brief How many messages this adapter has stamped so far.
    /// @details One per message it built, including a publish the bus later rejected: a gap is the honest
    /// way to show a rejection, as `publisher::sent` explains.
    [[nodiscard]]
    std::uint64_t sent() const noexcept {
        return out_.sent();
    }

    /// @brief Routes a stamped message on the calling thread.
    /// @throws std::invalid_argument if `topic` is malformed -- a publish target is concrete
    /// @details Exactly `publisher::publish_sync`: only handlers on the calling thread have finished when
    /// this returns.
    void publish_sync(topic topic, std::any payload) { out_.publish_sync(topic, std::move(payload)); }

    /// @brief Routes `msg` with the provenance the caller gave it.
    /// @details The relay and replay path: the stamp is passed through untouched rather than replaced
    /// with this adapter's.
    void publish_sync(message msg) { out_.publish_sync(std::move(msg)); }

    /// @brief Queues a stamped message for the bus's routing worker.
    /// @return `ACCEPTED` when the bus took ownership, `REJECTED` when it did not.
    [[nodiscard]]
    publish_status publish_async(topic topic, std::any payload) {
        return out_.publish_async(topic, std::move(payload));
    }

    /// @brief Queues `msg` with the provenance the caller gave it.
    [[nodiscard]]
    publish_status publish_async(message msg) {
        return out_.publish_async(std::move(msg));
    }

    /// @brief Waits until every message the bus accepted before this call has been routed.
    /// @param timeout how long to wait; the maximum value waits without limit
    /// @return true if the queue drained within `timeout`, false if it did not
    /// @note The bus has one routing queue, so this waits for every publisher's accepted messages, not
    /// just the ones this adapter sent.
    [[nodiscard]]
    bool flush(std::chrono::milliseconds timeout) {
        return out_.flush(timeout);
    }

    /// @brief Waits without limit for the bus's routing queue to drain.
    [[nodiscard]]
    bool flush() {
        return out_.flush();
    }

    /// @brief Registers `handler` for `pattern` under this adapter's defaults.
    /// @return the new subscription's id, or why the pattern was rejected; nothing is registered on
    /// failure
    /// @throws std::invalid_argument if `handler` is empty
    [[nodiscard]]
    result<subscription_id, error> subscribe(std::string_view pattern, handler handler) {
        return in_.subscribe(pattern, std::move(handler));
    }

    /// @brief Registers `handler` for `pattern` on `options`, with this adapter's error handler.
    [[nodiscard]]
    result<subscription_id, error> subscribe(std::string_view pattern, handler handler, dispatch_options options) {
        return in_.subscribe(pattern, std::move(handler), options);
    }

    /// @brief Registers `handler` for `pattern`, overriding both defaults.
    [[nodiscard]]
    result<subscription_id, error> subscribe(
        std::string_view pattern, handler handler, dispatch_options options, error_handler on_error
    ) {
        return in_.subscribe(pattern, std::move(handler), options, std::move(on_error));
    }

    /// @brief Cancels one of this adapter's registrations.
    /// @return true if `id` was this adapter's and still live
    [[nodiscard]]
    bool cancel(subscription_id id) {
        return in_.cancel(id);
    }

    /// @brief Cancels every registration, leaving the subscribe half empty and reusable.
    void cancel_all() noexcept { in_.cancel_all(); }

    /// @brief How many registrations this adapter still owns.
    [[nodiscard]]
    std::size_t count() const noexcept {
        return in_.count();
    }

    /// @brief Whether this adapter owns no registration.
    [[nodiscard]]
    bool empty() const noexcept {
        return in_.empty();
    }

    /// @brief Whether `id` is one of this adapter's registrations.
    [[nodiscard]]
    bool contains(subscription_id id) const noexcept {
        return in_.contains(id);
    }

    /// @brief The dispatch options a registration inherits unless it overrides them.
    [[nodiscard]]
    const dispatch_options& options() const noexcept {
        return in_.options();
    }

    /// @brief The error handler a registration inherits unless it overrides one.
    [[nodiscard]]
    const error_handler& on_error() const noexcept {
        return in_.on_error();
    }

private:
    /// @brief The publish half: the only door this type sends through, and the stamp every message
    /// carries.
    publisher out_;
    /// @brief The receive half: the registrations, and their cancellation when this adapter dies.
    subscriber in_;
};

} // namespace ba7lya::msgbus
