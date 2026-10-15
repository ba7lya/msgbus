///
/// @file msgbus_base.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The bus interface: subscribing to topic patterns and publishing to concrete topics.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026
///
// SPDX-License-Identifier: MIT

#pragma once

#include <any>
#include <chrono>
#include <string_view>
#include <utility>

#include "common.hxx"
#include "error.hxx"

namespace ba7lya::msgbus {

///
/// @brief The bus interface: what a publisher and a subscriber are entitled to assume of any bus.
/// @details Subscribers register a pattern, publishers name a concrete topic; the bus resolves the
/// first against the second. Two publishing entry points share one routing path and differ only in
/// whether the publisher waits: `publish_sync` routes on the calling thread, `publish_async` queues
/// and returns.
/// @note An abstract base, not the bus a caller instantiates: `msgbus` derives from it. It is here so
/// that code depending on the bus — an adapter, a test double — names the interface rather than the
/// implementation, and so a second implementation can exist without a second spelling of the API.
///
class msgbus_base {
public:
    virtual ~msgbus_base() = default;

    /// @brief Registers `handler` for every topic matching `pattern`.
    /// @param pattern a topic pattern, with `+` matching one level and `#` matching the rest
    /// @param handler called once per matching message; must not be empty
    /// @param options which executor runs the handler, and what to do when it falls behind
    /// @param on_error where the handler's exception goes; empty hands it back to the caller of a
    /// `publish_sync` — a publish that has returned has no caller left, and the bus's own error
    /// channel takes it instead
    /// @return the new subscription's id, or why the pattern was rejected; nothing is registered on failure
    [[nodiscard]]
    virtual result<subscription_id, error> subscribe(
        std::string_view pattern, handler handler, dispatch_options options = {}, error_handler on_error = {}
    ) = 0;

    /// @brief Cancels a subscription.
    /// @return true if `id` was live; false if it was unknown, already cancelled, or never issued.
    /// @details Idempotent: a second call on the same id returns false and changes nothing. A message
    /// already queued for a cancelled subscription is not delivered to it.
    virtual bool unsubscribe(subscription_id id) = 0;

    /// @brief Routes `msg` on the calling thread.
    /// @details The publisher is not queued behind other work. This does *not* promise that every
    /// handler has finished when this returns: a handler whose executor is another thread — a
    /// serial-port adapter, say — is handed to that thread, as it must be. Only handlers on the
    /// calling thread are complete on return.
    virtual void publish_sync(message msg) = 0;

    /// @brief Convenience: a concrete topic and a payload, with default provenance.
    void publish_sync(std::string_view topic, std::any payload) {
        publish_sync(message { topic_name(topic), std::move(payload), {} });
    }

    /// @brief Queues `msg` for the worker thread to route.
    /// @return whether the bus took ownership; `REJECTED` means it did not.
    /// @details Returns as soon as the message is queued, so no handler has run yet. Acceptance is
    /// not a promise of delivery: a subscription cancelled before the worker reaches the message
    /// does not see it.
    [[nodiscard]]
    virtual publish_status publish_async(message msg) = 0;

    /// @brief Convenience: a concrete topic and a payload, with default provenance.
    [[nodiscard]]
    publish_status publish_async(std::string_view topic, std::any payload) {
        return publish_async(message { topic_name(topic), std::move(payload), {} });
    }

    /// @brief Waits until every message accepted so far has been routed.
    /// @param timeout how long to wait; the maximum value waits without limit
    /// @return true if the queue drained within `timeout`, false if it did not
    /// @note Drain means "handed to executors", not "handlers finished", for the same reason
    /// `publish_sync` does not wait for another thread's handler.
    virtual bool flush(std::chrono::milliseconds timeout) = 0;

    /// @brief Waits without limit for the queue to drain.
    bool flush() { return flush(std::chrono::milliseconds::max()); }
};

} // namespace ba7lya::msgbus
