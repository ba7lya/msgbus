///
/// @file common.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The value types that cross the bus API: topics, messages, handlers and the policies they carry.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <any>
#include <chrono>
#include <cstdint>
#include <exception>
#include <expected>
#include <functional>
#include <string>

namespace ba7lya::msgbus {

///
/// @brief Either a value of `T` or an error of `E`.
/// @details Pattern parsing and trie lookups fail in ways the caller is expected to handle, so they
/// report by value instead of throwing; exceptions stay reserved for genuine programming errors —
/// an empty handler, an out-of-range access. No `void` specialisation is needed: the standard
/// `std::expected<void, E>` already means "succeeded, carrying nothing".
///
/// A bare alias, with no wrapper: the member names (`has_value`, `value`, `error`, `value_or`,
/// `operator bool`) and the conversion from `std::unexpected` are the standard ones. Reading the
/// branch that is not held throws `std::bad_expected_access<E>`.
/// @tparam T the value produced on success
/// @tparam E the error produced on failure
///
template<typename T, typename E>
using result = std::expected<T, E>;

///
/// @brief A concrete topic, as in `device/room1/temp`.
/// @details Named `topic_name` rather than `topic` so the word stays available for the concept: a
/// struct that names a member `topic_` and a type `topic` cannot declare `topic topic_;`.
///
using topic_name = std::string;

///
/// @brief Identifies one subscription for its whole lifetime. Never reused, so a stale id can only
/// fail to match rather than silently cancel a newer subscription.
///
using subscription_id = std::uint64_t;

///
/// @brief Everything about a message except its topic and payload.
/// @details Kept apart from the payload so a handler that only needs provenance — a logger, a router
/// that re-publishes — does not have to look through the payload to find it.
///
struct message_meta {
    /// @brief When the message was created, at the source.
    std::chrono::system_clock::time_point stamp_;
    /// @brief Which adapter or component produced it, for tracing.
    std::string source_;
    /// @brief Monotonic per-source sequence number.
    std::uint64_t seq_ = 0;
};

///
/// @brief The unit that flows over the bus: a topic, a payload, and its provenance.
/// @details The payload is a `std::any`, so a publisher and a subscriber agree on a type without the
/// bus knowing it. `std::any_cast` on the wrong type throws, which is why the type convention per
/// topic belongs to the application, not to the bus.
/// @note In-process only. `std::any` cannot cross a process boundary; a cross-process transport
/// would need its own encoding rather than this type.
///
struct message {
    topic_name topic_;
    std::any payload_;
    message_meta meta_;
};

///
/// @brief What a subscriber registers: called once per delivered message.
/// @details Handlers are invoked with exceptions isolated, so one throwing subscriber neither skips
/// its siblings nor reaches the publisher.
///
using handler = std::function<void(const message&)>;

///
/// @brief Where a subscriber's exception goes when there is no caller left to throw it at.
/// @details A synchronous publish can propagate a handler's exception to its caller; an asynchronous
/// one has already returned by the time the handler runs, so the exception has nowhere to go and is
/// handed here instead. Receives the message the failing handler was handling.
///
using error_handler = std::function<void(const message&, std::exception_ptr)>;

///
/// @brief The outcome of an asynchronous publish.
/// @details Acceptance means the bus took ownership of the message, not that any handler has run.
///
enum class publish_status {
    ACCEPTED, ///< Queued; the worker will dispatch it.
    REJECTED, ///< Not queued, because the bus was closed or the queue was full under `DROP_NEW`.
};

///
/// @brief What to do when the queue is full.
/// @details Chosen when the bus is constructed, not per publish: the right answer depends on what the
/// messages mean, and a caller in the middle of a hot loop should not have to re-decide.
///
enum class back_pressure {
    BLOCK,       ///< Wait for room. Risks stalling the publisher behind a slow consumer.
    DROP_OLDEST, ///< Evict the oldest queued message. Favours freshness, as telemetry does.
    DROP_NEW,    ///< Refuse the new message. Favours what is already queued, as commands do.
};

///
/// @brief How a subscriber wants its handlers run: on which executor, and what to do when that
/// executor cannot keep up.
/// @details Resolved once per subscription. The executor is the deciding factor for which thread a
/// handler runs on, and therefore for what a handler may safely touch without a lock.
///
struct dispatch_options {
    /// @brief The id of the executor the handler should run on. `0` is `dispatcher::INLINE`: the
    /// caller's own thread, with no executor involved and nothing queued.
    std::uint64_t executor_ = 0;
    /// @brief What to do when the executor's queue is full.
    back_pressure policy_ = back_pressure::DROP_OLDEST;
};

} // namespace ba7lya::msgbus
