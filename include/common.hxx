///
/// @file common.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The small value types several layers name and none owns: the publish outcome, the executor
/// id, and what a subscriber asks of its executor.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdint>

namespace ba7lya::msgbus {

///
/// @brief The outcome of an asynchronous publish.
/// @details Acceptance means the bus took ownership of the message, not that any handler has run.
///
enum class publish_status {
    ACCEPTED, ///< Queued; the worker will dispatch it.
    REJECTED, ///< Not queued, because the bus was closed or the queue was full under `DROP_NEW`.
};

///
/// @brief The id a subscriber names to say where its handler runs.
/// @details `0` is reserved for "the caller's own thread"; it is the value `dispatch_options` defaults
/// to, and `dispatcher::add` refuses it, so a registration can never shadow the inline path.
///
using executor_id = std::uint64_t;

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
/// @note A value type, declared apart from both the router and the executor on purpose: both name it,
/// and neither may include the other — the router is tested with no worker at all, and the executor
/// knows nothing of subscriptions. That is why it sits here, in the header every layer already
/// includes, rather than in `dispatcher.hxx` where the fan-out that consumes it lives.
///
struct dispatch_options {
    /// @brief The id of the executor the handler should run on.
    /// `0` is `dispatcher::INLINE`: the caller's own thread, with no executor involved and nothing queued.
    executor_id executor_ = 0;
    /// @brief What to do when the executor's queue is full.
    back_pressure policy_ = back_pressure::DROP_OLDEST;
};

} // namespace ba7lya::msgbus
