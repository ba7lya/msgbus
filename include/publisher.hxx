///
/// @file publisher.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The publishing half of a bus, as one component holds it: a publish-only view plus the
/// provenance every message it sends carries.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <any>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "common.hxx"
#include "message.hxx"
#include "msgbus_base.hxx"
#include "topic.hxx"

namespace ba7lya::msgbus {

///
/// @brief What a component publishes through: the publish half of a bus, and the identity stamped on
/// everything it sends.
/// @details The publish side alone, deliberately. A component that only sends — a sensor adapter, a
/// command handler — has no business holding `subscribe`, and taking a `publisher` says so in the
/// type: the subscribe entry points are not reachable from here at all.
///
/// The second reason it exists is provenance. `message_meta` has a field for the component that
/// produced a message and a per-source sequence number, and the bus cannot fill either in: it never
/// learns who called it. A publisher can, because it was constructed with a name. Every message built
/// from a topic and a payload is stamped with the send time, `source()`, and the next number from a
/// counter that only counts what this publisher sent. Two publishers in one component therefore need
/// distinct sources if their sequence numbers are to be read as a per-source stream — which is what
/// the field means.
///
/// @note A `message` handed over whole is passed through untouched, including its provenance. That is
/// the path for a relay or a replay, where the stamp is the original one and overwriting it would
/// destroy the only record of when the message really arrived.
/// @note Non-owning: it holds the bus by reference, so the bus must outlive it. Copies are refused —
/// two publishers named the same source would fight over the meaning of one sequence.
/// @note Thread-safe: the counter is atomic and the source is fixed at construction, so several
/// threads may publish through one publisher. They share one sequence, which is exactly the promise
/// `message_meta::seq_` makes.
///
class publisher {
public:
    /// @brief Binds to `bus` and names itself `source` in every message it stamps.
    /// @param bus the bus to publish on; must outlive this publisher
    /// @param source the component name for tracing; empty is allowed and means "unnamed", which a
    /// consumer sees as an empty `message_meta::source_` rather than as an error
    explicit publisher(msgbus_base& bus, std::string source = {})
        : bus_(bus)
        , source_(std::move(source)) {}

    publisher(const publisher&) = delete;
    publisher& operator=(const publisher&) = delete;
    publisher(publisher&&) = delete;
    publisher& operator=(publisher&&) = delete;

    ~publisher() = default;

    /// @brief The name this publisher stamps on its messages.
    [[nodiscard]]
    std::string_view source() const noexcept {
        return source_;
    }

    /// @brief How many messages this publisher has stamped so far.
    /// @details The last sequence number issued, and 0 before the first message.
    /// Loaded relaxed — it is a diagnostic counter, not a synchronisation point.
    [[nodiscard]]
    std::uint64_t sent() const noexcept {
        return seq_.load(std::memory_order_relaxed);
    }

    /// @brief Routes a stamped message on the calling thread.
    /// @throws std::invalid_argument if `topic` is malformed — a publish target is concrete
    /// @details Otherwise exactly `msgbus_base::publish_sync`: only handlers on the calling thread
    /// have finished when this returns.
    void publish_sync(topic topic, std::any payload) { bus_.publish_sync(make(topic, std::move(payload))); }

    /// @brief Routes `msg` with the provenance the caller gave it.
    void publish_sync(message msg) { bus_.publish_sync(std::move(msg)); }

    /// @brief Queues a stamped message for the bus's routing worker.
    /// @return `ACCEPTED` when the bus took ownership, `REJECTED` when it did not.
    /// @details Nothing is parsed here, so a malformed topic is not reported at this point: the
    /// worker finds it, and the bus's own error channel takes it.
    [[nodiscard]]
    publish_status publish_async(topic topic, std::any payload) {
        return bus_.publish_async(make(topic, std::move(payload)));
    }

    /// @brief Queues `msg` with the provenance the caller gave it.
    [[nodiscard]]
    publish_status publish_async(message msg) {
        return bus_.publish_async(std::move(msg));
    }

    /// @brief Waits until every message the bus accepted before this call has been routed.
    [[nodiscard]]
    bool flush(std::chrono::milliseconds timeout) {
        return bus_.flush(timeout);
    }

    /// @brief Waits without limit for the bus's routing queue to drain.
    [[nodiscard]]
    bool flush() {
        return bus_.flush();
    }

private:
    /// @brief Builds the message for a topic and a payload, with this publisher's provenance.
    /// @details The sequence is incremented even when the publish is later rejected: the number
    /// records what this publisher emitted, not what the bus accepted, and a gap is the honest way to
    /// show a rejection.
    [[nodiscard]]
    message make(topic topic, std::any payload) {
        message_meta meta;
        meta.stamp_ = std::chrono::system_clock::now();
        meta.source_ = source_;
        meta.seq_ = seq_.fetch_add(1, std::memory_order_relaxed) + 1;
        // `topic` is a borrowed view, so the message's own constructor is what takes the copy: moving
        // one here would copy anyway and would read as if the text were owned at this point.
        return message { topic, std::move(payload), std::move(meta) };
    }

    /// @brief The bus this publisher sends on.
    /// A reference: a publisher without a bus has nothing to say and no state worth constructing.
    msgbus_base& bus_;
    /// @brief The name stamped into `message_meta::source_`. Fixed at construction.
    const std::string source_;
    /// @brief The last sequence number issued. Atomic so that one publisher may serve several threads.
    std::atomic<std::uint64_t> seq_ = 0;
};

} // namespace ba7lya::msgbus
