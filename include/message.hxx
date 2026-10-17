///
/// @file message.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The unit the bus carries: a topic, an untyped payload, and the provenance that says where it
/// came from.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <any>
#include <chrono>
#include <cstdint>
#include <string>

#include "topic.hxx"

namespace ba7lya::msgbus {

///
/// @brief Everything about a message except its topic and payload.
/// @details Kept apart from the payload so a handler that only needs provenance — a logger, a router
/// that re-publishes — does not have to look through the payload to find it.
///
struct message_meta {
    /// @brief When the message was created, at the source.
    std::chrono::system_clock::time_point stamp_;
    /// @brief Monotonic per-source sequence number.
    std::uint64_t seq_ = 0;
    /// @brief Which adapter or component produced it, for tracing.
    std::string source_;
};

///
/// @brief The unit that flows over the bus: a topic, a payload, and its provenance.
/// @details The payload is a `std::any`, so a publisher and a subscriber agree on a type without the
/// bus knowing it. `std::any_cast` on the wrong type throws, which is why the type convention per
/// topic belongs to the application, not to the bus.
/// @note A `topic` only borrows, so this constructor is where the text becomes owned: the caller may
/// name a topic with a literal or with a view of a string that dies at the end of the call, and every
/// copy of the message carries its own text.
/// @note In-process only. `std::any` cannot cross a process boundary; a cross-process transport
/// would need its own encoding rather than this type.
///
struct message {
    message(topic name, std::any payload, message_meta meta = {})
        : topic_(name)
        , payload_(std::move(payload))
        , meta_(std::move(meta)) {}

    /// @brief The topic, owned: a queued message outlives the call that named it.
    /// @note A `std::string` rather than the `topic` alias: the alias borrows, and this member is the
    /// one place the text has to survive the call that named it.
    std::string topic_;
    std::any payload_;
    message_meta meta_;
};

} // namespace ba7lya::msgbus
