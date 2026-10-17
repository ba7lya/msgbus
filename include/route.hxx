///
/// @file route.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief One edge of a resolved topic: the subscription a message reached, and everything needed to
/// run it.
/// @version 0.1
/// @date 2026-10-18
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdint>

#include "common.hxx"
#include "handler.hxx"
#include "subscription.hxx"

namespace ba7lya::msgbus {

///
/// @brief One subscription a message resolved to: what to run, what to do when it throws, and where.
/// @details A copy of what the registration held at the moment the snapshot was taken. That is why a
/// subscription cancelled after the snapshot still runs: the callback here is already detached from
/// the table, so the router's lock is free again while the handler is still in flight.
///
/// The name is the routing table's, not the delivery's: the router resolves a topic into a set of
/// these, and the dispatcher later turns each one into a delivery — inline on the publisher's thread
/// or handed to an executor. A route can therefore exist without ever being delivered, which is
/// exactly what a `REJECTED` submit means.
///
struct route {
    /// @brief The subscription this route came from. Diagnostic: the callback is already bound.
    subscription_id id_ = 0;
    /// @brief The subscriber's handler, copied out of the table.
    handler callback_;
    /// @brief Where this handler's exception goes; empty returns it to the caller that ran the
    /// callback.
    error_handler on_error_;
    /// @brief Which executor should run this handler, and what to do when it falls behind.
    dispatch_options options_;
};

} // namespace ba7lya::msgbus
