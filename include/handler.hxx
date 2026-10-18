///
/// @file handler.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The callbacks a subscription is made of: what a matching message invokes, and where that
/// invocation's failure goes.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <exception>
#include <functional>

#include "message.hxx"

namespace ba7lya::msgbus {

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

} // namespace ba7lya::msgbus
