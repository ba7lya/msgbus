// SPDX-License-Identifier: MIT

///
/// @file subscription.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The handle's out-of-line half: the destructor, `cancel` and the cancellation both share.
/// @details They live here rather than in the header because cancelling means calling
/// `msgbus::unsubscribe`, and `subscription.hxx` forward-declares the bus rather than including it. A
/// handle can be dropped in any translation unit; only the library has to know what it calls.
/// @version 0.1
/// @date 2026-10-18
/// @copyright Copyright (c) 2026 BA7LYA
///

#include "msgbus.hxx"

namespace ba7lya::msgbus {

subscription::~subscription() noexcept { release(); }

bool subscription::cancel() {
    if (bus_ == nullptr) { return false; }
    // Emptied whether or not the id was still live: after this call the handle owns nothing, so its
    // destructor and a second `cancel` are both no-ops.
    const auto was_live = bus_->unsubscribe(id_);
    bus_ = nullptr;
    id_ = 0;
    return was_live;
}

void subscription::release() noexcept {
    // Best effort, because this is the destructor's path and a destructor has no caller to report to: the
    // bus's cancellation takes a lock, and a lock that fails would otherwise turn a dropped handle into
    // `std::terminate`. The handle forgets its id either way -- a registration that could not be reached is
    // one the bus is already tearing down.
    if (bus_ != nullptr) {
        try {
            static_cast<void>(bus_->unsubscribe(id_));
        }
        catch (...) {
            // See above: nothing to report, and nothing left to try.
        }
    }
    bus_ = nullptr;
    id_ = 0;
}

} // namespace ba7lya::msgbus
