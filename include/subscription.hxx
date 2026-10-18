///
/// @file subscription.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The handle a subscriber holds: a registration that cancels itself when it goes out of scope.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdint>
#include <unordered_set>
#include <utility>

namespace ba7lya::msgbus {

class msgbus;

///
/// @brief Identifies one subscription for its whole lifetime. Never reused, so a stale id can only
/// fail to match rather than silently cancel a newer subscription.
///
using subscription_id = std::uint64_t;

/// @brief The id set a trie node holds. A set rather than a list, so a subscriber registered under two
/// patterns that both match one topic is still reached once.
using subscription_set = std::unordered_set<subscription_id>;

///
/// @brief Owns one subscription for as long as it is alive; cancels it when destroyed.
/// @details What `subscribe` returns, so that a subscription's lifetime is the handle's lifetime. A
/// registration whose handle was dropped — a temporary, a loop-local, a member of an object that
/// died — is cancelled rather than left behind holding a callback nobody can reach.
///
/// Move-only, and moving it is not cancellation: the destination takes over the registration and the
/// source is left empty, so a handle stays a single ownership however often it is moved, and only the
/// last one standing has to be accounted for. The default-constructed handle owns nothing, which
/// makes it the value to hand back where there is no subscription to return.
///
/// Holds a pointer and an id rather than a reference or a `shared_ptr`. The bus cannot be destroyed
/// while a handle to it is alive — that is the subscriber's contract, the same one that already
/// forbids a closure capturing a reference to something that dies before the subscription does.
/// @note Not thread-safe as an object: one handle belongs to one thread's scope. Distinct handles on
/// distinct subscriptions are independent, and each cancellation takes the bus's own lock.
///
class subscription {
public:
    /// @brief An empty handle: it owns nothing, and its destructor has nothing to cancel.
    /// @details The difference from a live handle is meaningful rather than incidental — `operator
    /// bool` reads it, so a caller can hold "no subscription" without a second sentinel type.
    subscription() = default;

    /// @brief Binds to a subscription `id` the bus issued.
    /// @details Explicit rather than implicit: a handle should be minted only where an id is issued.
    subscription(msgbus& bus, subscription_id id) noexcept
        : bus_(&bus)
        , id_(id) {}

    subscription(const subscription&) = delete;
    subscription& operator=(const subscription&) = delete;

    /// @brief Takes `rhs`'s registration; `rhs` is left empty and no longer cancels anything.
    subscription(subscription&& rhs) noexcept
        : bus_(std::exchange(rhs.bus_, nullptr))
        , id_(std::exchange(rhs.id_, 0)) {}

    /// @brief Cancels this handle's subscription, if it still owns one, and takes `rhs`'s.
    subscription& operator=(subscription&& rhs) noexcept {
        // Self-move is left as the standard's "valid but unspecified" rather than special-cased: an
        // emptied handle is still a valid one, and no caller here writes `s = std::move(s)`.
        if (this != &rhs) {
            // Cancels what this handle held before overwriting it. Assigning over a live handle
            // abandons that subscription, and abandoning without cancelling would leak exactly the
            // registration this type exists to release.
            release();
            bus_ = std::exchange(rhs.bus_, nullptr);
            id_ = std::exchange(rhs.id_, 0);
        }
        return *this;
    }

    /// @brief Cancels the subscription, if this handle still owns one.
    /// @details The reason `subscribe` returns a handle at all: a subscription that is not explicitly
    /// cancelled goes away with its handle, so a subscriber cannot leak a registration by returning
    /// early, by throwing, or by simply not calling `unsubscribe`.
    /// @note Never throws. A cancellation cannot fail in a way a destructor could report.
    /// @note Defined with the library: cancelling means calling msgbus::unsubscribe, and this header
    /// forward-declares the bus rather than including it.
    ~subscription() noexcept;

    /// @brief Cancels the subscription now.
    /// @return true if it was still live; false if it was already cancelled or never issued
    /// @details Equivalent to destruction, except the handle survives and is empty afterwards — this
    /// is the explicit form, and the one to use when the caller wants to know whether it was in time.
    /// @note A message already queued for the bus's routing worker is resolved when the worker
    /// reaches it, not when it was published, so a cancellation made first means the subscription
    /// does not see that message.
    [[nodiscard]]
    bool cancel();

    /// @brief The id this handle owns, or 0 when it is empty.
    /// @note For diagnostics — logging which subscription a handler belongs to. Nothing a caller can
    /// do with the id that the handle does not already do, which is why no API here takes one.
    [[nodiscard]]
    subscription_id id() const noexcept {
        return id_;
    }

    /// @brief Whether the handle still owns a subscription.
    [[nodiscard]]
    bool owns() const noexcept {
        return bus_ != nullptr;
    }

    /// @brief Whether the handle still owns a subscription, as `owns()`.
    [[nodiscard]]
    explicit operator bool() const noexcept {
        return owns();
    }

    /// @brief Whether the two handles name the same subscription.
    /// @details An empty handle compares equal to another empty one, which is what makes "this
    /// subscription is gone" checkable without consulting the bus. A hidden friend, so the comparison
    /// is found by ADL alone and never by an unqualified search.
    [[nodiscard]]
    friend bool operator==(const subscription&, const subscription&) = default;

    /// @brief Whether this handle owns `id`.
    [[nodiscard]]
    friend bool operator==(const subscription& handle, subscription_id id) noexcept {
        return handle.id_ == id;
    }

private:
    /// @brief Cancels what this handle holds and empties it.
    void release() noexcept;

    /// @brief The bus that issued this subscription, or null when the handle is empty.
    /// @details A pointer, not a reference: a reference member would delete the assignment operators
    /// outright, and the empty handle needs a state that names no bus at all.
    /// @details Raw rather than smart, and deliberately so: this is an observer, not an owner. A
    /// `shared_ptr` would let a handle keep a stack-allocated bus alive and then delete it twice, and a
    /// `weak_ptr` would require the bus itself to be shared-owned — either one changes the ownership
    /// contract the API promises. The bus must outlive every handle to it, the same way it must outlive a
    /// publisher or a subscriber.
    msgbus* bus_ = nullptr;
    /// @brief The id being owned; 0 once the handle is empty.
    subscription_id id_ = 0;
};

} // namespace ba7lya::msgbus
