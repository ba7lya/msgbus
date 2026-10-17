///
/// @file subscriber.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The subscribing half of a bus, as one component holds it: a set of registrations that share
/// one lifetime and one dispatch policy.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <cstddef>
#include <expected>
#include <string_view>
#include <utility>
#include <vector>

#include "common.hxx"
#include "error.hxx"
#include "handler.hxx"
#include "msgbus_base.hxx"
#include "subscription.hxx"

namespace ba7lya::msgbus {

///
/// @brief A group of subscriptions owned together: a component subscribes through one, and every
/// registration it made goes away with it.
/// @details `subscribe` on the bus returns a handle per registration, and a component with a dozen
/// topics to hear about would otherwise have to keep a dozen handles alive itself. A subscriber is
/// that bookkeeping: it holds the handles, hands back the id of each one, and cancels the whole set
/// when it dies. That is the shape an adapter has — constructed on start, torn down on stop — and it
/// is what makes "this object is alive" and "this object is receiving messages" the same condition.
///
/// It also carries the defaults that a single component applies to all of its subscriptions: the
/// `dispatch_options` saying where its handlers run, and the `error_handler` its failures go to. Both
/// are chosen once at construction and used by every `subscribe` that does not override them, which
/// is the common case — a serial adapter's handlers all belong on its own executor.
///
/// @note Ids are returned rather than handles. One subscription should have one owner, so the handle
/// stays inside the subscriber and the caller refers to the registration by id: `cancel(id)` for one,
/// destruction for all of them.
/// @note Non-owning: it holds the bus by reference, so the bus must outlive it, and the subscriber
/// must be destroyed before the bus. A registration outliving the table that issued its id is the one
/// lifetime mistake this type cannot catch.
/// @note Not thread-safe as an object: registering and cancelling take the bus's own locks and are
/// individually safe, but the handle list is not protected. Confine one subscriber to one thread's
/// scope, or guard it; a handler on a worker thread may still call `bus.unsubscribe(id)` directly.
///
class subscriber {
public:
    /// @brief Binds to `bus`, with the defaults every registration below inherits.
    /// @param bus the bus to subscribe on; must outlive this subscriber
    /// @param options where handlers run and what to do when that executor falls behind
    /// @param on_error where a handler's exception goes; empty hands it to the bus's own error channel
    explicit subscriber(msgbus_base& bus, dispatch_options options = {}, error_handler on_error = {})
        : bus_(bus)
        , options_(options)
        , on_error_(std::move(on_error)) {}

    subscriber(const subscriber&) = delete;
    subscriber& operator=(const subscriber&) = delete;

    /// @brief Takes over `rhs`'s registrations; `rhs` is left empty and cancels nothing.
    /// @details The handles move with their ids, so the subscriptions are undisturbed — no cancellation
    /// and no re-registration happens across the move.
    subscriber(subscriber&& rhs) noexcept
        : bus_(rhs.bus_)
        , options_(rhs.options_)
        , on_error_(std::move(rhs.on_error_))
        , handles_(std::move(rhs.handles_)) {}

    /// @brief Cancels what this subscriber held, then takes over `rhs`'s registrations.
    subscriber& operator=(subscriber&& rhs) noexcept {
        if (this != &rhs) {
            cancel_all();
            options_ = rhs.options_;
            on_error_ = std::move(rhs.on_error_);
            handles_ = std::move(rhs.handles_);
        }
        return *this;
    }

    /// @brief Cancels every registration this subscriber still owns.
    /// @details What makes the subscriber a scope: a component that stops receiving does not have to
    /// unwrap itself in reverse order of subscription, or at all.
    ~subscriber() = default;

    /// @brief Registers `handler` for `pattern` under this subscriber's defaults.
    /// @return the new subscription's id, or why the pattern was rejected; nothing is registered on
    /// failure
    /// @throws std::invalid_argument if `handler` is empty — a programming error, as on the bus
    /// @details A duplicate pattern is not a failure: the bus issues a second id for it, and both
    /// handlers receive each matching message. The error the bus reports for a duplicate belongs to a
    /// matcher that refuses one, and is passed through unchanged when it does.
    [[nodiscard]]
    result<subscription_id, error> subscribe(std::string_view pattern, handler handler) {
        return subscribe(pattern, std::move(handler), options_, on_error_);
    }

    /// @brief Registers `handler` for `pattern` on `options`, with this subscriber's error handler.
    [[nodiscard]]
    result<subscription_id, error> subscribe(std::string_view pattern, handler handler, dispatch_options options) {
        return subscribe(pattern, std::move(handler), options, on_error_);
    }

    /// @brief Registers `handler` for `pattern`, overriding both defaults.
    /// @return the new subscription's id, or why the pattern was rejected
    /// @details The id is issued by the bus and is unique across the whole bus, not just this
    /// subscriber, so it can be passed to `msgbus_base::unsubscribe` as well as to `cancel` here.
    [[nodiscard]]
    result<subscription_id, error> subscribe(
        std::string_view pattern, handler handler, dispatch_options options, error_handler on_error
    ) {
        auto registered = bus_.subscribe(pattern, std::move(handler), options, std::move(on_error));
        if (!registered) { return std::unexpected(registered.error()); }

        const auto id = registered->id();
        handles_.push_back(std::move(*registered));
        return id;
    }

    /// @brief Cancels one of this subscriber's registrations.
    /// @param id an id returned by `subscribe` on this subscriber
    /// @return true if it was still live; false if it was unknown, already cancelled, or not this
    /// subscriber's
    /// @details Equivalent to dropping the handle the subscriber was holding. An id that belongs to
    /// another subscriber is refused rather than cancelled: this type cancels what it registered, and
    /// reaching across that boundary is what `msgbus_base::unsubscribe` is for.
    [[nodiscard]]
    bool cancel(subscription_id id) {
        for (auto it = handles_.begin(); it != handles_.end(); ++it) {
            if (*it == id) {
                const auto was_live = it->cancel();
                handles_.erase(it);
                return was_live;
            }
        }
        return false;
    }

    /// @brief Cancels every registration, leaving the subscriber empty and reusable.
    void cancel_all() noexcept { handles_.clear(); }

    /// @brief How many registrations this subscriber still owns.
    [[nodiscard]]
    std::size_t count() const noexcept {
        return handles_.size();
    }

    /// @brief Whether this subscriber owns no registration.
    [[nodiscard]]
    bool empty() const noexcept {
        return handles_.empty();
    }

    /// @brief Whether `id` is one of this subscriber's registrations.
    [[nodiscard]]
    bool contains(subscription_id id) const noexcept {
        for (const auto& handle : handles_) {
            if (handle == id) { return true; }
        }
        return false;
    }

    /// @brief The defaults this subscriber applies to a registration that does not override them.
    [[nodiscard]]
    const dispatch_options& options() const noexcept {
        return options_;
    }

    /// @brief The error handler this subscriber applies to a registration that does not override it.
    [[nodiscard]]
    const error_handler& on_error() const noexcept {
        return on_error_;
    }

private:
    /// @brief The bus these registrations were made on. A reference: a subscriber without a bus has
    /// nothing to subscribe to, so there is no empty state worth constructing.
    msgbus_base& bus_;
    /// @brief The dispatch options every registration inherits unless it overrides them.
    dispatch_options options_;
    /// @brief The error handler every registration inherits unless it overrides one.
    error_handler on_error_;
    /// @brief The live registrations. Their destruction is the cancellation, so no explicit teardown
    /// loop is written.
    std::vector<subscription> handles_;
};

} // namespace ba7lya::msgbus
