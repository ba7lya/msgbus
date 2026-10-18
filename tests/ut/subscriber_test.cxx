///
/// @file subscriber_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for `subscriber`: the registrations it owns, the defaults it applies, and the
/// cancellation its destruction performs.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#include <any>
#include <chrono>
#include <cstddef>
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common.hxx"
#include "dispatcher.hxx"
#include "error.hxx"
#include "handler.hxx"
#include "msgbus.hxx"
#include "subscriber.hxx"
#include "subscription.hxx"

namespace ba7lya::msgbus {

// The unit under test is in detail: this file is part of the library, so it may name it directly.

namespace {

/// @brief A bus that mints subscriptions but delivers nothing: enough to count registrations and to
/// inspect the options each one was made with.
class registering_bus final : public msgbus {
public:
    result<subscription, error> subscribe(
        std::string_view pattern, handler callback, dispatch_options options, error_handler on_error
    ) override {
        // The bus's own contract, kept here so the subscriber is tested against a bus that behaves
        // like the real one.
        if (!callback) { throw std::invalid_argument("the handler must not be empty"); }
        if (reject) { return std::unexpected(error::make(error_code::EMPTY_LEVEL, pattern)); }

        const auto id = next_id++;
        seen.push_back(registration { std::string(pattern), options, static_cast<bool>(on_error) });
        live.push_back(id);
        return subscription(*this, id);
    }

    /// The handle the fake mints cancels through this override: it is the bus's own by-id form.
    bool unsubscribe(subscription_id id) override {
        for (auto it = live.begin(); it != live.end(); ++it) {
            if (*it == id) {
                live.erase(it);
                cancelled.push_back(id);
                return true;
            }
        }
        return false;
    }

    void publish_sync(message) override {}

    publish_status publish_async(message) override { return publish_status::ACCEPTED; }

    bool flush(std::chrono::milliseconds) override { return true; }

    struct registration {
        std::string pattern;
        dispatch_options options;
        bool has_error_handler = false;
    };

    std::vector<registration> seen;
    std::vector<subscription_id> live;
    std::vector<subscription_id> cancelled;
    subscription_id next_id = 1;
    bool reject = false;
};

/// @brief A handler that does nothing; these cases assert on the registration, not the message.
[[nodiscard]]
handler noop() {
    return [](const message&) {};
}

} // namespace

// --- what the subscriber promises as a type ----------------------------------------------------------

TEST(subscriber_test, the_subscriber_is_not_copyable_but_is_movable) {
    // Copying would mean two objects cancelling one set of ids. Moving hands the set over, so the
    // registrations survive a change of address without being cancelled and re-made.
    static_assert(!std::is_copy_constructible_v<subscriber>);
    static_assert(!std::is_copy_assignable_v<subscriber>);
    static_assert(std::is_move_constructible_v<subscriber>);
    static_assert(std::is_move_assignable_v<subscriber>);
}

TEST(subscriber_test, a_new_subscriber_owns_nothing) {
    registering_bus bus;
    const subscriber listener(bus);

    EXPECT_TRUE(listener.empty());
    EXPECT_EQ(listener.count(), 0u);
}

// --- registration ------------------------------------------------------------------------------------

TEST(subscriber_test, subscribing_returns_the_id_the_bus_issued) {
    registering_bus bus;
    subscriber listener(bus);

    const auto first = listener.subscribe("a/b", noop());
    const auto second = listener.subscribe("a/c", noop());

    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(*first, 1u);
    EXPECT_EQ(*second, 2u);
    EXPECT_EQ(listener.count(), 2u);
    EXPECT_TRUE(listener.contains(*first));
    EXPECT_TRUE(listener.contains(*second));
}

TEST(subscriber_test, the_construction_defaults_are_applied_to_every_registration) {
    registering_bus bus;
    const dispatch_options options { 7, back_pressure::DROP_NEW };
    subscriber listener(bus, options, [](const message&, std::exception_ptr) {});

    ASSERT_TRUE(listener.subscribe("a", noop()).has_value());

    ASSERT_EQ(bus.seen.size(), 1u);
    EXPECT_EQ(bus.seen[0].options.executor_, 7u);
    EXPECT_EQ(bus.seen[0].options.policy_, back_pressure::DROP_NEW);
    EXPECT_TRUE(bus.seen[0].has_error_handler);
    EXPECT_EQ(listener.options().executor_, 7u);
}

TEST(subscriber_test, a_registration_can_override_the_construction_options) {
    registering_bus bus;
    subscriber listener(bus, dispatch_options { 7, back_pressure::DROP_NEW });

    ASSERT_TRUE(listener.subscribe("a", noop(), dispatch_options { 9, back_pressure::BLOCK }).has_value());

    ASSERT_EQ(bus.seen.size(), 1u);
    EXPECT_EQ(bus.seen[0].options.executor_, 9u);
    EXPECT_EQ(bus.seen[0].options.policy_, back_pressure::BLOCK);
}

TEST(subscriber_test, an_override_of_the_error_handler_is_passed_through) {
    registering_bus bus;
    subscriber listener(bus);

    ASSERT_TRUE(listener.subscribe("a", noop(), {}, [](const message&, std::exception_ptr) {}).has_value());

    ASSERT_EQ(bus.seen.size(), 1u);
    EXPECT_TRUE(bus.seen[0].has_error_handler);
}

TEST(subscriber_test, a_rejected_pattern_registers_nothing_and_is_reported) {
    registering_bus bus;
    subscriber listener(bus);
    bus.reject = true;

    const auto added = listener.subscribe("a//b", noop());

    ASSERT_FALSE(added.has_value());
    EXPECT_EQ(added.error().code_, error_code::EMPTY_LEVEL);
    EXPECT_EQ(listener.count(), 0u) << "a failed registration leaves no handle behind";
    EXPECT_TRUE(bus.live.empty());
}

// --- cancellation ------------------------------------------------------------------------------------

TEST(subscriber_test, cancelling_one_id_cancels_only_that_registration) {
    registering_bus bus;
    subscriber listener(bus);
    const auto first = listener.subscribe("a", noop()).value();
    const auto second = listener.subscribe("b", noop()).value();

    EXPECT_TRUE(listener.cancel(first));

    EXPECT_EQ(listener.count(), 1u);
    EXPECT_FALSE(listener.contains(first));
    EXPECT_TRUE(listener.contains(second));
    ASSERT_EQ(bus.cancelled.size(), 1u);
    EXPECT_EQ(bus.cancelled[0], first);
}

TEST(subscriber_test, cancelling_is_idempotent) {
    registering_bus bus;
    subscriber listener(bus);
    const auto id = listener.subscribe("a", noop()).value();

    EXPECT_TRUE(listener.cancel(id));
    EXPECT_FALSE(listener.cancel(id)) << "the second cancel has nothing left to cancel";
    EXPECT_EQ(bus.cancelled.size(), 1u);
}

TEST(subscriber_test, an_id_this_subscriber_does_not_own_is_refused) {
    // Reaching across that boundary is what `msgbus::unsubscribe` is for; a subscriber cancels
    // what it registered, and a foreign id is not a miss worth acting on.
    registering_bus bus;
    subscriber first(bus);
    subscriber second(bus);
    const auto id = second.subscribe("a", noop()).value();

    EXPECT_FALSE(first.cancel(id));
    EXPECT_TRUE(second.contains(id));
    EXPECT_TRUE(bus.cancelled.empty());
}

TEST(subscriber_test, destroying_the_subscriber_cancels_everything_it_holds) {
    registering_bus bus;
    {
        subscriber listener(bus);
        static_cast<void>(listener.subscribe("a", noop()));
        static_cast<void>(listener.subscribe("b", noop()));
    }

    EXPECT_EQ(bus.cancelled.size(), 2u);
    EXPECT_TRUE(bus.live.empty()) << "the bus's own handles were dropped with the subscriber's";
}

TEST(subscriber_test, cancelling_all_leaves_the_subscriber_empty_and_reusable) {
    registering_bus bus;
    subscriber listener(bus);
    static_cast<void>(listener.subscribe("a", noop()));

    listener.cancel_all();

    EXPECT_TRUE(listener.empty());
    EXPECT_EQ(bus.cancelled.size(), 1u);
    ASSERT_TRUE(listener.subscribe("b", noop()).has_value());
    EXPECT_EQ(listener.count(), 1u);
}

// --- moving ------------------------------------------------------------------------------------------

TEST(subscriber_test, moving_hands_the_registrations_over_without_cancelling_them) {
    registering_bus bus;
    subscriber source(bus);
    const auto id = source.subscribe("a", noop()).value();

    subscriber destination(std::move(source));

    EXPECT_TRUE(source.empty()) << "the moved-from subscriber owns nothing";
    EXPECT_TRUE(bus.cancelled.empty()) << "a move is not a cancellation";
    EXPECT_TRUE(destination.contains(id));
    EXPECT_EQ(destination.count(), 1u);
}

TEST(subscriber_test, move_assignment_cancels_what_the_destination_held_first) {
    registering_bus bus;
    subscriber source(bus);
    const auto kept = source.subscribe("a", noop()).value();
    subscriber destination(bus);
    const auto dropped = destination.subscribe("b", noop()).value();

    destination = std::move(source);

    ASSERT_EQ(bus.cancelled.size(), 1u);
    EXPECT_EQ(bus.cancelled[0], dropped) << "the destination's own registration was abandoned, not leaked";
    EXPECT_TRUE(destination.contains(kept));
    EXPECT_FALSE(destination.contains(dropped));
}

// --- what the subscriber refuses ---------------------------------------------------------------------

TEST(subscriber_test, an_empty_handler_throws_rather_than_being_registered) {
    // A programming error, consistent with the bus: an empty `std::function` would throw on the first
    // delivery, which is a worse place to find out.
    registering_bus bus;
    subscriber listener(bus);

    EXPECT_THROW(static_cast<void>(listener.subscribe("a", handler {})), std::invalid_argument);
}

} // namespace ba7lya::msgbus
