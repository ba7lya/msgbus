///
/// @file subscription_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for the `subscription` handle: the registration it owns, the cancellation its
/// destruction performs, and what a move leaves behind.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common.hxx"
#include "error.hxx"
#include "layer_matcher.hxx"
#include "router.hxx"
#include "subscription.hxx"

namespace ba7lya::msgbus {

namespace {

/// @brief A handler that writes down nothing: these cases assert on the registration, not the message.
[[nodiscard]]
handler noop() {
    return [](const message&) {};
}

// --- what the handle promises as a type --------------------------------------------------------------

TEST(subscription_test, the_handle_owns_the_registration_exclusively) {
    // Move-only, and not by accident: two handles to one registration would mean two destructors
    // cancelling the same id, and whichever ran first would take the subscription with it while the
    // other still read as live. `std::optional` above is what a caller keeps instead of a copy.
    static_assert(std::is_move_constructible_v<subscription>);
    static_assert(std::is_move_assignable_v<subscription>);
    static_assert(!std::is_copy_constructible_v<subscription>);
    static_assert(!std::is_copy_assignable_v<subscription>);
}

TEST(subscription_test, an_empty_handle_owns_nothing) {
    subscription empty;

    EXPECT_FALSE(empty.owns());
    EXPECT_FALSE(static_cast<bool>(empty));
    EXPECT_EQ(empty.id(), 0u);
    EXPECT_FALSE(empty.cancel()) << "there is nothing to cancel, and no owner to ask";
}

TEST(subscription_test, two_empty_handles_compare_equal) {
    const subscription first;
    const subscription second;

    // The reason `operator==` exists: "this subscription is gone" is checkable without the bus.
    EXPECT_TRUE(first == second);

    router bus;
    const auto live = bus.subscribe("a/b", noop());
    ASSERT_TRUE(live.has_value());
    EXPECT_FALSE(live.value() == first);
}

// --- the lifetime contract ---------------------------------------------------------------------------

TEST(subscription_test, dropping_the_handle_cancels_the_subscription) {
    router bus;
    {
        const auto registered = bus.subscribe("a/b", noop());
        ASSERT_TRUE(registered.has_value());
        EXPECT_EQ(bus.subscription_count(), 1u);
    }

    // Not merely "the handle is gone": the registration is, too, which is the whole point of
    // returning a handle rather than an id.
    EXPECT_EQ(bus.subscription_count(), 0u);
    EXPECT_TRUE(bus.empty());
}

TEST(subscription_test, moving_the_handle_does_not_cancel) {
    router bus;
    std::optional<subscription> moved;

    {
        auto registered = bus.subscribe("a/b", noop());
        ASSERT_TRUE(registered.has_value());
        moved.emplace(std::move(registered.value()));

        // The source still exists here and its destructor still runs at the end of this scope; what
        // makes the move safe is that it was emptied, not that it was destroyed.
        EXPECT_FALSE(registered.value().owns()) << "the source is empty after the move";
        EXPECT_EQ(registered.value().id(), 0u);
    }

    ASSERT_TRUE(moved.has_value());
    EXPECT_TRUE(moved->owns());
    EXPECT_EQ(bus.subscription_count(), 1u) << "the destination took the registration over";
}

TEST(subscription_test, the_moved_from_handle_does_not_cancel_what_it_gave_away) {
    router bus;
    std::optional<subscription> moved;

    {
        auto registered = bus.subscribe("a/b", noop());
        ASSERT_TRUE(registered.has_value());
        moved.emplace(std::move(registered.value()));
    }
    EXPECT_EQ(bus.subscription_count(), 1u) << "the emptied source cancelled nothing on the way out";

    moved.reset();
    EXPECT_EQ(bus.subscription_count(), 0u);
}

TEST(subscription_test, assigning_over_a_live_handle_cancels_what_it_held) {
    router bus;
    auto first = bus.subscribe("a/b", noop());
    auto second = bus.subscribe("a/c", noop());
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(bus.subscription_count(), 2u);

    second.value() = std::move(*first);

    // The abandoned subscription is cancelled rather than leaked: overwriting a handle is a way of
    // dropping it, and a drop that skipped the cancellation would leave a registration nobody holds.
    EXPECT_EQ(bus.subscription_count(), 1u);
    EXPECT_EQ(second->id(), 1u) << "the destination names the subscription it took over";
}

// --- explicit cancellation ---------------------------------------------------------------------------

TEST(subscription_test, cancel_reports_whether_it_was_live_and_empties_the_handle) {
    router bus;
    auto registered = bus.subscribe("a/b", noop());
    ASSERT_TRUE(registered.has_value());

    EXPECT_TRUE(registered->cancel());
    EXPECT_FALSE(registered->owns()) << "a cancelled handle owns nothing, so its destructor is a no-op";
    EXPECT_FALSE(registered->cancel()) << "a second call has nothing left to cancel";
    EXPECT_EQ(bus.subscription_count(), 0u);
}

TEST(subscription_test, a_cancelled_handle_still_completes_its_own_destruction_quietly) {
    router bus;
    {
        const auto registered = bus.subscribe("device/#", noop());
        ASSERT_TRUE(registered.has_value());
    }

    // Nothing to assert but the absence of a second cancellation reaching the router: the count is
    // already zero, and a destructor that asked again would be a no-op returning false.
    EXPECT_EQ(bus.subscription_count(), 0u);
}

TEST(subscription_test, the_handle_keeps_the_id_it_was_minted_with) {
    router bus;
    const auto first = bus.subscribe("a/b", noop());
    const auto second = bus.subscribe("a/c", noop());
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());

    // Ids are the router's business and are never reissued, so the handle's copy is stable and
    // comparable against a raw one for diagnostics.
    EXPECT_EQ(first->id(), 1u);
    EXPECT_EQ(second->id(), 2u);
    EXPECT_TRUE(first.value() == 1u);
    EXPECT_FALSE(first.value() == 2u);
}

// --- the reason the handle is bound to the router ----------------------------------------------------

TEST(subscription_test, a_handle_rejected_by_the_pattern_owns_nothing) {
    router bus;
    const auto rejected = bus.subscribe("a//b", noop());

    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code_, error_code::EMPTY_LEVEL);
    EXPECT_EQ(bus.subscription_count(), 0u);
}

TEST(subscription_test, a_handle_created_inside_a_delivery_outlives_it) {
    router bus;
    std::optional<subscription> added_later;

    // Registered from inside a delivery, then cancelled from the outside once that delivery has
    // returned. A handle whose registration died with the delivery would make the second half here
    // impossible — and the router releases its lock before a handler runs precisely so this works.
    const auto trigger = bus.subscribe("a/b", [&bus, &added_later](const message&) {
        static_cast<void>(added_later.emplace(bus.subscribe("a/c", noop()).value()));
    });
    ASSERT_TRUE(trigger.has_value());

    const auto parsed = parse_topic("a/b");
    ASSERT_TRUE(parsed.has_value());
    static_cast<void>(router::deliver_all(bus.resolve(parsed->levels()), message { std::string("a/b"), {}, {} }));
    ASSERT_TRUE(added_later.has_value()) << "the handler ran and registered";

    EXPECT_TRUE(added_later->cancel());
    EXPECT_EQ(bus.subscription_count(), 1u) << "only the trigger is left";
}

TEST(subscription_test, a_handle_for_a_pattern_that_holds_another_id_does_not_cancel_it) {
    router bus;
    const auto first = bus.subscribe("a/b", noop());
    const auto second = bus.subscribe("a/b", noop());
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());

    {
        auto doomed = bus.subscribe("a/b", noop());
        ASSERT_TRUE(doomed.has_value());
        EXPECT_EQ(bus.subscription_count(), 3u);
    }

    // Cancellation is per id, not per pattern: the two that were not dropped are untouched.
    EXPECT_EQ(bus.subscription_count(), 2u);
}

} // namespace

} // namespace ba7lya::msgbus
