///
/// @file adapter_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for `adapter`: the two halves it composes, the provenance it stamps, and the
/// registrations it owns.
/// @version 0.1
/// @date 2026-10-18
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#include <any>
#include <chrono>
#include <cstdint>
#include <exception>
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "adapter.hxx"
#include "common.hxx"
#include "error.hxx"
#include "handler.hxx"
#include "message.hxx"
#include "msgbus.hxx"
#include "subscription.hxx"

namespace ba7lya::msgbus {

// The unit under test is in detail: this file is part of the library, so it may name it directly.

namespace {

/// @brief A bus that records what was published and mints subscriptions without delivering anything.
/// @details The adapter is a facade, so what it can get wrong is a forward: a fake that keeps both the
/// messages and the registrations is the narrowest thing that shows every one of them landing.
class recording_bus final : public msgbus {
public:
    result<subscription, error> subscribe(
        std::string_view pattern, handler callback, dispatch_options options, error_handler on_error
    ) override {
        // The bus's own contract, kept here so the adapter is tested against a bus that behaves like the
        // real one.
        if (!callback) { throw std::invalid_argument("the handler must not be empty"); }
        if (reject) { return std::unexpected(error::make(error_code::EMPTY_LEVEL, pattern)); }

        const auto id = next_id++;
        registrations.push_back(registration { std::string(pattern), options, static_cast<bool>(on_error) });
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

    void publish_sync(message msg) override { sync.push_back(std::move(msg)); }

    publish_status publish_async(message msg) override {
        if (full) { return publish_status::REJECTED; }
        async.push_back(std::move(msg));
        return publish_status::ACCEPTED;
    }

    bool flush(std::chrono::milliseconds timeout) override {
        waits.push_back(timeout);
        return drained;
    }

    struct registration {
        std::string pattern;
        dispatch_options options;
        bool has_error_handler = false;
    };

    std::vector<message> sync;
    std::vector<message> async;
    std::vector<registration> registrations;
    std::vector<subscription_id> live;
    std::vector<subscription_id> cancelled;
    std::vector<std::chrono::milliseconds> waits;
    subscription_id next_id = 1;
    bool reject = false;
    bool full = false;
    bool drained = true;
};

/// @brief A handler that does nothing; these cases assert on the registration, not the message.
[[nodiscard]]
handler noop() {
    return [](const message&) {};
}

/// @brief An error handler that does nothing, for the cases that only care whether one is present.
[[nodiscard]]
error_handler ignoring_error() {
    return [](const message&, std::exception_ptr) {};
}

} // namespace

// --- what the adapter promises as a type -------------------------------------------------------------

TEST(adapter_test, the_adapter_is_neither_copyable_nor_movable) {
    // A copy would give one component two registration lists cancelling the same ids; a move would have
    // to move a publisher that refuses to move, because two objects named one source fork one sequence.
    static_assert(!std::is_copy_constructible_v<adapter>);
    static_assert(!std::is_copy_assignable_v<adapter>);
    static_assert(!std::is_move_constructible_v<adapter>);
    static_assert(!std::is_move_assignable_v<adapter>);
}

// --- the publish half ---------------------------------------------------------------------------------

TEST(adapter_test, a_synchronous_publish_is_stamped_with_the_adapters_source_and_sequence) {
    recording_bus bus;
    adapter bridge(bus, "serial-bridge");

    EXPECT_EQ(bridge.source(), "serial-bridge");
    EXPECT_EQ(bridge.sent(), 0u);

    bridge.publish_sync("device/room1/temp", 21);
    bridge.publish_sync("device/room1/temp", 22);

    ASSERT_EQ(bus.sync.size(), 2u);
    EXPECT_EQ(bus.sync[0].topic_, "device/room1/temp");
    EXPECT_EQ(bus.sync[0].meta_.source_, "serial-bridge");
    EXPECT_EQ(bus.sync[0].meta_.seq_, 1u);
    EXPECT_EQ(bus.sync[1].meta_.seq_, 2u);
    EXPECT_EQ(bridge.sent(), 2u);
}

TEST(adapter_test, an_asynchronous_publish_forwards_the_outcome) {
    recording_bus bus;
    adapter bridge(bus, "serial-bridge");

    EXPECT_EQ(bridge.publish_async("a/b", 1), publish_status::ACCEPTED);
    bus.full = true;
    EXPECT_EQ(bridge.publish_async("a/b", 2), publish_status::REJECTED);

    ASSERT_EQ(bus.async.size(), 1u);
    EXPECT_EQ(bus.async[0].meta_.seq_, 1u);
    // The counter records what the adapter emitted, not what the bus accepted: a rejection leaves a gap
    // rather than reissuing the number.
    EXPECT_EQ(bridge.sent(), 2u);
}

TEST(adapter_test, a_message_handed_over_whole_keeps_its_provenance) {
    recording_bus bus;
    adapter bridge(bus, "relay");

    message_meta original;
    original.source_ = "the-original-sender";
    original.seq_ = 9;
    bridge.publish_sync(message { "a/b", std::any { 1 }, original });

    ASSERT_EQ(bus.sync.size(), 1u);
    EXPECT_EQ(bus.sync[0].meta_.source_, "the-original-sender");
    EXPECT_EQ(bus.sync[0].meta_.seq_, 9u);
    // Nothing was stamped, because nothing was built: the relay path is the bus's, not the counter's.
    EXPECT_EQ(bridge.sent(), 0u);
}

TEST(adapter_test, flush_forwards_the_timeout_and_the_bus_answer) {
    recording_bus bus;
    adapter bridge(bus, "serial-bridge");

    EXPECT_TRUE(bridge.flush(std::chrono::milliseconds { 250 }));
    bus.drained = false;
    EXPECT_FALSE(bridge.flush());

    ASSERT_EQ(bus.waits.size(), 2u);
    EXPECT_EQ(bus.waits[0], std::chrono::milliseconds { 250 });
    // The no-argument form is the bus's own "wait without limit", reached through the publisher half.
    EXPECT_EQ(bus.waits[1], std::chrono::milliseconds::max());
}

// --- the subscribe half -------------------------------------------------------------------------------

TEST(adapter_test, subscribing_returns_the_id_the_bus_issued_and_counts_it) {
    recording_bus bus;
    adapter bridge(bus, "serial-bridge");

    EXPECT_TRUE(bridge.empty());

    const auto first = bridge.subscribe("cmd/+", noop());
    const auto second = bridge.subscribe("cmd/#", noop());

    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(*first, 1u);
    EXPECT_EQ(*second, 2u);
    EXPECT_EQ(bridge.count(), 2u);
    EXPECT_TRUE(bridge.contains(*first));
    EXPECT_FALSE(bridge.contains(99u));
}

TEST(adapter_test, the_constructor_defaults_are_applied_to_every_registration) {
    recording_bus bus;
    adapter bridge(
        bus, "serial-bridge", dispatch_options { .executor_ = 7, .policy_ = back_pressure::BLOCK }, ignoring_error()
    );

    ASSERT_TRUE(bridge.subscribe("a/b", noop()).has_value());

    EXPECT_EQ(bridge.options().executor_, 7u);
    EXPECT_EQ(bridge.options().policy_, back_pressure::BLOCK);
    EXPECT_TRUE(static_cast<bool>(bridge.on_error()));

    ASSERT_EQ(bus.registrations.size(), 1u);
    EXPECT_EQ(bus.registrations[0].pattern, "a/b");
    EXPECT_EQ(bus.registrations[0].options.executor_, 7u);
    EXPECT_TRUE(bus.registrations[0].has_error_handler);
}

TEST(adapter_test, an_options_argument_overrides_the_default_for_one_registration_only) {
    recording_bus bus;
    adapter bridge(bus, "serial-bridge", dispatch_options { .executor_ = 7 }, ignoring_error());

    ASSERT_TRUE(bridge.subscribe("a/b", noop()).has_value());
    ASSERT_TRUE(bridge.subscribe("a/c", noop(), dispatch_options { .executor_ = 3 }).has_value());

    ASSERT_EQ(bus.registrations.size(), 2u);
    EXPECT_EQ(bus.registrations[0].options.executor_, 7u);
    EXPECT_EQ(bus.registrations[1].options.executor_, 3u);
    // Only the options were overridden: the error handler is still the adapter's.
    EXPECT_TRUE(bus.registrations[1].has_error_handler);
}

TEST(adapter_test, the_full_overload_overrides_both_defaults) {
    recording_bus bus;
    adapter bridge(bus, "serial-bridge", dispatch_options { .executor_ = 7 }, ignoring_error());

    ASSERT_TRUE(bridge.subscribe("a/b", noop(), dispatch_options {}, error_handler {}).has_value());

    ASSERT_EQ(bus.registrations.size(), 1u);
    EXPECT_EQ(bus.registrations[0].options.executor_, 0u);
    EXPECT_FALSE(bus.registrations[0].has_error_handler);
}

TEST(adapter_test, a_rejected_pattern_registers_nothing) {
    recording_bus bus;
    bus.reject = true;
    adapter bridge(bus, "serial-bridge");

    const auto rejected = bridge.subscribe("a/#/b", noop());

    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code_, error_code::EMPTY_LEVEL);
    EXPECT_TRUE(bridge.empty());
    EXPECT_TRUE(bus.registrations.empty());
}

TEST(adapter_test, cancelling_one_registration_leaves_the_others_live) {
    recording_bus bus;
    adapter bridge(bus, "serial-bridge");

    const auto first = bridge.subscribe("a/+", noop());
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(bridge.subscribe("a/#", noop()).has_value());

    EXPECT_TRUE(bridge.cancel(*first));
    EXPECT_EQ(bridge.count(), 1u);
    EXPECT_FALSE(bridge.contains(*first));
    EXPECT_FALSE(bridge.cancel(*first));
    EXPECT_EQ(bus.cancelled, std::vector<subscription_id> { *first });
}

TEST(adapter_test, cancelling_all_leaves_the_adapter_empty_and_reusable) {
    recording_bus bus;
    adapter bridge(bus, "serial-bridge");

    ASSERT_TRUE(bridge.subscribe("a/+", noop()).has_value());
    ASSERT_TRUE(bridge.subscribe("a/#", noop()).has_value());

    bridge.cancel_all();

    EXPECT_TRUE(bridge.empty());
    EXPECT_EQ(bus.cancelled.size(), 2u);

    ASSERT_TRUE(bridge.subscribe("a/x", noop()).has_value());
    EXPECT_EQ(bridge.count(), 1u);
}

TEST(adapter_test, destroying_the_adapter_cancels_what_it_still_owned) {
    recording_bus bus;
    {
        adapter bridge(bus, "serial-bridge");
        ASSERT_TRUE(bridge.subscribe("a/+", noop()).has_value());
        ASSERT_TRUE(bridge.subscribe("a/#", noop()).has_value());
    }

    EXPECT_EQ(bus.cancelled.size(), 2u);
    EXPECT_TRUE(bus.live.empty());
}

// --- both halves over one bus -------------------------------------------------------------------------

TEST(adapter_test, an_adapter_hears_its_own_publish_on_the_one_bus) {
    msgbus bus;
    adapter bridge(bus, "serial-bridge");

    std::vector<std::uint64_t> heard;
    std::vector<std::string> sources;
    const auto id = bridge.subscribe("device/+/temp", [&heard, &sources](const message& msg) {
        heard.push_back(std::any_cast<std::uint64_t>(msg.payload_));
        sources.push_back(msg.meta_.source_);
    });
    ASSERT_TRUE(id.has_value());

    bridge.publish_sync("device/room1/temp", std::any { std::uint64_t { 21 } });

    // The loop is legal, and this is what it looks like: the component's own subscription, reached by the
    // component's own publish. The provenance is what makes the round trip visible in a trace.
    ASSERT_EQ(heard.size(), 1u);
    EXPECT_EQ(heard[0], 21u);
    EXPECT_EQ(sources[0], "serial-bridge");
    EXPECT_EQ(bridge.sent(), 1u);
}

TEST(adapter_test, an_asynchronous_publish_reaches_the_subscription_after_the_flush) {
    msgbus bus;
    adapter bridge(bus, "serial-bridge");

    std::uint64_t seen = 0;
    ASSERT_TRUE(
        bridge.subscribe(
                  "device/#", [&seen](const message& msg) { seen = std::any_cast<std::uint64_t>(msg.payload_); }
        ).has_value()
    );

    EXPECT_EQ(bridge.publish_async("device/room1/temp", std::any { std::uint64_t { 42 } }), publish_status::ACCEPTED);
    EXPECT_TRUE(bridge.flush(std::chrono::milliseconds { 2000 }));
    EXPECT_EQ(seen, 42u);
}

} // namespace ba7lya::msgbus
