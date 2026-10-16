///
/// @file router_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for the routing table: id issuing, matching, cancellation, exception isolation
/// and the snapshot a publisher delivers from.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026
///
// SPDX-License-Identifier: MIT

#include <algorithm>
#include <any>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <gtest/gtest.h>
#include <initializer_list>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common.hxx"
#include "error.hxx"
#include "layer_matcher.hxx"
#include "router.hxx"

namespace ba7lya::msgbus {

namespace {

///
/// @brief A subscriber that writes down the topics it was handed, so a test can assert who received
/// what without a thread-safe gtest expectation in the way.
/// @details Synchronised because the concurrency cases deliver from several threads at once; the
/// single-threaded cases pay only the uncontended lock.
///
class recorder {
public:
    /// @brief The handler to register; records `tag` and the message's topic.
    /// @note Not `const`: it appends to `received_`, and a mutex alone does not make that lawful from
    /// a const member — so this mutates and `received_` needs no `mutable`.
    [[nodiscard]]
    handler callback(std::string_view tag = {}) {
        return [this, tag](const message& msg) {
            const std::scoped_lock guard(mutex_);
            received_.push_back(tag.empty() ? msg.topic_ : std::string(tag) + ":" + msg.topic_);
        };
    }

    /// @brief How many messages reached this recorder.
    [[nodiscard]]
    std::size_t count() const {
        const std::scoped_lock guard(mutex_);
        return received_.size();
    }

    /// @brief Whether exactly one message reached it, and it named `topic`.
    [[nodiscard]]
    bool received_only(std::string_view topic) const {
        const std::scoped_lock guard(mutex_);
        return received_.size() == 1 && received_.front() == topic;
    }

    /// @brief Everything it received, as a sorted list — deliveries are ordered arbitrarily.
    [[nodiscard]]
    std::vector<std::string> sorted() const {
        const std::scoped_lock guard(mutex_);
        std::vector<std::string> out = received_;
        std::ranges::sort(out);
        return out;
    }

    void clear() {
        const std::scoped_lock guard(mutex_);
        received_.clear();
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::string> received_;
};

/// @brief Swallows a `[[nodiscard]]` result a case does not assert on.
/// @details `static_cast<void>` would do, but clang-tidy still reads the discarded call as a
/// `bugprone-unused-return-value`; a named sink states the intent and settles the check.
void sink(const auto&... unused) { static_cast<void>(sizeof...(unused)); }

/// @brief Registers `callback`, failing the test if the pattern is rejected.
/// @return the subscription id, or 0 after a failure
/// @note Not `[[nodiscard]]`, unlike `router::subscribe` itself: the check that matters — the pattern
/// was accepted — has already happened here, so a case that does not need the id may drop it.
subscription_id subscribe(
    router& bus, std::string_view pattern, handler callback, dispatch_options options = {}, error_handler on_error = {}
) {
    const auto registered = bus.subscribe(pattern, std::move(callback), options, std::move(on_error));
    EXPECT_TRUE(registered.has_value()) << "on " << pattern;
    return registered.value_or(0);
}

/// @brief Resolves `topic` against `bus` and returns the matching ids, sorted so the expectation
/// reads as a set — the delivery order is unspecified and must not be asserted on.
[[nodiscard]]
std::vector<subscription_id> resolved_ids(const router& bus, std::string_view topic) {
    const auto parsed = parse_topic(topic);
    if (!parsed) {
        ADD_FAILURE() << "expected " << topic << " to be a topic";
        return {};
    }
    std::vector<subscription_id> ids;
    for (const delivery& route : bus.resolve(parsed.value().levels())) { ids.push_back(route.id_); }
    std::ranges::sort(ids);
    return ids;
}

/// @brief Asserts that `topic` reaches exactly `expected`, matching on the ids alone.
void expect_resolved(const router& bus, std::string_view topic, std::initializer_list<subscription_id> expected) {
    std::vector<subscription_id> wanted(expected.begin(), expected.end());
    std::ranges::sort(wanted);
    EXPECT_EQ(resolved_ids(bus, topic), wanted) << "on " << topic;
}

/// @brief Resolves and delivers `topic`, as a synchronous publisher would.
/// @return what `deliver_all` returned: the first unhandled exception, or null
[[nodiscard]]
std::exception_ptr publish(const router& bus, std::string_view topic) {
    const auto parsed = parse_topic(topic);
    if (!parsed) {
        ADD_FAILURE() << "expected " << topic << " to be a topic";
        return nullptr;
    }
    const std::vector<delivery> routes = bus.resolve(parsed.value().levels());
    return router::deliver_all(routes, message { topic_name(topic), {}, {} });
}

/// @brief A message-carrier for the cases that assert on the payload the handler saw.
[[nodiscard]]
message make_message(std::string_view topic, std::string_view payload) {
    return message { topic_name(topic), std::string(payload), {} };
}

// --- registration and ids --------------------------------------------------------------------------

TEST(router_test, a_subscription_receives_its_message) {
    router bus;
    recorder received;
    subscribe(bus, "device/room1/temp", received.callback());

    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr);
    EXPECT_TRUE(received.received_only("device/room1/temp"));
}

TEST(router_test, subscribe_returns_a_nonzero_id) {
    router bus;

    const auto first = subscribe(bus, "device/room1/temp", [](const message&) {});
    const auto second = subscribe(bus, "device/room2/temp", [](const message&) {});

    EXPECT_NE(first, 0u);
    EXPECT_NE(second, 0u);
    EXPECT_NE(first, second);
}

TEST(router_test, a_rejected_pattern_registers_nothing) {
    router bus;
    recorder received;

    const auto rejected = bus.subscribe("device//temp", received.callback());

    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code_, error_code::EMPTY_LEVEL);
    EXPECT_EQ(bus.subscription_count(), 0u);
    EXPECT_TRUE(bus.empty());
    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr);
    EXPECT_EQ(received.count(), 0u);
}

TEST(router_test, a_rejected_pattern_does_not_consume_an_id) {
    router bus;
    const auto first = subscribe(bus, "device/room1/temp", [](const message&) {});

    const auto rejected = bus.subscribe("device/#/temp", [](const message&) {});
    ASSERT_FALSE(rejected.has_value());

    const auto second = subscribe(bus, "device/room1/temp", [](const message&) {});
    EXPECT_EQ(second, first + 1);
}

TEST(router_test, subscribing_an_empty_handler_throws) {
    router bus;
    const handler empty;

    EXPECT_THROW(sink(bus.subscribe("device/room1/temp", empty)), std::invalid_argument);
    EXPECT_EQ(bus.subscription_count(), 0u);
}

TEST(router_test, a_pattern_may_be_held_by_two_ids) {
    router bus;
    const auto first = subscribe(bus, "device/room1/temp", [](const message&) {});
    const auto second = subscribe(bus, "device/room1/temp", [](const message&) {});

    expect_resolved(bus, "device/room1/temp", { first, second });
}

// --- matching and delivery -------------------------------------------------------------------------

TEST(router_test, a_literal_pattern_receives_only_its_own_topic) {
    router bus;
    recorder own;
    recorder other;
    subscribe(bus, "device/room1/temp", own.callback());
    subscribe(bus, "device/room1/humidity", other.callback());

    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr);
    EXPECT_TRUE(own.received_only("device/room1/temp"));
    EXPECT_EQ(other.count(), 0u);
}

TEST(router_test, a_single_level_wildcard_receives_exactly_one_level) {
    router bus;
    recorder received;
    subscribe(bus, "device/+/temp", received.callback());

    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr);
    EXPECT_TRUE(received.received_only("device/room1/temp"));

    received.clear();
    EXPECT_EQ(publish(bus, "device/room1/hall2/temp"), nullptr);
    EXPECT_EQ(received.count(), 0u);
}

TEST(router_test, a_multi_level_wildcard_receives_the_subtree_and_its_own_node) {
    router bus;
    recorder received;
    subscribe(bus, "device/#", received.callback());

    EXPECT_EQ(publish(bus, "device"), nullptr);
    EXPECT_EQ(publish(bus, "device/room1"), nullptr);
    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr);

    EXPECT_EQ(received.sorted(), (std::vector<std::string> { "device", "device/room1", "device/room1/temp" }));
}

TEST(router_test, overlapping_patterns_deliver_to_each_subscription_once) {
    router bus;
    recorder broad;
    recorder narrow;
    const auto broad_id = subscribe(bus, "device/#", broad.callback());
    const auto narrow_id = subscribe(bus, "device/room1/temp", narrow.callback());

    // Deduplication is per id: the matcher collects ids into a set, so an id reached through several
    // of its own patterns is still reached once. Two distinct ids that both match stay two deliveries.
    expect_resolved(bus, "device/room1/temp", { broad_id, narrow_id });
    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr);
    EXPECT_EQ(broad.count(), 1u);
    EXPECT_EQ(narrow.count(), 1u);
}

TEST(router_test, a_topic_matching_nothing_delivers_nothing) {
    router bus;
    recorder received;
    subscribe(bus, "device/room1/temp", received.callback());

    EXPECT_EQ(resolved_ids(bus, "device/room2/temp"), (std::vector<subscription_id> {}));
    EXPECT_EQ(publish(bus, "device/room2/temp"), nullptr);
    EXPECT_EQ(received.count(), 0u);
}

TEST(router_test, every_matching_subscription_is_delivered_exactly_once) {
    router bus;
    recorder first;
    recorder second;
    recorder third;
    subscribe(bus, "device/room1/temp", first.callback());
    subscribe(bus, "device/+/temp", second.callback());
    subscribe(bus, "device/#", third.callback());

    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr);
    EXPECT_EQ(first.count(), 1u);
    EXPECT_EQ(second.count(), 1u);
    EXPECT_EQ(third.count(), 1u);
}

// --- cancellation ----------------------------------------------------------------------------------

TEST(router_test, a_removed_subscription_no_longer_receives) {
    router bus;
    recorder received;
    const auto id = subscribe(bus, "device/room1/temp", received.callback());

    ASSERT_TRUE(bus.unsubscribe(id));

    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr);
    EXPECT_EQ(received.count(), 0u);
    expect_resolved(bus, "device/room1/temp", {});
}

TEST(router_test, unsubscribe_reports_whether_the_id_was_live) {
    router bus;
    const auto id = subscribe(bus, "device/room1/temp", [](const message&) {});

    EXPECT_TRUE(bus.unsubscribe(id));
    EXPECT_FALSE(bus.unsubscribe(id));
}

TEST(router_test, unsubscribe_is_idempotent) {
    router bus;
    const auto id = subscribe(bus, "device/+/temp", [](const message&) {});

    EXPECT_TRUE(bus.unsubscribe(id));
    EXPECT_FALSE(bus.unsubscribe(id));
    EXPECT_FALSE(bus.unsubscribe(id));
    EXPECT_EQ(bus.subscription_count(), 0u);
}

TEST(router_test, unsubscribing_an_unknown_id_returns_false) {
    router bus;
    subscribe(bus, "device/room1/temp", [](const message&) {});

    EXPECT_FALSE(bus.unsubscribe(9999));
    EXPECT_EQ(bus.subscription_count(), 1u);
}

TEST(router_test, an_id_is_never_reused_after_unsubscribe) {
    router bus;
    const auto first = subscribe(bus, "device/room1/temp", [](const message&) {});
    ASSERT_TRUE(bus.unsubscribe(first));

    const auto second = subscribe(bus, "device/room1/temp", [](const message&) {});

    EXPECT_NE(second, first);
    EXPECT_FALSE(bus.unsubscribe(first)) << "a stale id must not cancel a newer subscription";
    EXPECT_EQ(bus.subscription_count(), 1u);
}

TEST(router_test, unsubscribe_removes_the_pattern_it_held) {
    router bus;
    recorder first;
    recorder second;
    const auto id = subscribe(bus, "device/#", first.callback());
    const auto kept = subscribe(bus, "sensor/+/temp", second.callback());

    ASSERT_TRUE(bus.unsubscribe(id));

    EXPECT_EQ(bus.subscription_count(), 1u);
    EXPECT_FALSE(bus.empty());
    expect_resolved(bus, "device/room1/temp", {});
    expect_resolved(bus, "sensor/room1/temp", { kept });
    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr);
    EXPECT_EQ(first.count(), 0u);
}

// --- exception isolation ---------------------------------------------------------------------------

TEST(router_test, a_throwing_handler_does_not_skip_its_siblings) {
    router bus;
    recorder sibling;
    subscribe(bus, "device/room1/temp", [](const message&) { throw std::runtime_error("from the handler"); });
    subscribe(bus, "device/+/temp", sibling.callback());

    EXPECT_NE(publish(bus, "device/room1/temp"), nullptr);
    EXPECT_EQ(sibling.count(), 1u);
}

TEST(router_test, a_throwing_handler_goes_to_its_own_error_handler) {
    router bus;
    recorder reported;
    const error_handler on_error = [&reported](const message& msg, const std::exception_ptr& failure) {
        try {
            std::rethrow_exception(failure);
        }
        catch (const std::runtime_error& error) {
            reported.callback()(message { msg.topic_ + "/" + error.what(), {}, {} });
        }
    };
    subscribe(bus, "device/room1/temp", [](const message&) { throw std::runtime_error("boom"); }, {}, on_error);

    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr) << "a handled failure does not reach the caller";
    EXPECT_TRUE(reported.received_only("device/room1/temp/boom"));
}

TEST(router_test, a_throwing_handler_with_no_error_handler_is_returned_to_the_caller) {
    router bus;
    recorder sibling;
    subscribe(bus, "device/room1/temp", [](const message&) { throw std::runtime_error("boom"); });
    subscribe(bus, "sensor/room1/temp", sibling.callback());

    const std::exception_ptr failure = publish(bus, "device/room1/temp");

    ASSERT_NE(failure, nullptr);
    EXPECT_EQ(sibling.count(), 0u) << "the sibling subscribes to another topic";
    try {
        std::rethrow_exception(failure);
        ADD_FAILURE() << "expected the failure to be rethrowable";
    }
    catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "boom");
    }
}

TEST(router_test, an_error_handler_that_throws_does_not_stop_delivery) {
    router bus;
    recorder sibling;
    const error_handler on_error
        = [](const message&, const std::exception_ptr&) { throw std::runtime_error("handler too"); };
    subscribe(bus, "device/#", [](const message&) { throw std::runtime_error("boom"); }, {}, on_error);
    subscribe(bus, "device/room1/temp", sibling.callback());

    const std::exception_ptr failure = publish(bus, "device/room1/temp");

    EXPECT_EQ(failure, nullptr) << "the first route consumed its own failure; the second succeeded";
    EXPECT_EQ(sibling.count(), 1u);
}

TEST(router_test, only_the_first_unhandled_exception_is_returned) {
    router bus;
    subscribe(bus, "device/#", [](const message&) { throw std::runtime_error("first"); });
    subscribe(bus, "device/room1/temp", [](const message&) { throw std::runtime_error("second"); });

    const auto parsed = parse_topic("device/room1/temp");
    ASSERT_TRUE(parsed.has_value());
    const std::vector<delivery> routes = bus.resolve(parsed.value().levels());
    ASSERT_EQ(routes.size(), 2u);

    const std::exception_ptr failure = router::deliver_all(routes, make_message("device/room1/temp", ""));
    ASSERT_NE(failure, nullptr);

    std::string message_text;
    try {
        std::rethrow_exception(failure);
    }
    catch (const std::runtime_error& error) {
        message_text = error.what();
    }
    EXPECT_TRUE(message_text == "first" || message_text == "second") << message_text;
}

TEST(router_test, each_subscription_is_handed_the_message_it_was_sent) {
    router bus;
    std::string seen;
    subscribe(bus, "device/room1/temp", [&seen](const message& msg) {
        seen = std::any_cast<std::string>(msg.payload_);
    });

    const auto parsed = parse_topic("device/room1/temp");
    ASSERT_TRUE(parsed.has_value());
    const std::vector<delivery> routes = bus.resolve(parsed.value().levels());
    sink(router::deliver_all(routes, make_message("device/room1/temp", "22.5")));

    EXPECT_EQ(seen, "22.5");
}

// --- snapshot timing -------------------------------------------------------------------------------

TEST(router_test, resolve_reflects_a_subscription_registered_before_it) {
    router bus;
    recorder received;
    const auto id = subscribe(bus, "device/#", received.callback());

    expect_resolved(bus, "device/room1/temp", { id });
}

TEST(router_test, a_subscription_cancelled_before_resolve_is_not_delivered) {
    router bus;
    recorder received;
    const auto id = subscribe(bus, "device/#", received.callback());
    recorder sibling;
    subscribe(bus, "device/room1/temp", sibling.callback());

    ASSERT_TRUE(bus.unsubscribe(id));

    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr);
    EXPECT_EQ(received.count(), 0u);
    EXPECT_EQ(sibling.count(), 1u);
}

TEST(router_test, a_subscription_cancelled_after_the_snapshot_still_runs) {
    router bus;
    recorder received;
    const auto id = subscribe(bus, "device/room1/temp", received.callback());

    const auto parsed = parse_topic("device/room1/temp");
    ASSERT_TRUE(parsed.has_value());
    const std::vector<delivery> routes = bus.resolve(parsed.value().levels());
    ASSERT_EQ(routes.size(), 1u);

    ASSERT_TRUE(bus.unsubscribe(id)) << "the subscription is live when the snapshot is taken";

    // Deliberate: the snapshot holds its own copy of the callback, and the contract does not name
    // this window. Re-checking liveness per delivery would buy a second lock on the hot path.
    static_cast<void>(router::deliver_all(routes, make_message("device/room1/temp", "")));
    EXPECT_EQ(received.count(), 1u);
}

TEST(router_test, a_snapshot_does_not_see_a_subscription_added_after_it) {
    router bus;
    recorder late;
    const auto parsed = parse_topic("device/room1/temp");
    ASSERT_TRUE(parsed.has_value());

    const std::vector<delivery> routes = bus.resolve(parsed.value().levels());
    ASSERT_TRUE(routes.empty());

    subscribe(bus, "device/room1/temp", late.callback());

    static_cast<void>(router::deliver_all(routes, make_message("device/room1/temp", "")));
    EXPECT_EQ(late.count(), 0u);
}

TEST(router_test, dispatch_options_travel_with_the_delivery) {
    router bus;
    constexpr std::uint64_t PORT_EXECUTOR = 7;
    subscribe(bus, "device/#", [](const message&) {}, dispatch_options { PORT_EXECUTOR, back_pressure::DROP_NEW });

    const auto parsed = parse_topic("device/room1/temp");
    ASSERT_TRUE(parsed.has_value());
    const std::vector<delivery> routes = bus.resolve(parsed.value().levels());

    ASSERT_EQ(routes.size(), 1u);
    EXPECT_EQ(routes.front().options_.executor_, PORT_EXECUTOR);
    EXPECT_EQ(routes.front().options_.policy_, back_pressure::DROP_NEW);
}

TEST(router_test, resolve_on_a_bad_topic_is_the_callers_problem) {
    router bus;
    subscribe(bus, "device/#", [](const message&) {});

    // The router never parses, so a caller that skipped `parse_topic` gets whatever the matcher
    // makes of its levels. This is the parse failing, before the router is reached at all.
    const auto parsed = parse_topic("device/room1/+");

    ASSERT_FALSE(parsed.has_value());
    EXPECT_EQ(parsed.error().code_, error_code::WILDCARD_IN_TOPIC);
}

// --- reentrancy ------------------------------------------------------------------------------------

TEST(router_test, a_handler_may_subscribe_from_within_a_delivery) {
    router bus;
    recorder added_later;
    subscribe(bus, "device/room1/temp", [&bus, &added_later](const message&) {
        sink(bus.subscribe("sensor/#", added_later.callback()));
    });

    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr) << "the lock is released before the handler runs";
    EXPECT_EQ(bus.subscription_count(), 2u);
}

TEST(router_test, a_handler_may_unsubscribe_from_within_a_delivery) {
    router bus;
    recorder later;
    const auto later_id = subscribe(bus, "sensor/#", later.callback());
    subscribe(bus, "device/room1/temp", [&bus, later_id](const message&) { sink(bus.unsubscribe(later_id)); });

    EXPECT_EQ(publish(bus, "device/room1/temp"), nullptr);
    EXPECT_EQ(bus.subscription_count(), 1u);
    EXPECT_FALSE(bus.unsubscribe(later_id));
}

// --- concurrency -------------------------------------------------------------------------------------

TEST(router_test, concurrent_subscribe_and_unsubscribe_keep_the_id_sets_coherent) {
    router bus;
    std::atomic<bool> stop { false };
    std::atomic<int> issued { 0 };
    std::atomic<bool> cancelled_the_first { false };

    constexpr int ROUNDS = 200;
    std::thread adder([&bus, &stop, &issued] {
        for (int i = 0; i < ROUNDS; ++i) {
            if (bus.subscribe("device/#", [](const message&) {})) { ++issued; }
        }
        stop = true;
    });

    // Waits for the first id to exist, so that the cancellation below is guaranteed to hit a live
    // subscription rather than racing the thread that has not started yet: `issued` is bumped only
    // after `subscribe` returned, and only this thread ever removes id 1, so observing a non-zero
    // count is observing id 1 as live and the call cannot miss.
    //
    // That first cancellation is deliberately outside the loop below. Leaving it inside makes the
    // test depend on the adder still running when this thread is finally scheduled -- in Release the
    // adder gets through all ROUNDS rounds and sets `stop` first, the loop body then never executes,
    // and the test goes red over a cancellation that was never attempted rather than over anything
    // it means to assert. From the second call on it hammers the same id, which by then is a no-op;
    // the point is that neither thread ever disturbs the other.
    std::thread canceller([&bus, &stop, &issued, &cancelled_the_first] {
        while (issued.load() == 0) {}
        cancelled_the_first = bus.unsubscribe(1);
        while (!stop) {
            if (bus.unsubscribe(1)) { cancelled_the_first = true; }
        }
    });

    adder.join();
    canceller.join();

    EXPECT_EQ(issued.load(), ROUNDS);
    EXPECT_TRUE(cancelled_the_first.load());
    EXPECT_EQ(bus.subscription_count(), static_cast<std::size_t>(ROUNDS - 1))
        << "id 1 was cancelled; the rest remain live";
    EXPECT_FALSE(bus.empty());
}

TEST(router_test, concurrent_publish_and_subscribe_never_observe_a_half_registered_subscription) {
    router bus;
    recorder received;
    std::atomic<bool> stop { false };
    std::atomic<std::size_t> delivered { 0 };

    subscribe(bus, "device/#", received.callback());

    std::thread publisher([&bus, &stop, &delivered] {
        while (!stop) {
            delivered += parse_topic("device/room1/temp").has_value();
            static_cast<void>(publish(bus, "device/room1/temp"));
        }
    });

    std::thread subscriber([&bus, &stop] {
        constexpr int ROUNDS = 300;
        for (int i = 0; i < ROUNDS; ++i) {
            sink(bus.subscribe("device/+/temp", [](const message&) {}));
        }
        stop = true;
    });

    publisher.join();
    subscriber.join();

    EXPECT_GT(delivered.load(), 0u);
    EXPECT_GE(received.count(), delivered.load()) << "the fixed subscription is never skipped";
}

TEST(router_test, concurrent_unsubscribe_while_publishing_does_not_race_the_handler_copy) {
    router bus;
    recorder received;
    std::atomic<bool> stop { false };
    std::atomic<int> failures { 0 };

    // Id 1 is never reissued, so the cancel either finds a live id and removes it, or misses and
    // changes nothing. Either thread may win that race, and the delivery must be sound either way.
    const auto id = subscribe(bus, "device/#", received.callback());
    ASSERT_EQ(id, 1u);

    std::thread canceller([&bus, &stop] {
        while (!stop) { sink(bus.unsubscribe(1)); }
    });

    constexpr int ROUNDS = 400;
    std::thread publisher([&bus, &stop, &failures] {
        for (int i = 0; i < ROUNDS; ++i) {
            try {
                sink(publish(bus, "device/room1/temp"));
            }
            catch (...) {
                ++failures;
            }
        }
        stop = true;
    });

    canceller.join();
    publisher.join();

    EXPECT_EQ(failures.load(), 0);
}

} // namespace

} // namespace ba7lya::msgbus
