///
/// @file msgbus_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for the bus itself: the two publishing paths, the routing worker behind the
/// asynchronous one, executor-bound delivery, the failure policy of each path, back pressure, the
/// counters, and shutdown.
/// @version 0.1
/// @date 2026-10-16
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#include <any>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
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
#include "dispatcher.hxx"
#include "error.hxx"
#include "executor.hxx"
#include "msgbus.hxx"
#include "msgbus_base.hxx"
#include "subscription.hxx"

namespace ba7lya::msgbus {

namespace {

///
/// @brief One delivery as a subscriber saw it: which subscription, and on which thread.
/// @details The thread is recorded because "which executor does this handler run on" is the whole
/// point of `dispatch_options`, and the only honest witness of it is the handler's own observation.
///
struct receipt {
    std::string tag;
    std::thread::id thread;
};

///
/// @brief A subscriber that writes down what it was handed, so a test can assert who received what
/// without a gtest expectation running on a worker thread.
/// @details Synchronised because the asynchronous cases deliver from the routing worker, an executor
/// thread, or the caller — the single-threaded cases pay only the uncontended lock.
///
class recorder {
public:
    /// @brief The handler to register; records `tag` (or the message's topic) and the thread it ran on.
    [[nodiscard]]
    handler callback(std::string_view tag = {}) {
        return [this, tag](const message& msg) {
            const std::scoped_lock guard(mutex_);
            received_.push_back(receipt { tag.empty() ? msg.topic_ : std::string(tag), std::this_thread::get_id() });
        };
    }

    /// @brief How many messages reached this recorder.
    [[nodiscard]]
    std::size_t count() const {
        const std::scoped_lock guard(mutex_);
        return received_.size();
    }

    /// @brief Whether exactly one message reached it, and it carried `tag`.
    [[nodiscard]]
    bool received_only(std::string_view tag) const {
        const std::scoped_lock guard(mutex_);
        return received_.size() == 1 && received_.front().tag == tag;
    }

    /// @brief Everything it received, as a sorted list of tags — delivery order is unspecified.
    [[nodiscard]]
    std::vector<std::string> sorted() const {
        const std::scoped_lock guard(mutex_);
        std::vector<std::string> out;
        out.reserve(received_.size());
        for (const receipt& entry : received_) { out.push_back(entry.tag); }
        std::ranges::sort(out);
        return out;
    }

    /// @brief The thread every delivery so far ran on, or a default id if there were none.
    [[nodiscard]]
    std::thread::id thread_of_first() const {
        const std::scoped_lock guard(mutex_);
        return received_.empty() ? std::thread::id {} : received_.front().thread;
    }

    /// @brief Whether every delivery so far ran on one thread, and that thread is not `other`.
    [[nodiscard]]
    bool all_on_one_thread_other_than(std::thread::id other) const {
        const std::scoped_lock guard(mutex_);
        if (received_.empty()) { return false; }
        for (const receipt& entry : received_) {
            if (entry.thread != received_.front().thread || entry.thread == other) { return false; }
        }
        return true;
    }

    void clear() {
        const std::scoped_lock guard(mutex_);
        received_.clear();
    }

private:
    mutable std::mutex mutex_;
    std::vector<receipt> received_;
};

///
/// @brief Collects what the bus handed to its error sink, without asserting from a worker thread.
///
class failure_log {
public:
    /// @brief The sink to install; records the failure and lets the holder inspect it later.
    [[nodiscard]]
    exception_sink sink() {
        return [this](std::exception_ptr failure) {
            const std::scoped_lock guard(mutex_);
            failures_.push_back(std::move(failure));
        };
    }

    [[nodiscard]]
    std::size_t count() const {
        const std::scoped_lock guard(mutex_);
        return failures_.size();
    }

    /// @brief Re-throws the first failure, for a test that wants to check its type.
    /// @details Only meaningful from the test's own thread, and only once — the log is not a queue.
    void rethrow_first() const {
        const std::scoped_lock guard(mutex_);
        if (!failures_.empty()) { std::rethrow_exception(failures_.front()); }
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::exception_ptr> failures_;
};

///
/// @brief A rendezvous a handler holds on until the test lets it go, and which tells the test when the
/// handler has arrived. It replaces every `sleep` a timing-based case would otherwise need: the test
/// waits for `wait_entered()` — proof the handler is inside — instead of guessing how long it takes to
/// get there.
///
class gate {
public:
    /// @brief Called from a handler: announces arrival, then blocks until `open()`.
    void hold() {
        std::unique_lock lock(mutex_);
        entered_ = true;
        cv_.notify_all();
        cv_.wait(lock, [this] { return open_; });
    }

    /// @brief Called from the test: blocks until some handler has entered `hold()`.
    void wait_entered() {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return entered_; });
    }

    /// @brief Releases every handler that is holding, and every later one.
    void open() {
        {
            const std::unique_lock lock(mutex_);
            open_ = true;
        }
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_ = false;
    bool open_ = false;
};

/// @brief Swallows a `[[nodiscard]]` result a case does not assert on.
void sink(const auto&... unused) { static_cast<void>(sizeof...(unused)); }

/// @brief A message carrying `topic` and an integer payload, for the cases that assert on provenance.
[[nodiscard]]
message make_message(std::string_view topic, int payload = 0, std::string source = {}, std::uint64_t seq = 0) {
    return message {
        topic_name(topic), std::any { payload },
          message_meta { {}, std::move(source), seq }
    };
}

///
/// @brief What a case arms: every handle it registered on a bus, held until the holder dies.
/// @details `subscribe` returns an owning handle, and a handle destroyed at the end of its statement
/// cancels the subscription right there. A case that armed a subscription and then published — leaving
/// the result as a bare statement — would therefore be arming nothing, and the whole file would go on
/// passing while testing an empty table. Parking the handles here for the length of the case is what
/// makes `armed.add(...)` as a statement mean "registered, and it stays registered".
///
/// The declaration order matters: a holder is declared after the bus it registers on and destroyed
/// before it, so every handle reaches its router while the router is still alive.
///
class registrations {
public:
    registrations() = default;

    registrations(const registrations&) = delete;
    registrations& operator=(const registrations&) = delete;
    registrations(registrations&&) = delete;
    registrations& operator=(registrations&&) = delete;

    /// @brief Cancels every subscription still held, in reverse order of registration.
    ~registrations() = default;

    /// @brief Registers `callback` for `pattern`, keeping the handle alive here.
    /// @return the id the bus issued, or 0 if the pattern was rejected
    /// @note Not `[[nodiscard]]`: most cases arm a subscription for its effect and never look at the
    /// id. The failure it could report is the rejected pattern, and that has already been asserted on
    /// — a case that means to assert on the rejection calls the bus's own `subscribe` instead.
    /// @note The bus issues the id and the router stores it, so the value returned here is the same
    /// one `resolve` reports for this registration and the one `unsubscribe` takes.
    subscription_id add(
        msgbus_base& bus, std::string_view pattern, handler callback, dispatch_options options = {},
        error_handler on_error = {}
    ) {
        auto registered = bus.subscribe(pattern, std::move(callback), options, std::move(on_error));
        EXPECT_TRUE(registered.has_value()) << "on " << pattern;
        if (!registered.has_value()) { return 0; }

        subscription handle = *std::move(registered);
        const subscription_id id = handle.id();
        handles_.push_back(std::move(handle));
        return id;
    }

    /// @brief How many handles are still held, which is how many are still live.
    [[nodiscard]]
    std::size_t count() const noexcept {
        return handles_.size();
    }

private:
    /// @brief The handles, one per registration, in the order they were armed.
    std::vector<subscription> handles_;
};

// --- construction and lifecycle -----------------------------------------------------------------------

TEST(msgbus_test, a_new_bus_has_no_subscriptions_and_has_routed_nothing) {
    const msgbus bus;

    EXPECT_TRUE(bus.empty());
    EXPECT_EQ(bus.subscription_count(), 0u);
    EXPECT_EQ(bus.routed(), 0u);
    EXPECT_EQ(bus.dropped(), 0u);
    EXPECT_FALSE(bus.on_exception());
}

TEST(msgbus_test, the_routing_worker_is_registered_under_the_rowter_id) {
    msgbus bus;

    EXPECT_TRUE(bus.executors().contains(msgbus::ROUTER));
    EXPECT_EQ(bus.executors().size(), 1u);
    EXPECT_FALSE(bus.executors().contains(dispatcher::INLINE)) << "0 names the caller's thread, never a thread";
}

TEST(msgbus_test, add_executor_refuses_the_inline_id_and_the_routing_id) {
    msgbus bus;

    EXPECT_THROW(sink(bus.add_executor(dispatcher::INLINE)), std::invalid_argument);
    EXPECT_FALSE(bus.add_executor(msgbus::ROUTER)) << "the bus cannot route without its worker";
    EXPECT_FALSE(bus.add_executor(99) && false) << "the first registration of 99 must succeed";
}

TEST(msgbus_test, add_executor_reports_a_taken_id_without_disturbing_the_holder) {
    msgbus bus;
    registrations armed;
    recorder received;

    ASSERT_TRUE(bus.add_executor(7));
    armed.add(bus, "a/b", received.callback(), dispatch_options { 7 });

    EXPECT_FALSE(bus.add_executor(7)) << "re-registering must be refused, not silently replace";

    bus.publish_sync("a/b", std::any { 1 });
    // `flush` drains the routing queue, and this delivery never enters it: the subscription named
    // executor 7, so the handler runs on 7's own worker. What has to be waited for is that executor.
    // A `.find(7)` that came back null here would be the defect this test exists to rule out.
    EXPECT_TRUE(bus.executors().find(7)->drain(std::chrono::milliseconds { 2000 }));
    EXPECT_TRUE(received.received_only("a/b")) << "the original executor still serves the subscription";
}

TEST(msgbus_test, remove_executor_refuses_the_rowter_and_reports_unknown_ids) {
    msgbus bus;

    EXPECT_FALSE(bus.remove_executor(msgbus::ROUTER));
    EXPECT_FALSE(bus.remove_executor(42));
    ASSERT_TRUE(bus.add_executor(7));
    EXPECT_TRUE(bus.remove_executor(7));
    EXPECT_FALSE(bus.remove_executor(7)) << "removal is idempotent";
}

TEST(msgbus_test, subscribing_an_empty_handler_throws_and_registers_nothing) {
    msgbus bus;
    const handler empty;

    EXPECT_THROW(sink(bus.subscribe("a/b", empty)), std::invalid_argument);
    EXPECT_EQ(bus.subscription_count(), 0u);
}

TEST(msgbus_test, subscribe_reports_a_rejected_pattern_by_value) {
    msgbus bus;
    recorder received;

    const auto rejected = bus.subscribe("a//b", received.callback());

    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code_, error_code::EMPTY_LEVEL);
    EXPECT_TRUE(bus.empty());
}

// --- synchronous publishing ---------------------------------------------------------------------------

TEST(msgbus_test, publish_sync_runs_an_inline_handler_before_it_returns) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/b", received.callback());

    bus.publish_sync("a/b", std::any { 42 });

    EXPECT_TRUE(received.received_only("a/b")) << "no flush: the handler must already have run";
    EXPECT_EQ(bus.routed(), 1u);
    EXPECT_EQ(bus.worker_stats().submitted_, 0u) << "an inline delivery never reaches the routing worker";
}

TEST(msgbus_test, publish_sync_runs_the_handler_on_the_callers_thread) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/b", received.callback());

    bus.publish_sync("a/b", std::any { 42 });

    ASSERT_EQ(received.count(), 1u);
    EXPECT_EQ(received.thread_of_first(), std::this_thread::get_id());
}

TEST(msgbus_test, publish_sync_reaches_every_matching_subscription_once) {
    msgbus bus;
    registrations armed;
    recorder first;
    recorder second;
    recorder third;
    armed.add(bus, "a/+", first.callback("first"));
    armed.add(bus, "a/#", second.callback("second"));
    armed.add(bus, "a/b", second.callback("second"));
    armed.add(bus, "b/#", third.callback("third"));

    bus.publish_sync("a/b", std::any { 1 });

    EXPECT_TRUE(first.received_only("first"));
    // Two of the patterns above match — `a/#` and `a/b` — and they share one recorder, so the
    // message is delivered once per matching subscription. `routed()` counts messages, not
    // deliveries, which is why it stays at 1.
    EXPECT_EQ(second.sorted(), (std::vector<std::string> { "second", "second" }))
        << "one delivery per matching subscription, not one per message";
    EXPECT_EQ(third.count(), 0u);
    EXPECT_EQ(bus.routed(), 1u);
}

TEST(msgbus_test, publish_sync_carries_the_messages_provenance_to_the_handler) {
    msgbus bus;
    registrations armed;
    std::mutex mutex;
    std::string source;
    std::uint64_t seq = 0;
    int payload = 0;
    armed.add(bus, "a/b", [&](const message& msg) {
        const std::scoped_lock guard(mutex);
        source = msg.meta_.source_;
        seq = msg.meta_.seq_;
        payload = std::any_cast<int>(msg.payload_);
    });

    bus.publish_sync(make_message("a/b", 17, "modbus-adapter", 3));

    const std::scoped_lock guard(mutex);
    EXPECT_EQ(payload, 17);
    EXPECT_EQ(source, "modbus-adapter");
    EXPECT_EQ(seq, 3u);
}

TEST(msgbus_test, publish_sync_matches_a_subtree_including_its_own_node) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "device/#", received.callback());

    bus.publish_sync("device", std::any { 1 });
    bus.publish_sync("device/room1", std::any { 2 });
    bus.publish_sync("device/room1/temp", std::any { 3 });

    EXPECT_EQ(received.count(), 3u) << "`#` matches the node itself as well as everything below it";
    EXPECT_EQ(bus.routed(), 3u);
}

TEST(msgbus_test, publish_sync_throws_on_a_malformed_topic_and_routes_nothing) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/#", received.callback());

    EXPECT_THROW(bus.publish_sync("a//b", std::any { 1 }), std::invalid_argument);
    EXPECT_THROW(bus.publish_sync("", std::any { 1 }), std::invalid_argument);

    EXPECT_EQ(received.count(), 0u);
    EXPECT_EQ(bus.routed(), 0u) << "a parse failure happens before anything is routed";
}

TEST(msgbus_test, publish_sync_rejects_a_wildcard_in_the_published_topic) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/#", received.callback());

    EXPECT_THROW(bus.publish_sync("a/+", std::any { 1 }), std::invalid_argument);
    EXPECT_THROW(bus.publish_sync("a/#", std::any { 1 }), std::invalid_argument);

    EXPECT_EQ(received.count(), 0u);
}

TEST(msgbus_test, publish_sync_is_accepted_through_a_base_reference) {
    msgbus bus;
    registrations armed;
    recorder received;
    msgbus_base& abstract = bus;
    armed.add(bus, "a/b", received.callback());

    abstract.publish_sync("a/b", std::any { 1 });

    EXPECT_TRUE(received.received_only("a/b"));
}

// --- asynchronous publishing --------------------------------------------------------------------------

TEST(msgbus_test, publish_async_returns_before_the_handler_has_run) {
    // Real threads: the routing worker is gated inside the handler, so the publisher demonstrably does
    // not wait for it.
    msgbus bus;
    registrations armed;
    gate g;
    std::atomic<bool> ran { false };
    armed.add(
        bus, "a/b",
        [&](const message&) {
            g.hold();
            ran = true;
        },
        dispatch_options { msgbus::ROUTER }
    );

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED);
    g.wait_entered();
    EXPECT_FALSE(ran.load()) << "the publisher had already returned while the handler was still holding";

    g.open();
    EXPECT_TRUE(bus.flush());
    EXPECT_TRUE(ran.load());
    EXPECT_EQ(bus.routed(), 1u);
}

TEST(msgbus_test, publish_async_routes_the_message_on_the_routing_worker) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/b", received.callback(), dispatch_options { msgbus::ROUTER });

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED);
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));

    ASSERT_EQ(received.count(), 1u);
    EXPECT_EQ(received.sorted(), (std::vector<std::string> { "a/b" }));
    EXPECT_NE(received.thread_of_first(), std::this_thread::get_id());
    EXPECT_EQ(bus.routed(), 1u);
    EXPECT_EQ(bus.worker_stats().completed_, 1u);
}

TEST(msgbus_test, publish_async_runs_an_inline_subscription_on_the_routing_worker_too) {
    // The worker routes the message and finds a delivery that named no executor: it runs there, on the
    // worker, because the worker is the thread that reached it.
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/b", received.callback());

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED);
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));

    ASSERT_EQ(received.count(), 1u);
    EXPECT_NE(received.thread_of_first(), std::this_thread::get_id());
}

TEST(msgbus_test, publish_async_is_accepted_when_nothing_matches) {
    msgbus bus;

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED);

    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));
    EXPECT_EQ(bus.routed(), 1u) << "routed counts what was routed, not what was delivered";
}

TEST(msgbus_test, publish_async_reports_a_malformed_topic_to_the_error_sink) {
    // The parse happens on the worker, where there is no caller left to throw at.
    failure_log log;
    msgbus bus(log.sink());

    EXPECT_EQ(bus.publish_async("a//b", std::any { 1 }), publish_status::ACCEPTED) << "nothing is parsed at once";
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));

    EXPECT_EQ(log.count(), 1u);
    EXPECT_THROW(log.rethrow_first(), std::invalid_argument);
    EXPECT_EQ(bus.routed(), 0u);
    EXPECT_EQ(bus.worker_stats().failed_, 1u);
}

TEST(msgbus_test, publish_async_is_rejected_after_the_bus_is_released) {
    msgbus bus;

    bus.release();

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::REJECTED);
    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::REJECTED);
}

TEST(msgbus_test, publish_async_carries_the_messages_provenance_to_the_handler) {
    msgbus bus;
    registrations armed;
    std::mutex mutex;
    std::string source;
    armed.add(bus, "a/b", [&](const message& msg) {
        const std::scoped_lock guard(mutex);
        source = msg.meta_.source_;
    });

    EXPECT_EQ(bus.publish_async(make_message("a/b", 5, "opcua-adapter")), publish_status::ACCEPTED);
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));

    const std::scoped_lock guard(mutex);
    EXPECT_EQ(source, "opcua-adapter");
}

// --- back pressure ------------------------------------------------------------------------------------

TEST(msgbus_test, publish_async_rejects_when_the_routing_queue_is_full_under_drop_new) {
    // Real threads: a gated handler occupies the worker, so the one-slot queue can be filled exactly.
    msgbus bus(1, back_pressure::DROP_NEW);
    registrations armed;
    gate g;
    std::atomic<int> ran { 0 };
    armed.add(
        bus, "a/b",
        [&](const message&) {
            g.hold();
            ++ran;
        },
        dispatch_options { msgbus::ROUTER }
    );

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED); // the worker takes it
    g.wait_entered();
    EXPECT_EQ(bus.publish_async("a/b", std::any { 2 }), publish_status::ACCEPTED); // fills the only slot

    EXPECT_EQ(bus.publish_async("a/b", std::any { 3 }), publish_status::REJECTED);

    g.open();
    EXPECT_TRUE(bus.flush());
    EXPECT_EQ(ran.load(), 2) << "the two accepted messages ran; the refused one never existed";
    EXPECT_EQ(bus.routed(), 2u);
}

TEST(msgbus_test, publish_async_evicts_the_oldest_message_under_drop_oldest) {
    // Real threads: the evicted message is the one that would have run second.
    msgbus bus(1, back_pressure::DROP_OLDEST);
    registrations armed;
    gate g;
    std::mutex mutex;
    std::vector<int> seen;
    const auto record = [&mutex, &seen](const message& msg) {
        const std::scoped_lock guard(mutex);
        seen.push_back(std::any_cast<int>(msg.payload_));
    };
    armed.add(
        bus, "a/b",
        [&](const message& msg) {
            g.hold();
            record(msg);
        },
        dispatch_options { msgbus::ROUTER }
    );
    armed.add(bus, "a/b", record, dispatch_options { msgbus::ROUTER });

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED);
    g.wait_entered();
    EXPECT_EQ(bus.publish_async("a/b", std::any { 2 }), publish_status::ACCEPTED);

    EXPECT_EQ(bus.publish_async("a/b", std::any { 3 }), publish_status::ACCEPTED) << "an eviction is an acceptance";

    g.open();
    EXPECT_TRUE(bus.flush());

    const std::scoped_lock guard(mutex);
    // Two subscriptions share this topic, so each surviving message is seen twice. The eviction is
    // the point: 2 was never delivered, displaced in the routing queue by 3 before its turn came.
    EXPECT_EQ(seen, (std::vector<int> { 1, 1, 3, 3 })) << "message 2 was evicted by 3";
    EXPECT_EQ(bus.routed(), 2u);
}

TEST(msgbus_test, publish_async_under_block_never_rejects_a_full_queue) {
    // Real threads: the gated worker holds the queue full while a publisher parks in publish_async.
    msgbus bus(1, back_pressure::BLOCK);
    registrations armed;
    gate g;
    std::atomic<int> ran { 0 };
    armed.add(
        bus, "a/b",
        [&](const message&) {
            g.hold();
            ++ran;
        },
        dispatch_options { msgbus::ROUTER }
    );

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED);
    g.wait_entered();
    EXPECT_EQ(bus.publish_async("a/b", std::any { 2 }), publish_status::ACCEPTED);

    std::atomic<publish_status> result { publish_status::ACCEPTED };
    std::atomic<bool> attempting { false };
    std::thread publisher([&] {
        attempting = true;
        result = bus.publish_async("a/b", std::any { 3 });
    });

    while (!attempting.load()) {}
    g.open();
    publisher.join();

    EXPECT_EQ(result.load(), publish_status::ACCEPTED) << "a full queue makes BLOCK wait, not refuse";
    EXPECT_TRUE(bus.flush());
    EXPECT_EQ(ran.load(), 3);
    EXPECT_EQ(bus.dropped(), 0u);
}

TEST(msgbus_test, publish_async_from_the_routing_worker_is_rejected_when_the_queue_is_full) {
    // Real threads: the handler runs on the routing worker and publishes into the very queue it is
    // draining, which is full. Running inline is impossible for a deferred closure and queuing would
    // deadlock, so the refusal is the documented answer.
    msgbus bus(1, back_pressure::DROP_NEW);
    registrations armed;
    gate g;
    std::atomic<publish_status> nested { publish_status::ACCEPTED };
    armed.add(
        bus, "a/b",
        [&](const message&) {
            g.hold();
            nested = bus.publish_async("a/b", std::any { 9 });
        },
        dispatch_options { msgbus::ROUTER }
    );

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED); // the worker takes it
    g.wait_entered();
    EXPECT_EQ(bus.publish_async("a/b", std::any { 2 }), publish_status::ACCEPTED); // fills the only slot

    g.open();
    EXPECT_TRUE(bus.flush());

    EXPECT_EQ(nested.load(), publish_status::REJECTED);
}

// --- flush --------------------------------------------------------------------------------------------

TEST(msgbus_test, flush_on_an_idle_bus_is_true_at_once) {
    msgbus bus;

    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 0 }));
    EXPECT_TRUE(bus.flush());
}

TEST(msgbus_test, flush_waits_until_the_queued_message_has_been_routed) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/b", received.callback(), dispatch_options { msgbus::ROUTER });

    for (int i = 0; i < 64; ++i) { EXPECT_EQ(bus.publish_async("a/b", std::any { i }), publish_status::ACCEPTED); }
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 5000 }));

    EXPECT_EQ(received.count(), 64u);
    EXPECT_EQ(bus.routed(), 64u);
}

TEST(msgbus_test, flush_does_not_wait_for_an_executor_thread) {
    // Real threads: the delivery has reached the executor, which is holding it, and that is as far as
    // flush's promise goes.
    msgbus bus;
    registrations armed;
    gate g;
    ASSERT_TRUE(bus.add_executor(7));
    recorder received;
    std::atomic<bool> entered { false };
    armed.add(
        bus, "a/b",
        [&](const message& msg) {
            entered = true;
            g.hold();
            received.callback()(msg);
        },
        dispatch_options { 7 }
    );

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED);
    while (!entered.load()) {}

    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 })) << "the routing queue is empty; the executor's is not";
    EXPECT_EQ(bus.routed(), 1u);

    g.open();
    EXPECT_TRUE(bus.executors().find(7)->drain());
    EXPECT_EQ(received.count(), 1u);
}

TEST(msgbus_test, flush_from_the_routing_worker_throws) {
    // Real threads: a handler running on the routing worker asks the bus to drain the queue it is
    // currently draining. The subscriber's own error handler is where the failure is caught.
    msgbus bus;
    registrations armed;
    std::exception_ptr failure;
    std::atomic<bool> reported { false };
    armed.add(
        bus, "a/b", [&](const message&) { sink(bus.flush()); }, dispatch_options { msgbus::ROUTER },
        [&](const message&, std::exception_ptr cause) {
            failure = cause;
            // The runner was polling `reported` when the handler thread wrote it, so the flag has to be
            // atomic — and it is what tells the reader below that `failure` is safe to read at all.
            reported.store(true, std::memory_order_release);
        }
    );

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED);
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));
    // A drained routing queue does not yet mean a reported failure: the handler sets that flag before the
    // worker picks up the next queue item, so there is a window. Waiting for the flag is what closes it —
    // the previous shape read a plain `std::exception_ptr` that the worker may still have been writing,
    // and an unsynchronised read of a library type is exactly what turns into a `std::terminate`.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds { 2000 };
    while (!reported.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {}

    ASSERT_TRUE(reported.load(std::memory_order_acquire))
        << "the throw must have reached the subscription's error handler";
    const std::exception_ptr cause = failure;
    try {
        std::rethrow_exception(cause);
        ADD_FAILURE() << "expected std::invalid_argument";
    }
    catch (const std::invalid_argument&) {
    }
    catch (...) {
        ADD_FAILURE() << "expected std::invalid_argument";
    }
}

// --- executor dispatch --------------------------------------------------------------------------------

TEST(msgbus_test, a_handler_named_by_dispatch_options_runs_on_that_executors_thread) {
    msgbus bus;
    registrations armed;
    ASSERT_TRUE(bus.add_executor(7));
    recorder on_executor;
    recorder on_router;
    armed.add(bus, "a/#", on_executor.callback("executor"), dispatch_options { 7 });
    armed.add(bus, "a/#", on_router.callback("router"), dispatch_options { msgbus::ROUTER });

    bus.publish_sync("a/b", std::any { 1 });
    EXPECT_TRUE(bus.executors().find(7)->drain());
    EXPECT_TRUE(bus.flush());

    const std::thread::id caller = std::this_thread::get_id();
    ASSERT_EQ(on_executor.count(), 1u);
    ASSERT_EQ(on_router.count(), 1u);
    EXPECT_NE(on_executor.thread_of_first(), caller);
    EXPECT_NE(on_router.thread_of_first(), caller);
    EXPECT_NE(on_executor.thread_of_first(), on_router.thread_of_first())
        << "two executors are two threads, which is the isolation a subscriber names one for";
}

TEST(msgbus_test, every_delivery_to_one_executor_runs_on_one_thread) {
    msgbus bus;
    registrations armed;
    ASSERT_TRUE(bus.add_executor(7));
    recorder received;
    armed.add(bus, "a/#", received.callback(), dispatch_options { 7 });

    for (int i = 0; i < 32; ++i) { bus.publish_sync("a/b", std::any { i }); }
    EXPECT_TRUE(bus.executors().find(7)->drain());

    EXPECT_EQ(received.count(), 32u);
    EXPECT_TRUE(received.all_on_one_thread_other_than(std::this_thread::get_id()));
}

TEST(msgbus_test, publish_sync_does_not_wait_for_an_executor_bound_delivery) {
    // Real threads: only the calling thread's handlers are complete when publish_sync returns.
    msgbus bus;
    registrations armed;
    gate g;
    ASSERT_TRUE(bus.add_executor(7));
    std::atomic<bool> ran { false };
    armed.add(
        bus, "a/b",
        [&](const message&) {
            g.hold();
            ran = true;
        },
        dispatch_options { 7 }
    );

    bus.publish_sync("a/b", std::any { 1 });
    EXPECT_FALSE(ran.load()) << "the delivery was handed over, not finished";

    g.open();
    EXPECT_TRUE(bus.executors().find(7)->drain());
    EXPECT_TRUE(ran.load());
}

TEST(msgbus_test, a_subscription_naming_an_unregistered_executor_is_dropped_and_reported) {
    // The handler is not run inline: putting it on a thread dispatch_options did not name would be a
    // worse lie than a lost delivery.
    failure_log log;
    msgbus bus(log.sink());
    registrations armed;
    recorder received;
    armed.add(bus, "a/b", received.callback(), dispatch_options { 42 });

    bus.publish_sync("a/b", std::any { 1 });

    EXPECT_EQ(received.count(), 0u);
    EXPECT_EQ(bus.dropped(), 1u);
    EXPECT_EQ(log.count(), 1u);
    EXPECT_THROW(log.rethrow_first(), std::runtime_error);
}

TEST(msgbus_test, removing_an_executor_makes_its_subscriptions_undeliverable) {
    msgbus bus;
    registrations armed;
    ASSERT_TRUE(bus.add_executor(7));
    recorder received;
    armed.add(bus, "a/b", received.callback(), dispatch_options { 7 });
    ASSERT_TRUE(bus.remove_executor(7));

    bus.publish_sync("a/b", std::any { 1 });

    EXPECT_EQ(received.count(), 0u);
    EXPECT_EQ(bus.dropped(), 1u);
}

TEST(msgbus_test, one_topic_can_reach_an_inline_and_an_executor_bound_subscriber_together) {
    msgbus bus;
    registrations armed;
    ASSERT_TRUE(bus.add_executor(7));
    recorder inline_sub;
    recorder executor_sub;
    recorder worker_sub;
    armed.add(bus, "a/b", inline_sub.callback("inline"));
    armed.add(bus, "a/b", executor_sub.callback("executor"), dispatch_options { 7 });
    armed.add(bus, "a/b", worker_sub.callback("worker"), dispatch_options { msgbus::ROUTER });

    bus.publish_sync("a/b", std::any { 1 });

    EXPECT_TRUE(inline_sub.received_only("inline"));
    bus.publish_sync("a/b", std::any { 2 });
    EXPECT_TRUE(bus.executors().find(7)->drain());
    EXPECT_TRUE(bus.flush());

    EXPECT_EQ(executor_sub.count(), 2u);
    EXPECT_EQ(worker_sub.count(), 2u);
}

// --- exception policy ---------------------------------------------------------------------------------

TEST(msgbus_test, a_synchronous_failure_with_no_error_handler_reaches_the_publisher) {
    msgbus bus;
    registrations armed;
    armed.add(bus, "boom", [](const message&) { throw std::runtime_error("bang"); });

    EXPECT_THROW(bus.publish_sync("boom", std::any { 1 }), std::runtime_error);
}

TEST(msgbus_test, a_synchronous_failure_with_an_error_handler_is_consumed_there) {
    msgbus bus;
    registrations armed;
    std::mutex mutex;
    std::size_t count = 0;
    armed.add(
        bus, "boom", [](const message&) { throw std::runtime_error("bang"); }, {},
        [&](const message& msg, std::exception_ptr cause) {
            const std::scoped_lock guard(mutex);
            ++count;
            EXPECT_EQ(msg.topic_, "boom");
            EXPECT_TRUE(cause != nullptr);
        }
    );

    EXPECT_NO_THROW(bus.publish_sync("boom", std::any { 1 }));
    EXPECT_NO_THROW(bus.publish_sync("boom", std::any { 2 }));

    const std::scoped_lock guard(mutex);
    EXPECT_EQ(count, 2u);
    EXPECT_EQ(bus.routed(), 2u) << "an handled failure does not stop the message being routed";
}

TEST(msgbus_test, a_synchronous_failure_does_not_skip_the_other_subscribers) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/b", [](const message&) { throw std::runtime_error("bang"); });
    armed.add(bus, "a/b", received.callback());

    EXPECT_THROW(bus.publish_sync("a/b", std::any { 1 }), std::runtime_error);

    EXPECT_TRUE(received.received_only("a/b")) << "one throwing subscriber must not cost its siblings a delivery";
}

TEST(msgbus_test, an_asynchronous_failure_with_no_error_handler_reaches_the_error_sink) {
    // There is no caller left by the time the worker runs the handler, so the bus's own channel takes it.
    failure_log log;
    msgbus bus(log.sink());
    registrations armed;
    armed.add(
        bus, "boom", [](const message&) { throw std::runtime_error("bang"); }, dispatch_options { msgbus::ROUTER }
    );

    EXPECT_EQ(bus.publish_async("boom", std::any { 1 }), publish_status::ACCEPTED);
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));

    EXPECT_EQ(log.count(), 1u);
    EXPECT_THROW(log.rethrow_first(), std::runtime_error);
}

TEST(msgbus_test, an_asynchronous_failure_on_an_executor_thread_reaches_the_error_sink) {
    failure_log log;
    msgbus bus(log.sink());
    registrations armed;
    ASSERT_TRUE(bus.add_executor(7));
    armed.add(bus, "boom", [](const message&) { throw std::runtime_error("bang"); }, dispatch_options { 7 });

    EXPECT_EQ(bus.publish_async("boom", std::any { 1 }), publish_status::ACCEPTED);
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));
    EXPECT_TRUE(bus.executors().find(7)->drain());

    EXPECT_EQ(log.count(), 1u);
}

TEST(msgbus_test, an_executor_bound_failure_goes_to_its_own_error_handler_not_the_sink) {
    failure_log log;
    msgbus bus(log.sink());
    registrations armed;
    ASSERT_TRUE(bus.add_executor(7));
    std::atomic<int> handled { 0 };
    armed.add(
        bus, "boom", [](const message&) { throw std::runtime_error("bang"); }, dispatch_options { 7 },
        [&](const message&, std::exception_ptr) { ++handled; }
    );

    bus.publish_sync("boom", std::any { 1 });
    EXPECT_TRUE(bus.executors().find(7)->drain());
    EXPECT_TRUE(bus.flush());

    EXPECT_EQ(handled.load(), 1);
    EXPECT_EQ(log.count(), 0u) << "the subscriber's own channel is the nearer one";
}

TEST(msgbus_test, an_error_sink_that_throws_does_not_take_the_worker_down) {
    // Real threads: the sink is user code called from a worker; a throwing sink would otherwise unwind
    // into a thread function and terminate the process.
    msgbus bus([](std::exception_ptr) { throw std::logic_error("sink failed"); });
    registrations armed;
    armed.add(
        bus, "boom", [](const message&) { throw std::runtime_error("bang"); }, dispatch_options { msgbus::ROUTER }
    );
    recorder received;
    armed.add(bus, "a/b", received.callback(), dispatch_options { msgbus::ROUTER });

    EXPECT_EQ(bus.publish_async("boom", std::any { 1 }), publish_status::ACCEPTED);
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED) << "the worker survived";
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));
    EXPECT_EQ(received.count(), 1u);
}

TEST(msgbus_test, the_error_sink_is_isolated_from_a_failure_it_cannot_handle) {
    failure_log log;
    msgbus bus(log.sink());
    registrations armed;
    ASSERT_TRUE(bus.add_executor(7));
    armed.add(bus, "boom", [](const message&) { throw std::runtime_error("bang"); }, dispatch_options { 7 });
    armed.add(bus, "unmapped", [](const message&) {}, dispatch_options { 42 });

    bus.publish_sync("unmapped", std::any { 1 });
    EXPECT_EQ(bus.dropped(), 1u) << "an unregistered executor is reported, not thrown";

    EXPECT_EQ(bus.publish_async("boom", std::any { 1 }), publish_status::ACCEPTED);
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));
    EXPECT_TRUE(bus.executors().find(7)->drain());
    EXPECT_EQ(log.count(), 2u);
}

// --- liveness and counters ----------------------------------------------------------------------------

TEST(msgbus_test, routed_counts_every_message_the_worker_finished) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/#", received.callback(), dispatch_options { msgbus::ROUTER });

    for (int i = 0; i < 100; ++i) { EXPECT_EQ(bus.publish_async("a/b", std::any { i }), publish_status::ACCEPTED); }
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 5000 }));

    EXPECT_EQ(received.count(), 100u);
    EXPECT_EQ(bus.routed(), 100u);
    const executor_stats stats = bus.worker_stats();
    EXPECT_EQ(stats.submitted_, 100u);
    EXPECT_EQ(stats.completed_, 100u);
    EXPECT_EQ(stats.failed_, 0u);
    EXPECT_EQ(stats.dropped_, 0u);
}

TEST(msgbus_test, routed_counts_a_synchronous_publish_that_nothing_matched) {
    msgbus bus;

    bus.publish_sync("a/b", std::any { 1 });
    bus.publish_sync("a/c", std::any { 2 });

    EXPECT_EQ(bus.routed(), 2u);
    EXPECT_EQ(bus.dropped(), 0u);
}

TEST(msgbus_test, subscription_count_and_empty_track_registration) {
    msgbus bus;
    registrations armed;
    recorder received;

    ASSERT_TRUE(bus.empty());
    const subscription_id id = armed.add(bus, "a/b", received.callback());
    EXPECT_EQ(bus.subscription_count(), 1u);
    EXPECT_FALSE(bus.empty());

    EXPECT_TRUE(bus.unsubscribe(id));
    EXPECT_EQ(bus.subscription_count(), 0u);
    EXPECT_TRUE(bus.empty());

    EXPECT_FALSE(bus.unsubscribe(id)) << "cancelling twice reports the second as a miss";
    EXPECT_FALSE(bus.unsubscribe(12345)) << "an unknown id is a miss too";
}

TEST(msgbus_test, an_unsubscribed_id_still_holds_a_second_pattern_of_its_own) {
    msgbus bus;
    registrations armed;
    recorder received;
    const subscription_id id = armed.add(bus, "a/#", received.callback());

    bus.publish_sync("a/b", std::any { 1 });
    EXPECT_TRUE(bus.unsubscribe(id));
    bus.publish_sync("a/b", std::any { 2 });

    EXPECT_EQ(received.count(), 1u);
    EXPECT_EQ(bus.routed(), 2u);
}

TEST(msgbus_test, a_cancelled_subscription_does_not_see_a_message_queued_before_the_cancellation) {
    // Real threads: the routing worker is gated on an earlier message, so the target message is still in
    // the queue when the subscription is cancelled — and resolution happens when the worker reaches it.
    msgbus bus;
    registrations armed;
    gate g;
    armed.add(bus, "gate", [&g](const message&) { g.hold(); }, dispatch_options { msgbus::ROUTER });
    recorder received;
    const subscription_id id = armed.add(bus, "a/b", received.callback(), dispatch_options { msgbus::ROUTER });

    EXPECT_EQ(bus.publish_async("gate", std::any { 1 }), publish_status::ACCEPTED);
    g.wait_entered();
    EXPECT_EQ(bus.publish_async("a/b", std::any { 2 }), publish_status::ACCEPTED);
    EXPECT_TRUE(bus.unsubscribe(id));

    g.open();
    EXPECT_TRUE(bus.flush());

    EXPECT_EQ(received.count(), 0u) << "a subscription cancelled before the worker resolved it sees nothing";
}

TEST(msgbus_test, a_handler_may_subscribe_from_within_a_delivery) {
    // The router's lock is released before any handler runs, so this cannot deadlock.
    msgbus bus;
    registrations armed;
    recorder late;
    armed.add(bus, "a/b", [&](const message&) { sink(armed.add(bus, "a/c", late.callback())); });

    bus.publish_sync("a/b", std::any { 1 });
    EXPECT_EQ(late.count(), 0u);
    bus.publish_sync("a/c", std::any { 2 });

    EXPECT_EQ(late.count(), 1u);
    EXPECT_EQ(bus.subscription_count(), 2u);
}

TEST(msgbus_test, a_handler_may_unsubscribe_itself_from_within_a_delivery) {
    msgbus bus;
    registrations armed;
    recorder received;
    subscription_id id = 0;
    id = armed.add(bus, "a/b", [&](const message& msg) {
        received.callback()(msg);
        EXPECT_TRUE(bus.unsubscribe(id));
    });

    bus.publish_sync("a/b", std::any { 1 });
    bus.publish_sync("a/b", std::any { 2 });

    EXPECT_EQ(received.count(), 1u);
    EXPECT_TRUE(bus.empty());
}

// --- release and lifetime -----------------------------------------------------------------------------

TEST(msgbus_test, release_is_idempotent) {
    msgbus bus;

    bus.release();
    bus.release();

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::REJECTED);
}

TEST(msgbus_test, release_routes_every_message_accepted_before_it) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/b", received.callback(), dispatch_options { msgbus::ROUTER });

    for (int i = 0; i < 32; ++i) { EXPECT_EQ(bus.publish_async("a/b", std::any { i }), publish_status::ACCEPTED); }

    bus.release();

    EXPECT_EQ(received.count(), 32u) << "an ACCEPTED publish was a promise; release must keep it";
    EXPECT_EQ(bus.routed(), 32u);
}

TEST(msgbus_test, release_waits_for_a_delivery_already_handed_to_another_executor) {
    msgbus bus;
    registrations armed;
    gate g;
    ASSERT_TRUE(bus.add_executor(7));
    std::atomic<bool> ran { false };
    armed.add(
        bus, "a/b",
        [&](const message&) {
            g.hold();
            ran = true;
        },
        dispatch_options { 7 }
    );

    EXPECT_EQ(bus.publish_async("a/b", std::any { 1 }), publish_status::ACCEPTED);
    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 2000 }));

    std::thread reopener([&g] { g.open(); });
    bus.release();
    reopener.join();

    EXPECT_TRUE(ran.load()) << "release joins every executor, not just the routing worker";
}

// The destructor's own drain cannot be observed on its own here. `release()` routes what was
// accepted to the subscriptions that are live when it runs, and a registration is live only while
// its handle is — `~subscription` cancels it, and `subscription`'s contract forbids a handle
// outliving its owner. A handle declared in this scope is therefore already gone by the time
// `~msgbus` runs, so the barrier has to be taken while the handle is still alive. What this case
// pins down is the other half: the destructor that follows a `release()` neither routes a message
// a second time nor loses one.
TEST(msgbus_test, the_destructor_after_a_release_routes_nothing_a_second_time) {
    std::atomic<int> ran { 0 };
    {
        msgbus bus;
        registrations armed;
        armed.add(bus, "a/b", [&ran](const message&) { ++ran; }, dispatch_options { msgbus::ROUTER });
        for (int i = 0; i < 16; ++i) { EXPECT_EQ(bus.publish_async("a/b", std::any { i }), publish_status::ACCEPTED); }
        bus.release();
    }

    EXPECT_EQ(ran.load(), 16) << "each accepted publish reaches the handler exactly once";
}

TEST(msgbus_test, a_bus_is_neither_copyable_nor_movable) {
    static_assert(!std::is_copy_constructible_v<msgbus>);
    static_assert(!std::is_move_constructible_v<msgbus>);
    static_assert(!std::is_copy_assignable_v<msgbus>);
    static_assert(!std::is_move_assignable_v<msgbus>);
    static_assert(std::is_base_of_v<msgbus_base, msgbus>);
}

// --- concurrency --------------------------------------------------------------------------------------

TEST(msgbus_test, concurrent_synchronous_publishers_are_all_routed) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/b", received.callback());
    constexpr int THREADS = 4;
    constexpr int PER_THREAD = 250;

    std::vector<std::thread> publishers;
    publishers.reserve(THREADS);
    for (int t = 0; t < THREADS; ++t) {
        publishers.emplace_back([&bus] {
            for (int i = 0; i < PER_THREAD; ++i) { bus.publish_sync("a/b", std::any { i }); }
        });
    }
    for (std::thread& publisher : publishers) { publisher.join(); }

    EXPECT_EQ(received.count(), static_cast<std::size_t>(THREADS) * PER_THREAD);
    EXPECT_EQ(bus.routed(), static_cast<std::uint64_t>(THREADS) * PER_THREAD);
}

TEST(msgbus_test, concurrent_asynchronous_publishers_are_all_routed) {
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/b", received.callback(), dispatch_options { msgbus::ROUTER });
    constexpr int THREADS = 4;
    constexpr int PER_THREAD = 250;

    std::vector<std::thread> publishers;
    publishers.reserve(THREADS);
    for (int t = 0; t < THREADS; ++t) {
        publishers.emplace_back([&bus] {
            for (int i = 0; i < PER_THREAD; ++i) { sink(bus.publish_async("a/b", std::any { i })); }
        });
    }
    for (std::thread& publisher : publishers) { publisher.join(); }

    EXPECT_TRUE(bus.flush(std::chrono::milliseconds { 5000 }));
    EXPECT_EQ(received.count(), static_cast<std::size_t>(THREADS) * PER_THREAD);
    EXPECT_EQ(bus.worker_stats().completed_, static_cast<std::uint64_t>(THREADS) * PER_THREAD);
}

TEST(msgbus_test, a_publisher_racing_a_subscription_never_sees_a_half_registered_one) {
    // Real threads: one thread publishes while another registers and cancels, so every delivery either
    // happens on a live subscription or not at all — never on a callback being torn down.
    msgbus bus;
    std::atomic<int> delivered { 0 };
    std::atomic<bool> stop { false };
    constexpr int ROUNDS = 500;

    std::thread publisher([&bus, &delivered, &stop] {
        for (int i = 0; i < ROUNDS; ++i) { bus.publish_sync("a/b", std::any { i }); }
        stop = true;
    });

    std::thread registrar([&bus, &delivered, &stop] {
        while (!stop.load()) {
            auto id = bus.subscribe("a/b", [&delivered](const message&) { ++delivered; });
            if (!id) { continue; }
            // Cancelled through the handle rather than through a second bus call: the id belongs to
            // the registration, and the handle is what owns it.
            sink(id->cancel());
        }
    });

    publisher.join();
    registrar.join();

    EXPECT_EQ(bus.routed(), ROUNDS);
    EXPECT_GE(delivered.load(), 0);
    EXPECT_LE(delivered.load(), ROUNDS);
}

TEST(msgbus_test, a_publisher_racing_release_is_either_accepted_or_rejected) {
    // Real threads: no asynchronous publish may be lost — an ACCEPTED one must be routed, since release
    // drains before it returns.
    msgbus bus;
    registrations armed;
    recorder received;
    armed.add(bus, "a/b", received.callback(), dispatch_options { msgbus::ROUTER });
    constexpr int ROUNDS = 400;
    std::atomic<int> accepted { 0 };
    std::atomic<int> rejected { 0 };

    std::thread publisher([&] {
        for (int i = 0; i < ROUNDS; ++i) {
            switch (bus.publish_async("a/b", std::any { i })) {
            case publish_status::ACCEPTED: ++accepted; break;
            case publish_status::REJECTED: ++rejected; break;
            }
        }
    });
    std::thread reaper([&bus] { bus.release(); });

    publisher.join();
    reaper.join();
    bus.release();

    EXPECT_EQ(accepted.load() + rejected.load(), ROUNDS);
    EXPECT_EQ(received.count(), static_cast<std::size_t>(accepted.load()));
    EXPECT_EQ(bus.routed(), static_cast<std::uint64_t>(accepted.load()));
}

TEST(msgbus_test, concurrent_subscribe_and_unsubscribe_keep_the_count_coherent) {
    msgbus bus;
    constexpr int THREADS = 4;
    constexpr int PER_THREAD = 200;
    std::atomic<bool> failed { false };

    std::vector<std::thread> workers;
    workers.reserve(THREADS);
    for (int t = 0; t < THREADS; ++t) {
        workers.emplace_back([&bus, &failed] {
            for (int i = 0; i < PER_THREAD; ++i) {
                auto id = bus.subscribe("a/b", [](const message&) {});
                if (!id) {
                    failed = true;
                    continue;
                }
                // The only per-thread claim that holds here. `subscribe` issued this handle to this
                // thread, so no other thread can be holding it, and a `false` means the registration
                // was already gone -- the defect this test exists to catch. Nothing bus-wide is
                // asserted inside the loop: `subscription_count()` is a snapshot of all four
                // threads' work, and the other three are between their own subscribe and their
                // own cancel for nearly the whole run, so neither `!= 0` nor `== 0` is a
                // statement this thread can make about them. The bus-wide invariant is the one
                // that needs every thread stopped, so it is asserted once, after the join.
                if (!id->cancel()) { failed = true; }
            }
        });
    }
    for (std::thread& worker : workers) { worker.join(); }

    EXPECT_FALSE(failed.load());
    EXPECT_EQ(bus.subscription_count(), 0u);
    EXPECT_TRUE(bus.empty());
}

} // namespace

} // namespace ba7lya::msgbus
