///
/// @file executor_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for the executor: FIFO delivery, the three back-pressure policies under a full
/// queue, re-entrant inline submits, the exception sink, and the shutdown and drain contracts.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026
///
// SPDX-License-Identifier: MIT

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <vector>

#include "common.hxx"
#include "executor.hxx"

namespace ba7lya::msgbus {

namespace {

static_assert(!std::is_copy_constructible_v<executor>);
static_assert(!std::is_move_constructible_v<executor>);
static_assert(!std::is_copy_assignable_v<executor>);
static_assert(!std::is_move_assignable_v<executor>);

/// @brief Swallows a `[[nodiscard]]` result a case does not assert on.
void sink(const auto&... unused) { static_cast<void>(sizeof...(unused)); }

///
/// @brief A rendezvous a work item holds on until the test lets it go, and which tells the test when
/// the item has arrived. It replaces every `sleep` a timing-based test would otherwise need: the test
/// waits for `wait_entered()` — proof the worker is inside the item — instead of guessing how long
/// the worker takes to get there.
///
class gate {
public:
    /// @brief Called from a work item: announces arrival, then blocks until `open()`.
    void hold() {
        std::unique_lock lock(mutex_);
        entered_ = true;
        cv_.notify_all();
        cv_.wait(lock, [this] { return open_; });
    }

    /// @brief Called from the test: blocks until some item has entered `hold()`.
    void wait_entered() {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return entered_; });
    }

    /// @brief Releases every item that is holding, and every later one.
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

// --- construction ------------------------------------------------------------------------------------

TEST(executor_test, the_worker_thread_starts_with_the_executor) {
    const executor exec;

    EXPECT_TRUE(exec.accepting());
    EXPECT_TRUE(exec.is_running());
    EXPECT_FALSE(exec.is_on_worker_thread());
    EXPECT_EQ(exec.pending(), 0u);
    EXPECT_EQ(exec.stats().submitted_, 0u);
}

TEST(executor_test, capacity_reports_the_bound_it_was_built_with) {
    const executor bounded(7);
    const executor unbounded;

    EXPECT_EQ(bounded.capacity(), 7u);
    EXPECT_EQ(unbounded.capacity(), executor::UNBOUNDED);
}

// --- submit ------------------------------------------------------------------------------------------

TEST(executor_test, submit_runs_the_item_on_the_worker_and_is_observable_through_stats) {
    executor exec;
    std::atomic<bool> ran { false };

    EXPECT_EQ(exec.submit([&ran] { ran = true; }), submit_status::ACCEPTED);
    EXPECT_TRUE(exec.drain());

    EXPECT_TRUE(ran.load());
    EXPECT_EQ(exec.pending(), 0u);
    const executor_stats stats = exec.stats();
    EXPECT_EQ(stats.submitted_, 1u);
    EXPECT_EQ(stats.completed_, 1u);
    EXPECT_EQ(stats.failed_, 0u);
}

TEST(executor_test, submit_rejects_an_empty_item_without_touching_the_queue) {
    executor exec;

    EXPECT_THROW(sink(exec.submit(work_item {})), std::invalid_argument);

    EXPECT_EQ(exec.pending(), 0u);
    const executor_stats stats = exec.stats();
    EXPECT_EQ(stats.submitted_, 0u);
    EXPECT_EQ(stats.rejected_, 0u);
    EXPECT_EQ(stats.completed_, 0u);
    EXPECT_EQ(stats.failed_, 0u);
}

TEST(executor_test, items_run_in_submission_order) {
    executor exec;
    std::mutex mutex;
    std::vector<int> order;

    for (int tag = 0; tag < 3; ++tag) {
        EXPECT_EQ(
            exec.submit([&mutex, &order, tag] {
                const std::scoped_lock guard(mutex);
                order.push_back(tag);
            }),
            submit_status::ACCEPTED
        );
    }
    EXPECT_TRUE(exec.drain());

    const std::scoped_lock guard(mutex);
    EXPECT_EQ(order, (std::vector<int> { 0, 1, 2 }));
}

TEST(executor_test, an_unbounded_executor_never_applies_a_policy) {
    executor exec;

    EXPECT_EQ(exec.capacity(), executor::UNBOUNDED);
    EXPECT_EQ(exec.submit([] {}, back_pressure::BLOCK), submit_status::ACCEPTED);
    EXPECT_EQ(exec.submit([] {}, back_pressure::DROP_OLDEST), submit_status::ACCEPTED);
    EXPECT_EQ(exec.submit([] {}, back_pressure::DROP_NEW), submit_status::ACCEPTED);
    EXPECT_TRUE(exec.drain());

    const executor_stats stats = exec.stats();
    EXPECT_EQ(stats.submitted_, 3u);
    EXPECT_EQ(stats.completed_, 3u);
    EXPECT_EQ(stats.dropped_, 0u);
    EXPECT_EQ(stats.rejected_, 0u);
}

TEST(executor_test, a_policy_is_ignored_while_the_queue_has_room) {
    // Real threads: the worker is gated inside an item, so the queue is empty and has room.
    executor exec(2);
    gate g;

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();

    EXPECT_EQ(exec.submit([] {}, back_pressure::DROP_NEW), submit_status::ACCEPTED) << "the queue has room";
    EXPECT_EQ(exec.pending(), 1u);

    g.open();
    EXPECT_TRUE(exec.drain());
    EXPECT_EQ(exec.stats().rejected_, 0u);
}

// --- back pressure: REJECT (DROP_NEW) under a full queue -----------------------------------------------

TEST(executor_test, a_reject_policy_refuses_when_full_and_drops_nothing) {
    // Real threads: the gated worker holds the only slot's counterpart, so the queue stays full.
    executor exec(1);
    gate g;

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();
    EXPECT_EQ(exec.submit([] {}), submit_status::ACCEPTED); // fills the queue

    EXPECT_EQ(exec.submit([] {}, back_pressure::DROP_NEW), submit_status::REJECTED);

    const executor_stats stats = exec.stats();
    EXPECT_EQ(stats.rejected_, 1u);
    EXPECT_EQ(stats.dropped_, 0u) << "a refusal evicts nothing";
    EXPECT_EQ(exec.pending(), 1u);

    g.open();
    EXPECT_TRUE(exec.drain());
}

// --- back pressure: DROP_OLDEST under a full queue -----------------------------------------------------

TEST(executor_test, a_drop_oldest_evicts_the_longest_waiting_item) {
    // Real threads: the gated worker keeps the queue full so an eviction is forced.
    executor exec(1);
    gate g;
    std::atomic<bool> evicted_ran { false };
    std::atomic<bool> survivor_ran { false };

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();
    EXPECT_EQ(exec.submit([&evicted_ran] { evicted_ran = true; }), submit_status::ACCEPTED);

    EXPECT_EQ(exec.submit([&survivor_ran] { survivor_ran = true; }), submit_status::DROPPED_OLDEST);

    EXPECT_EQ(exec.stats().dropped_, 1u);
    EXPECT_EQ(exec.pending(), 1u);

    g.open();
    EXPECT_TRUE(exec.drain());
    EXPECT_FALSE(evicted_ran.load()) << "the evicted closure must never run";
    EXPECT_TRUE(survivor_ran.load());
}

TEST(executor_test, drop_oldest_at_capacity_one_keeps_only_the_newest) {
    // Real threads: each queued item evicts the one before it, so only the last survives.
    executor exec(1);
    gate g;
    std::atomic<int> ran { 0 };

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();

    EXPECT_EQ(exec.submit([&ran] { ran += 100; }), submit_status::ACCEPTED);      // youngest of one
    EXPECT_EQ(exec.submit([&ran] { ran += 10; }), submit_status::DROPPED_OLDEST); // evicts 100
    EXPECT_EQ(exec.submit([&ran] { ran += 1; }), submit_status::DROPPED_OLDEST);  // evicts 10

    EXPECT_EQ(exec.stats().dropped_, 2u);
    EXPECT_EQ(exec.pending(), 1u);

    g.open();
    EXPECT_TRUE(exec.drain());
    EXPECT_EQ(ran.load(), 1) << "only the newest closure survived";
    EXPECT_EQ(exec.stats().completed_, 2u) << "the gated item and the survivor";
}

// --- back pressure: BLOCK under a full queue -----------------------------------------------------------

TEST(executor_test, a_block_submit_waits_for_room_rather_than_rejecting) {
    // Real threads: the worker is gated and the queue is full, so a BLOCK submit cannot be accepted
    // until the gate opens and the worker frees a slot. Parking itself is not observable through the
    // public API; the ACCEPTED return under a full queue is the evidence that it did not reject.
    executor exec(1);
    gate g;

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();
    EXPECT_EQ(exec.submit([] {}), submit_status::ACCEPTED);

    std::atomic<submit_status> result { submit_status::REJECTED };
    std::atomic<bool> attempting { false };
    std::thread blocker([&] {
        attempting = true;
        result = exec.submit([] {}, back_pressure::BLOCK);
    });

    while (!attempting.load()) {}
    g.open();
    blocker.join();

    EXPECT_EQ(result.load(), submit_status::ACCEPTED);
    EXPECT_EQ(exec.stats().rejected_, 0u);
    EXPECT_TRUE(exec.drain());
}

TEST(executor_test, a_block_submit_is_released_by_release_returning_closed) {
    // Real threads: the worker is gated and the queue is full, so the BLOCK submitter waits. The
    // blocker opens the gate once its submit returns, which is what lets release()'s join complete.
    executor exec(1);
    gate g;

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();
    EXPECT_EQ(exec.submit([] {}), submit_status::ACCEPTED);

    std::atomic<submit_status> result { submit_status::ACCEPTED };
    std::thread blocker([&] {
        result = exec.submit([] {}, back_pressure::BLOCK);
        g.open(); // the gated worker may now finish, so release() will not wait forever
    });

    exec.release();
    blocker.join();

    EXPECT_EQ(result.load(), submit_status::CLOSED) << "the parked submit lost to shutdown and never hung";
    EXPECT_FALSE(exec.accepting());
    EXPECT_FALSE(exec.is_running());
    EXPECT_EQ(exec.stats().rejected_, 1u);
}

// --- re-entrancy -------------------------------------------------------------------------------------

TEST(executor_test, a_submit_from_the_executors_own_worker_runs_inline) {
    // Real threads: the closure runs on the worker thread and submits to its own executor. The rule is
    // uniform across all three policies — none may queue, block or reject.
    executor exec(1);
    std::atomic<int> nested_runs { 0 };
    std::atomic<bool> all_inline { true };

    EXPECT_EQ(
        exec.submit([&exec, &nested_runs, &all_inline] {
            for (const back_pressure policy :
                 { back_pressure::BLOCK, back_pressure::DROP_OLDEST, back_pressure::DROP_NEW }) {
                const submit_status status = exec.submit([&nested_runs] { ++nested_runs; }, policy);
                if (status != submit_status::ACCEPTED) { all_inline = false; }
            }
            // Each nested closure runs inline, so all three have run before this line.
            if (nested_runs.load() != 3) { all_inline = false; }
        }),
        submit_status::ACCEPTED
    );

    EXPECT_TRUE(exec.drain());

    EXPECT_TRUE(all_inline.load());
    EXPECT_EQ(nested_runs.load(), 3);
    const executor_stats stats = exec.stats();
    EXPECT_EQ(stats.submitted_, 4u) << "the outer item plus three inline ones";
    EXPECT_EQ(stats.completed_, 4u);
}

TEST(executor_test, a_reentrant_submit_is_never_queued) {
    // Real threads: the worker submits from inside an item while another item is already queued; the
    // inline closure must run before that queued one, so the ordering pins the "runs inline" rule.
    executor exec(2);
    gate g;
    std::mutex mutex;
    std::vector<int> order;
    const auto record = [&mutex, &order](int tag) {
        const std::scoped_lock guard(mutex);
        order.push_back(tag);
    };
    std::atomic<submit_status> nested { submit_status::REJECTED };

    EXPECT_EQ(
        exec.submit([&] {
            record(0);
            g.hold();
            record(1);
            nested = exec.submit([&] { record(3); });
            record(2);
        }),
        submit_status::ACCEPTED
    );
    g.wait_entered();

    EXPECT_EQ(exec.submit([&] { record(4); }), submit_status::ACCEPTED); // queued behind the gated item

    g.open();
    EXPECT_TRUE(exec.drain());

    EXPECT_EQ(nested.load(), submit_status::ACCEPTED);
    const std::scoped_lock guard(mutex);
    EXPECT_EQ(order, (std::vector<int> { 0, 1, 3, 2, 4 }));
}

TEST(executor_test, a_reentrant_submit_does_not_deadlock_under_block) {
    // Real threads: the queue is full when the worker submits to itself under BLOCK — the exact case
    // that would self-deadlock if the submit queued instead of running inline.
    executor exec(1);
    gate g;
    std::atomic<submit_status> nested { submit_status::REJECTED };
    std::atomic<bool> nested_ran { false };

    EXPECT_EQ(
        exec.submit([&] {
            g.hold();
            nested = exec.submit([&nested_ran] { nested_ran = true; }, back_pressure::BLOCK);
        }),
        submit_status::ACCEPTED
    );
    g.wait_entered();

    EXPECT_EQ(exec.submit([] {}), submit_status::ACCEPTED); // fills the only queue slot

    g.open();
    EXPECT_TRUE(exec.drain());

    EXPECT_EQ(nested.load(), submit_status::ACCEPTED);
    EXPECT_TRUE(nested_ran.load());
}

// --- drain -------------------------------------------------------------------------------------------

TEST(executor_test, drain_on_an_idle_executor_is_true_immediately) {
    executor exec;

    EXPECT_TRUE(exec.drain(std::chrono::milliseconds(0)));
    EXPECT_TRUE(exec.drain());
    EXPECT_EQ(exec.stats().completed_, 0u);
}

TEST(executor_test, drain_reports_true_only_when_queue_empty_and_nothing_in_flight) {
    // Real threads: the worker is gated mid-item, so the queue is empty but something is in flight.
    executor exec(2);
    gate g;

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();

    EXPECT_FALSE(exec.drain(std::chrono::milliseconds(0))) << "a queue-only predicate would say true";

    g.open();
    EXPECT_TRUE(exec.drain());
}

TEST(executor_test, drain_after_release_returns_true_immediately) {
    executor exec;
    exec.release();

    EXPECT_FALSE(exec.is_running());
    EXPECT_TRUE(exec.drain(std::chrono::milliseconds(0)));
    EXPECT_TRUE(exec.drain());
}

TEST(executor_test, drain_from_the_executors_own_worker_throws) {
    // Real threads: drain() runs on the worker thread, inside an item, where it cannot succeed.
    executor exec;
    std::atomic<bool> threw { false };
    std::atomic<bool> completed { false };

    EXPECT_EQ(
        exec.submit([&] {
            try {
                sink(exec.drain(std::chrono::milliseconds(0)));
            }
            catch (const std::invalid_argument&) {
                threw = true;
            }
            completed = true;
        }),
        submit_status::ACCEPTED
    );

    EXPECT_TRUE(exec.drain());
    EXPECT_TRUE(threw.load());
    EXPECT_TRUE(completed.load()) << "the throw must not abort the item";
    EXPECT_EQ(exec.stats().completed_, 1u);
}

// --- pending -----------------------------------------------------------------------------------------

TEST(executor_test, pending_reports_the_queue_size_while_the_worker_is_gated) {
    // Real threads: the gated worker holds one item in flight while four more pile up behind it.
    executor exec(4);
    gate g;

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();

    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(exec.submit([] {}), submit_status::ACCEPTED);
    }
    EXPECT_EQ(exec.pending(), 4u);

    EXPECT_EQ(exec.submit([] {}, back_pressure::DROP_NEW), submit_status::REJECTED);
    EXPECT_EQ(exec.stats().rejected_, 1u);

    g.open();
    EXPECT_TRUE(exec.drain());
    EXPECT_EQ(exec.pending(), 0u);
}

TEST(executor_test, pending_excludes_the_item_in_flight) {
    // Real threads: the only item is running on the worker thread, so it is not pending.
    executor exec(1);
    gate g;
    std::atomic<bool> on_worker { false };

    EXPECT_EQ(
        exec.submit([&g, &on_worker, &exec] {
            on_worker = exec.is_on_worker_thread();
            g.hold();
        }),
        submit_status::ACCEPTED
    );
    g.wait_entered();

    EXPECT_EQ(exec.pending(), 0u) << "the running item is in flight, not queued";
    EXPECT_TRUE(exec.is_running());
    EXPECT_TRUE(on_worker.load());

    g.open();
    EXPECT_TRUE(exec.drain());
}

TEST(executor_test, pending_never_exceeds_capacity_under_concurrent_submits) {
    // Real threads: a reader samples pending() while a submitter hammers the bounded queue.
    executor exec(3);
    constexpr int ROUNDS = 300;
    std::atomic<bool> stop { false };
    std::atomic<std::size_t> worst { 0 };

    std::thread reader([&exec, &stop, &worst] {
        while (!stop.load()) {
            const std::size_t seen = exec.pending();
            std::size_t previous = worst.load();
            while (seen > previous && !worst.compare_exchange_weak(previous, seen)) {}
        }
    });

    std::thread submitter([&exec, &stop] {
        for (int i = 0; i < ROUNDS; ++i) {
            sink(exec.submit([] {}, back_pressure::DROP_NEW));
        }
        stop = true;
    });

    submitter.join();
    reader.join();

    EXPECT_LE(worst.load(), 3u);
    EXPECT_TRUE(exec.drain());
    EXPECT_EQ(exec.pending(), 0u);
}

// --- exception isolation -------------------------------------------------------------------------------

TEST(executor_test, an_exception_escaping_an_item_goes_to_the_sink) {
    std::atomic<int> sink_calls { 0 };
    std::exception_ptr captured;
    executor exec(executor::UNBOUNDED, [&sink_calls, &captured](std::exception_ptr failure) {
        ++sink_calls;
        captured = failure;
    });

    EXPECT_EQ(exec.submit([] { throw std::runtime_error("boom"); }), submit_status::ACCEPTED);
    EXPECT_TRUE(exec.drain());

    EXPECT_EQ(sink_calls.load(), 1);
    ASSERT_NE(captured, nullptr);
    try {
        std::rethrow_exception(captured);
        ADD_FAILURE() << "the sink should have received a rethrowable exception";
    }
    catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "boom");
    }

    const executor_stats stats = exec.stats();
    EXPECT_EQ(stats.failed_, 1u);
    EXPECT_EQ(stats.completed_, 0u);
    EXPECT_EQ(stats.submitted_, 1u);
}

TEST(executor_test, a_throwing_item_does_not_stop_the_queue) {
    std::atomic<int> sink_calls { 0 };
    executor exec(executor::UNBOUNDED, [&sink_calls](std::exception_ptr) { ++sink_calls; });
    std::atomic<bool> second_ran { false };

    EXPECT_EQ(exec.submit([] { throw std::runtime_error("boom"); }), submit_status::ACCEPTED);
    EXPECT_EQ(exec.submit([&second_ran] { second_ran = true; }), submit_status::ACCEPTED);
    EXPECT_TRUE(exec.drain());

    EXPECT_TRUE(second_ran.load());
    EXPECT_EQ(sink_calls.load(), 1);
    const executor_stats stats = exec.stats();
    EXPECT_EQ(stats.failed_, 1u);
    EXPECT_EQ(stats.completed_, 1u);
}

TEST(executor_test, an_empty_sink_swallows_the_exception) {
    executor exec;
    EXPECT_EQ(exec.submit([] { throw std::runtime_error("boom"); }), submit_status::ACCEPTED);

    EXPECT_TRUE(exec.drain()) << "no std::terminate, and the barrier still holds";
    EXPECT_EQ(exec.stats().failed_, 1u);
    EXPECT_TRUE(exec.is_running()) << "the worker survived";
}

TEST(executor_test, a_sink_that_throws_is_swallowed) {
    std::atomic<int> sink_calls { 0 };
    executor exec(executor::UNBOUNDED, [&sink_calls](std::exception_ptr) {
        ++sink_calls;
        throw std::runtime_error("the sink itself failed");
    });
    std::atomic<bool> later_ran { false };

    EXPECT_EQ(exec.submit([] { throw std::runtime_error("boom"); }), submit_status::ACCEPTED);
    EXPECT_EQ(exec.submit([&later_ran] { later_ran = true; }), submit_status::ACCEPTED);

    EXPECT_TRUE(exec.drain());
    EXPECT_EQ(sink_calls.load(), 1);
    EXPECT_TRUE(later_ran.load()) << "the worker kept draining";
    const executor_stats stats = exec.stats();
    EXPECT_EQ(stats.failed_, 1u);
    EXPECT_EQ(stats.completed_, 1u);
}

TEST(executor_test, a_submitted_item_that_throws_does_not_terminate_the_process) {
    // The run() loop's try/catch is all that stands between a throwing closure and std::terminate;
    // reaching the assertions below is itself the assertion.
    executor exec(executor::UNBOUNDED, [](std::exception_ptr) {});
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(exec.submit([] { throw std::logic_error("x"); }), submit_status::ACCEPTED);
    }

    EXPECT_TRUE(exec.drain());
    EXPECT_EQ(exec.stats().failed_, 8u);
    EXPECT_TRUE(exec.is_running());
}

// --- close / release / destruction ---------------------------------------------------------------------

TEST(executor_test, release_stops_accepting_further_work) {
    executor exec;
    exec.release();

    EXPECT_EQ(exec.submit([] {}, back_pressure::BLOCK), submit_status::CLOSED);
    EXPECT_EQ(exec.submit([] {}, back_pressure::DROP_OLDEST), submit_status::CLOSED);
    EXPECT_EQ(exec.submit([] {}, back_pressure::DROP_NEW), submit_status::CLOSED);

    const executor_stats stats = exec.stats();
    EXPECT_EQ(stats.rejected_, 3u);
    EXPECT_EQ(stats.submitted_, 0u);
}

TEST(executor_test, release_is_idempotent) {
    executor exec;

    exec.release();
    exec.release();
    exec.close();

    EXPECT_FALSE(exec.accepting());
    EXPECT_FALSE(exec.is_running());
}

TEST(executor_test, release_drains_items_accepted_before_it) {
    // Real threads: release() joins the worker, so it must also wait for the gated in-flight item.
    executor exec;
    gate g;
    std::atomic<int> ran { 0 };

    EXPECT_EQ(
        exec.submit([&g, &ran] {
            g.hold();
            ++ran;
        }),
        submit_status::ACCEPTED
    );
    g.wait_entered();
    EXPECT_EQ(exec.submit([&ran] { ++ran; }), submit_status::ACCEPTED);
    EXPECT_EQ(exec.submit([&ran] { ++ran; }), submit_status::ACCEPTED);

    std::thread reaper([&exec] { exec.release(); });
    g.open();
    reaper.join();

    EXPECT_EQ(ran.load(), 3) << "every accepted item ran";
    EXPECT_EQ(exec.stats().completed_, 3u);
    EXPECT_FALSE(exec.is_running());
}

TEST(executor_test, close_stops_intake_but_the_worker_still_drains) {
    // Real threads: close() must not discard the items accepted before it.
    executor exec(4);
    gate g;
    std::atomic<int> ran { 0 };

    EXPECT_EQ(
        exec.submit([&g, &ran] {
            g.hold();
            ++ran;
        }),
        submit_status::ACCEPTED
    );
    g.wait_entered();
    EXPECT_EQ(exec.submit([&ran] { ++ran; }), submit_status::ACCEPTED);
    EXPECT_EQ(exec.submit([&ran] { ++ran; }), submit_status::ACCEPTED);

    exec.close();
    EXPECT_FALSE(exec.accepting());
    EXPECT_EQ(exec.submit([] {}), submit_status::CLOSED);

    g.open();
    exec.release();

    EXPECT_EQ(ran.load(), 3);
    EXPECT_EQ(exec.stats().completed_, 3u);
    EXPECT_FALSE(exec.is_running());
}

TEST(executor_test, close_is_callable_from_the_executors_own_worker) {
    // Real threads: a work item stops its own executor; the item queued behind it still runs.
    executor exec(4);
    gate g;
    std::atomic<int> ran { 0 };
    std::atomic<bool> closed_from_item { false };

    EXPECT_EQ(
        exec.submit([&] {
            g.hold();
            exec.close();
            closed_from_item = true;
            ++ran;
        }),
        submit_status::ACCEPTED
    );
    g.wait_entered();
    EXPECT_EQ(exec.submit([&ran] { ++ran; }), submit_status::ACCEPTED);

    g.open();
    exec.release();

    EXPECT_TRUE(closed_from_item.load());
    EXPECT_FALSE(exec.accepting());
    EXPECT_FALSE(exec.is_running());
    EXPECT_EQ(ran.load(), 2u);
}

TEST(executor_test, destruction_waits_for_work_in_flight) {
    // Real threads: the destructor runs release(), which must wait for the gated in-flight item.
    std::atomic<int> ran { 0 };
    gate g;
    std::unique_ptr<executor> exec = std::make_unique<executor>();

    EXPECT_EQ(
        exec->submit([&g, &ran] {
            g.hold();
            ++ran;
        }),
        submit_status::ACCEPTED
    );
    g.wait_entered();

    std::thread opener([&g] { g.open(); });
    exec.reset(); // ~executor() == release(): blocks until the item has run and the worker joined
    opener.join();

    EXPECT_EQ(ran.load(), 1);
}

// --- submit_deferred ----------------------------------------------------------------------------------

TEST(executor_test, deferred_builds_the_item_only_once_there_is_room_and_then_runs_it) {
    // Real threads: the gated worker holds the only slot's counterpart, so the queue stays full and the
    // build is what must wait.
    executor exec(1);
    gate g;
    std::atomic<bool> built { false };
    std::atomic<bool> ran { false };

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();
    EXPECT_EQ(exec.submit([] {}), submit_status::ACCEPTED); // fills the only queue slot

    std::atomic<submit_status> result { submit_status::CLOSED };
    std::atomic<bool> attempting { false };
    std::thread builder([&] {
        attempting = true;
        result = exec.submit_deferred(
            [&built, &ran]() -> work_item {
                built = true;
                return [&ran] { ran = true; };
            },
            back_pressure::BLOCK
        );
    });

    while (!attempting.load()) {}
    EXPECT_FALSE(built.load()) << "the closure must not be built while the submit cannot be accepted";

    g.open();
    builder.join();

    EXPECT_EQ(result.load(), submit_status::ACCEPTED);
    EXPECT_TRUE(built.load());
    EXPECT_TRUE(exec.drain());
    EXPECT_TRUE(ran.load());
    EXPECT_EQ(exec.stats().completed_, 3u) << "the gated item, the filler, and the deferred one";
}

TEST(executor_test, deferred_under_drop_new_rejects_without_building) {
    // Real threads: the queue stays full, so the refusal is the designed answer and the build never runs.
    executor exec(1);
    gate g;
    std::atomic<bool> built { false };

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();
    EXPECT_EQ(exec.submit([] {}), submit_status::ACCEPTED); // fills the only queue slot

    EXPECT_EQ(
        exec.submit_deferred(
            [&built]() -> work_item {
                built = true;
                return [] {};
            },
            back_pressure::DROP_NEW
        ),
        submit_status::REJECTED
    );

    EXPECT_FALSE(built.load()) << "a refused submit must not have paid for the closure";
    EXPECT_EQ(exec.stats().rejected_, 1u);
    EXPECT_EQ(exec.stats().dropped_, 0u);
    EXPECT_EQ(exec.pending(), 1u);

    g.open();
    EXPECT_TRUE(exec.drain());
}

TEST(executor_test, deferred_under_drop_oldest_evicts_once_the_item_is_built) {
    // Real threads: the eviction happens after the build, so the victim is still queued while the build
    // runs and only the newcomer survives to run.
    executor exec(1);
    gate g;
    std::atomic<bool> victim_ran { false };
    std::atomic<bool> survivor_ran { false };

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();
    EXPECT_EQ(exec.submit([&victim_ran] { victim_ran = true; }), submit_status::ACCEPTED);

    EXPECT_EQ(
        exec.submit_deferred([&survivor_ran]() -> work_item { return [&survivor_ran] { survivor_ran = true; }; }),
        submit_status::DROPPED_OLDEST
    );

    EXPECT_EQ(exec.stats().dropped_, 1u) << "the longest-waiting queued item was evicted";
    EXPECT_EQ(exec.pending(), 1u);

    g.open();
    EXPECT_TRUE(exec.drain());
    EXPECT_FALSE(victim_ran.load()) << "the evicted closure must never run";
    EXPECT_TRUE(survivor_ran.load());
}

TEST(executor_test, deferred_evicts_nothing_when_the_build_fails) {
    // Real threads: the eviction waits for the build precisely so that a build which throws costs nothing
    // but the submit — the item that was to replace the oldest one never arrives, so the oldest one must
    // still be there to run.
    executor exec(1);
    gate g;
    std::atomic<bool> victim_ran { false };

    EXPECT_EQ(exec.submit([&g] { g.hold(); }), submit_status::ACCEPTED);
    g.wait_entered();
    EXPECT_EQ(exec.submit([&victim_ran] { victim_ran = true; }), submit_status::ACCEPTED);

    EXPECT_THROW(
        sink(exec.submit_deferred([]() -> work_item { throw std::logic_error("build failed"); })), std::logic_error
    );

    EXPECT_EQ(exec.stats().dropped_, 0u) << "nothing was evicted for a closure that never existed";
    EXPECT_EQ(exec.pending(), 1u);

    g.open();
    EXPECT_TRUE(exec.drain());
    EXPECT_TRUE(victim_ran.load()) << "the item that stayed queued must still have run";
}

TEST(executor_test, deferred_rejects_an_empty_build_without_touching_the_queue) {
    executor exec;

    EXPECT_THROW(sink(exec.submit_deferred([]() -> work_item { return {}; })), std::invalid_argument);
    EXPECT_THROW(sink(exec.submit_deferred(work_item_deferred {})), std::invalid_argument);

    EXPECT_EQ(exec.pending(), 0u);
    const executor_stats stats = exec.stats();
    EXPECT_EQ(stats.submitted_, 0u);
    EXPECT_EQ(stats.rejected_, 0u);
    EXPECT_EQ(stats.completed_, 0u);
}

TEST(executor_test, deferred_from_the_executors_own_worker_is_rejected_without_building) {
    // Real threads: the worker defers a closure to its own full queue — the case that would deadlock if
    // the build ran and then the submit blocked for room only this worker could free.
    executor exec(1);
    gate g;
    std::atomic<submit_status> nested { submit_status::ACCEPTED };
    std::atomic<bool> built { false };
    std::atomic<bool> ran { false };

    EXPECT_EQ(
        exec.submit([&] {
            g.hold();
            nested = exec.submit_deferred(
                [&built, &ran]() -> work_item {
                    built = true;
                    return [&ran] { ran = true; };
                },
                back_pressure::BLOCK
            );
        }),
        submit_status::ACCEPTED
    );
    g.wait_entered();
    EXPECT_EQ(exec.submit([] {}), submit_status::ACCEPTED); // fills the only queue slot

    g.open();
    EXPECT_TRUE(exec.drain());

    EXPECT_EQ(nested.load(), submit_status::REJECTED) << "a deferral cannot be run inline without being built";
    EXPECT_FALSE(built.load());
    EXPECT_FALSE(ran.load());
    EXPECT_EQ(exec.stats().rejected_, 1u);
}

TEST(executor_test, deferred_is_closed_once_the_executor_is_released) {
    executor exec;
    std::atomic<bool> built { false };

    exec.release();

    EXPECT_EQ(
        exec.submit_deferred([&built]() -> work_item {
            built = true;
            return [] {};
        }),
        submit_status::CLOSED
    );

    EXPECT_FALSE(built.load()) << "a closed executor must not have paid for the closure either";
    EXPECT_EQ(exec.stats().rejected_, 1u);
}

TEST(executor_test, a_deferred_closure_runs_on_the_worker_thread) {
    executor exec;
    std::atomic<bool> on_worker { false };

    EXPECT_EQ(
        exec.submit_deferred([&exec, &on_worker]() -> work_item {
            return [&exec, &on_worker] { on_worker = exec.is_on_worker_thread(); };
        }),
        submit_status::ACCEPTED
    );

    EXPECT_TRUE(exec.drain());
    EXPECT_TRUE(on_worker.load());
}

// --- concurrency --------------------------------------------------------------------------------------

TEST(executor_test, a_submit_racing_release_is_either_queued_or_closed) {
    // Real threads: one thread submits, another releases; no submit may be lost or double-counted, and
    // every accepted item must run because release() drains before it returns.
    executor exec;
    constexpr int ROUNDS = 200;
    std::atomic<int> accepted { 0 };
    std::atomic<int> closed { 0 };
    std::atomic<int> unexpected { 0 };

    std::thread submitter([&exec, &accepted, &closed, &unexpected] {
        for (int i = 0; i < ROUNDS; ++i) {
            switch (exec.submit([] {})) {
            case submit_status::ACCEPTED: ++accepted; break;
            case submit_status::CLOSED:   ++closed; break;
            default:                      ++unexpected; break;
            }
        }
    });
    std::thread reaper([&exec] { exec.release(); });

    submitter.join();
    reaper.join();
    exec.release();

    EXPECT_EQ(unexpected.load(), 0);
    EXPECT_EQ(accepted.load() + closed.load(), ROUNDS);
    EXPECT_EQ(exec.stats().submitted_, static_cast<std::uint64_t>(accepted.load()));
    EXPECT_EQ(exec.stats().completed_, static_cast<std::uint64_t>(accepted.load()));
}

TEST(executor_test, concurrent_submits_are_all_accounted_for) {
    // Real threads: several submitters against one unbounded executor.
    executor exec;
    constexpr int THREADS = 4;
    constexpr int PER_THREAD = 100;
    constexpr std::uint64_t TOTAL = static_cast<std::uint64_t>(THREADS) * static_cast<std::uint64_t>(PER_THREAD);

    std::vector<std::thread> submitters;
    submitters.reserve(THREADS);
    for (int t = 0; t < THREADS; ++t) {
        submitters.emplace_back([&exec] {
            for (int i = 0; i < PER_THREAD; ++i) {
                sink(exec.submit([] {}));
            }
        });
    }
    for (std::thread& submitter : submitters) { submitter.join(); }

    EXPECT_TRUE(exec.drain());
    const executor_stats stats = exec.stats();
    EXPECT_EQ(stats.submitted_, TOTAL);
    EXPECT_EQ(stats.completed_, TOTAL);
    EXPECT_EQ(stats.dropped_, 0u);
    EXPECT_EQ(stats.rejected_, 0u);
}

} // namespace

} // namespace ba7lya::msgbus
