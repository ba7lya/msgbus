///
/// @file sync_and_async.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The two publish entry points side by side: which thread a handler runs on, and who owns a
/// handler's unhandled failure.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT
//
// `publish_sync` routes a message on the calling thread and returns; `publish_async` hands it to the
// bus's routing worker. Everything after the topic is parsed is shared between them, so the two entry
// points differ in exactly two things, and this example makes both of them visible.
//
// The first is which thread a handler runs on. A subscription that names no executor -- the default --
// runs on the thread that routed the message: the publisher's for one entry point, the routing
// worker's for the other. One subscription, one `dispatch_options`, two threads, and the whole
// difference is which method the publisher called.
//
// The second is who owns a handler's unhandled failure. A synchronous publish still has a caller when
// the handler throws, so the throw travels back out of `publish_sync` and into that caller's `catch`.
// An asynchronous one returned long before the handler ran, so the throw has nowhere left to go and
// the bus's own `on_exception` sink takes it instead. A subscriber that supplies its own
// `error_handler` keeps the failure on either path.
//
// The middle of the example parks the routing worker inside a handler, queues a backlog behind it, and
// publishes one message synchronously -- which does not join that queue, because that queue is not the
// publisher's to route through. The order this produces is a property of the design rather than of how
// the scheduler felt on the day, and the gate is what makes it one.
//
// Every subscription here names no executor, and that is load-bearing rather than lazy. An inline
// subscription is what makes `flush` a complete barrier: the routing worker runs the handler inside
// the very work item it is executing, so the worker is not idle until the handler has returned, and a
// `flush` that returned is a flush whose deliveries are done. A subscription naming an executor would
// hand its handler to that executor and give the guarantee up -- `flush` promises "handed over", not
// "finished".

#include <algorithm>
#include <any>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <mutex>
#include <semaphore>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common.hxx"
#include "error.hxx"
#include "logger.hxx"
#include "msgbus.hxx"
#include "subscription.hxx"

namespace {

using namespace ba7lya::msgbus;

/// @brief One topic per subscription, so no two patterns here overlap.
///
/// Overlap would be legal and useless: `resolve` returns one delivery per matching subscription, so a
/// reading reachable by two readers would make every count below a question about the matcher rather
/// than about the entry points. Four topics, four subscriptions, one delivery each.
constexpr topic topic_readings = "demo/readings";
constexpr topic topic_gate = "demo/gate";
constexpr topic topic_throwing = "demo/throwing";
constexpr topic topic_caught = "demo/caught";

/// @brief The asynchronous backlog, queued behind the gated message and routed in this order.
constexpr std::uint64_t backlog[] { 100U, 101U, 102U };

/// @brief The synchronous reading, published while the worker is parked and delivered before any of
/// the backlog. It is last in the source and first in the ledger, which is the whole point.
constexpr std::uint64_t cutting_in = 200U;

/// @brief What the reader was handed, and on which thread, in the order it was handed over.
///
/// The thread id is the evidence rather than the claim: the same subscription has to be reached on the
/// publisher's thread by one entry point and on the routing worker's by the other, and this is what
/// makes that checkable instead of assertable.
///
/// Locked rather than trusting in the schedule. Some of the reads below happen while the routing
/// worker is parked and could not be writing, but the reader is also reached on the worker by every
/// asynchronous publish, and a container read without a lock on the strength of "it is parked right
/// now" is exactly the reasoning that stops being true when someone edits the example.
struct ledger {
    /// @brief Records one delivery and the thread that made it.
    void record(std::uint64_t reading) {
        const std::lock_guard lock(mutex_);
        order_.push_back(reading);
        threads_.push_back(std::this_thread::get_id());
    }

    /// @brief The readings recorded so far, copied out under the lock.
    [[nodiscard]]
    std::vector<std::uint64_t> order() const {
        const std::lock_guard lock(mutex_);
        return order_;
    }

    /// @brief The thread each reading was recorded on, copied out under the lock.
    [[nodiscard]]
    std::vector<std::thread::id> threads() const {
        const std::lock_guard lock(mutex_);
        return threads_;
    }

    /// @brief How many readings have been recorded.
    [[nodiscard]]
    std::size_t size() const {
        const std::lock_guard lock(mutex_);
        return order_.size();
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::uint64_t> order_;
    std::vector<std::thread::id> threads_;
};

/// @brief Opens a gate when it goes out of scope, and only then.
///
/// The ordering section parks the routing worker inside a handler and opens the gate at its end.
/// Between those two points nothing may return early: a `return 1` there would unwind into the bus's
/// destructor with the worker still waiting on a semaphore nobody is going to post, and the program
/// would hang where the intent was to fail. This guard is what makes an early return safe, which is
/// the only reason it exists.
///
/// There is deliberately no `open()`: the gate is opened by the scope ending, so there is exactly one
/// place it can happen and no way to do it twice.
struct gate_latch {
    explicit gate_latch(std::binary_semaphore& gate) noexcept
        : gate_(&gate) {}

    gate_latch(const gate_latch&) = delete;
    gate_latch& operator=(const gate_latch&) = delete;
    gate_latch(gate_latch&&) = delete;
    gate_latch& operator=(gate_latch&&) = delete;

    ~gate_latch() { gate_->release(); }

private:
    std::binary_semaphore* gate_;
};

/// @brief A recorded order as `[200, 100, 101, 102]`, for one line of output.
[[nodiscard]]
std::string render_order(const std::vector<std::uint64_t>& readings) {
    std::string text = "[";
    for (std::size_t i = 0; i < readings.size(); ++i) {
        if (i != 0) { text += ", "; }
        text += std::to_string(readings[i]);
    }
    return text + ']';
}

/// @brief One pass of the ordering sequence on a bus the caller owns, recording into `reader`.
///
/// The steps `main` narrates below, run as a unit: park the routing worker inside a handler, queue the
/// backlog behind it, publish the synchronous reading, open the gate, wait for the routing queue. It
/// is the same sequence written twice on purpose -- the narration is the example's subject and has to
/// be readable one step at a time, and a helper that hid those steps would leave the verify block
/// checking something other than what was printed.
///
/// @return false if the pass could not complete: a rejected subscription, a refused publish, or a
/// routing queue that did not drain within the deadline
[[nodiscard]]
bool run_sequence(msgbus& bus, ledger& reader) {
    std::binary_semaphore entered { 0 };
    std::binary_semaphore gate { 0 };

    const auto gated = bus.subscribe(topic_gate, [&entered, &gate](const message&) {
        entered.release();
        gate.acquire();
    });
    const auto readings = bus.subscribe(topic_readings, [&reader](const message& msg) {
        reader.record(std::any_cast<std::uint64_t>(msg.payload_));
    });
    // Nothing is parked yet, so returning here leaves no one waiting.
    if (!gated || !readings) { return false; }

    auto queued = true;
    {
        const gate_latch open_at_scope_end { gate };

        queued = bus.publish_async(topic_gate, std::any { std::uint64_t { 0 } }) == publish_status::ACCEPTED;
        if (queued) { entered.acquire(); }

        for (const auto reading : backlog) {
            if (!queued) { break; }
            queued = bus.publish_async(topic_readings, std::any { reading }) == publish_status::ACCEPTED;
        }
        if (queued) { bus.publish_sync(topic_readings, std::any { cutting_in }); }
    }

    // The flush comes **before** `entered`, `gate` and the two handles go out of scope, and that order
    // is what makes the pass safe. The handles have to stay armed until the queue is empty -- the
    // backlog was queued for `readings`, and a subscription is resolved when the worker reaches each
    // message rather than when it was published -- and the semaphores have to stay alive until the
    // worker has left `gate.acquire()`, which is what the flush makes true.
    const auto drained = bus.flush(std::chrono::milliseconds { 2000 });
    return queued && drained;
}

} // namespace

int main() {
    using namespace ba7lya::msgbus;

    const auto main_thread = std::this_thread::get_id();

    // Declared **before** the bus, so they are destroyed **after** it.
    //
    // Every handler below captures these by reference, and a handler can still be running while `main`
    // unwinds: the bus's destructor drains its routing worker, and that drain is the last place a
    // handler runs. A subscriber's closure capturing something that dies first is the one thing no
    // ownership scheme in the library can repair, and this declaration order is how it is kept.
    std::atomic<std::size_t> sink_calls = 0;
    ledger reader;
    std::atomic<std::size_t> handled = 0;
    std::binary_semaphore entered { 0 };
    std::binary_semaphore gate { 0 };

    // The sink is the only setting this example names, so the one-argument overload keeps the queue
    // bound and the back-pressure policy at their defaults rather than restating them.
    msgbus bus { [&sink_calls](std::exception_ptr) { sink_calls.fetch_add(1, std::memory_order_relaxed); } };

    // Declared **after** the bus, so they are destroyed **before** it: every subscription is cancelled
    // while the bus is still running, and the drain the destructor does finds nothing armed. The other
    // order would leave handlers being reached for subscriptions the caller had already dropped.
    std::vector<subscription> armed;

    std::size_t sync_published = 0;
    std::size_t async_published = 0;

    std::cout << "=== sync and async in one bus ===\n"
              << "The entry point -- not the subscription -- picks the thread and owns the failure.\n\n";

    // --- arming ------------------------------------------------------------
    //
    // Every subscription goes through here, and none of them names an executor. `subscribe` returns an
    // owning handle whose destructor cancels the subscription, so a handle dropped at the end of the
    // statement would unsubscribe before the first publish; `arm` keeps each one in `armed`, which is
    // what `subscription_count` is checked against below.
    const auto arm = [&bus, &armed](std::string_view pattern, handler callback, error_handler on_error = {}) {
        auto id = bus.subscribe(pattern, std::move(callback), dispatch_options {}, std::move(on_error));
        if (!id) { return false; }
        armed.push_back(*std::move(id));
        return true;
    };

    const auto all_armed
        = arm(topic_gate,
              [&entered, &gate](const message&) {
                  // Parks the routing worker until the gate opens. `entered` tells
                  // this thread the worker is here, which is what makes everything
                  // below deterministic instead of a race the example wins only on a
                  // quiet machine.
                  entered.release();
                  gate.acquire();
              })
       && arm(topic_readings,
              [&reader](const message& msg) {
                  reader.record(std::any_cast<std::uint64_t>(msg.payload_));
                  bus_log(direction::SUBSCRIBE, msg.topic_, msg.payload_, "reader");
              })
       && arm(topic_throwing, [](const message&) { throw std::runtime_error("the handler failed"); })
       && arm(
              topic_caught, [](const message&) { throw std::runtime_error("the handler failed"); },
              [&handled](const message&, std::exception_ptr) { handled.fetch_add(1, std::memory_order_relaxed); }
       );
    if (!all_armed) {
        std::cerr << "a subscription was rejected\n";
        return 1;
    }

    std::cout << "--- the subscriptions, and a gate ---\n"
              << "  " << armed.size() << " subscriptions armed, none of them naming an executor\n"
              << "  so each one runs on whichever thread routed its message\n\n";

    // --- the ordering section ----------------------------------------------
    //
    // Everything from here to the end of the block runs with the routing worker parked inside the
    // gated handler. That is why it is one block: the gate opens when the block ends, and `gate_latch`
    // opens it on every way out, including an early return.
    {
        const gate_latch open_at_scope_end { gate };

        std::cout << "--- block the routing worker ---\n";
        const std::any marker { std::uint64_t { 0 } };
        const auto gated_status = bus.publish_async(topic_gate, marker);
        bus_log(direction::PUBLISH_ASYNC, topic_gate, marker);
        ++async_published;
        if (gated_status != publish_status::ACCEPTED) {
            std::cerr << "  the bus refused the message\n";
            return 1;
        }
        entered.acquire();
        std::cout << "  the routing worker is inside the handler, parked\n"
                  << "  routed so far: " << bus.routed() << " -- the worker has not finished the message it holds\n\n";

        std::cout << "--- an async backlog the worker cannot reach ---\n";
        for (const auto reading : backlog) {
            const std::any payload { reading };
            const auto outcome = bus.publish_async(topic_readings, payload);
            bus_log(direction::PUBLISH_ASYNC, topic_readings, payload);
            ++async_published;
            if (outcome != publish_status::ACCEPTED) {
                std::cerr << "  the bus refused " << reading << '\n';
                return 1;
            }
        }
        std::cout << "  three messages accepted; the reader has recorded " << reader.size()
                  << ", because the worker is still parked and nothing has been routed\n\n";

        std::cout << "--- a sync publish cuts in front of it ---\n";
        const std::any cutting { cutting_in };
        bus_log(direction::PUBLISH_SYNC, topic_readings, cutting);
        bus.publish_sync(topic_readings, cutting);
        ++sync_published;
        const auto parked = reader.order();
        std::cout << "  publish_sync did not join the worker's queue: it routed here, on this thread\n"
                  << "  the reader has recorded " << render_order(parked) << " while the backlog is still waiting\n\n";
    }

    // The gate is open. The worker finishes the handler it was parked in, then takes the backlog in
    // the order it was queued -- and the queue is not empty until it has, so this is a complete
    // barrier for everything above. It is one because every subscription here is inline: the handler
    // runs inside the work item, so `!running_` means "and the handler returned".
    const auto drained_ordering = bus.flush(std::chrono::milliseconds { 2000 });
    const auto routed_after_ordering = bus.routed();
    const auto worker_after_ordering = bus.worker_stats();
    const auto sink_after_ordering = sink_calls.load();
    // What this section published, so the verification below compares against the counters as they
    // stood here rather than against the totals the failures section adds to afterwards.
    const auto async_after_ordering = async_published;
    const auto sync_after_ordering = sync_published;

    std::cout << "--- release the gate: the backlog arrives, in order ---\n"
              << "  the reader has recorded " << render_order(reader.order()) << '\n'
              << "  publish_sync(200) came first because it was routed on this thread, and the three it cut in "
                 "front of followed in the order they were queued\n\n";

    // --- failures ----------------------------------------------------------
    //
    // Two subscriptions on two topics, both naming no executor, differing only in whether they carry
    // an `error_handler`. Throwing is what both handlers do; who ends up holding the throw is the
    // question, and the answer is different for each of the four publishes below.
    std::cout << "--- what each entry point does with a failure ---\n";

    const std::any failing { std::uint64_t { 0 } };

    // A synchronous publish still has a caller when its handler throws. There is no `error_handler`
    // above, so `router::deliver` returns the failure rather than consuming it, the dispatcher returns
    // it in turn, and `deliver_now` rethrows it -- on this thread, which means it travels all the way
    // out of `publish_sync` and lands in the `catch` below.
    auto caught = false;
    std::size_t expected_routed = 0;
    const auto routed_before_failures = bus.routed();
    try {
        bus_log(direction::PUBLISH_SYNC, topic_throwing, failing);
        bus.publish_sync(topic_throwing, failing);
    }
    catch (const std::runtime_error& failure) {
        caught = true;
        std::cout << "  publish_sync propagated \"" << failure.what() << "\" to its caller\n";
    }

    // The same throw on the asynchronous path. The worker's `deliver_now` rethrows it exactly as this
    // thread's did -- but by then the work item is all that is left of the publish, and the executor
    // catches whatever escapes one and hands it to the bus's own sink, which is the `on_exception`
    // role built at the top. That sink is the channel for a failure with no caller left, and this is
    // the only publish here that reaches it.
    const auto throwing_status = bus.publish_async(topic_throwing, failing);
    bus_log(direction::PUBLISH_ASYNC, topic_throwing, failing);
    ++async_published;
    if (throwing_status != publish_status::ACCEPTED) {
        std::cerr << "  the bus refused the message\n";
        return 1;
    }

    // A subscriber that supplies its own `error_handler` keeps the failure to itself on either path:
    // `router::deliver` calls it and returns null, so nothing propagates to a publisher, the bus's
    // sink is not reached either, and the message counts as routed like any other.
    for (const auto& name : { topic_caught }) {
        bus_log(direction::PUBLISH_SYNC, name, failing);
        bus.publish_sync(name, failing);
        ++sync_published;
        ++expected_routed;
    }
    const auto caught_status = bus.publish_async(topic_caught, failing);
    bus_log(direction::PUBLISH_ASYNC, topic_caught, failing);
    ++async_published;
    ++expected_routed;
    if (caught_status != publish_status::ACCEPTED) {
        std::cerr << "  the bus refused the message\n";
        return 1;
    }

    const auto drained_failures = bus.flush(std::chrono::milliseconds { 2000 });
    const auto sink_total = sink_calls.load();

    std::cout << "  the throw from the asynchronous publish reached the bus's own sink, which has now been "
              << "called " << sink_total << " time(s)\n"
              << "  the two with a subscriber's own error handler were handled " << handled.load() << " time(s) "
              << "and counted as routed: " << bus.routed() - routed_before_failures << " of the four publishes "
              << "moved routed()\n\n";

    // --- summary -----------------------------------------------------------
    const auto order = reader.order();
    const auto threads = reader.threads();
    const auto on_main = static_cast<std::size_t>(std::count(threads.begin(), threads.end(), main_thread));

    std::cout << "--- summary ---\n"
              << "  the reader recorded " << render_order(order) << '\n'
              << "  " << threads.size() << " deliveries: " << on_main << " on this thread, " << threads.size() - on_main
              << " on the routing worker\n"
              << "  " << bus.routed() << " routed, " << bus.dropped() << " dropped\n";

    // --- verify ------------------------------------------------------------
    //
    // Every claim above, in one place, so the printed output can be checked against the assertion
    // rather than believed. A claim that only holds on a quiet machine is a claim this block is
    // supposed to catch, which is why the counts are checked before any thread id is compared: "every
    // asynchronous delivery ran off this thread" is true and worthless when there were none.
    std::cout << "\n--- verify ---\n";
    auto agreed = true;
    const auto require = [&agreed](bool condition, const std::string& claim) {
        if (condition) { return; }
        std::cerr << "  FAILED: " << claim << '\n';
        agreed = false;
    };

    require(bus.subscription_count() == armed.size(), "every subscription is armed, and the bus counts exactly those");

    require(!order.empty() && order.size() == 4, "four deliveries were recorded, so nothing below is vacuous");
    require(
        order == std::vector<std::uint64_t> { cutting_in, backlog[0], backlog[1], backlog[2] },
        "the synchronous publish arrived first, then the backlog in the order it was queued"
    );
    require(threads.size() == order.size(), "there is one thread id per delivery");

    require(!threads.empty() && threads.front() == main_thread, "the synchronous delivery ran on this thread");
    require(on_main == 1, "exactly one delivery ran on this thread");
    require(
        threads.size() == 4 && threads[1] == threads[2] && threads[2] == threads[3],
        "the three asynchronous deliveries share one thread: the bus has one routing worker"
    );

    // `submitted_` counts the closures handed to the routing worker, and the claim that it equals the
    // asynchronous publish count holds **because no subscription here names `ROUTER`**: the bus's own
    // routing is inline, so it never submits to the worker it is running on. A subscriber naming
    // `ROUTER` would make this a count of something else.
    //
    // The claim `submitted_ == async_after_ordering` holds **because no subscription here names
    // `ROUTER`**: the bus's own routing is inline, so it never submits to the worker it is running on.
    // A subscriber naming `ROUTER` would make this a count of something else.
    require(
        drained_ordering && worker_after_ordering.submitted_ == async_after_ordering,
        "the worker routed every accepted message, submitted once per asynchronous publish"
    );
    // One routed message per delivery in this section -- the gated one, the three backlog ones, and the
    // synchronous one -- and `routed()` is incremented once per message rather than once per delivery,
    // so the synchronous publish counts exactly once even though it is the one that cut in front.
    require(
        routed_after_ordering == async_after_ordering + sync_after_ordering,
        "every accepted message in the ordering section was counted as routed exactly once"
    );
    require(
        worker_after_ordering.completed_ == 4 && worker_after_ordering.failed_ == 0,
        "the worker completed the four work items and none of them failed"
    );
    require(sink_after_ordering == 0, "no failure had reached the bus's own sink before the failures section");
    require(drained_ordering && drained_failures, "the routing queue drained within the deadline both times");

    require(caught, "a synchronous publish propagated its handler's throw to the caller");
    require(sink_total == 1, "only the asynchronous publish reached the bus's own sink, once");
    require(handled.load() == 2, "a subscriber's own error handler kept both of its throws");
    require(expected_routed == 2, "the two handled failures were the two that moved routed()");
    require(bus.dropped() == 0, "nothing was dropped: no unregistered executor, no refused submit");

    // And the order itself, from a bus that was not part of the narration. One pass could be luck; the
    // same answer every time is what the gate is for.
    msgbus probe;
    for (int pass = 1; pass <= 3 && agreed; ++pass) {
        ledger again;
        if (!run_sequence(probe, again)) {
            std::cerr << "  FAILED: pass " << pass << " on a second bus did not complete\n";
            agreed = false;
            break;
        }
        const auto probe_order = again.order();
        require(probe_order == order, "a second bus delivered the same order for the same sequence");
        std::cout << "  pass " << pass << " on a second bus: " << render_order(probe_order) << '\n';
    }

    if (!agreed) { return 1; }
    std::cout << "  every claim above held\n";
    return 0;
}
