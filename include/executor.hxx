///
/// @file executor.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The thread a subscriber asked for: a bounded FIFO of opaque closures run by one worker.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

#include "common.hxx"

namespace ba7lya::msgbus {

///
/// @brief One unit of queued work: an opaque closure, owning whatever it captured.
/// @details Move-only, not `std::function`, because the closure a dispatcher builds owns its captures —
/// a `std::shared_ptr<const message>`, a bound handler, an error sink — and `std::function` would force
/// all of them to be copyable, copying the closure on every call. The executor never looks inside: it
/// runs the closure and knows nothing of topics, patterns or messages. Whether a fan-out to N
/// subscriptions shares one message or copies it is the dispatcher's decision; the executor's only
/// requirement is that the closure be move-constructible.
///
using work_item = std::move_only_function<void()>;

///
/// @brief A closure that is built only once the queue has room for it.
/// @details `submit_deferred` calls this instead of taking an already-built `work_item`, so the caller
/// does not have to construct — and therefore capture and hold — what the closure needs before it knows
/// the submit will be accepted. A caller that would otherwise build a shared message and then park on a
/// `BLOCK` submit keeps nothing alive while it waits.
///
using work_item_deferred = std::move_only_function<work_item()>;

///
/// @brief Where a failure that escapes a work item goes.
/// @details Deliberately **not** `error_handler`: that type takes the offending `message`, and the
/// executor has none and must not learn what one is. A closure can still throw — a dispatcher bug, an
/// allocation, an `any_cast` — and letting it escape the worker thread would call `std::terminate`, so
/// it is caught and handed here instead. Empty means swallow, matching the branch `router::deliver`
/// takes when a handler throws and no error sink was set.
///
using exception_sink = std::move_only_function<void(std::exception_ptr)>;

///
/// @brief What `submit` did with the closure it was handed.
/// @details Finer than `publish_status`, which cannot express an eviction: a caller must be able to see
/// that accepting one message cost a queued one its delivery. The dispatcher maps `ACCEPTED` and
/// `DROPPED_OLDEST` to `publish_status::ACCEPTED`, and `REJECTED` and `CLOSED` to
/// `publish_status::REJECTED`.
///
enum class submit_status {
    ACCEPTED,       ///< Queued, or run inline on the executor's own worker thread.
    DROPPED_OLDEST, ///< Queued, and the longest-waiting queued item was evicted to make room.
    REJECTED,       ///< Refused because the queue was full under `back_pressure::DROP_NEW`.
    CLOSED,         ///< Refused because `close()` has run; the executor no longer accepts work.
};

///
/// @brief Counters a test can assert on without racing the worker.
/// @details Plain values read under the executor's lock, not atomics: the lock is already taken to
/// answer `pending()`, so a counter needs no second synchronisation object. They exist so the test suite
/// can observe an eviction or a swallowed failure that the return value of one `submit` cannot show.
///
struct executor_stats {
    std::uint64_t submitted_ = 0; ///< Closures accepted, including those run inline.
    std::uint64_t dropped_ = 0;   ///< Queued closures evicted by `back_pressure::DROP_OLDEST`.
    std::uint64_t rejected_ = 0;  ///< Closures refused by a full queue or by `close()`.
    std::uint64_t completed_ = 0; ///< Closures that ran and returned normally.
    std::uint64_t failed_ = 0;    ///< Closures that threw; each was handed to the exception sink.
};

///
/// @brief A single worker thread and its bounded FIFO of opaque closures.
/// @details The executor is the thread a subscriber asked for through `dispatch_options::executor_`. A
/// dispatcher resolves a topic to a snapshot of deliveries and hands each executor-bound one here as a
/// closure; the executor runs the closures in submission order on its one worker thread. It holds no
/// topic, no pattern and no message, and never calls back into the router — it only runs what it is
/// handed. The id-to-executor mapping and the choice of which closure carries a message are the
/// dispatcher's, not this header's.
///
/// Capacity is fixed at construction (`UNBOUNDED` means no bound); the back-pressure policy travels
/// with each `submit`, because `dispatch_options` carries it per subscription and one executor may have
/// to honour several. Under `BLOCK` a submitter waits for room — but never when it is the executor's own
/// worker, which runs the closure inline instead, so a handler cannot deadlock on itself.
///
/// Shutdown is explicit. `close()` stops intake and lets the worker finish what was already accepted;
/// `release()` is `close()` and a join. Items queued when shutdown begins **run** — they were already
/// accepted, and discarding them silently would make an `ACCEPTED` a lie. Intake is the only thing
/// shutdown cancels: a `submit` not yet accepted when `close()` runs — one parked in `not_full_` under
/// `BLOCK`, most visibly — returns `CLOSED`, so a caller for which losing that item is unacceptable
/// orders `close()` after its last `submit` instead of racing it. `close()` may be called from the
/// worker thread itself; `release()` and the destructor must not — destroying the object whose member
/// is executing on the worker is undefined, exactly as it is for a handler in flight.
///
/// @note Neither copyable nor movable: it owns a thread, a mutex and a queue, and the worker captures
/// `this`, so a move would leave that capture pointing at the wrong object.
/// @note `drain()` is stronger than `msgbus::flush`. `flush` means "handed to executors, not handlers
/// finished"; it waits on the bus's routing queue and must not call `executor::drain`. `drain` means the
/// queue is empty *and* nothing is in flight — the executor-level barrier tests and shutdown use.
///
class executor {
public:
    /// @brief The capacity that means "no bound".
    static constexpr std::size_t UNBOUNDED = 0;

    /// @brief Builds an executor with its own worker thread.
    /// @param capacity the most items that may be queued at once; `UNBOUNDED` for no bound
    /// @param on_exception where an exception escaping a work item goes; empty swallows it
    /// @details The worker thread starts here, not lazily on the first submit: an executor that a
    /// subscriber named should have a thread and an id from the moment it exists, and deferring the
    /// start would only move a creation failure into a submit path that has no error channel. One
    /// thread per executor, never a shared pool: the isolation — a handler on executor X always running
    /// on the same thread — is the whole reason a subscriber names one, and a pool would forfeit it.
    /// `capacity == UNBOUNDED` makes every policy inert: `has_room()` is always true, so BLOCK never
    /// blocks, DROP_OLDEST never evicts and DROP_NEW never rejects.
    /// @throws std::system_error if the worker thread cannot be created.
    explicit executor(std::size_t capacity = UNBOUNDED, exception_sink on_exception = {})
        : capacity_(capacity)
        , on_exception_(std::move(on_exception))
        , worker_([this](std::stop_token stop) { run(stop); }) {}

    executor(const executor&) = delete;
    executor& operator=(const executor&) = delete;
    executor(executor&&) = delete;
    executor& operator=(executor&&) = delete;

    /// @brief Shuts the executor down: `release()`. Never throws.
    /// @details One teardown path, so the destructor cannot end up not draining. It blocks until every
    /// item already accepted has run and the worker has joined, so destroying an executor with work
    /// outstanding is the caller's choice to wait, not a silent discard.
    ~executor() { release(); }

    /// @brief Hands `item` to this executor, applying `policy` when the queue is full.
    /// @param item the closure to run; must not be empty
    /// @param policy what to do when the queue is full; ignored while there is room
    /// @return ACCEPTED, DROPPED_OLDEST, REJECTED or CLOSED
    /// @throws std::invalid_argument if `item` is empty — a programming error, not a queue outcome.
    /// @details The policy is per submit, not fixed at construction: `dispatch_options` carries it per
    /// subscription, so one executor serving a telemetry pattern (DROP_OLDEST) and a command pattern
    /// (DROP_NEW) must honour each.
    /// @details A submit from this executor's own worker never queues, under any policy: the worker is
    /// the only thread that could free a slot, so BLOCK would wait for itself; DROP_OLDEST would evict a
    /// sibling the worker could run now; DROP_NEW would refuse work it can run now. The closure runs
    /// inline instead, before anything still queued. This is not a corner case — a handler on X that
    /// publishes synchronously into a topic routed back to X reaches it through the public API — so it
    /// is prevented rather than documented away. A chain of such submits recurses; bounding it would be
    /// a second policy the executor has no basis to choose.
    /// @note After `close()` or `release()`, every policy returns CLOSED.
    [[nodiscard]]
    submit_status submit(work_item item, back_pressure policy = back_pressure::DROP_OLDEST) {
        if (!item) { throw std::invalid_argument("the work item must not be empty"); }

        std::unique_lock lock(mutex_);

        // Re-entrancy: see the method comment. One uniform rule for all three policies.
        if (std::this_thread::get_id() == worker_id_) {
            if (!accepting_) {
                ++rejected_;
                return submit_status::CLOSED;
            }
            ++submitted_;
            lock.unlock();
            const bool ok = invoke(item);
            {
                const std::unique_lock guard(mutex_);
                if (ok) { ++completed_; }
                else { ++failed_; }
            }
            return submit_status::ACCEPTED;
        }

        if (!accepting_) {
            ++rejected_;
            return submit_status::CLOSED;
        }

        if (policy == back_pressure::BLOCK) {
            not_full_.wait(lock, [this] { return has_room() || !accepting_; });
            if (!accepting_) {
                ++rejected_;
                return submit_status::CLOSED;
            }
        }
        else if (policy == back_pressure::DROP_NEW && !has_room()) {
            ++rejected_;
            return submit_status::REJECTED;
        }

        if (policy == back_pressure::DROP_OLDEST && !has_room()) {
            // The longest-waiting item loses, not a named subscription: the executor holds no ids and
            // the policy only promises freshness. The victim dies after the lock is released, so
            // releasing what it captured — the last reference to a shared message, say — never runs
            // under the executor's mutex.
            work_item victim = std::move(queue_.front());
            queue_.pop_front();
            queue_.push_back(std::move(item));
            ++submitted_;
            ++dropped_;
            not_empty_.notify_one();
            lock.unlock();
            return submit_status::DROPPED_OLDEST;
        }

        queue_.push_back(std::move(item));
        ++submitted_;
        not_empty_.notify_one();
        return submit_status::ACCEPTED;
    }

    /// @brief Runs `build` to produce a closure, applying `policy` when the queue is full, then submits it.
    /// @param build called only once room exists, and **only on the path that will queue** — see below
    /// @param policy what to do when the queue is full, exactly as in `submit`
    /// @return `ACCEPTED` or `DROPPED_OLDEST` (the closure was queued), `REJECTED` (not queued), `CLOSED`
    /// @throws std::invalid_argument if `build` returns an empty closure — a programming error.
    /// @details The reason this exists rather than a caller building a closure and calling `submit`: a
    /// `back_pressure::BLOCK` submit may park, and a caller that has already captured what the closure
    /// needs — a shared message, say — holds it for as long as the wait lasts. Here the closure is built
    /// **after** the caller has secured the room, so nothing is retained while a submit waits.
    /// @details The third possibility — a submit made *from this executor's own worker*, which `submit`
    /// would run inline — is refused with `REJECTED` rather than deadlocking on a full queue. `build` is
    /// not called in that case, so a caller must expect it may not be.
    /// @note One consequence of the refusal, for a caller that routes messages: a handler running on
    /// executor X that publishes asynchronously can have that publish rejected once X's own queue is
    /// full. That is a full queue, reported honestly, not a dropped message.
    [[nodiscard]]
    submit_status submit_deferred(work_item_deferred build, back_pressure policy = back_pressure::DROP_OLDEST) {
        if (!build) { throw std::invalid_argument("the deferred work item must not be empty"); }

        std::unique_lock lock(mutex_);

        if (std::this_thread::get_id() == worker_id_) {
            // Running it inline is what `submit` does, but a deferred item cannot be run without first
            // building it, and the only thread that runs this one is the worker itself: the queue could
            // never free under it. A deadlock is the one outcome that must not be reachable here, so the
            // item is refused and the caller learns it did not happen.
            ++rejected_;
            return submit_status::REJECTED;
        }

        if (!accepting_) {
            ++rejected_;
            return submit_status::CLOSED;
        }

        // Note the `accepting_` test inside each predicate, as in `submit`: a `BLOCK` here waits on a
        // queue whose worker has been told to stop, so it must wake on that rather than on room.
        switch (policy) {
        case back_pressure::BLOCK:
            not_full_.wait(lock, [this] { return has_room() || !accepting_; });
            if (!accepting_) {
                ++rejected_;
                return submit_status::CLOSED;
            }
            break;
        case back_pressure::DROP_NEW:
            if (!has_room()) {
                ++rejected_;
                return submit_status::REJECTED;
            }
            break;
        case back_pressure::DROP_OLDEST:
            // Nothing to do here, unlike in `submit`: there is no wait to secure a slot for, because an
            // eviction is itself the way to make room — so it is decided below, after the build.
            // Deciding it here would cost a queued item its delivery whenever `build` throws or returns
            // nothing, because the item that was to replace it would never arrive.
            break;
        }

        // Room is secured — for `BLOCK`, by the wait above; for `DROP_OLDEST`, by the eviction below.
        // Build first: it cannot park, so the slot stays held from here to the `push_back`, and no second
        // waiter can slip in.
        work_item item = build();
        if (!item) { throw std::invalid_argument("the deferred work item must not be empty"); }

        if (policy == back_pressure::DROP_OLDEST && !has_room()) {
            // As in `submit`: the longest-waiting item loses, and the victim dies after the lock is
            // released.
            work_item victim = std::move(queue_.front());
            queue_.pop_front();
            queue_.push_back(std::move(item));
            ++submitted_;
            ++dropped_;
            not_empty_.notify_one();
            lock.unlock();
            victim = nullptr;
            return submit_status::DROPPED_OLDEST;
        }

        queue_.push_back(std::move(item));
        ++submitted_;
        not_empty_.notify_one();
        return submit_status::ACCEPTED;
    }

    /// @brief Stops accepting work; the worker finishes what was already queued, then exits.
    /// @details Idempotent, and safe to call from the executor's own worker thread (a work item that
    /// wants to stop its own executor). It does not join: the worker drains the queue and returns on its
    /// own, and `release()` is what waits for it. Nothing already accepted is discarded — a queue that a
    /// `DROP_NEW`-policy command path filled must not be dropped silently at teardown.
    void close() noexcept {
        {
            const std::unique_lock lock(mutex_);
            accepting_ = false;
        }
        worker_.request_stop();
        not_empty_.notify_all();
        not_full_.notify_all();
        idle_.notify_all();
    }

    /// @brief `close()` and then waits for the worker to finish; idempotent.
    /// @details Every item accepted before the call has run by the time this returns. It must not be
    /// called from the executor's own worker thread: a thread cannot join itself, so the join is skipped
    /// there — the call returns rather than hanging, but an external thread must then destroy the
    /// object, because destroying it from the worker is the one teardown this class forbids.
    void release() noexcept {
        close();

        bool on_worker = false;
        {
            const std::unique_lock lock(mutex_);
            on_worker = std::this_thread::get_id() == worker_id_;
        }
        if (!on_worker && worker_.joinable()) { worker_.join(); }
    }

    /// @brief Waits until the queue is empty and nothing is in flight, or until `timeout` elapses.
    /// @param timeout how long to wait; the maximum value waits without limit
    /// @return true if the executor became quiescent within `timeout`, false otherwise
    /// @throws std::invalid_argument if called from the executor's own worker thread — that wait could
    /// only miss its deadline, and a programming error should say so rather than time out.
    /// @details "Drained" means the queue is empty **and** no item is in flight, not the queue alone:
    /// the worker pops an item before it runs it, so a queue-only predicate would let a drain-then-assert
    /// test race the item that is running. A `false` return undoes nothing — work continues; the deadline
    /// is the caller's. There is no busy-waiting: the wait is a condition-variable wait, signalled by the
    /// worker whenever it goes idle and by `close()`.
    /// @note Relation to `msgbus::flush`: `flush` only promises "handed to executors", so it waits on the
    /// bus's own routing queue and must not call this. This is the stronger executor-level barrier.
    [[nodiscard]]
    bool drain(std::chrono::milliseconds timeout = std::chrono::milliseconds::max()) {
        std::unique_lock lock(mutex_);
        if (std::this_thread::get_id() == worker_id_) {
            throw std::invalid_argument("drain() cannot be called from the executor's own worker thread");
        }

        const auto quiescent = [this] { return queue_.empty() && !running_; };
        // wait_for adds the duration to the clock to build a deadline, which overflows for a duration
        // this large, so the unbounded case takes wait() and never builds one.
        if (timeout == std::chrono::milliseconds::max()) {
            idle_.wait(lock, quiescent);
            return true;
        }
        return idle_.wait_for(lock, timeout, quiescent);
    }

    /// @brief How many items are queued; the one in flight is not counted.
    [[nodiscard]]
    std::size_t pending() const noexcept {
        const std::unique_lock lock(mutex_);
        return queue_.size();
    }

    /// @brief The bound the executor was built with; `UNBOUNDED` means unbounded.
    [[nodiscard]]
    std::size_t capacity() const noexcept {
        return capacity_;
    }

    /// @brief Whether the executor still accepts work.
    [[nodiscard]]
    bool accepting() const noexcept {
        const std::unique_lock lock(mutex_);
        return accepting_;
    }

    /// @brief Whether the worker thread has not yet exited.
    /// @details True from construction until the worker's exit block runs, which only `close()` or
    /// `release()` brings about — so it is true for the whole pre-shutdown life, including while the
    /// worker is parked on an empty queue, and false once the worker has returned.
    [[nodiscard]]
    bool is_running() const noexcept {
        const std::unique_lock lock(mutex_);
        return live_;
    }

    /// @brief Whether this call is running on the executor's own worker thread.
    [[nodiscard]]
    bool is_on_worker_thread() const noexcept {
        const std::unique_lock lock(mutex_);
        return std::this_thread::get_id() == worker_id_;
    }

    /// @brief A consistent snapshot of the counters, read under the lock.
    [[nodiscard]]
    executor_stats stats() const noexcept {
        const std::unique_lock lock(mutex_);
        return { submitted_, dropped_, rejected_, completed_, failed_ };
    }

private:
    /// @brief Whether the queue can take one more item. Caller holds `mutex_`.
    [[nodiscard]]
    bool has_room() const noexcept {
        return capacity_ == UNBOUNDED || queue_.size() < capacity_;
    }

    /// @brief Runs `item`, isolating whatever it throws. Never throws.
    /// @return true if the closure returned normally, false if it threw (the throw went to the sink)
    /// @details The single place a work item is invoked, shared by the worker loop and the inline
    /// re-entrant path, so isolation holds identically on both. A throw escaping a `std::jthread`
    /// function would call `std::terminate`, taking the whole gateway with it — a bus is a delivery
    /// mechanism, and one bad closure must cost only its own delivery.
    [[nodiscard]]
    bool invoke(work_item& item) noexcept {
        try {
            item();
            return true;
        }
        catch (...) {
            if (on_exception_) {
                try {
                    on_exception_(std::current_exception());
                }
                catch (...) {
                    // The sink itself threw. Nowhere is left to report it; swallowing keeps the worker
                    // alive and the queue draining.
                }
            }
            return false;
        }
    }

    /// @brief The worker body. Never throws.
    void run(std::stop_token stop) noexcept {
        {
            const std::unique_lock lock(mutex_);
            worker_id_ = std::this_thread::get_id();
        }

        for (;;) {
            work_item item;
            {
                std::unique_lock lock(mutex_);
                // Wake on a new item or on a stop request. close() clears accepting_ and requests stop,
                // so a worker parked here is released even with an empty queue.
                not_empty_.wait(lock, [this, &stop] { return !queue_.empty() || stop.stop_requested(); });

                if (queue_.empty()) {
                    // Stopping with nothing left to run: this is the exit, and every item accepted
                    // before close() has run. Wake the waiters that a running item was holding.
                    live_ = false;
                    not_full_.notify_all();
                    idle_.notify_all();
                    return;
                }

                item = std::move(queue_.front());
                queue_.pop_front();
                running_ = true;
                not_full_.notify_one();
            }

            const bool ok = invoke(item);

            {
                const std::unique_lock lock(mutex_);
                running_ = false;
                if (ok) { ++completed_; }
                else { ++failed_; }
                idle_.notify_all();
            }
        }
    }

    /// @brief The queue bound; `UNBOUNDED` means no bound. Fixed for the executor's life.
    const std::size_t capacity_;
    /// @brief Where an exception escaping a work item goes; empty swallows it. Not `const`: a
    /// `std::move_only_function` is not callable through a const reference, and this is never reassigned.
    exception_sink on_exception_;
    /// @brief Guards everything below except `capacity_`, which never changes.
    mutable std::mutex mutex_;
    /// @brief Signalled when an item is queued; the worker waits here.
    std::condition_variable not_empty_;
    /// @brief Signalled when a slot frees; a `BLOCK` submitter waits here.
    std::condition_variable not_full_;
    /// @brief Signalled when the queue empties; `drain` waits here.
    std::condition_variable idle_;
    /// @brief The pending closures, oldest at the front. Bounded by `capacity_`.
    std::deque<work_item> queue_;
    /// @brief The worker's thread id, published by the worker before it touches the queue. Every read
    /// is under `mutex_`, so the pre-start `{}` can only compare unequal.
    std::thread::id worker_id_ {};
    /// @brief Whether the executor still accepts work; false from the moment `close()` runs.
    bool accepting_ = true;
    /// @brief The in-flight half of the drain predicate.
    bool running_ = false;
    /// @brief `is_running()`: set clear only as the worker leaves, so it reads "not yet exited".
    bool live_ = true;
    /// @brief Closures accepted, including those run inline.
    std::uint64_t submitted_ = 0;
    /// @brief Queued closures evicted by `DROP_OLDEST`.
    std::uint64_t dropped_ = 0;
    /// @brief Closures refused by a full queue or by `close()`.
    std::uint64_t rejected_ = 0;
    /// @brief Closures that ran and returned normally.
    std::uint64_t completed_ = 0;
    /// @brief Closures that threw; each went to `on_exception_`.
    std::uint64_t failed_ = 0;
    /// @brief The one worker. Declared last so its thread starts only after every member above is
    /// initialized.
    std::jthread worker_;
};

} // namespace ba7lya::msgbus
