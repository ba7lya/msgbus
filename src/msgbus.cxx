// SPDX-License-Identifier: MIT

///
/// @file msgbus.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The bus's translation unit: `msgbus::impl` and the entry points that forward to it.
/// @details One translation unit for the whole bus, because the implementation is one object: the router,
/// the dispatcher, the executors and the routing worker are held by `msgbus::impl`, and a second unit would
/// only be a second copy of the same includes. The class is declared incomplete in `msgbus.hxx` -- a program
/// gets the bus's declaration and never this type -- and defined here, where the components it holds are
/// complete. Splitting it later is a matter of giving each component a unit of its own.
/// @version 0.1
/// @date 2026-10-18
/// @copyright Copyright (c) 2026 BA7LYA
///

#include "msgbus.hxx"

#include <any>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common.hxx"
#include "dispatcher.hxx"
#include "error.hxx"
#include "executor.hxx"
#include "handler.hxx"
#include "layer_matcher.hxx"
#include "matcher.hxx"
#include "message.hxx"
#include "prefix_matcher.hxx"
#include "router.hxx"
#include "subscription.hxx"
#include "topic.hxx"

namespace ba7lya::msgbus {

///
/// @brief The machinery behind `msgbus`: the routing table, the executors, and the routing worker.
/// @details Three parts, each with one job. The `router` owns the subscription table and answers which
/// subscriptions a topic reaches. The `dispatcher` owns the executors and answers where each delivery runs.
/// This class owns the **publishing path**: it is the piece that knows a topic must be parsed once, that a
/// synchronous publish routes on the calling thread while an asynchronous one is queued, and that a handler
/// failure on the worker's own thread has no caller left to throw at.
///
/// It is a nested type of the bus it implements: `msgbus.hxx` declares it incomplete and this unit defines
/// it, so a program gets the bus's declaration and never this type.
/// @details The synchronous and the asynchronous publish share everything after the parse, which is why they
/// are two entry points on one class rather than two classes: `publish_async` is `publish_sync` handed to a
/// thread. Splitting them would duplicate the routing, the failure policy and the executor lookup, and would
/// make a bus that supports both impossible to name.
/// @note Not copyable or movable: it owns a thread and two mutexes.
///
class msgbus::impl {
public:
    /// @brief Builds the engine with every setting at its default.
    /// @details `LAYER` matching, a 4096-message routing queue, `DROP_NEW`, and no failure sink.
    /// @throws std::system_error if the worker thread cannot be created
    explicit impl(msgbus& bus)
        : impl(bus, matcher_type::LAYER) {}

    /// @brief Builds the engine and starts its routing worker.
    /// @param type the pattern language subscriptions are resolved with
    /// @param queue_capacity how many published messages may wait to be routed;
    /// `UNBOUNDED` for no bound
    /// @param policy what `publish_async` does when that queue is full
    /// @param on_exception where a handler's failure goes when no caller is left to throw it at
    /// @throws std::system_error if the worker thread cannot be created
    impl(
        msgbus& bus, matcher_type type, std::size_t queue_capacity = 4096,
        back_pressure policy = back_pressure::DROP_NEW, exception_sink on_exception = exception_sink {}
    )
        : bus_(bus)
        , router_(make_matcher(type))
        , dispatcher_()
        , on_exception_(std::move(on_exception))
        , policy_(policy)
        , worker_(std::make_shared<executor>(queue_capacity, [this](std::exception_ptr failure) {
            report(std::move(failure));
        })) {
        const auto registered = dispatcher_.add(msgbus::ROUTER, worker_);
        // Nothing else has run yet, so the only way this fails is a bug in the class itself.
        if (!registered) { throw std::logic_error("the routing worker's id is already taken"); }
    }

    /// @brief Builds the engine with a failure sink and the default pattern language, queue bound and
    /// policy.
    /// @details The settings are positional, so a caller naming only the sink would otherwise have to
    /// restate the others; this overload is that call.
    explicit impl(msgbus& bus, exception_sink on_exception)
        : impl(bus, matcher_type::LAYER, 4096, back_pressure::DROP_NEW, std::move(on_exception)) {}

    impl(const impl&) = delete;
    impl& operator=(const impl&) = delete;
    impl(impl&&) = delete;
    impl& operator=(impl&&) = delete;

    /// @brief Shuts the bus down: `release()`.
    ~impl() noexcept { release(); }

    /// @brief Registers `handler` for every topic matching `pattern`.
    /// @details No lock beyond the router's: the subscription table is the router's alone, so a subscribe
    /// racing a publish is coherent for the same reason a subscribe racing a resolve is.
    /// @note The handle is bound to the bus, because that is what a caller holds and what the contract
    /// names: a handle cancels through `msgbus::unsubscribe`, the same id-based form a caller can call
    /// directly. The router issues the id; the bus owns the router, so both outlive any handle.
    [[nodiscard]]
    result<subscription, error> subscribe(
        std::string_view pattern, handler handler, dispatch_options options = {}, error_handler on_error = {}
    ) {
        auto registered = router_.subscribe(pattern, std::move(handler), options, std::move(on_error));
        if (!registered) { return std::unexpected(std::move(registered).error()); }
        return subscription(bus_, *registered);
    }

    /// @brief Cancels a subscription by id.
    /// @details A message already queued for the routing worker is resolved **when the worker reaches it**,
    /// not when it was published, so a subscription cancelled first does not see it.
    bool unsubscribe(subscription_id id) { return router_.unsubscribe(id); }

    /// @brief Parses `msg`'s topic and routes it on the calling thread.
    /// @throws std::invalid_argument if the topic is malformed, and any handler exception this thread
    /// carried out.
    void publish_sync(message msg) {
        auto shared = std::make_shared<const message>(std::move(msg));
        deliver_now(shared);
    }

    /// @brief Queues `msg` for the routing worker.
    /// @return `ACCEPTED` when the bus took it; `REJECTED` when the queue was full under the configured
    /// policy or the bus has been released
    [[nodiscard]]
    publish_status publish_async(message msg) {
        // The closure is built **by the executor**, on the far side of its admission decision: nothing is
        // captured here, so a `BLOCK` submit waits — on a message it has not yet had to allocate, and on a
        // closure it has not yet had to build — and there is no path where a closure is built and then has
        // to be unwound because the submit was refused.
        const auto outcome = worker_->submit_deferred(
            [this, msg = std::move(msg)]() -> work_item {
                const auto shared = std::make_shared<const message>(std::move(msg));
                return [this, shared] { deliver_now(shared); };
            },
            policy_
        );
        return outcome == submit_status::REJECTED || outcome == submit_status::CLOSED ? publish_status::REJECTED
                                                                                      : publish_status::ACCEPTED;
    }

    /// @brief Waits until every message accepted so far has been routed.
    [[nodiscard]]
    bool flush(std::chrono::milliseconds timeout) {
        return worker_->drain(timeout);
    }

    /// @brief The bus's own error channel.
    [[nodiscard]]
    const exception_sink& on_exception() const noexcept {
        return on_exception_;
    }

    /// @brief Waits until the executor registered under `id` has run everything it accepted.
    /// @details One lookup and one drain, in that order: an unknown id answers false rather than waiting on
    /// something that is not there.
    [[nodiscard]]
    bool drain_executor(executor_id id, std::chrono::milliseconds timeout) {
        const auto exec = dispatcher_.find(id);
        return exec != nullptr && exec->drain(timeout);
    }

    /// @brief Registers an executor of the bus's own making under `id`.
    [[nodiscard]]
    bool add_executor(executor_id id, std::size_t capacity = UNBOUNDED) {
        return dispatcher_.add(id, capacity, [this](std::exception_ptr failure) { report(std::move(failure)); });
    }

    /// @brief Stops and forgets the executor registered under `id`.
    [[nodiscard]]
    bool remove_executor(executor_id id) {
        if (id == msgbus::ROUTER) { return false; }
        return dispatcher_.remove(id);
    }

    /// @brief How many subscriptions are live.
    [[nodiscard]]
    std::size_t subscription_count() const noexcept {
        return router_.subscription_count();
    }

    /// @brief Whether any subscription is live.
    [[nodiscard]]
    bool empty() const noexcept {
        return router_.empty();
    }

    /// @brief How many messages the routing worker has finished routing.
    [[nodiscard]]
    std::uint64_t routed() const noexcept {
        return routed_.load(std::memory_order_relaxed);
    }

    /// @brief How many deliveries went nowhere.
    [[nodiscard]]
    std::uint64_t dropped() const noexcept {
        return dispatcher_.dropped();
    }

    /// @brief The routing worker's counters, read as one snapshot.
    [[nodiscard]]
    executor_stats worker_stats() const noexcept {
        return worker_->stats();
    }

    /// @brief Stops the worker and every executor, then waits for each.
    /// @details Close then join, in that order and in two passes: the first pass stops all intake, so
    /// nothing accepted after it can be queued behind a worker that has already been joined. Idempotent.
    /// @note Safe to call from inside a handler: the wait for the routing queue is skipped there -- the
    /// worker is draining itself on its way out -- and the join is skipped for the same reason.
    void release() noexcept {
        // Two calls, not one: the first stops the worker accepting and the second waits for it, and
        // between them nothing can be submitted — a publisher that raced the first would otherwise be
        // parked on a queue whose worker has already been told to stop.
        worker_->close();
        // `drain` throws when the caller is the worker itself, which is exactly the handler that releases
        // the bus from inside it. Teardown is noexcept, and the join below returns without waiting in that
        // case anyway, so the throw is the one thing this cannot let escape.
        try {
            static_cast<void>(worker_->drain());
        }
        catch (...) {
            // Only the self-drain case; the worker has already stopped accepting and will finish the queue.
        }
        worker_->release();
        dispatcher_.release();
    }

private:
    /// @brief Builds the concrete matcher `type` names.
    /// @details The one place the enum and the classes are tied together, so the constructor does not name
    /// an implementation and a third kind is one line here.
    [[nodiscard]]
    static std::unique_ptr<matcher> make_matcher(matcher_type type) {
        switch (type) {
        case matcher_type::PREFIX: return std::make_unique<prefix_matcher>();
        case matcher_type::LAYER:
        default:                   return std::make_unique<layer_matcher>();
        }
    }

    /// @brief Parses `msg->topic_`, resolves it, and hands the deliveries to the dispatcher.
    /// @details One body shared by `publish_sync` and the worker, so a synchronous and an asynchronous
    /// publish cannot drift apart in what they deliver or in what they do about failure.
    void deliver_now(const std::shared_ptr<const message>& msg) {
        const auto parsed = parse_topic(msg->topic_);
        if (!parsed) {
            throw std::invalid_argument(
                std::string("cannot publish to \"") + msg->topic_
                + "\": " + std::string(to_string(parsed.error().code_))
            );
        }

        const auto routes = router_.resolve(parsed->levels());
        // The dispatcher's own error handler is this bus's channel: by the time it is called, every
        // remaining caller has returned.
        const auto failure = dispatcher_.dispatch(routes, msg, [this](const message&, std::exception_ptr cause) {
            report(std::move(cause));
        });
        if (failure) { std::rethrow_exception(failure); }
        static_cast<void>(routed_.fetch_add(1, std::memory_order_relaxed));
    }

    /// @brief Hands a failure to the configured sink, if there is one, without ever throwing.
    /// @details The sink is user code called from a worker, so it is isolated the same way a handler is: a
    /// throwing sink would otherwise unwind into a thread function and terminate the process.
    void report(std::exception_ptr failure) noexcept {
        if (!on_exception_) { return; }
        try {
            on_exception_(std::move(failure));
        }
        catch (...) {
            // Nowhere above this to report it; dropping it keeps the worker alive.
        }
    }

    /// @brief The bus this implementation is behind. A reference: the bus owns this object, so the
    /// reference cannot dangle, and a handle minted here can name the bus rather than a component.
    msgbus& bus_;
    /// @brief The subscription table and the topic-to-subscriber resolution.
    router router_;
    /// @brief The executors, including the routing worker placed under `ROUTER`.
    dispatcher dispatcher_;
    /// @brief The bus-level failure sink, shared with `worker_` from construction.
    exception_sink on_exception_;
    /// @brief What `publish_async` does when the routing queue is full. Fixed at construction.
    back_pressure policy_ = back_pressure::DROP_NEW;
    /// @brief The routing worker, and the executor asynchronous publishes queue on.
    /// @details Held by `shared_ptr` because the dispatcher's registry owns its executors; this is the
    /// bus's own reference, used for `submit`, `drain` and shutdown.
    std::shared_ptr<executor> worker_;
    /// @brief Messages the worker has finished routing.
    std::atomic<std::uint64_t> routed_ = 0;
};

msgbus::msgbus()
    : impl_(std::make_unique<impl>(*this)) {}

msgbus::msgbus(matcher_type type, std::size_t queue_capacity, back_pressure policy, exception_sink on_exception)
    : impl_(std::make_unique<impl>(*this, type, queue_capacity, policy, std::move(on_exception))) {}

msgbus::msgbus(exception_sink on_exception)
    : impl_(std::make_unique<impl>(*this, std::move(on_exception))) {}

msgbus::~msgbus() = default;

result<subscription, error> msgbus::subscribe(
    std::string_view pattern, handler handler, dispatch_options options, error_handler on_error
) {
    return impl_->subscribe(pattern, std::move(handler), options, std::move(on_error));
}

bool msgbus::unsubscribe(subscription_id id) { return impl_->unsubscribe(id); }

void msgbus::publish_sync(message msg) { impl_->publish_sync(std::move(msg)); }

publish_status msgbus::publish_async(message msg) { return impl_->publish_async(std::move(msg)); }

bool msgbus::flush(std::chrono::milliseconds timeout) { return impl_->flush(timeout); }

const exception_sink& msgbus::on_exception() const noexcept { return impl_->on_exception(); }

bool msgbus::drain_executor(executor_id id, std::chrono::milliseconds timeout) {
    return impl_->drain_executor(id, timeout);
}

bool msgbus::add_executor(executor_id id, std::size_t capacity) { return impl_->add_executor(id, capacity); }

bool msgbus::remove_executor(executor_id id) { return impl_->remove_executor(id); }

std::size_t msgbus::subscription_count() const noexcept { return impl_->subscription_count(); }

bool msgbus::empty() const noexcept { return impl_->empty(); }

std::uint64_t msgbus::routed() const noexcept { return impl_->routed(); }

std::uint64_t msgbus::dropped() const noexcept { return impl_->dropped(); }

executor_stats msgbus::worker_stats() const noexcept { return impl_->worker_stats(); }

void msgbus::release() noexcept { impl_->release(); }

} // namespace ba7lya::msgbus
