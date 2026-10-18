///
/// @file pubsub.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The components a gateway is made of: a `notifier` that answers a command and rules on a value, a
/// `monitor` that republishes a change, two `sensor`s that sample on a clock, and two `device`s that watch
/// for changes -- over one shared bus.
/// @version 0.1
/// @date 2026-10-18
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT
//
// A gateway is made of components, and most of them are one half of a conversation: a device that
// receives, a notifier that reads a value and writes another. `publisher` and `subscriber` are those
// halves as types -- a `publisher` has no `subscribe` to call, and a `subscriber` has no `publish` --
// and `adapter` is the one type that holds both, for the component that is genuinely two-way.
//
// `main` holds one object of every type this file defines and runs one scenario through all of them. Seven
// things that makes checkable rather than asserted:
//
//   * Provenance. Every message a notifier builds is stamped with its `source()` and the next number
//     from its own counter. Two publishers on one topic therefore produce two sequence streams, each
//     starting at 1 -- `message_meta::seq_` is a per-source count, not a global one.
//   * Ownership. `subscriber::subscribe` returns an id, not a handle: the subscriber keeps every handle
//     it was given, so all of them go away with it, and "this component is alive" and "this component is
//     receiving" are the same condition.
//   * Round trips. The notifier is commanded and answers by publishing from inside the handler: a
//     delivery that starts another delivery, which is legal because the router released its lock before
//     running the handler, and visible in the trace as one `PUB` inside another's `SUB`.
//   * Shapes. None of the three types has a virtual function, so a component may inherit one or hold one
//     as a member. The two devices and the monitor are the same comparison run three ways: one pattern,
//     three receivers, and the three logs must agree on what each of them heard.
//   * Clocks. A sensor may sample on a thread of its own through an RAII `ticker`, whose handle scope is
//     the clock's scope. Both shapes of sensor share that one type, and every sample goes through the same
//     `publish_sync` a caller would use: routed, stamped and delivered like any other message.
//   * Changes. A delivered message is not a changed value: `device_1` remembers the last reading and calls
//     `on_change` only when the new one differs, and `monitor` republishes the changed value on a topic of
//     its own -- seven deliveries, five changes, both counted.
//   * Rules. A notifier goes one step further and publishes by rule: a repeat is not a change, and a change
//     that stays on the same side of the threshold is not worth a message. `seen`, `changes` and
//     `notifications` are three counters because they answer three different questions.
//
// Every subscription here names no executor, so handlers run inline: on the publishing thread for
// `publish_sync`, on the bus's routing worker for `publish_async`. That is what makes `flush` a
// complete barrier below -- the worker runs an inline handler inside the work item it is executing, so
// a worker that is idle is a worker whose deliveries have returned.

#include <any>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "adapter.hxx"
#include "common.hxx"
#include "error.hxx"
#include "logger.hxx"
#include "message.hxx"
#include "msgbus.hxx"
#include "publisher.hxx"
#include "subscriber.hxx"

namespace {

using namespace ba7lya::msgbus;

/// @brief The two sites a component may be installed on, as strings of their own.
///
/// `topic` is a `std::string_view`: naming a topic is not owning it. A component may therefore hold the
/// text in a `std::string` member and hand the `publisher` a view of it -- the bus copies the text into
/// each `message`, so the view only has to outlive the call that publishes it.
constexpr std::string_view site_line1 = "site/line1";
constexpr std::string_view site_backup = "site/backup";

/// @brief What one delivery showed: the topic, the sender, the sender's sequence, the payload, and the
/// thread it ran on.
///
/// The first four belong to the message; the last belongs to the delivery. The same message reaching
/// two subscriptions on two threads is one message and two thread ids, which is why the thread is
/// recorded here rather than read from the message -- the bus does not stamp a message with a thread,
/// it only decides which one runs the handler.
struct arrival {
    std::string topic_;
    std::string source_;
    std::uint64_t seq_ = 0;
    std::uint64_t payload_ = 0;
    std::thread::id thread_;
};

/// @brief "1 subscription", "2 subscriptions" -- so the lines below do not read "1 subscriptions".
/// @param many the irregular plural, for the nouns that do not take an `s`; empty means `singular + "s"`
[[nodiscard]]
std::string plural(std::size_t count, std::string_view singular, std::string_view many = {}) {
    const auto noun = count == 1 ? std::string { singular }
                                 : (many.empty() ? std::string { singular } + 's' : std::string { many });
    return std::to_string(count) + " " + noun;
}

/// @brief Records one delivery, and logs it on whichever thread ran the handler.
void record(std::vector<arrival>& into, const message& msg, std::string_view name) {
    auto& seen = into.emplace_back();
    seen.topic_ = msg.topic_;
    seen.source_ = msg.meta_.source_;
    seen.seq_ = msg.meta_.seq_;
    seen.payload_ = std::any_cast<std::uint64_t>(msg.payload_);
    seen.thread_ = std::this_thread::get_id();
    bus_log(direction::SUBSCRIBE, msg.topic_, msg.payload_, name);
}

///
/// @brief An RAII clock: owns a thread that calls `tick` every `period`, and stops it when destroyed.
/// @details A thread, an interval and a callback, and nothing bus-specific -- which is why it sits here
/// rather than inside one sensor. Both sensors below use the same clock and differ only in how they reach
/// the bus, so "the two shapes behave the same" is a property of the example rather than of two copies of
/// this loop.
///
/// The handle is the clock's lifetime: `stop` requests the stop and joins, the destructor calls it, and a
/// caller therefore starts a clock by initialising a handle and stops it by letting the handle go.
/// `std::jthread` is what makes the stop a wake-up rather than a wait for the next period, and what makes a
/// forgotten handle a join instead of a `std::terminate`.
///
/// @note Move-only, and non-owning towards whatever `tick` captures: the callback runs on this thread, so
/// the object it reaches through must outlive the handle. That is the contract `subscription` has with the
/// table that issued it, spelled the same way.
/// @note `tick` must not throw: it runs as a thread function's body, and an exception escaping one is
/// `std::terminate`. A publish that can throw -- a malformed topic, a handler with no error handler -- is
/// therefore the caller's to arrange for.
///
class ticker {
public:
    /// @brief Starts the clock: `tick` runs once per `period` on a thread of its own.
    /// @param tick what each period does, on the clock's thread
    /// @param period the interval between one call and the next
    ticker(std::function<void()> tick, std::chrono::milliseconds period)
        : worker_([tick = std::move(tick), period](const std::stop_token token) {
            std::mutex mutex;
            std::condition_variable_any wake;
            const auto stopping = [&token] { return token.stop_requested(); };
            std::unique_lock lock(mutex);
            // The wait wakes on the period or on a stop request, and its predicate says which of the two it
            // was: true means "stop", so the loop exits; false means the period expired, so it ticks. (A
            // predicate that is always false would make both cases return false, and the loop would exit on
            // the first period.)
            while (!wake.wait_for(lock, token, period, stopping)) {
                // Called outside the lock: whatever the callback publishes may run a handler on this very
                // thread, and that handler has no business holding the mutex the stop path takes.
                lock.unlock();
                tick();
                lock.lock();
            }
        }) {}

    ticker(const ticker&) = delete;
    ticker& operator=(const ticker&) = delete;
    ticker(ticker&&) noexcept = default;
    ticker& operator=(ticker&&) noexcept = default;

    /// @brief Stops the thread and waits for it to return; idempotent.
    /// @details The wait is interrupted rather than completed: the thread is parked on the condition
    /// variable, and the stop token wakes it, so stopping costs a wake-up and not one whole period.
    void stop() noexcept {
        if (!worker_.joinable()) { return; }
        worker_.request_stop();
        worker_.join();
    }

    /// @brief Stops the clock: what makes `start_sampling` an RAII call rather than a switch.
    ~ticker() { stop(); }

    /// @brief Whether this handle is still driving a thread.
    [[nodiscard]]
    bool running() const noexcept {
        return worker_.joinable();
    }

private:
    /// @brief The thread this handle owns, and therefore the lifetime it controls.
    std::jthread worker_;
};

///
/// @brief A publisher by inheritance, with an attribute of its own and a clock.
/// @details `publish_sync`, `sent` and `source` arrive with the base; `reading`, `sample` and
/// `start_sampling` are what the derived type adds. That is the state a bare `publisher` has no place for
/// -- the base publishes whatever payload it is handed, while a sensor publishes what it measured, so the
/// attribute and the message cannot drift apart. This one is given its reading at construction; a real
/// sensor would update it from the hardware.
///
/// The clock is the shared `ticker` above: `start_sampling` hands back one, and the handle's scope is the
/// clock's scope. The sensor keeps no thread of its own, so neither a caller nor a destructor here has to
/// stop anything. Every sample goes through `publish_sync`, which means the samples are routed exactly as a
/// caller's publishes are: inline on that thread when the subscription names no executor, and stamped with
/// this sensor's source and sequence.
///
class sensor_1 : public publisher {
public:
    /// @brief Binds to `bus`, names itself `source`, and takes `reading` as what it currently measures.
    sensor_1(msgbus& bus, std::string source, std::uint64_t reading)
        : publisher(bus, std::move(source))
        , reading_(reading) {}

    /// @brief The reading this sensor currently holds.
    [[nodiscard]]
    std::uint64_t reading() const noexcept {
        return reading_;
    }

    /// @brief Publishes the held reading on `where`, on the calling thread.
    void sample(topic where) { publish_sync(where, std::any { reading_ }); }

    /// @brief Starts a clock that samples every `period`, and hands ownership of it back to the caller.
    /// @param where the topic each sample is published on; copied, because the thread outlives the call
    /// @param period the interval between one sample and the next
    /// @return the running clock; destroying it stops the thread, `ticker::stop` stops it early
    /// @note Each call starts one thread, so hold one handle rather than calling twice: two handles are two
    /// clocks sampling the same sensor.
    [[nodiscard]]
    ticker start_sampling(topic where, std::chrono::milliseconds period) {
        return ticker([this, where = std::string(where)] { sample(where); }, period);
    }

private:
    /// @brief What the sensor measures, and therefore what every `sample` carries.
    std::uint64_t reading_ = 0;
};

///
/// @brief The same sensor by composition: a `publisher` and an attribute, reached through forwards.
/// @details Every capability `sensor_1` inherits, this one holds as a member: `sample` forwards to the
/// publisher it keeps, `start_sampling` hands the same `ticker` a callback that goes through that member, and
/// `sent` reads the counter inside it. Nothing is inherited, which is the other half of the trade -- a caller
/// here cannot publish an arbitrary payload, only the reading the sensor holds, because that is the door this
/// component chose to open. One forward per door is the price, and the list of doors is the API.
///
class sensor_2 {
public:
    /// @brief Binds to `bus`, names itself `source`, and takes `reading` as what it currently measures.
    sensor_2(msgbus& bus, std::string source, std::uint64_t reading)
        : out_(bus, std::move(source))
        , reading_(reading) {}

    /// @brief The reading this sensor currently holds.
    [[nodiscard]]
    std::uint64_t reading() const noexcept {
        return reading_;
    }

    /// @brief Publishes the held reading on `where`, on the calling thread.
    void sample(topic where) { out_.publish_sync(where, std::any { reading_ }); }

    /// @brief Starts a clock that samples every `period`, and hands ownership of it back to the caller.
    /// @param where the topic each sample is published on; copied, because the thread outlives the call
    /// @param period the interval between one sample and the next
    /// @return the running clock -- the same `ticker` `sensor_1::start_sampling` returns
    [[nodiscard]]
    ticker start_sampling(topic where, std::chrono::milliseconds period) {
        return ticker([this, where = std::string(where)] { sample(where); }, period);
    }

    /// @brief How many messages this sensor has stamped.
    [[nodiscard]]
    std::uint64_t sent() const noexcept {
        return out_.sent();
    }

private:
    /// @brief The publish-only view the component keeps to itself: the one door out to the bus.
    publisher out_;
    /// @brief What the sensor measures, and therefore what every `sample` carries.
    std::uint64_t reading_ = 0;
};

// --- the same capability, inherited or held ---------------------------------
//
// `publisher`, `subscriber` and `adapter` have no virtual functions and no virtual destructor: they are
// values, and a component may either inherit one or hold one as a member. Both work, and the trade is
// visible in the six types rather than argued in prose.
//
//   * Inheritance hands the whole API over. `device_1` writes one method and gets
//     `subscribe`, `cancel`, `count` and the rest with the base; that is also its cost -- everything is
//     public to a caller holding the derived type, and the base pointer must never be the owner, because
//     the destructor is not virtual.
//   * Composition hands nothing over. `device_2` exposes `subscribe` and nothing else, which is how a
//     component states its doors; the cost is one forward per method it decides to keep.
//
// Each pair is exercised the same way: the two listeners and the two two-way shapes subscribe to
// `shapes/+`, the two speakers and the two two-way shapes publish on it, and every receiver must end up
// with the same four sources. "Both shapes behave the same" is then checkable rather than asserted.

///
/// @brief A subscriber by inheritance, with a change detector: `subscribe` and the rest arrive with the base,
/// and `on_change` adds a callback that runs only when a delivered value differs from the one before it.
/// @details What a device usually wants from a bus that delivers messages: the reading, not the repeats. The
/// component remembers the last value it saw, so a topic published every 100ms with the same number reaches
/// the callback once -- while the registration still records every delivery, which is what makes "delivered"
/// and "changed" two checkable counts rather than one hope.
/// @note The comparison is by payload value, so this shape assumes the `std::uint64_t` the example publishes
/// everywhere. A bus carries `std::any` and cannot compare what it does not know, so the type convention is
/// the component's to hold.
/// @note One state per component, not per registration: two patterns heard by one `device_1` share the last
/// value, and both callbacks run on whichever thread delivered the message.
///
class device_1 : public subscriber {
public:
    /// @brief Binds to `bus` under `name`, which is also the trace annotation.
    device_1(msgbus& bus, std::string name)
        : subscriber(bus)
        , name_(std::move(name)) {}

    /// @brief Installs the callback that runs when a delivered value differs from the previous one.
    /// @param callback where a change goes; it is handed the message that carried the new value
    void on_change(handler callback) { on_change_ = std::move(callback); }

    /// @brief Arms this listener on `pattern`, recording every delivery and reporting the changes.
    /// @note The base's own overloads are here too, brought back by the `using`: this one adds the recorder
    /// and the change detector, while `subscribe(pattern, handler)` still registers any handler at all.
    using subscriber::subscribe;

    [[nodiscard]]
    result<subscription_id, error> subscribe(std::string_view pattern) {
        return subscriber::subscribe(pattern, [this](const message& msg) {
            record(heard_, msg, name_);
            const auto value = std::any_cast<std::uint64_t>(msg.payload_);
            // The first delivery is always a change: there is no previous value for it to repeat.
            if (last_ && *last_ == value) { return; }
            last_ = value;
            ++changes_;
            if (on_change_) { on_change_(msg); }
        });
    }

    /// @brief Everything this listener has heard, in the order it arrived.
    [[nodiscard]]
    const std::vector<arrival>& heard() const noexcept {
        return heard_;
    }

    /// @brief How many deliveries carried a value different from the one before.
    [[nodiscard]]
    std::size_t changes() const noexcept {
        return changes_;
    }

private:
    /// @brief The trace annotation, and the name the listener knows itself by.
    std::string name_;
    /// @brief The callback `on_change` installs; empty until it is called.
    handler on_change_;
    /// @brief The last value delivered, empty until the first delivery.
    std::optional<std::uint64_t> last_;
    /// @brief How many deliveries changed the value.
    std::size_t changes_ = 0;
    /// @brief The deliveries.
    std::vector<arrival> heard_;
};

///
/// @brief A subscriber by composition: only `subscribe` is reachable, and `cancel` is not.
///
class device_2 {
public:
    /// @brief Binds to `bus` under `name`, which is also the trace annotation.
    device_2(msgbus& bus, std::string name)
        : in_(bus)
        , name_(std::move(name)) {}

    /// @brief Arms this listener on `pattern`, recording every delivery.
    [[nodiscard]]
    result<subscription_id, error> subscribe(std::string_view pattern) {
        return in_.subscribe(pattern, [this](const message& msg) { record(heard_, msg, name_); });
    }

    /// @brief Everything this listener has heard, in the order it arrived.
    [[nodiscard]]
    const std::vector<arrival>& heard() const noexcept {
        return heard_;
    }

private:
    /// @brief The registration the component owns and does not hand out.
    subscriber in_;
    /// @brief The trace annotation.
    std::string name_;
    /// @brief The deliveries.
    std::vector<arrival> heard_;
};

///
/// @brief The component that watches values and publishes by rule: an `adapter`, the topics it reports on,
/// the command it answers, and a threshold it notifies on.
/// @details A notifier is the part of a gateway that turns readings into messages worth sending. It is built
/// on `adapter`, so both halves are there: `sample_sync` and `sample_async` publish what the hardware reads,
/// `arm` answers a command, and `watch` subscribes to a value and runs the rules over it. `source()` is the
/// name on everything it sends and `sent()` is that one counter, whichever path the message took.
///
/// The rules are two, and both are about *change*:
///
///   * A value that repeats is not a change: a topic published every 100ms with the same number costs
///     `inspect` one comparison and produces nothing.
///   * A threshold crossing is a notification: the rule publishes `notify/high` when the value reaches
///     `threshold`, and `notify/normal` when it comes back below it. A value that changed but stayed on the
///     same side produced no message -- which is why there are three counters, `seen`, `changes` and
///     `notifications`, and why "delivered", "changed" and "notified" are three different questions.
///
/// Both halves are usable, and neither is mandatory: `backup` below is armed exactly like `line1` and still
/// answers nothing, because the command is addressed to line1's site alone, and neither of the two watches --
/// the section that exercises the rules is the one with a value to watch.
///
/// @note Inherits its bus handle publicly, which is the one place this file breaks from the composition the
/// subscriber side uses: a `notifier` *is* an `adapter` with topics, a command and a rule on top, so a caller
/// may publish through it or subscribe on it without reaching for a second member.
/// @note A rule publishes from inside the handler that triggered it -- see `inspect` for why that is legal.
///
class notifier : public adapter {
public:
    /// @brief Binds to `bus`, names itself `source`, installs under `site`, and rules on `threshold`.
    /// @param bus the bus to publish and subscribe on; must outlive this notifier
    /// @param source the component name stamped on every message
    /// @param site the topic prefix this notifier reports under and takes commands on
    /// @param threshold the value at or above which the rule notifies
    notifier(msgbus& bus, std::string source, std::string_view site, std::uint64_t threshold)
        : adapter(bus, std::move(source))
        , temperature_(std::string(site) + "/device7/sensor/temperature")
        , humidity_(std::string(site) + "/device7/sensor/humidity")
        , command_(std::string(site) + "/device7/cmd/sample")
        , high_(std::string(site) + "/notify/high")
        , normal_(std::string(site) + "/notify/normal")
        , threshold_(threshold) {}

    /// @brief Registers the handler that answers `cmd/sample` for this notifier's own site.
    /// @return the id the bus issued, or why the pattern was rejected; nothing is registered on failure
    [[nodiscard]]
    result<subscription_id, error> arm() {
        return subscribe(command_, [this](const message& command) { answer(command); });
    }

    /// @brief Watches `pattern`: every delivery is compared with the last value and run through the rules.
    /// @return the id the bus issued, or why the pattern was rejected; nothing is registered on failure
    [[nodiscard]]
    result<subscription_id, error> watch(std::string_view pattern) {
        return subscribe(pattern, [this](const message& msg) { inspect(msg); });
    }

    /// @brief Publishes a temperature reading on the calling thread.
    void sample_sync(std::uint64_t reading) {
        const std::any payload { reading };
        bus_log(direction::PUBLISH_SYNC, temperature_, payload);
        publish_sync(temperature_, payload);
    }

    /// @brief Queues a humidity reading for the bus's routing worker.
    /// @return `ACCEPTED` when the bus took the message; `REJECTED` when it did not
    [[nodiscard]]
    publish_status sample_async(std::uint64_t reading) {
        const std::any payload { reading };
        bus_log(direction::PUBLISH_ASYNC, humidity_, payload);
        return publish_async(humidity_, payload);
    }

    /// @brief How many commands this notifier has answered.
    [[nodiscard]]
    std::size_t answered() const noexcept {
        return answered_;
    }

    /// @brief How many values the watch has seen.
    [[nodiscard]]
    std::size_t seen() const noexcept {
        return seen_;
    }

    /// @brief How many of those differed from the value before them.
    [[nodiscard]]
    std::size_t changes() const noexcept {
        return changes_;
    }

    /// @brief How many crossings the rules published.
    [[nodiscard]]
    std::size_t notifications() const noexcept {
        return notifications_;
    }

    /// @brief The value at or above which the rule notifies.
    [[nodiscard]]
    std::uint64_t threshold() const noexcept {
        return threshold_;
    }

private:
    /// @brief Answers one command by publishing the reading it names.
    /// @details Published from inside a handler, which is legal and worth seeing: the router resolved the
    /// command, took its snapshot and released its lock before calling this, so the reply is a delivery
    /// that starts another delivery rather than a lock taken twice.
    /// @note The probe is simulated, so the command carries the value the hardware would return -- which
    /// is what lets the example check the reply instead of inventing a sensor.
    void answer(const message& command) {
        ++answered_;
        bus_log(direction::SUBSCRIBE, command.topic_, command.payload_, source());
        const std::any payload { std::any_cast<std::uint64_t>(command.payload_) };
        bus_log(direction::PUBLISH_SYNC, temperature_, payload);
        publish_sync(temperature_, payload);
    }

    /// @brief Runs the rules over one delivered value, and publishes whatever they decide.
    /// @details Published from inside a handler, for the same reason `answer` does: the router resolved the
    /// message and released its lock before this ran, so the notification is a delivery that starts another
    /// delivery rather than a lock taken twice.
    void inspect(const message& msg) {
        ++seen_;
        const auto value = std::any_cast<std::uint64_t>(msg.payload_);
        // A repeat is not a change, and a change on the same side of the threshold is not a notification.
        if (last_ && *last_ == value) { return; }
        const auto previous = last_;
        last_ = value;
        ++changes_;
        const auto was_above = previous && *previous >= threshold_;
        const auto is_above = value >= threshold_;
        if (was_above == is_above) { return; }

        ++notifications_;
        const auto& where = is_above ? high_ : normal_;
        const std::any payload { value };
        bus_log(direction::PUBLISH_SYNC, where, payload);
        publish_sync(where, payload);
    }

    /// @brief The topics this notifier reports on, takes commands on and notifies on. Owned, because the
    /// views handed to the bus must outlive the calls that name them.
    std::string temperature_;
    std::string humidity_;
    std::string command_;
    std::string high_;
    std::string normal_;
    /// @brief The value at or above which a crossing notifies.
    std::uint64_t threshold_ = 0;
    /// @brief The last value the watch delivered, empty until the first.
    std::optional<std::uint64_t> last_;
    /// @brief Values seen, values changed and notifications published: three different questions.
    std::size_t seen_ = 0;
    std::size_t changes_ = 0;
    std::size_t notifications_ = 0;
    /// @brief How many commands this notifier has answered.
    std::size_t answered_ = 0;
};

///
/// @brief A monitor by composition: the topics it watches, the changes it sees, and where a change goes.
/// @details It subscribes to the topics it cares about, remembers the last value each delivery carried, and
/// -- with `on_change` set -- republishes a changed value on a topic of its own. A repeat is not a change, so
/// a topic that repeats a number costs the monitor one comparison and produces nothing.
///
/// The doors are the ones a composing component chooses: `subscribe` and `say` are here because the shape
/// comparison needs the same surface as the inheriting twin, and `on_change` is the one this component adds.
/// Nothing of the `adapter` it holds leaks out -- `cancel`, `options` and the rest stay inside.
///
class monitor {
public:
    /// @brief Binds to `bus` and names itself `source`.
    monitor(msgbus& bus, std::string source)
        : io_(bus, source)
        , name_(std::move(source)) {}

    /// @brief Arms this monitor on `pattern`, recording every delivery and watching for changes.
    [[nodiscard]]
    result<subscription_id, error> subscribe(std::string_view pattern) {
        return io_.subscribe(pattern, [this](const message& msg) { inspect(msg); });
    }

    /// @brief Publishes one reading on the calling thread.
    void say(topic where, std::uint64_t reading) { io_.publish_sync(where, std::any { reading }); }

    /// @brief Sets the topic a changed value is republished on; empty means changes are only counted.
    void on_change(topic where) { change_topic_ = where; }

    /// @brief How many values differed from the one before.
    [[nodiscard]]
    std::size_t changes() const noexcept {
        return changes_;
    }

    /// @brief Everything this monitor has heard, in the order it arrived.
    [[nodiscard]]
    const std::vector<arrival>& heard() const noexcept {
        return heard_;
    }

    /// @brief How many messages this monitor has stamped, the republished changes included.
    [[nodiscard]]
    std::uint64_t sent() const noexcept {
        return io_.sent();
    }

private:
    /// @brief Records one delivery, and republishes the value when it is a change.
    void inspect(const message& msg) {
        record(heard_, msg, name_);
        const auto value = std::any_cast<std::uint64_t>(msg.payload_);
        // A repeat is not a change, and a change with nowhere to go is only a count.
        if (last_ && *last_ == value) { return; }
        last_ = value;
        ++changes_;
        if (change_topic_.empty()) { return; }

        const std::any payload { value };
        bus_log(direction::PUBLISH_SYNC, change_topic_, payload);
        io_.publish_sync(change_topic_, payload);
    }

    /// @brief Both halves, kept to itself: the component's only door out to the bus.
    adapter io_;
    /// @brief The trace annotation.
    std::string name_;
    /// @brief Where a changed value is republished, empty until `on_change` sets it.
    std::string change_topic_;
    /// @brief The last value delivered, empty until the first delivery.
    std::optional<std::uint64_t> last_;
    /// @brief The deliveries.
    std::vector<arrival> heard_;
    /// @brief How many of them were a change.
    std::size_t changes_ = 0;
};

} // namespace

int main() {
    using namespace ba7lya::msgbus;

    // One bus, and one object of every type this file defines. `arrival` is the value type the logs hold, so
    // its objects live inside those vectors rather than beside them; `ticker` is the clock this scenario
    // starts, and it exists only as long as its handle does.
    msgbus bus;

    std::cout << "=== one object of every type ===\n"
              << "Two sensors publish, two devices and a monitor hear them, a notifier rules on the same values\n"
              << "and answers a command, and one sensor samples on a clock.\n\n";

    sensor_1 probe_1(bus, "sensor-1", 25);
    sensor_2 probe_2(bus, "sensor-2", 40);
    device_1 reader_1(bus, "device-1");
    device_2 reader_2(bus, "device-2");
    notifier guard(bus, "guard", "plant", 30);
    monitor echo(bus, "monitor");

    // --- wiring -------------------------------------------------------------
    //
    // The three receivers watch the same pattern, so they must end up with the same list of sources: that is
    // the inheritance-versus-composition comparison, run as a side effect of the script below instead of as a
    // section of its own. The notifier watches the same values from the other side, and answers its command.
    std::vector<std::uint64_t> changed;
    reader_1.on_change([&changed](const message& msg) {
        changed.push_back(std::any_cast<std::uint64_t>(msg.payload_));
    });

    const auto wire = [](auto& component, std::string_view pattern) {
        const auto id = component.subscribe(pattern);
        if (id) { return true; }
        std::cerr << pattern << " was rejected: " << to_string(id.error().code_) << '\n';
        return false;
    };
    if (!wire(reader_1, "plant/+/value") || !wire(reader_2, "plant/+/value") || !wire(echo, "plant/+/value")) {
        return 1;
    }
    echo.on_change("plant/echo");

    const auto watching = guard.watch("plant/+/value");
    if (!watching) {
        std::cerr << "the notifier's watch pattern was rejected: " << to_string(watching.error().code_) << '\n';
        return 1;
    }
    const auto armed = guard.arm();
    if (!armed) {
        std::cerr << "the notifier's command pattern was rejected: " << to_string(armed.error().code_) << '\n';
        return 1;
    }

    std::cout << plural(bus.subscription_count(), "subscription") << " armed\n\n";

    // --- the script ---------------------------------------------------------
    //
    // Two readings the sensors own, then a line that repeats, crosses the notifier's threshold twice, and
    // settles above it. Every receiver below sees seven messages; only five of them are a change.
    //
    // The line's own messages go through `msgbus::publish_sync` directly, so they carry no source: a
    // provenance stamp is what a `publisher` adds, and these were not published through one. That is why the
    // first two deliveries name a component and the last five do not.
    std::cout << "--- seven messages, one script ---\n";
    probe_1.sample("plant/probe-1/value");
    probe_2.sample("plant/probe-2/value");
    for (const std::uint64_t value : { 25, 25, 40, 40, 31 }) {
        bus.publish_sync("plant/line/value", std::any { value });
    }
    std::cout << "  the receivers each hold " << plural(reader_1.heard().size(), "delivery", "deliveries") << "\n\n";

    // --- a command, answered from inside the handler ------------------------
    //
    // The notifier's `arm` registered a concrete topic, so this publish reaches the one notifier and nothing
    // else. The reply is published from inside the handler -- the router released its lock before calling it
    // -- and carries the notifier's own sequence rather than the command's.
    std::cout << "--- a command the notifier answers ---\n";
    bus.publish_sync("plant/device7/cmd/sample", std::any { std::uint64_t { 23180 } });
    std::cout << "  the notifier answered " << guard.answered() << " and has stamped " << guard.sent()
              << " messages: the reply and its notifications\n\n";

    // --- the clock ----------------------------------------------------------
    //
    // `start_sampling` hands back a `ticker`, whose scope is the clock's. Its samples go out on a topic no
    // receiver above watches, so the counts stay comparable; the handle's destruction stops and joins.
    //
    // The count is read *after* the scope, not before it: the clock may take one more sample between a read
    // inside the scope and the join that ends it, and the final count is what the silence below is measured
    // against.
    std::cout << "--- the clock ---\n";
    const auto before_clock = probe_1.sent();
    {
        const auto clock = probe_1.start_sampling("plant/pulse", std::chrono::milliseconds { 10 });
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds { 5 };
        while (probe_1.sent() - before_clock < 3 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds { 1 });
        }
    } // the handle's destructor requests the stop and joins here
    const auto stamped = probe_1.sent();
    std::this_thread::sleep_for(std::chrono::milliseconds { 30 });
    const auto settled_after = probe_1.sent();
    std::cout << "  the clock took " << (stamped - before_clock) << " samples; 30ms after its handle went out of\n"
              << "  scope the counter still read " << settled_after << "\n\n";

    // --- summary ------------------------------------------------------------
    std::cout << "--- summary ---\n"
              << "  stamped: sensor-1 " << probe_1.sent() << ", sensor-2 " << probe_2.sent() << ", notifier "
              << guard.sent() << ", monitor " << echo.sent() << '\n'
              << "  heard: device-1 " << reader_1.heard().size() << ", device-2 " << reader_2.heard().size()
              << ", monitor " << echo.heard().size() << "; device-1 saw " << reader_1.changes() << " changes\n"
              << "  the notifier saw " << guard.seen() << " values, " << guard.changes() << " changes and published "
              << guard.notifications() << " notifications\n"
              << "  " << bus.routed() << " routed, " << bus.dropped() << " dropped, "
              << plural(bus.subscription_count(), "subscription") << " live\n";

    // --- verify -------------------------------------------------------------
    //
    // Every claim above, in one place, so the printed output can be checked against the assertion rather than
    // believed.
    std::cout << "\n--- verify ---\n";
    auto agreed = true;
    const auto require = [&agreed](bool condition, const std::string& claim) {
        if (condition) { return; }
        std::cerr << "  FAILED: " << claim << '\n';
        agreed = false;
    };

    // The shape comparison: one pattern, three receivers, and the same list of sources in each of them. The
    // last five carry no source at all, because `bus.publish_sync` stamps nothing -- the first two name the
    // sensor whose `publisher` built them.
    const std::vector<std::string> expected { "sensor-1", "sensor-2", "", "", "", "", "" };
    const auto sources_of = [](const std::vector<arrival>& log) {
        std::vector<std::string> names;
        names.reserve(log.size());
        for (const auto& seen : log) { names.push_back(seen.source_); }
        return names;
    };
    require(sources_of(reader_1.heard()) == expected, "the inherited device heard the seven messages, in order");
    require(sources_of(reader_2.heard()) == expected, "the held device heard the same seven through its one forward");
    require(sources_of(echo.heard()) == expected, "the monitor heard the same seven, its two halves behind forwards");

    // A delivery is not a change, and a change is not a notification: three counters, three questions.
    require(
        reader_1.changes() == 5 && changed == std::vector<std::uint64_t> { 25, 40, 25, 40, 31 },
        "device-1 counted five changes, and on_change saw exactly those values"
    );
    require(echo.changes() == 5 && echo.sent() == 5, "the monitor republished the five changes and nothing else");
    require(
        guard.seen() == 7 && guard.changes() == 5 && guard.notifications() == 3,
        "the notifier saw seven values, five changes and three threshold crossings"
    );
    require(guard.answered() == 1, "the command woke the notifier it named, once");

    // The clock, and the fact that the handle's scope is what stops it.
    require(stamped - before_clock >= 3, "the clock took at least three samples");
    require(settled_after == stamped, "the clock stopped when its handle went out of scope");

    // Every message any object stamped was routed exactly once, and nothing was dropped. The six publishes
    // main made itself are the only ones no counter here accounts for: five values and one command.
    require(
        bus.routed() == probe_1.sent() + probe_2.sent() + guard.sent() + echo.sent() + 6,
        "routed() is the sum of the objects' counters plus main's six publishes: every message went through once"
    );
    require(bus.subscription_count() == 5, "five subscriptions are live: three receivers, a watch and a command");
    require(bus.dropped() == 0, "nothing was dropped: no subscription names an executor");

    if (!agreed) { return 1; }
    std::cout << "  every claim above held\n";
    return 0;
}
