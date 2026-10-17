///
/// @file basic_matching.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The smallest useful bus: two overlapping patterns, one reading published, and who
/// heard what.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT
//
// The smallest useful bus: two subscriptions on overlapping patterns, one sensor reading published
// into the hierarchy, and a look at which subscriber saw what.
//
// It is written the way a gateway's own code would be, so it doubles as the answer to "how do I arm a
// bus". The three things worth noticing are marked in the code: where a handler's thread comes from,
// how a pattern's breadth decides what a subscriber hears, and which of the two publish entry points
// to reach for.
//
// Every event goes through `logger.hxx`, so the trace reads as timestamp, thread, direction, topic,
// payload -- and the thread field is what answers the question the comments below only raise. The
// same lines stop being printed once `configure_logging` is given `enabled_ = false`, which the last
// section below shows without changing a single call site.

#include <any>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "common.hxx"
#include "error.hxx"
#include "logger.hxx"
#include "msgbus.hxx"

namespace {

using namespace ba7lya::msgbus;

/// @brief Everything one subscriber saw, so the example can print it at the end.
struct tally {
    std::string name_;
    std::vector<std::string> topics_;
};

/// @brief Records one delivery and logs it, on whichever thread ran the handler.
///
/// The thread field is the whole point of the line: a handler runs on the thread its subscription
/// named, and the id logged beside the topic is the evidence. Neither subscription below names an
/// executor, so every `SUB` line carries the id of the `PUB` line above it.
void report(tally& subscriber, const ba7lya::msgbus::message& msg) {
    subscriber.topics_.emplace_back(msg.topic_);
    bus_log(direction::SUBSCRIBE, msg.topic_, msg.payload_, subscriber.name_);
}

} // namespace

int main() {
    using namespace ba7lya::msgbus;

    // A bus is one object. Nothing else needs constructing: the routing worker starts with it.
    msgbus bus;

    tally line_reader { "line-reader", {} };
    tally site_alarm { "site-alarm", {} };

    std::cout << "=== basic matching ===\n"
              << "Two overlapping patterns, three readings, and the thread each handler ran on.\n\n";

    // --- the subscriptions -------------------------------------------------
    //
    // `+` stands for exactly one level and `#` for the rest of the topic, so the two patterns below
    // overlap on purpose: `device7` matches both, and the example prints both deliveries.
    //
    // Neither names `dispatch_options`, so both run on the thread that publishes. That is the right
    // choice for a handler that only appends to a buffer -- and the wrong one for a handler that
    // reads a socket, which is what `bus.executors().add(...)` and a `dispatch_options` naming that
    // executor are for.
    //
    // Each handle is named rather than left as a temporary, and that is what keeps its subscription
    // armed: `subscribe` returns a handle that cancels on destruction, so a bare `bus.subscribe(...)`
    // statement would unsubscribe before the first publish below. These two live to the end of `main`.
    const auto one_device
        = bus.subscribe("site/+/device7/sensor/+", [&line_reader](const message& msg) { report(line_reader, msg); });
    if (!one_device) {
        std::cerr << "the pattern is not valid: " << to_string(one_device.error().code_) << '\n';
        return 1;
    }

    const auto whole_line = bus.subscribe("site/#", [&site_alarm](const message& msg) { report(site_alarm, msg); });
    if (!whole_line) {
        std::cerr << "the pattern is not valid: " << to_string(whole_line.error().code_) << '\n';
        return 1;
    }

    std::cout << bus.subscription_count() << " subscriptions armed\n\n";

    // --- a synchronous publish --------------------------------------------
    //
    // `publish_sync` resolves and delivers on this thread before returning, so by the next line the
    // deliveries above have already run. Reach for it when the publisher wants to be sure the message
    // is routed -- a command being acknowledged, a test, a shutdown path.
    std::cout << "--- publish_sync, one reading ---\n";
    const std::any first { std::uint64_t { 23175 } };
    bus_log(direction::PUBLISH_SYNC, "site/line1/device7/sensor/temperature", first);
    bus.publish_sync("site/line1/device7/sensor/temperature", first);
    std::cout << '\n';

    // --- an asynchronous publish ------------------------------------------
    //
    // `publish_async` hands the message to the bus's own routing worker and returns immediately, which
    // is what a serial-port reader wants: it goes straight back to the port. `ACCEPTED` means the
    // message is queued rather than routed -- the worker has not seen it yet -- and `flush` is how a
    // caller waits until it has. Without the flush the program could exit before the worker ran.
    //
    // The two `PUB` lines below are logged before either handler has run, and both carry this thread's
    // id; the four `SUB` lines that follow carry the routing worker's, all on one id because one
    // worker runs them one after another.
    //
    // A third argument is the payload, and any type will do: the bus never looks inside it. What the
    // handler does with `std::any_cast` is the handler's business, and a subscriber that expects a
    // payload of the wrong type is a contract the bus cannot check.
    std::cout << "--- publish_async, two more readings ---\n";
    for (const std::uint64_t reading : { 23176U, 23177U }) {
        const std::any payload { reading };
        const publish_status outcome = bus.publish_async("site/line1/device7/sensor/temperature", payload);
        bus_log(direction::PUBLISH_ASYNC, "site/line1/device7/sensor/temperature", payload);
        if (outcome != publish_status::ACCEPTED) {
            std::cerr << "the bus refused the message\n";
            return 1;
        }
    }
    static_cast<void>(bus.flush());
    std::cout << '\n';

    // --- what actually happened -------------------------------------------
    //
    // Three publishes, two subscribers, six deliveries: both patterns match all three topics, so
    // each sees three. `#` and a trailing `+` agree here because `+` stands for the last level --
    // the whole topic is one level deeper than `device7/sensor` either way. That coincidence is
    // worth checking against the printed lines above: change the topic's shape and the two counts
    // must part company, or one of the patterns is not doing what its author thought.
    std::cout << "--- summary ---\n";
    std::size_t total = 0;
    for (const tally& subscriber : { line_reader, site_alarm }) {
        std::cout << "  " << subscriber.name_ << " saw " << subscriber.topics_.size() << '\n';
        total += subscriber.topics_.size();
    }
    std::cout << "  " << total << " deliveries from " << bus.routed() << " routed messages\n";

    // --- turning the trace off --------------------------------------------
    //
    // The switch is a logger configuration, not a second call site: the same `bus_log` calls run, and
    // with `enabled_` false they return after one level check. Four more deliveries happen below and
    // print nothing, because a gateway in production logs what it is told to log and the tracing
    // itself must not be the thing that decides.
    std::cout << "\n--- logging off ---\n";
    configure_logging(log_config { .enabled_ = false });
    bus.publish_sync("site/line1/device7/sensor/temperature", std::any { std::uint64_t { 23178 } });
    std::cout << "  logging_enabled() is " << std::boolalpha << logging_enabled()
              << ", and the publish above printed nothing\n";
    std::cout << "  line-reader saw " << line_reader.topics_.size() << ", site-alarm saw " << site_alarm.topics_.size()
              << " -- the deliveries happened either way\n";
    configure_logging();

    return 0;
}
