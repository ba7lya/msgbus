// SPDX-License-Identifier: MIT
//
// The smallest useful bus: two subscriptions on overlapping patterns, one sensor reading published
// into the hierarchy, and a look at which subscriber saw what.
//
// It is written the way a gateway's own code would be, so it doubles as the answer to "how do I arm a
// bus". The three things worth noticing are marked in the code: where a handler's thread comes from,
// how a pattern's breadth decides what a subscriber hears, and which of the two publish entry points
// to reach for.

#include <any>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common.hxx"
#include "error.hxx"
#include "msgbus.hxx"

namespace {

/// @brief Everything one subscriber saw, so the example can print it at the end.
struct tally {
    std::string name_;
    std::vector<std::string> topics_;
};

/// @brief Prints one delivery, on whichever thread ran the handler.
///
/// The thread id is the whole point of the line: a handler runs on the thread its subscription named,
/// and the id printed beside the topic is the evidence.
void report(const tally& subscriber, const ba7lya::msgbus::message& msg) {
    std::cout << "  [" << subscriber.name_ << "] " << msg.topic_ << "  (thread " << std::this_thread::get_id() << ")\n";
}

} // namespace

int main() {
    using namespace ba7lya::msgbus;

    // A bus is one object. Nothing else needs constructing: the routing worker starts with it.
    msgbus bus;

    tally line_reader { "line-reader", {} };
    tally site_alarm { "site-alarm", {} };

    // --- the subscriptions -------------------------------------------------
    //
    // `+` stands for exactly one level and `#` for the rest of the topic, so the two patterns below
    // overlap on purpose: `device7` matches both, and the example prints both deliveries.
    //
    // Neither names `dispatch_options`, so both run on the thread that publishes. That is the right
    // choice for a handler that only appends to a buffer -- and the wrong one for a handler that
    // reads a socket, which is what `bus.executors().add(...)` and a `dispatch_options` naming that
    // executor are for.
    const auto one_device = bus.subscribe("site/+/device7/sensor/+", [&line_reader](const message& msg) {
        line_reader.topics_.emplace_back(msg.topic_);
        report(line_reader, msg);
    });
    if (!one_device) {
        std::cerr << "the pattern is not valid: " << to_string(one_device.error().code_) << '\n';
        return 1;
    }

    const auto whole_line = bus.subscribe("site/#", [&site_alarm](const message& msg) {
        site_alarm.topics_.emplace_back(msg.topic_);
        report(site_alarm, msg);
    });
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
    std::cout << "publish_sync, one reading:\n";
    bus.publish_sync("site/line1/device7/sensor/temperature", std::any { std::uint64_t { 23175 } });

    // --- an asynchronous publish ------------------------------------------
    //
    // `publish_async` hands the message to the bus's own routing worker and returns immediately, which
    // is what a serial-port reader wants: it goes straight back to the port. The message is still
    // routed -- `ACCEPTED` is a promise, not a hope -- and `flush` is how a caller waits for the
    // promise to be kept. Without the flush the program could exit before the worker ran.
    //
    // A third argument is the payload, and any type will do: the bus never looks inside it. What the
    // handler does with `std::any_cast` is the handler's business, and a subscriber that expects a
    // payload of the wrong type is a contract the bus cannot check.
    std::cout << "\npublish_async, two more readings:\n";
    for (const std::uint64_t reading : { 23176U, 23177U }) {
        const publish_status outcome = bus.publish_async("site/line1/device7/sensor/temperature", std::any { reading });
        if (outcome != publish_status::ACCEPTED) {
            std::cerr << "the bus refused the message\n";
            return 1;
        }
    }
    static_cast<void>(bus.flush());

    // --- what actually happened -------------------------------------------
    //
    // Three publishes, two subscribers, six deliveries: both patterns match all three topics, so
    // each sees three. `#` and a trailing `+` agree here because `+` stands for the last level --
    // the whole topic is one level deeper than `device7/sensor` either way. That coincidence is
    // worth checking against the printed lines above: change the topic's shape and the two counts
    // must part company, or one of the patterns is not doing what its author thought.
    std::cout << "\nsummary\n";
    std::size_t total = 0;
    for (const tally& subscriber : { line_reader, site_alarm }) {
        std::cout << "  " << subscriber.name_ << " saw " << subscriber.topics_.size() << '\n';
        total += subscriber.topics_.size();
    }
    std::cout << "  " << total << " deliveries from " << bus.routed() << " routed messages\n";

    return 0;
}
