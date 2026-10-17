///
/// @file wildcards.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The wildcard vocabulary one pattern at a time: five overlapping subscriptions,
/// three publishes, and a matrix of who heard what.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT
//
// The wildcard vocabulary, one level at a time: five subscriptions whose patterns overlap on purpose,
// three publishes, and a matrix of who heard what. Where `basic_matching.cxx` shows the smallest bus
// that works, this one shows why a pattern's shape decides its reach -- `+` takes exactly one level,
// `#` takes everything below it, and a literal takes only itself.

#include <algorithm>
#include <any>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common.hxx"
#include "error.hxx"
#include "logger.hxx"
#include "msgbus.hxx"
#include "subscription.hxx"

namespace {

/// @brief One subscriber's pattern, its name, and everything it heard.
struct tally {
    std::string name_;
    std::string pattern_;
    std::vector<std::string> topics_;
};

/// @brief The three topics, chosen so each pattern's reach is visible.
///
/// `site/line1/device7/sensor/temperature` is the full depth. `site/line1/device7` stops above the
/// sensor levels, so `#` still reaches it while a pattern needing those levels does not. And
/// `site/backup/device7/sensor/temperature` differs from the first in one level, which is enough to
/// cut off every pattern that spells `line1`.
///
constexpr auto topic_deep = "site/line1/device7/sensor/temperature";
constexpr auto topic_shallow = "site/line1/device7";
constexpr auto topic_other = "site/backup/device7/sensor/temperature";

/// @brief Everything the three publishes carry, cycled so a line names which publish it belongs to.
constexpr std::uint64_t readings[] { 23175, 23176, 23177 };

/// @brief The three topics, in the order every loop below walks them.
constexpr std::string_view topics[] { topic_deep, topic_shallow, topic_other };

/// @brief How many of the three topics a pattern matches, measured on a bus of its own.
///
/// This is the same matcher the bus below uses, reached through the public `subscribe` / `unsubscribe`
/// pair rather than inferred from the printed lines. It runs on a second bus, not the one being
/// measured: the probe publishes, and against the measured bus those three publishes would land on the
/// armed subscribers and grow every tally by the pattern just probed. A throwaway in-process bus asks
/// the question in isolation for the price of a default-constructed object.
///
std::size_t count_reach(ba7lya::msgbus::msgbus& bus, std::string_view pattern) {
    std::size_t seen = 0;
    auto id = bus.subscribe(pattern, [&seen](const ba7lya::msgbus::message&) { ++seen; });
    if (!id) { return 0; }
    for (const std::string_view topic : topics) { bus.publish_sync(topic, std::any { readings[0] }); }
    // Cancelled through the handle, which is the whole point of the handle: the probe bus is reused
    // for every pattern below, and a registration left behind would answer the next question too.
    static_cast<void>(id->cancel());
    return seen;
}

} // namespace

int main() {
    using namespace ba7lya::msgbus;

    msgbus bus;

    std::cout << "=== wildcards ===\n"
              << "`+` takes one level, `#` takes the rest, a literal takes only itself.\n\n";

    // --- the five patterns -------------------------------------------------
    //
    // Ordered from the narrowest to the widest, which is also the order of the table at the end.
    //
    //   site/line1/device7/sensor/temperature   one exact topic, no wildcard at all
    //   site/+/device7/sensor/temperature       `+` stands for one level: line1 or backup, not both
    //   site/line1/#                            `#` stands for the rest, so depth is free
    //   site/+/device7/#                        both wildcards, anchored on `device7`
    //   site/#                                  everything under site, at any depth
    //
    // Every pattern is registered before anything is published, so each publish below reaches the
    // same five subscriptions and the counts are comparable.
    tally exact { "exact", "site/line1/device7/sensor/temperature", {} };
    tally one_level { "one-level", "site/+/device7/sensor/temperature", {} };
    tally rest_of_line { "rest-of-line", "site/line1/#", {} };
    tally both { "both", "site/+/device7/#", {} };
    tally everything { "everything", "site/#", {} };

    const std::vector<std::pair<tally*, const char*>> armed {
        std::pair { &exact,        exact.pattern_.c_str()        },
        std::pair { &one_level,    one_level.pattern_.c_str()    },
        std::pair { &rest_of_line, rest_of_line.pattern_.c_str() },
        std::pair { &both,         both.pattern_.c_str()         },
        std::pair { &everything,   everything.pattern_.c_str()   },
    };

    // A delivery line names the subscriber that ran, never the pattern that matched it: the pattern
    // is the subscription's business and the concrete topic is the message's.
    //
    // The handles are kept in `armed_handles` rather than dropped at the end of each iteration, and
    // that is what keeps the subscribers armed: `subscribe` returns an owning handle, so a loop-local
    // one is destroyed -- and its subscription cancelled -- before the next pattern is even asked for.
    // The vector is declared after the bus and destroyed before it, so every handle outlives the bus.
    std::vector<subscription> armed_handles;
    armed_handles.reserve(armed.size());
    for (const auto& [subscriber, pattern] : armed) {
        auto id = bus.subscribe(pattern, [subscriber](const message& msg) {
            subscriber->topics_.emplace_back(msg.topic_);
            bus_log(direction::SUBSCRIBE, msg.topic_, msg.payload_, subscriber->name_);
        });
        if (!id) {
            std::cerr << pattern << " is not a valid pattern: " << to_string(id.error().code_) << '\n';
            return 1;
        }
        armed_handles.push_back(*std::move(id));
    }

    // A sixth pattern is deliberately invalid: `#` must end a pattern, so it cannot stand in the
    // middle of one. The grammar rejects it rather than matching nothing, which is the difference
    // between a typo the caller hears about and a subscription that silently never fires.
    if (const auto bad = bus.subscribe("site/#/device7", [](const message&) {})) {
        std::cerr << "site/#/device7 should not be a valid pattern\n";
        return 1;
    }
    else { std::cout << "site/#/device7 rejected: " << to_string(bad.error().code_) << "\n\n"; }

    std::cout << bus.subscription_count() << " subscriptions armed\n\n";

    // --- the publishes -----------------------------------------------------
    //
    // One per topic, all synchronous, so the lines below appear in the order printed here and the
    // matrix at the end needs no flush to be complete.
    std::cout << "--- publishes ---\n";
    for (std::size_t i = 0; i < 3; ++i) {
        const std::any payload { readings[i] };
        bus_log(direction::PUBLISH_SYNC, topics[i], payload);
        bus.publish_sync(topics[i], payload);
        std::cout << '\n';
    }

    // --- who heard what ----------------------------------------------------
    //
    // A delivery per (pattern, matching topic). Reading down a row says how wide that pattern is;
    // reading across a column says which patterns the topic reached. The `+`/`#` combination is the
    // one worth checking twice: it reaches the deep and the shallow topic under `line1` and the deep
    // topic under `backup`, because `+` takes any one level and `#` then takes the rest -- including
    // none at all.
    std::cout << "--- reach ---\n";
    std::size_t total = 0;
    for (const auto& [subscriber, pattern] : armed) {
        std::cout << "  " << subscriber->pattern_ << '\n';
        for (const std::string_view topic : { topic_deep, topic_shallow, topic_other }) {
            const bool heard
                = std::find(subscriber->topics_.begin(), subscriber->topics_.end(), topic) != subscriber->topics_.end();
            std::cout << "    " << (heard ? "hit " : "  - ") << "  " << topic << '\n';
        }
        std::cout << "    " << subscriber->topics_.size() << " deliveries\n";
        total += subscriber->topics_.size();
    }
    std::cout << "\n  " << total << " deliveries from " << bus.routed() << " routed messages\n";

    // The table above is a claim about the matcher, so make it falsifiable: for each pattern, ask a
    // bus of its own the same question and compare. A pattern whose printed row disagrees with its
    // probed reach is a pattern that means something other than its author thought.
    std::cout << "\n--- verify ---\n";
    msgbus probe; // the probe's own bus: its publishes must not reach the subscribers above
    bool agreed = true;
    for (const auto& [subscriber, pattern] : armed) {
        const std::size_t reached = count_reach(probe, pattern);
        if (reached != subscriber->topics_.size()) {
            std::cerr << "  " << pattern << ": saw " << subscriber->topics_.size() << " deliveries above but reaches "
                      << reached << " of the three topics\n";
            agreed = false;
            continue;
        }
        std::cout << "  " << pattern << " reaches " << reached << " of the three topics\n";
    }
    if (!agreed) { return 1; }

    // And the empty case: a pattern that matches nothing is still a valid subscription, so a caller
    // cannot tell a typo from a topic that has not been published yet by the pattern's validity alone.
    std::cout << "  site/line9/# reaches " << count_reach(probe, "site/line9/#") << " of the three topics\n";

    return 0;
}
