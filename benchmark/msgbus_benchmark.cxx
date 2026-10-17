///
/// @file msgbus_benchmark.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The bus, measured: what parsing, matching, resolving and each publish entry point
/// cost a gateway's own publishing threads.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT
//
// The bus, measured.
//
// What is timed here is the publishing path, because that is the one a gateway's adapters sit on: a
// serial-port reader, a Modbus poller and an MQTT bridge each publish from their own thread, and
// what they all pay for is the routing, not the delivery. Every subscription below is therefore
// inline -- the numbers are the routing table's, and putting a handler on an executor would turn
// them into the cost of a thread hop, which is a different question with a different answer.
//
// The file is ordered the way a publish travels: parse, then resolve, then the bus entry points,
// then registration, then the pieces that sit underneath (`router`, `layer_matcher`, `executor`).

#include <any>
#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common.hxx"
#include "dispatcher.hxx"
#include "error.hxx"
#include "executor.hxx"
#include "layer_matcher.hxx"
#include "msgbus.hxx"
#include "router.hxx"
#include "subscription.hxx"

namespace ba7lya::msgbus::bench {

namespace {

/// @brief The concrete topic every benchmark below publishes to.
///
/// Spelled once, because the failure mode this guards against is a benchmark that quietly stops
/// matching: change the pattern in one place and leave the topic behind, and the resolve returns an
/// empty snapshot while the number still looks plausible.
constexpr topic k_device_topic = "site/line1/device7/sensor/telemetry";

/// @brief A payload the handlers may look at, so the delivery cannot be optimised away.
///
/// A `std::uint64_t` in a `std::any` rather than a small integer: the bus's own interface is
/// `std::any`, and something smaller would measure the small-buffer path instead of the payload.
[[nodiscard]]
std::any sample_payload(std::uint64_t seed) {
    return std::any { seed };
}

/// @brief Registers `count` inline subscriptions on `pattern`, and reports refusal to the caller.
///
/// A refused subscription here means the benchmark's own fixture is malformed, not that the bus
/// misbehaved, so it is reported rather than asserted away: a silently empty fan-out would turn
/// every number in this file into a measurement of the parse alone.
///
/// The handles are collected before the function returns rather than left as loop temporaries, and
/// that is not tidiness: `subscribe` returns a handle that cancels its subscription when destroyed,
/// so a temporary dropped at the end of its iteration -- and every one of them would be, by the time
/// this loop finished -- leaves the bus with an empty table and the benchmark measuring nothing. The
/// caller holds the vector for the length of its own run.
///
/// The failure is returned rather than reported here because Google Benchmark 1.9.5 has no free
/// reporting entry point -- `SkipWithError` is a member of `State`, which this helper does not hold.
/// Returning it also reads better: `false` means "do not measure what you were about to measure",
/// and every caller below turns it into `state.SkipWithError`, whose message is what the reporter
/// prints in place of a number.
[[nodiscard]]
bool add_inline_subscriptions(
    msgbus& bus, std::string_view pattern, std::size_t count, std::vector<subscription>& armed
) {
    armed.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        auto added = bus.subscribe(pattern, [](const message&) {});
        if (!added) { return false; }
        armed.push_back(*std::move(added));
    }
    return true;
}

} // namespace

/// @brief The subscription pattern covering the first `depth` levels of the topic above.
///
/// The bus matches level by level, so a benchmark parameterised by depth has to spell the pattern
/// out rather than hand a number to the matcher.
[[nodiscard]]
std::string_view pattern_for_depth(std::size_t depth) {
    switch (depth) {
    case 1:  return "site";
    case 2:  return "site/line1";
    case 3:  return "site/line1/device7";
    case 4:  return "site/line1/device7/sensor";
    default: return k_device_topic;
    }
}

/// @brief Splitting a topic into levels: the floor under every publish number in this file.
///
/// Every publish does this exactly once, and it allocates a `std::string` per level, so this is
/// where the allocator shows up. Nothing else here can be faster than it.
static void bm_parse_topic(::benchmark::State& state) {
    for (auto _ : state) {
        const auto parsed = parse_topic(k_device_topic);
        if (!parsed) {
            state.SkipWithError("the benchmark's own topic was rejected");
            return;
        }
        ::benchmark::DoNotOptimize(parsed->size());
    }
}

BENCHMARK(bm_parse_topic)->Unit(::benchmark::kNanosecond);

/// @brief Registering a pattern and dropping it again, on a matcher of `depth` levels already.
///
/// An insert plus a prune per iteration, which is what a gateway that re-subscribes on reconnect
/// pays. The pattern is a leaf at the full depth so the insert cannot stop early on a shared prefix.
static void bm_matcher_cycle(::benchmark::State& state) {
    layer_matcher matcher;
    for (std::int64_t level = 1; level <= state.range(0); ++level) {
        static_cast<void>(matcher.add(pattern_for_depth(static_cast<std::size_t>(level)), 1));
    }

    subscription_id next = 100;
    for (auto _ : state) {
        const auto added = matcher.add("site/#", next);
        if (!added) {
            state.SkipWithError("the benchmark's own subscription pattern was rejected");
            return;
        }
        static_cast<void>(matcher.remove(*added));
        ++next;
    }
}

BENCHMARK(bm_matcher_cycle)->Arg(1)->Arg(5)->Unit(::benchmark::kMicrosecond);

/// @brief The trie walk alone, with the result discarded as it comes back.
///
/// `layer_matcher` is the pattern index rather than the public API, and the walk is the whole of
/// what it does on the read path. What the walk costs to allocate is part of the number: the match
/// now returns its own set, so the allocation is inside the measurement rather than hoisted out of
/// it the way a caller's reused set would be.
static void bm_matcher_match(::benchmark::State& state) {
    layer_matcher matcher;
    for (std::int64_t i = 0; i < state.range(0); ++i) {
        const auto added = matcher.add("site/#", static_cast<subscription_id>(i + 1));
        if (!added) {
            state.SkipWithError("the benchmark's own subscription pattern was rejected");
            return;
        }
    }
    const auto parsed = parse_topic(k_device_topic);
    if (!parsed) {
        state.SkipWithError("the benchmark's own topic was rejected");
        return;
    }

    for (auto _ : state) { ::benchmark::DoNotOptimize(matcher.match(parsed->texts())); }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(bm_matcher_match)->Arg(1)->Arg(64)->Arg(512)->Unit(::benchmark::kNanosecond);

/// @brief Resolving one parsed topic through the router, with no delivery at all.
///
/// Separates the two halves of `publish_sync`: the walk and the snapshot, with the fan-out and the
/// `std::any` removed. The router is the piece the bus wraps a thread and a queue around, so this is
/// its own cost, and any change to the snapshot's shape has a number here that moves.
static void bm_router_resolve(::benchmark::State& state) {
    router routes;
    // The handles outlive the loop for the reason the table has no fan-out in this benchmark: the
    // routing cost is what is measured, and a table emptied before the first `resolve` would be the
    // parse alone, timed repeatedly.
    std::vector<subscription> armed;
    for (std::int64_t i = 0; i < state.range(0); ++i) {
        auto added = routes.subscribe("site/#", [](const message&) {});
        if (!added) {
            state.SkipWithError("the benchmark's own subscription pattern was rejected");
            return;
        }
        armed.push_back(*std::move(added));
    }
    const auto parsed = parse_topic(k_device_topic);
    if (!parsed) {
        state.SkipWithError("the benchmark's own topic was rejected");
        return;
    }

    for (auto _ : state) { ::benchmark::DoNotOptimize(routes.resolve(parsed->levels())); }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(bm_router_resolve)->Arg(1)->Arg(64)->Arg(512)->Unit(::benchmark::kNanosecond);

/// @brief `publish_sync` through a trie of `#` patterns: parse, resolve, deliver inline, return.
///
/// The baseline sink, and the number a gateway's synchronous path lives at. The handlers are
/// deliberately empty -- a handler that did work would put its cost inside a routing measurement.
static void bm_publish_sync_fanout(::benchmark::State& state) {
    msgbus bus;
    std::vector<subscription> armed;
    if (!add_inline_subscriptions(bus, "site/#", static_cast<std::size_t>(state.range(0)), armed)) {
        state.SkipWithError("the benchmark's own subscription pattern was rejected");
        return;
    }

    for (auto _ : state) { bus.publish_sync(k_device_topic, sample_payload(1)); }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(bm_publish_sync_fanout)->Arg(1)->Arg(8)->Arg(64)->Arg(512)->Unit(::benchmark::kMicrosecond);

/// @brief `publish_sync` with a subscription that does **not** match, which is the common case.
///
/// A gateway publishes far more topics than anything subscribes to, so this is the realistic number
/// and the one above is the ceiling. Read together they bracket the walk: the difference between
/// them is the fan-out.
static void bm_publish_sync_no_match(::benchmark::State& state) {
    msgbus bus;
    std::vector<subscription> armed;
    for (std::int64_t i = 0; i < state.range(0); ++i) {
        auto added = bus.subscribe("other/line1/device7/sensor/telemetry", [](const message&) {});
        if (!added) {
            state.SkipWithError("the benchmark's own subscription pattern was rejected");
            return;
        }
        armed.push_back(*std::move(added));
    }

    for (auto _ : state) { bus.publish_sync(k_device_topic, sample_payload(1)); }
}

BENCHMARK(bm_publish_sync_no_match)->Arg(1)->Arg(64)->Arg(512)->Unit(::benchmark::kMicrosecond);

/// @brief `publish_sync` against topics of increasing depth, one matching subscription at each.
///
/// The trie walks one node per level, so this is where a deeper hierarchy shows up. A subscription is
/// registered at every depth so each row has the same fan-out and only the walk length varies.
static void bm_publish_sync_depth(::benchmark::State& state) {
    msgbus bus;
    std::vector<subscription> armed;
    const auto depth = state.range(0);
    for (std::int64_t level = 1; level <= depth; ++level) {
        if (!add_inline_subscriptions(bus, pattern_for_depth(static_cast<std::size_t>(level)), 1, armed)) {
            state.SkipWithError("the benchmark's own subscription pattern was rejected");
            return;
        }
    }

    for (auto _ : state) { bus.publish_sync(k_device_topic, sample_payload(1)); }
    state.SetItemsProcessed(state.iterations() * depth);
}

BENCHMARK(bm_publish_sync_depth)->Arg(1)->Arg(3)->Arg(5)->Unit(::benchmark::kMicrosecond);

/// @brief `publish_async` followed by a drain: what an adapter's thread pays per message when it
/// hands the routing to the bus's worker instead of doing it itself.
///
/// The drain is inside the timed loop and is not optional. A loop that only submitted would measure
/// a queue insert, and it would also fill the queue without bound, so it would end up timing the
/// drop policy rather than the bus. Draining each message makes this the honest "one publish, one
/// route" number -- what a gateway's asynchronous throughput actually is.
static void bm_publish_async_round_trip(::benchmark::State& state) {
    msgbus bus;
    std::vector<subscription> armed;
    if (!add_inline_subscriptions(bus, "site/#", static_cast<std::size_t>(state.range(0)), armed)) {
        state.SkipWithError("the benchmark's own subscription pattern was rejected");
        return;
    }

    for (auto _ : state) {
        const auto outcome = bus.publish_async(k_device_topic, sample_payload(1));
        if (outcome != publish_status::ACCEPTED) {
            state.SkipWithError("the routing queue refused a message");
            return;
        }
        ::benchmark::DoNotOptimize(bus.flush());
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(bm_publish_async_round_trip)->Arg(1)->Arg(64)->Unit(::benchmark::kMicrosecond);

/// @brief The cost of a publish the queue has no room for, under `DROP_OLDEST`.
///
/// The queue is bounded at one and never drained inside the loop, so every publish after the first
/// evicts its predecessor. This is the only benchmark here whose subject is the queue's bound rather
/// than the routing table, and it is the price a slow subscriber puts on its publisher.
static void bm_publish_async_drop_oldest(::benchmark::State& state) {
    msgbus bus { matcher_type::LAYER, 1, back_pressure::DROP_OLDEST };
    std::vector<subscription> armed;
    if (!add_inline_subscriptions(bus, "site/#", 1, armed)) {
        state.SkipWithError("the benchmark's own subscription pattern was rejected");
        return;
    }

    // The return value is kept rather than cast away: the eviction this benchmark exists to measure
    // is reported on it and nowhere else, so `DoNotOptimize` on it is also what keeps the eviction
    // from being optimised out of the loop.
    for (auto _ : state) { ::benchmark::DoNotOptimize(bus.publish_async(k_device_topic, sample_payload(1))); }
    static_cast<void>(bus.flush());
}

BENCHMARK(bm_publish_async_drop_oldest)->Unit(::benchmark::kMicrosecond);

/// @brief Subscribing one pattern on a bus that already holds `subscribers` others, then dropping it.
///
/// An `unordered_map` insert plus a trie insert under the router's exclusive lock, so this is the
/// number that says whether a reconnect re-subscribing a few hundred topics is a problem.
static void bm_subscribe_unsubscribe(::benchmark::State& state) {
    msgbus bus;
    std::vector<subscription> armed;
    if (!add_inline_subscriptions(bus, "site/#", static_cast<std::size_t>(state.range(0)), armed)) {
        state.SkipWithError("the benchmark's own subscription pattern was rejected");
        return;
    }

    for (auto _ : state) {
        auto added = bus.subscribe("site/line1/#", [](const message&) {});
        if (!added) {
            state.SkipWithError("the benchmark's own subscription pattern was rejected");
            return;
        }
        // Cancelled through the handle rather than through a second bus call: `bm_subscribe_unsubscribe`
        // measures a registration's whole life, and the handle is what a caller would hold.
        static_cast<void>(added->cancel());
    }
}

BENCHMARK(bm_subscribe_unsubscribe)->Arg(0)->Arg(64)->Arg(512)->Unit(::benchmark::kMicrosecond);

/// @brief A gateway's real shape: many concrete topics registered, published to in rotation.
///
/// Thousands of leaf patterns and a handful of broad ones is not what the fan-out benchmarks above
/// model -- those hold the topic fixed and vary one subscription. Here the subscriptions are all
/// distinct leaves and the topic moves, so a change in the trie's branching has a number that moves
/// rather than one drowned by the fan-out.
static void bm_wide_table(::benchmark::State& state) {
    msgbus bus;
    const auto count = static_cast<std::size_t>(state.range(0));

    // Holding the strings, not `topic` values: each name below is built from an integer, so a vector
    // of views would keep only the address of text that dies at the end of its iteration.
    std::vector<std::string> topics;
    topics.reserve(count);
    std::vector<subscription> armed;
    armed.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        topics.push_back("site/line" + std::to_string(i) + "/sensor/temp");
        auto added = bus.subscribe(topics.back(), [](const message&) {});
        if (!added) {
            state.SkipWithError("the benchmark's own subscription pattern was rejected");
            return;
        }
        armed.push_back(*std::move(added));
    }

    std::size_t next = 0;
    for (auto _ : state) {
        bus.publish_sync(topics[next % topics.size()], sample_payload(1));
        ++next;
    }
}

BENCHMARK(bm_wide_table)->Arg(64)->Arg(1024)->Unit(::benchmark::kMicrosecond);

/// @brief The per-delivery instrumentation the dispatcher consults, isolated from the delivery.
///
/// The dispatcher's own cost is otherwise invisible: it sits between the resolve above and the
/// handler inside the fan-out, so a change there would be lost in the noise. These three are its
/// read-only entry points, and they are what a hot-path handler actually calls.
static void bm_executor_probe(::benchmark::State& state) {
    executor runner;

    for (auto _ : state) {
        ::benchmark::DoNotOptimize(runner.is_on_worker_thread());
        ::benchmark::DoNotOptimize(runner.pending());
        ::benchmark::DoNotOptimize(runner.stats());
    }
}

BENCHMARK(bm_executor_probe)->Unit(::benchmark::kNanosecond);

} // namespace ba7lya::msgbus::bench

BENCHMARK_MAIN();
