///
/// @file logger.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The one log line per bus event a gateway's trace is built from.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT
//
// One log line per bus event, so a gateway's trace of a topic hierarchy reads the same way everywhere:
//
//   2026-10-17 01:14:13.630 [19952] PUB(sync)   site/line1/device7/sensor/temperature  23175
//   2026-10-17 01:14:13.630 [19952] SUB         site/line1/device7/sensor/temperature  23175  (line-reader)
//   2026-10-17 01:14:13.631 [19952] PUB(async)  site/line1/device7/sensor/temperature  23176
//   2026-10-17 01:14:13.631 [7748]  SUB         site/line1/device7/sensor/temperature  23176  (site-alarm)
//
// The fields are the ones that make a bus trace readable: when it happened, on which thread, in which
// direction, on which topic, and what crossed. The direction field names the three ways a message
// moves -- `PUB(sync)` is one a publish call routed on its own thread, `PUB(async)` one handed to the
// routing worker, `SUB` one arriving at a handler -- and the thread field is what makes the difference
// evidence rather than claim: a synchronous publish and the `SUB` lines it produces carry one id,
// while an asynchronous publish and the `SUB` lines it produces do not.
//
// This header does not log the bus itself: the bus never calls it, and a message that is routed is not
// a message that was printed. It is the formatter a caller wraps its own `publish`/handler code in,
// which is the only place that knows whether an event is worth a line.
//
// Two switches do two different jobs, and neither replaces the other:
//
//   * `ba7lya.msgbus_BUILD_LOG` in CMakeLists.txt decides whether the trace is compiled in at all.
//     Off, this header emits no spdlog call and the library reaches no spdlog symbol, while every call
//     site stays written exactly as it was -- so the two builds differ in cost, not in source. It is
//     the switch a build sets when the program's own logging already covers the bus.
//   * `log_config` / `configure_logging` decides at run time whether the compiled-in trace prints.
//     `{ .enabled_ = false }` puts spdlog's level at `off`, so a silent run costs one comparison per
//     event instead of one line. It is the switch a deployment flips.
//
// The timestamp and the thread id come from `spdlog`'s pattern, and the sink and the writer thread from
// its logger. This header uses `spdlog`'s default logger, so a caller that already configured one keeps
// its sink and only the pattern changes.

#pragma once

// The build-time switch, spelled by CMakeLists.txt for every consumer of the library. The fallback
// covers a translation unit that includes this header without going through that target -- a
// hand-written compile line, a tree that vendored the header -- where logging is on rather than
// silently absent, that being the failure mode nobody would notice.
#if !defined(BA7LYA_MSGBUS_LOG_ENABLED)
#define BA7LYA_MSGBUS_LOG_ENABLED 1
#endif

#include <any>
#include <cstdint>
#include <memory>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>

namespace ba7lya::msgbus {

///
/// @brief The direction the event in a log line travelled, at the granularity a trace needs.
/// @details Publish is split in two because the two publish entry points differ in exactly the way a
/// trace exists to show: which thread did the routing. `PUBLISH_SYNC` routes on the caller's thread,
/// so its deliveries carry the caller's id; `PUBLISH_ASYNC` hands the message to the routing worker,
/// so its deliveries carry the worker's. Reading the tag and the thread together answers "why is this
/// handler on another thread" without a trip to the source.
///
enum class direction {
    PUBLISH_SYNC,  ///< A message entering the bus on the publishing thread.
    PUBLISH_ASYNC, ///< A message handed to the routing worker.
    SUBSCRIBE,     ///< A message reaching one subscriber's handler.
};

///
/// @brief What the logger should do at run time: print, or stay quiet.
///
struct log_config {
    bool enabled_ = true;                                   ///< `false` silences every line.
    spdlog::level::level_enum level_ = spdlog::level::info; ///< The level used while enabled.
};

#if BA7LYA_MSGBUS_LOG_ENABLED

///
/// @brief A payload rendered as text, for the last field of the line.
/// @details The bus is type-blind -- the payload is a `std::any` and nothing on the delivery path looks inside
/// it -- so this is the one place that has to. A type it does not know prints its name instead of
/// nothing, which keeps a wrong-type publish visible in the trace rather than silent.
///
[[nodiscard]]
inline std::string payload_text(const std::any& payload) {
    if (const auto* number = std::any_cast<std::uint64_t>(&payload)) { return std::to_string(*number); }
    if (const auto* number = std::any_cast<std::int64_t>(&payload)) { return std::to_string(*number); }
    if (const auto* text = std::any_cast<std::string>(&payload)) { return *text; }
    if (const auto* text = std::any_cast<const char*>(&payload)) { return *text; }
    if (!payload.has_value()) { return "<no payload>"; }
    return std::string("<") + payload.type().name() + ">";
}

///
/// @brief The logger every line goes through, with the trace pattern installed on first use.
/// @details `%Y-%m-%d %H:%M:%S.%e` is the local wall clock to the millisecond and `%t` the thread id; the rest
/// of the line is built by `bus_log`. The pattern is installed lazily rather than at namespace scope
/// so a translation unit that includes this header without logging anything does not perturb spdlog's
/// global state on the way in.
///
[[nodiscard]]
inline spdlog::logger& bus_logger() {
    static const std::shared_ptr<spdlog::logger> instance = [] {
        std::shared_ptr<spdlog::logger> logger = spdlog::default_logger();
        logger->set_pattern("%Y-%m-%d %H:%M:%S.%e [%t] %v");
        return logger;
    }();
    return *instance;
}

///
/// @brief Applies a logger configuration, which is how a caller turns the trace on or off.
///
inline void configure_logging(const log_config& config = {}) {
    bus_logger().set_level(config.enabled_ ? config.level_ : spdlog::level::off);
}

///
/// @brief Whether a line would be printed -- the readable form of the level `configure_logging` set.
///
[[nodiscard]]
inline bool logging_enabled() {
    return bus_logger().should_log(spdlog::level::info);
}

///
/// @brief Writes one event line.
/// @details The timestamp is taken when the line is written, not read from `message::meta_`: `publish_sync(
/// topic, payload)` and its asynchronous twin build the message with default provenance, so a stamp
/// carried by the message would be the epoch. A gateway that cares about source time stamps its own
/// messages before publishing them, and passes its own stamp in the payload.
/// The whole line goes through one `logger.info`, so the tag, the topic and the payload are formatted
/// together and an interleaved trace cannot split them across threads.
/// @param way whether the message is being published, and how, or delivered
/// @param topic the concrete topic; a delivery's own topic, never the pattern that matched it
/// @param payload what the message carries
/// @param note a trailing annotation: the subscriber's name on a delivery. Publish lines have none --
/// the direction already says sync or async, and repeating it here would be two places to get wrong.
///
inline void bus_log(direction way, std::string_view topic, const std::any& payload, std::string_view note = {}) {
    spdlog::logger& logger = bus_logger();
    if (!logger.should_log(spdlog::level::info)) { return; }

    std::string tail = payload_text(payload);
    if (!note.empty()) {
        tail += "  (";
        tail += note;
        tail += ')';
    }

    std::string_view tag = "SUB";
    switch (way) {
    case direction::PUBLISH_SYNC:  tag = "PUB(sync)"; break;
    case direction::PUBLISH_ASYNC: tag = "PUB(async)"; break;
    case direction::SUBSCRIBE:     tag = "SUB"; break;
    }
    logger.info("{:<10} {}  {}", tag, topic, tail);
}

#else // !BA7LYA_MSGBUS_LOG_ENABLED

// `ba7lya.msgbus_BUILD_LOG=OFF`. The same three names survive, each compiled to nothing, because the
// call sites do not know which build they are in: the trace's cost disappears and its source stays.
// `payload_text` and `bus_logger` are absent rather than empty -- they exist only to reach spdlog, and
// with the trace compiled out nothing may.

///
/// @brief The run-time switch of a build that has no run-time trace: accepted, and ignored.
///
inline void configure_logging(const log_config& = {}) {}

///
/// @brief `false`, always: this build prints no lines whatever `configure_logging` was handed.
///
[[nodiscard]]
inline bool logging_enabled() {
    return false;
}

///
/// @brief The trace call of a build that has no trace: accepted, and compiled away.
///
inline void bus_log(direction, std::string_view, const std::any&, std::string_view = {}) {}

#endif // BA7LYA_MSGBUS_LOG_ENABLED

} // namespace ba7lya::msgbus
