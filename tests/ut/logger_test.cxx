// SPDX-License-Identifier: MIT

///
/// @file logger_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for the trace's injection point: the logger a line goes through.
/// @version 0.1
/// @date 2026-10-18
/// @copyright Copyright (c) 2026 BA7LYA
///

#include <any>
#include <cctype>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>
#include <sstream>
#include <string>

#include "logger.hxx"

namespace ba7lya::msgbus {

namespace {

#if BA7LYA_MSGBUS_LOG_ENABLED

/// @brief A logger that writes into `captured`, with the trace level on.
[[nodiscard]]
std::shared_ptr<spdlog::logger> capturing_logger(std::ostringstream& captured, spdlog::level::level_enum level) {
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(captured);
    auto logger = std::make_shared<spdlog::logger>("test", sink);
    logger->set_level(level);
    return logger;
}

TEST(logger_test, an_injected_logger_receives_the_trace) {
    std::ostringstream captured;
    auto logger = capturing_logger(captured, spdlog::level::info);

    set_logger(logger);
    bus_log(direction::SUBSCRIBE, "site/line1/device7", std::any { std::uint64_t { 7 } }, "device-1");
    logger->flush();
    set_logger(nullptr);

    const auto line = captured.str();
    EXPECT_NE(line.find("SUB"), std::string::npos);
    EXPECT_NE(line.find("site/line1/device7"), std::string::npos);
    EXPECT_NE(line.find("7"), std::string::npos);
    EXPECT_NE(line.find("device-1"), std::string::npos);
}

TEST(logger_test, the_injected_logger_decides_whether_a_line_is_written) {
    std::ostringstream captured;
    auto logger = capturing_logger(captured, spdlog::level::off);

    set_logger(logger);
    EXPECT_FALSE(logging_enabled());
    bus_log(direction::PUBLISH_SYNC, "a/b", std::any { 1 });
    logger->flush();
    set_logger(nullptr);

    EXPECT_TRUE(captured.str().empty());
}

TEST(logger_test, an_injected_logger_takes_the_trace_pattern) {
    std::ostringstream captured;
    auto logger = capturing_logger(captured, spdlog::level::info);

    set_logger(logger);
    bus_log(direction::PUBLISH_SYNC, "a/b", std::any { 1 });
    logger->flush();
    set_logger(nullptr);

    // The pattern is the library's, not the caller's: an injected logger prints the same columns -- a date,
    // then the thread id in brackets -- as the default one.
    const auto line = captured.str();
    ASSERT_GE(line.size(), 10u);
    EXPECT_TRUE(std::isdigit(static_cast<unsigned char>(line[0])));
    EXPECT_EQ(line[4], '-');
    EXPECT_EQ(line[7], '-');
    EXPECT_NE(line.find('['), std::string::npos);
}

#endif // BA7LYA_MSGBUS_LOG_ENABLED

} // namespace

} // namespace ba7lya::msgbus
