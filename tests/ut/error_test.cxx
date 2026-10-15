///
/// @file error_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for the error vocabulary and the `result` alias over it.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026
///
// SPDX-License-Identifier: MIT

#include <expected>
#include <gtest/gtest.h>
#include <string>
#include <tuple>
#include <utility>

#include "common.hxx"
#include "error.hxx"

namespace ba7lya::msgbus {

namespace {

TEST(error_test, make_records_no_position) {
    const auto made = error::make(error_code::INVALID_WILDCARD, "device/**");

    EXPECT_EQ(made.where_, error::npos);
}

TEST(error_test, make_at_records_where_the_failure_starts) {
    const auto made = error::make(error_code::INVALID_WILDCARD, "device/**", 7);

    EXPECT_EQ(made.where_, 7u);
}

TEST(error_test, errors_compare_by_code_text_and_position) {
    const auto lhs = error::make(error_code::EMPTY_LEVEL, "a//b", 2);
    const auto rhs = error::make(error_code::EMPTY_LEVEL, "a//b", 2);
    const auto elsewhere = error::make(error_code::EMPTY_LEVEL, "a//b", 3);

    EXPECT_EQ(lhs, rhs);
    EXPECT_FALSE(lhs == elsewhere);
}

TEST(error_test, differing_codes_are_not_equal) {
    const auto lhs = error::make(error_code::EMPTY_LEVEL, "a//b", 2);
    const auto rhs = error::make(error_code::INVALID_CHARACTER, "a//b", 2);

    EXPECT_FALSE(lhs == rhs);
}

TEST(result_test, a_constructed_value_is_reported_as_success) {
    const result<int, error> outcome(42);

    EXPECT_TRUE(outcome.has_value());
    EXPECT_TRUE(static_cast<bool>(outcome));
    EXPECT_EQ(outcome.value(), 42);
}

TEST(result_test, a_constructed_error_is_reported_as_failure) {
    const result<int, error> outcome(std::unexpected(error::make(error_code::EMPTY_LEVEL, "device//temp", 7)));

    EXPECT_FALSE(outcome.has_value());
    EXPECT_FALSE(static_cast<bool>(outcome));
    EXPECT_EQ(outcome.error().code_, error_code::EMPTY_LEVEL);
    EXPECT_EQ(outcome.error().text_, "device//temp");
    EXPECT_EQ(outcome.error().where_, 7u);
}

TEST(result_test, value_or_returns_the_value_when_present) {
    const result<int, error> outcome(42);

    EXPECT_EQ(outcome.value_or(-1), 42);
}

TEST(result_test, value_or_returns_the_fallback_on_error) {
    const result<int, error> outcome(std::unexpected(error::make(error_code::EMPTY_INPUT, "")));

    EXPECT_EQ(outcome.value_or(-1), -1);
}

TEST(result_test, reading_the_value_of_a_failure_throws) {
    result<int, error> outcome(std::unexpected(error::make(error_code::EMPTY_INPUT, "")));

    EXPECT_THROW(std::ignore = outcome.value(), std::bad_expected_access<error>);
}

TEST(result_test, the_void_case_reports_success_by_default) {
    const result<void, error> outcome;

    EXPECT_TRUE(outcome.has_value());
    EXPECT_TRUE(static_cast<bool>(outcome));
}

TEST(result_test, the_void_case_reports_a_carried_error) {
    const result<void, error> outcome(std::unexpected(error::make(error_code::NO_SUCH_LEVEL, "device/missing", 7)));

    EXPECT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().code_, error_code::NO_SUCH_LEVEL);
}

} // namespace

} // namespace ba7lya::msgbus
