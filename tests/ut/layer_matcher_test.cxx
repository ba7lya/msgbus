///
/// @file layer_matcher_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for the topic grammar and the matcher built on it.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#include <algorithm>
#include <cstddef>
#include <expected>
#include <gtest/gtest.h>
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "layer_matcher.hxx"

namespace ba7lya::msgbus {

namespace {

/// @brief Splits a topic or pattern and reports the levels' spellings, so one expectation covers the split.
[[nodiscard]]
std::vector<std::string> spellings(const topic_path& path) {
    std::vector<std::string> out;
    for (const auto& level : path.levels_) { out.emplace_back(level.text()); }
    return out;
}

/// @brief Asserts that parsing `text` fails the way `code` says, at `where`.
void expect_parse_error(std::string_view text, bool allow_wildcards, error_code code, std::size_t where) {
    const auto parsed = parse_topic_levels(text, allow_wildcards);

    ASSERT_FALSE(parsed.has_value()) << "expected " << text << " to be rejected";
    EXPECT_EQ(parsed.error().code_, code) << "on " << text;
    EXPECT_EQ(parsed.error().where_, where) << "on " << text;
    EXPECT_EQ(parsed.error().text_, text) << "on " << text;
}

/// @brief The subscriptions matching `topic`, sorted so the assertion reads as a set.
[[nodiscard]]
std::vector<subscription_id> matched_ids(const layer_matcher& matcher, std::string_view topic) {
    const auto matched = matcher.match_topic(topic);
    if (!matched) {
        ADD_FAILURE() << "expected " << topic << " to be a topic";
        return {};
    }
    std::vector<subscription_id> ids(matched.value().begin(), matched.value().end());
    std::sort(ids.begin(), ids.end());
    return ids;
}

/// @brief Registers `id` under `pattern`, which the test already knows is a valid one.
void subscribe(layer_matcher& matcher, std::string_view pattern, subscription_id id) {
    const auto added = matcher.add(pattern, id);
    ASSERT_TRUE(added.has_value()) << "on " << pattern;
}

/// @brief Asserts that `topic` reaches exactly `expected` subscriptions.
void expect_match(
    const layer_matcher& matcher, std::string_view topic, std::initializer_list<subscription_id> expected
) {
    std::vector<subscription_id> wanted(expected.begin(), expected.end());
    std::sort(wanted.begin(), wanted.end());
    EXPECT_EQ(matched_ids(matcher, topic), wanted) << "on " << topic;
}

// --- the grammar -----------------------------------------------------------------------------------

TEST(parse_test, a_pattern_splits_on_separators) {
    const auto parsed = parse_pattern("device/room1/temp");

    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(spellings(parsed.value()), (std::vector<std::string> { "device", "room1", "temp" }));
    EXPECT_TRUE(parsed.value().all_literal());
}

TEST(parse_test, a_single_level_pattern_splits_into_one_level) {
    const auto parsed = parse_pattern("device");

    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed.value().size(), 1u);
    EXPECT_EQ(parsed.value().levels_[0].kind_, level_kind::LITERAL);
}

TEST(parse_test, a_pattern_keeps_wildcards_as_the_whole_level) {
    const auto parsed = parse_pattern("device/+/temp/#");

    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(spellings(parsed.value()), (std::vector<std::string> { "device", "+", "temp", "#" }));
    EXPECT_EQ(parsed.value().levels_[1].kind_, level_kind::SINGLE_LEVEL);
    EXPECT_EQ(parsed.value().levels_[3].kind_, level_kind::MULTI_LEVEL);
    EXPECT_FALSE(parsed.value().all_literal());
}

TEST(parse_test, a_lone_wildcard_is_a_whole_pattern) {
    const auto single = parse_pattern("+");
    const auto multi = parse_pattern("#");

    ASSERT_TRUE(single.has_value());
    ASSERT_TRUE(multi.has_value());
    EXPECT_EQ(single.value().levels_[0].kind_, level_kind::SINGLE_LEVEL);
    EXPECT_EQ(multi.value().levels_[0].kind_, level_kind::MULTI_LEVEL);
}

TEST(parse_test, a_wildcard_character_beside_text_is_rejected_at_its_own_offset) {
    // `+` and `#` are wildcards only as a whole level. A level that merely contains one is rejected rather
    // than read as a name, so a device called `x+` has to be spelled some other way.
    expect_parse_error("+x", true, error_code::INVALID_WILDCARD, 0);
    expect_parse_error("x+", true, error_code::INVALID_WILDCARD, 1);
    expect_parse_error("de+vice", true, error_code::INVALID_WILDCARD, 2);
    expect_parse_error("x#", true, error_code::INVALID_WILDCARD, 1);
}

TEST(parse_test, punctuation_that_is_not_a_wildcard_is_literal_text) {
    const auto parsed = parse_pattern("device/room-1/temp.1");

    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(spellings(parsed.value()), (std::vector<std::string> { "device", "room-1", "temp.1" }));
    EXPECT_TRUE(parsed.value().all_literal());
}

TEST(parse_test, an_empty_text_is_rejected_as_a_whole) {
    expect_parse_error("", true, error_code::EMPTY_INPUT, error::npos);
    expect_parse_error("", false, error_code::EMPTY_INPUT, error::npos);
}

TEST(parse_test, an_empty_level_is_rejected_where_it_starts) {
    expect_parse_error("/device", true, error_code::EMPTY_LEVEL, 0);
    expect_parse_error("device/", true, error_code::EMPTY_LEVEL, 7);
    expect_parse_error("device//temp", true, error_code::EMPTY_LEVEL, 7);
    expect_parse_error("device/temp/", true, error_code::EMPTY_LEVEL, 12);
}

TEST(parse_test, a_multi_level_wildcard_must_end_the_pattern) {
    expect_parse_error("device/#/temp", true, error_code::TRAILING_WILDCARD_NOT_LAST, 7);
    expect_parse_error("device/#/", true, error_code::TRAILING_WILDCARD_NOT_LAST, 7);
    expect_parse_error("#/#", true, error_code::TRAILING_WILDCARD_NOT_LAST, 0);
    expect_parse_error("#/device", true, error_code::TRAILING_WILDCARD_NOT_LAST, 0);
}

TEST(parse_test, a_multi_level_wildcard_is_fine_as_the_last_level) {
    const auto parsed = parse_pattern("device/#");

    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed.value().size(), 2u);
}

TEST(parse_test, a_wildcard_glued_to_text_is_rejected_as_a_stray_wildcard) {
    expect_parse_error("device+/temp", true, error_code::INVALID_WILDCARD, 6);
    expect_parse_error("device/tem#p", true, error_code::INVALID_WILDCARD, 10);
    expect_parse_error("device/##", true, error_code::INVALID_WILDCARD, 7);
}

TEST(parse_test, whitespace_and_control_characters_are_rejected) {
    expect_parse_error("device/te mp", true, error_code::INVALID_CHARACTER, 9);
    expect_parse_error(" device", true, error_code::INVALID_CHARACTER, 0);
    expect_parse_error("device/temp\t", true, error_code::INVALID_CHARACTER, 11);
    expect_parse_error("device/\n", true, error_code::INVALID_CHARACTER, 7);
}

TEST(parse_test, a_topic_rejects_every_wildcard) {
    const auto plus = parse_topic("device/+/temp");
    const auto hash = parse_topic("device/#");

    ASSERT_FALSE(plus.has_value());
    EXPECT_EQ(plus.error().code_, error_code::WILDCARD_IN_TOPIC);
    EXPECT_EQ(plus.error().where_, 7u);
    ASSERT_FALSE(hash.has_value());
    EXPECT_EQ(hash.error().code_, error_code::WILDCARD_IN_TOPIC);
    EXPECT_EQ(hash.error().where_, 7u);
}

TEST(parse_test, a_topic_rejects_a_glued_wildcard_as_a_stray_wildcard) {
    // Not `WILDCARD_IN_TOPIC`: the level is neither `+` nor `#`, so the wildcard is misplaced rather than
    // simply disallowed here.
    expect_parse_error("device+/temp", false, error_code::INVALID_WILDCARD, 6);
}

TEST(parse_test, a_topic_uses_the_same_split_as_a_pattern) {
    const auto topic = parse_topic("device/room1/temp");

    ASSERT_TRUE(topic.has_value());
    EXPECT_EQ(spellings(topic.value()), (std::vector<std::string> { "device", "room1", "temp" }));
}

TEST(parse_test, a_path_spells_itself_back_out) {
    const auto parsed = parse_pattern("device/+/temp/#");

    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed.value().spell(), "device/+/temp/#");
}

TEST(parse_test, a_path_round_trips_through_its_levels) {
    const auto parsed = parse_pattern("device/+/#");

    ASSERT_TRUE(parsed.has_value());
    const auto rebuilt = topic_path::from_levels(parsed.value().levels());
    EXPECT_EQ(rebuilt, parsed.value());
    EXPECT_EQ(rebuilt.spell(), "device/+/#");
}

// --- matching --------------------------------------------------------------------------------------

TEST(match_test, an_empty_matcher_matches_nothing) {
    const layer_matcher matcher;

    EXPECT_TRUE(matcher.empty());
    EXPECT_EQ(matcher.subscription_count(), 0u);
    expect_match(matcher, "device/temp", {});
}

TEST(match_test, a_literal_pattern_matches_its_own_topic) {
    layer_matcher matcher;
    subscribe(matcher, "device/room1/temp", 1);

    expect_match(matcher, "device/room1/temp", { 1 });
    expect_match(matcher, "device/room1", {});
    expect_match(matcher, "device/room1/temp/hot", {});
    expect_match(matcher, "device/room2/temp", {});
    expect_match(matcher, "other/room1/temp", {});
}

TEST(match_test, a_single_level_wildcard_matches_exactly_one_level) {
    layer_matcher matcher;
    subscribe(matcher, "device/+/temp", 1);

    expect_match(matcher, "device/room1/temp", { 1 });
    expect_match(matcher, "device/x/temp", { 1 });
    expect_match(matcher, "device/temp", {});
    expect_match(matcher, "device/a/b/temp", {});
    expect_match(matcher, "device/room1/temp/hot", {});
}

TEST(match_test, a_multi_level_wildcard_also_matches_its_own_node) {
    layer_matcher matcher;
    subscribe(matcher, "device/#", 1);

    // Zero levels below `device` is part of "the rest", so the bare device topic matches too.
    expect_match(matcher, "device", { 1 });
    expect_match(matcher, "device/a", { 1 });
    expect_match(matcher, "device/a/b/c", { 1 });
    expect_match(matcher, "devices", {});
    expect_match(matcher, "other", {});
}

TEST(match_test, a_bare_multi_level_wildcard_matches_every_topic) {
    layer_matcher matcher;
    subscribe(matcher, "#", 1);

    expect_match(matcher, "device", { 1 });
    expect_match(matcher, "device/temp", { 1 });
    expect_match(matcher, "a/b/c/d/e", { 1 });
}

TEST(match_test, a_bare_single_level_wildcard_matches_one_level_topics) {
    layer_matcher matcher;
    subscribe(matcher, "+", 1);

    expect_match(matcher, "device", { 1 });
    expect_match(matcher, "device/temp", {});
}

TEST(match_test, wildcards_work_at_the_root) {
    layer_matcher matcher;
    subscribe(matcher, "+/temp", 1);

    expect_match(matcher, "device/temp", { 1 });
    expect_match(matcher, "other/temp", { 1 });
    expect_match(matcher, "temp", {});
    expect_match(matcher, "device/temp/hot", {});
}

TEST(match_test, a_literal_and_a_wildcard_coexist_at_one_level) {
    layer_matcher matcher;
    subscribe(matcher, "device/temp", 1);
    subscribe(matcher, "device/+", 2);

    expect_match(matcher, "device/temp", { 1, 2 });
    expect_match(matcher, "device/humidity", { 2 });
}

TEST(match_test, a_literal_winning_the_branch_does_not_hide_the_wildcard) {
    layer_matcher matcher;
    subscribe(matcher, "a/b/c", 1);
    subscribe(matcher, "a/b/+", 2);
    subscribe(matcher, "a/+/c", 3);
    subscribe(matcher, "a/+/+", 4);

    expect_match(matcher, "a/b/c", { 1, 2, 3, 4 });
}

TEST(match_test, several_multi_level_wildcards_can_match_one_topic) {
    layer_matcher matcher;
    subscribe(matcher, "#", 1);
    subscribe(matcher, "a/#", 2);
    subscribe(matcher, "a/b/#", 3);

    expect_match(matcher, "a/b/c", { 1, 2, 3 });
    expect_match(matcher, "a", { 1, 2 });
    expect_match(matcher, "a/b", { 1, 2, 3 });
}

TEST(match_test, a_multi_level_wildcard_is_not_a_single_level_wildcard) {
    layer_matcher matcher;
    subscribe(matcher, "device/+", 1);

    expect_match(matcher, "device/room1", { 1 });
    expect_match(matcher, "device/room1/temp", {});
    expect_match(matcher, "device", {});
}

TEST(match_test, a_subscriber_reached_by_two_patterns_is_matched_once) {
    layer_matcher matcher;
    subscribe(matcher, "device/+/temp", 1);
    subscribe(matcher, "device/#", 1);

    const auto matched = matcher.match_topic("device/room1/temp");

    ASSERT_TRUE(matched.has_value());
    EXPECT_EQ(matched.value().size(), 1u);
    expect_match(matcher, "device/room1/temp", { 1 });
}

TEST(match_test, a_subscriber_reached_by_many_patterns_is_matched_once) {
    layer_matcher matcher;
    for (const auto* pattern : { "#", "a/#", "a/+/+", "a/b/+", "a/+/c", "a/b/c" }) { subscribe(matcher, pattern, 1); }

    const auto matched = matcher.match_topic("a/b/c");

    ASSERT_TRUE(matched.has_value());
    EXPECT_EQ(matched.value().size(), 1u);
}

TEST(match_test, overlapping_subscribers_are_all_matched) {
    layer_matcher matcher;
    subscribe(matcher, "device/#", 1);
    subscribe(matcher, "device/+/temp", 2);
    subscribe(matcher, "device/room1/temp", 3);
    subscribe(matcher, "device/room1/temp", 4);

    expect_match(matcher, "device/room1/temp", { 1, 2, 3, 4 });
    expect_match(matcher, "device/room1/humidity", { 1 });
}

TEST(match_test, a_multi_level_wildcard_does_not_reach_past_its_own_level) {
    layer_matcher matcher;
    subscribe(matcher, "device/room1/#", 1);

    expect_match(matcher, "device/room1", { 1 });
    expect_match(matcher, "device/room2", {});
    expect_match(matcher, "device", {});
}

TEST(match_test, matching_a_topic_that_is_not_a_topic_fails) {
    layer_matcher matcher;
    subscribe(matcher, "device/#", 1);

    for (const auto* bad : { "", "/device", "device/", "device/+/temp", "device//temp", "device/te mp" }) {
        EXPECT_FALSE(matcher.match_topic(bad).has_value()) << "on " << bad;
    }
}

TEST(match_test, matching_returns_the_subscriptions_of_that_topic_alone) {
    layer_matcher matcher;
    subscribe(matcher, "a", 1);
    subscribe(matcher, "b", 2);

    const auto first = parse_topic("a");
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(matcher.match(first.value().texts()), (subscription_set { 1 }));

    const auto second = parse_topic("b");
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(matcher.match(second.value().texts()), (subscription_set { 2 }));
}

// --- registration ----------------------------------------------------------------------------------

TEST(registry_test, adding_returns_the_id_it_was_given) {
    layer_matcher matcher;

    const auto added = matcher.add("device/temp", 42);

    ASSERT_TRUE(added.has_value());
    EXPECT_EQ(added.value(), 42u);
    EXPECT_EQ(matcher.subscription_count(), 1u);
}

TEST(registry_test, a_rejected_pattern_registers_nothing) {
    layer_matcher matcher;

    const auto added = matcher.add("device/#/temp", 1);

    ASSERT_FALSE(added.has_value());
    EXPECT_EQ(added.error().code_, error_code::TRAILING_WILDCARD_NOT_LAST);
    EXPECT_TRUE(matcher.empty());
    EXPECT_EQ(matcher.subscription_count(), 0u);
    EXPECT_TRUE(matcher.collect().empty());
}

TEST(registry_test, the_same_pattern_twice_for_one_id_is_a_duplicate) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("device/+", 1).has_value());

    const auto again = matcher.add("device/+", 1);

    ASSERT_FALSE(again.has_value());
    EXPECT_EQ(again.error().code_, error_code::DUPLICATE_PATTERN);
    EXPECT_EQ(again.error().text_, "device/+");
    EXPECT_EQ(matcher.pattern_count(1), 1u);
}

TEST(registry_test, the_same_id_may_hold_several_patterns) {
    layer_matcher matcher;

    ASSERT_TRUE(matcher.add("device/+", 1).has_value());
    ASSERT_TRUE(matcher.add("device/#", 1).has_value());

    EXPECT_EQ(matcher.pattern_count(1), 2u);
    EXPECT_EQ(matcher.subscription_count(), 1u);
    expect_match(matcher, "device/temp", { 1 });
}

TEST(registry_test, distinct_ids_may_hold_the_same_pattern) {
    layer_matcher matcher;

    ASSERT_TRUE(matcher.add("device/room1", 1).has_value());
    ASSERT_TRUE(matcher.add("device/room1", 2).has_value());

    EXPECT_EQ(matcher.subscription_count(), 2u);
    expect_match(matcher, "device/room1", { 1, 2 });

    const subscription_set* payload = matcher.payload_at("device/room1");
    ASSERT_NE(payload, nullptr);
    EXPECT_EQ(*payload, (subscription_set { 1, 2 }));
}

TEST(registry_test, removing_drops_every_pattern_the_id_held) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("device/+", 1).has_value());
    ASSERT_TRUE(matcher.add("device/#", 1).has_value());
    ASSERT_TRUE(matcher.add("other", 2).has_value());

    EXPECT_TRUE(matcher.remove(1));

    EXPECT_EQ(matcher.pattern_count(1), 0u);
    EXPECT_EQ(matcher.subscription_count(), 1u);
    EXPECT_EQ(matcher.payload_at("device/+"), nullptr);
    EXPECT_EQ(matcher.payload_at("device/#"), nullptr);
    expect_match(matcher, "device/temp", {});
    expect_match(matcher, "other", { 2 });
}

TEST(registry_test, removing_prunes_the_levels_it_emptied) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("a/b/c", 1).has_value());

    EXPECT_TRUE(matcher.remove(1));

    EXPECT_TRUE(matcher.empty());
    EXPECT_EQ(matcher.payload_at("a/b/c"), nullptr);
    EXPECT_EQ(matcher.payload_at("a/b"), nullptr);
    EXPECT_EQ(matcher.payload_at("a"), nullptr);
    EXPECT_TRUE(matcher.collect().empty());
}

TEST(registry_test, removing_one_subscriber_keeps_the_others) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("device/#", 1).has_value());
    ASSERT_TRUE(matcher.add("device/#", 2).has_value());

    EXPECT_TRUE(matcher.remove(1));

    EXPECT_EQ(matcher.subscription_count(), 1u);
    expect_match(matcher, "device/temp", { 2 });
}

TEST(registry_test, removing_a_shared_branch_leaves_the_other_branch) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("a/b/c", 1).has_value());
    ASSERT_TRUE(matcher.add("a/b/d", 2).has_value());

    EXPECT_TRUE(matcher.remove(1));

    // The `a/b` level outlives its own payload, because a sibling pattern still hangs below it.
    EXPECT_EQ(matcher.payload_at("a/b"), nullptr);
    expect_match(matcher, "a/b/d", { 2 });
    expect_match(matcher, "a/b/c", {});
}

TEST(registry_test, removing_is_idempotent) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("device", 1).has_value());

    EXPECT_TRUE(matcher.remove(1));
    EXPECT_FALSE(matcher.remove(1));
    EXPECT_FALSE(matcher.remove(999));
    EXPECT_TRUE(matcher.empty());
}

TEST(registry_test, a_pattern_can_be_registered_again_after_a_removal) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("device/+", 1).has_value());
    EXPECT_TRUE(matcher.remove(1));

    const auto again = matcher.add("device/+", 1);

    ASSERT_TRUE(again.has_value());
    expect_match(matcher, "device/temp", { 1 });
}

TEST(registry_test, removing_one_id_leaves_the_pruned_tree_usable) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("a/b", 1).has_value());
    ASSERT_TRUE(matcher.add("a/+", 2).has_value());

    EXPECT_TRUE(matcher.remove(1));

    EXPECT_EQ(matcher.payload_at("a/b"), nullptr);
    // The literal branch went with its only subscriber, but the wildcard sibling still covers `a/b`, so
    // pruning a branch is not the same as unsubscribing what a wildcard reaches.
    expect_match(matcher, "a/x", { 2 });
    expect_match(matcher, "a/b", { 2 });
}

// --- inspection ------------------------------------------------------------------------------------

TEST(inspection_test, payload_at_finds_a_registered_pattern) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("device/room1", 7).has_value());

    const subscription_set* payload = matcher.payload_at("device/room1");

    ASSERT_NE(payload, nullptr);
    EXPECT_EQ(*payload, (subscription_set { 7 }));
}

TEST(inspection_test, payload_at_sees_only_a_literal_path_a_pattern_ends_at) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("device/+", 1).has_value());
    ASSERT_TRUE(matcher.add("device/#", 2).has_value());
    ASSERT_TRUE(matcher.add("device/room1", 3).has_value());

    // A wildcard level is a branch rather than a name, so a path spelling one reaches nothing.
    EXPECT_EQ(matcher.payload_at("device/+"), nullptr);
    EXPECT_EQ(matcher.payload_at("device/#"), nullptr);
    // `#` descends no level of its own: its payload sits beside the `device` node, and `device/#` does not
    // end at `device`, so the node there holds nothing.
    EXPECT_EQ(matcher.payload_at("device"), nullptr);
    EXPECT_EQ(matcher.payload_at("dev"), nullptr);

    const subscription_set* payload = matcher.payload_at("device/room1");
    ASSERT_NE(payload, nullptr);
    EXPECT_EQ(*payload, (subscription_set { 3 }));
}

TEST(inspection_test, payload_at_reports_a_prefix_that_holds_no_pattern) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("a/b/c", 1).has_value());

    EXPECT_EQ(matcher.payload_at("a"), nullptr);
    EXPECT_EQ(matcher.payload_at("a/b"), nullptr);
    EXPECT_EQ(matcher.payload_at("a/b/c/d"), nullptr);
    EXPECT_NE(matcher.payload_at("a/b/c"), nullptr);
}

TEST(inspection_test, payload_at_rejects_text_that_is_not_a_pattern) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("a", 1).has_value());

    EXPECT_EQ(matcher.payload_at(""), nullptr);
    EXPECT_EQ(matcher.payload_at("a/"), nullptr);
    EXPECT_EQ(matcher.payload_at("a/#/b"), nullptr);
}

TEST(inspection_test, collect_reports_every_pattern_and_its_ids) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("device/+/temp", 1).has_value());
    ASSERT_TRUE(matcher.add("device/#", 2).has_value());
    ASSERT_TRUE(matcher.add("device/+/temp", 3).has_value());

    std::vector<std::pair<std::string, std::vector<subscription_id>>> found;
    for (const auto& [path, ids] : matcher.collect()) {
        std::vector<subscription_id> sorted(ids.begin(), ids.end());
        std::sort(sorted.begin(), sorted.end());
        found.emplace_back(path.spell(), std::move(sorted));
    }
    std::sort(found.begin(), found.end());

    EXPECT_EQ(
        found, (std::vector<std::pair<std::string, std::vector<subscription_id>>> {
                   { "device/#",      { 2 }    },
                   { "device/+/temp", { 1, 3 } },
    })
    );
}

TEST(inspection_test, collect_follows_removals) {
    layer_matcher matcher;
    ASSERT_TRUE(matcher.add("a/b", 1).has_value());
    ASSERT_TRUE(matcher.add("a/+", 2).has_value());
    ASSERT_TRUE(matcher.add("other/#", 3).has_value());

    EXPECT_TRUE(matcher.remove(1));
    EXPECT_TRUE(matcher.remove(3));

    ASSERT_EQ(matcher.collect().size(), 1u);
    EXPECT_EQ(matcher.collect()[0].first.spell(), "a/+");
}

} // namespace

} // namespace ba7lya::msgbus
