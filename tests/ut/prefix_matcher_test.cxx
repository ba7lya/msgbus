///
/// @file prefix_matcher_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for the wildcard-free matcher: a subscription hears everything below its path.
/// @version 0.1
/// @date 2026-10-17
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

#include "matcher.hxx"
#include "prefix_matcher.hxx"
#include "topic.hxx"

namespace ba7lya::msgbus {

// The unit under test is in detail: this file is part of the library, so it may name it directly.

namespace {

/// @brief The subscriptions matching `topic`, sorted so the assertion reads as a set.
[[nodiscard]]
std::vector<subscription_id> matched_ids(const prefix_matcher& matcher, std::string_view topic) {
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
void subscribe(prefix_matcher& matcher, std::string_view pattern, subscription_id id) {
    const auto added = matcher.add(pattern, id);
    ASSERT_TRUE(added.has_value()) << "on " << pattern;
}

/// @brief Asserts that `topic` reaches exactly `expected` subscriptions.
void expect_match(
    const prefix_matcher& matcher, std::string_view topic, std::initializer_list<subscription_id> expected
) {
    std::vector<subscription_id> wanted(expected.begin(), expected.end());
    std::sort(wanted.begin(), wanted.end());
    EXPECT_EQ(matched_ids(matcher, topic), wanted) << "on " << topic;
}

// --- matching --------------------------------------------------------------------------------------

TEST(prefix_match_test, an_empty_matcher_matches_nothing) {
    const prefix_matcher matcher;

    EXPECT_TRUE(matcher.empty());
    EXPECT_EQ(matcher.subscription_count(), 0u);
    expect_match(matcher, "device/temp", {});
}

TEST(prefix_match_test, a_prefix_covers_its_own_topic) {
    prefix_matcher matcher;
    subscribe(matcher, "device/room1", 1);

    // The whole of the difference from `layer_matcher`: no `#` is needed to say "this and below".
    expect_match(matcher, "device/room1", { 1 });
}

TEST(prefix_match_test, a_prefix_covers_everything_below_it) {
    prefix_matcher matcher;
    subscribe(matcher, "site/line1", 1);

    expect_match(matcher, "site/line1/device7", { 1 });
    expect_match(matcher, "site/line1/device7/temp", { 1 });
    expect_match(matcher, "site/line1/a/b/c/d", { 1 });
}

TEST(prefix_match_test, a_prefix_covers_nothing_above_or_beside_it) {
    prefix_matcher matcher;
    subscribe(matcher, "site/line1", 1);

    expect_match(matcher, "site", {});
    expect_match(matcher, "site/line2", {});
    expect_match(matcher, "site/line1x", {});
    expect_match(matcher, "other/line1", {});
    expect_match(matcher, "site/line1x/device7", {});
}

TEST(prefix_match_test, prefixes_nest_and_each_contributes) {
    prefix_matcher matcher;
    subscribe(matcher, "site", 1);
    subscribe(matcher, "site/line1", 2);
    subscribe(matcher, "site/line1/device7", 3);

    expect_match(matcher, "site/line1/device7/temp", { 1, 2, 3 });
    expect_match(matcher, "site/line1/device7", { 1, 2, 3 });
    expect_match(matcher, "site/line1", { 1, 2 });
    expect_match(matcher, "site", { 1 });
}

TEST(prefix_match_test, a_subscriber_whose_prefixes_both_cover_a_topic_is_matched_once) {
    prefix_matcher matcher;
    subscribe(matcher, "site", 1);
    subscribe(matcher, "site/line1", 1);

    expect_match(matcher, "site/line1/device7", { 1 });
}

TEST(prefix_match_test, matching_returns_the_subscriptions_of_that_topic_alone) {
    prefix_matcher matcher;
    subscribe(matcher, "a", 1);
    subscribe(matcher, "b", 2);

    const auto first = parse_topic("a/x");
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(matcher.match(first.value().texts()), (subscription_set { 1 }));

    const auto second = parse_topic("b/y");
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(matcher.match(second.value().texts()), (subscription_set { 2 }));
}

// --- registration ----------------------------------------------------------------------------------

TEST(prefix_registry_test, a_wildcard_pattern_is_rejected) {
    prefix_matcher matcher;

    // The topic grammar, not the pattern grammar: a wildcard here is the same mistake as a wildcard in a
    // published topic, so it is rejected by the same code path.
    const auto plus = matcher.add("device/+", 1);
    const auto hash = matcher.add("device/#", 1);

    ASSERT_FALSE(plus.has_value());
    EXPECT_EQ(plus.error().code_, error_code::WILDCARD_IN_TOPIC);
    ASSERT_FALSE(hash.has_value());
    EXPECT_EQ(hash.error().code_, error_code::WILDCARD_IN_TOPIC);
    EXPECT_TRUE(matcher.empty());
    EXPECT_EQ(matcher.subscription_count(), 0u);
}

TEST(prefix_registry_test, a_malformed_path_registers_nothing) {
    prefix_matcher matcher;

    const auto added = matcher.add("device//temp", 1);

    ASSERT_FALSE(added.has_value());
    EXPECT_EQ(added.error().code_, error_code::EMPTY_LEVEL);
    EXPECT_TRUE(matcher.empty());
    EXPECT_TRUE(matcher.collect().empty());
}

TEST(prefix_registry_test, adding_returns_the_id_it_was_given) {
    prefix_matcher matcher;

    const auto added = matcher.add("device/temp", 42);

    ASSERT_TRUE(added.has_value());
    EXPECT_EQ(added.value(), 42u);
    EXPECT_EQ(matcher.subscription_count(), 1u);
}

TEST(prefix_registry_test, the_same_prefix_twice_for_one_id_is_a_duplicate) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("device/temp", 1).has_value());

    const auto again = matcher.add("device/temp", 1);

    ASSERT_FALSE(again.has_value());
    EXPECT_EQ(again.error().code_, error_code::DUPLICATE_PATTERN);
    EXPECT_EQ(again.error().text_, "device/temp");
    EXPECT_EQ(matcher.pattern_count(1), 1u);
}

TEST(prefix_registry_test, the_same_id_may_hold_several_prefixes) {
    prefix_matcher matcher;

    ASSERT_TRUE(matcher.add("site", 1).has_value());
    ASSERT_TRUE(matcher.add("other", 1).has_value());

    EXPECT_EQ(matcher.pattern_count(1), 2u);
    EXPECT_EQ(matcher.subscription_count(), 1u);
    expect_match(matcher, "site/line1", { 1 });
    expect_match(matcher, "other/line1", { 1 });
}

TEST(prefix_registry_test, distinct_ids_may_hold_the_same_prefix) {
    prefix_matcher matcher;

    ASSERT_TRUE(matcher.add("site/line1", 1).has_value());
    ASSERT_TRUE(matcher.add("site/line1", 2).has_value());

    EXPECT_EQ(matcher.subscription_count(), 2u);
    expect_match(matcher, "site/line1/device7", { 1, 2 });

    const subscription_set* payload = matcher.payload_at("site/line1");
    ASSERT_NE(payload, nullptr);
    EXPECT_EQ(*payload, (subscription_set { 1, 2 }));
}

TEST(prefix_registry_test, removing_drops_every_prefix_the_id_held) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("site", 1).has_value());
    ASSERT_TRUE(matcher.add("other", 1).has_value());
    ASSERT_TRUE(matcher.add("third", 2).has_value());

    EXPECT_TRUE(matcher.remove(1));

    EXPECT_EQ(matcher.pattern_count(1), 0u);
    EXPECT_EQ(matcher.subscription_count(), 1u);
    EXPECT_EQ(matcher.payload_at("site"), nullptr);
    expect_match(matcher, "site/line1", {});
    expect_match(matcher, "third/line1", { 2 });
}

TEST(prefix_registry_test, removing_prunes_the_levels_it_emptied) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("a/b/c", 1).has_value());

    EXPECT_TRUE(matcher.remove(1));

    EXPECT_TRUE(matcher.empty());
    EXPECT_EQ(matcher.payload_at("a/b/c"), nullptr);
    EXPECT_EQ(matcher.payload_at("a/b"), nullptr);
    EXPECT_EQ(matcher.payload_at("a"), nullptr);
    EXPECT_TRUE(matcher.collect().empty());
}

TEST(prefix_registry_test, removing_one_subscriber_keeps_the_others) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("site", 1).has_value());
    ASSERT_TRUE(matcher.add("site", 2).has_value());

    EXPECT_TRUE(matcher.remove(1));

    EXPECT_EQ(matcher.subscription_count(), 1u);
    expect_match(matcher, "site/line1", { 2 });
}

TEST(prefix_registry_test, removing_a_shared_branch_leaves_the_other_branch) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("a/b/c", 1).has_value());
    ASSERT_TRUE(matcher.add("a/b/d", 2).has_value());

    EXPECT_TRUE(matcher.remove(1));

    EXPECT_EQ(matcher.payload_at("a/b"), nullptr);
    expect_match(matcher, "a/b/d/x", { 2 });
    expect_match(matcher, "a/b/c/x", {});
}

TEST(prefix_registry_test, removing_is_idempotent) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("device", 1).has_value());

    EXPECT_TRUE(matcher.remove(1));
    EXPECT_FALSE(matcher.remove(1));
    EXPECT_FALSE(matcher.remove(999));
    EXPECT_TRUE(matcher.empty());
}

TEST(prefix_registry_test, a_prefix_can_be_registered_again_after_a_removal) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("site/line1", 1).has_value());
    EXPECT_TRUE(matcher.remove(1));

    const auto again = matcher.add("site/line1", 1);

    ASSERT_TRUE(again.has_value());
    expect_match(matcher, "site/line1/device7", { 1 });
}

// --- inspection ------------------------------------------------------------------------------------

TEST(prefix_inspection_test, payload_at_finds_a_registered_prefix) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("device/room1", 7).has_value());

    const subscription_set* payload = matcher.payload_at("device/room1");

    ASSERT_NE(payload, nullptr);
    EXPECT_EQ(*payload, (subscription_set { 7 }));
}

TEST(prefix_inspection_test, payload_at_reports_a_path_that_holds_no_prefix) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("a/b/c", 1).has_value());

    EXPECT_EQ(matcher.payload_at("a"), nullptr);
    EXPECT_EQ(matcher.payload_at("a/b"), nullptr);
    EXPECT_EQ(matcher.payload_at("a/b/c/d"), nullptr);
    EXPECT_NE(matcher.payload_at("a/b/c"), nullptr);
}

TEST(prefix_inspection_test, payload_at_rejects_text_that_is_not_a_topic) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("a", 1).has_value());

    EXPECT_EQ(matcher.payload_at(""), nullptr);
    EXPECT_EQ(matcher.payload_at("a/"), nullptr);
    EXPECT_EQ(matcher.payload_at("a/+"), nullptr);
}

TEST(prefix_inspection_test, collect_reports_every_prefix_and_its_ids) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("site/line1", 1).has_value());
    ASSERT_TRUE(matcher.add("site", 2).has_value());
    ASSERT_TRUE(matcher.add("site/line1", 3).has_value());

    std::vector<std::pair<std::string, std::vector<subscription_id>>> found;
    for (const auto& [path, ids] : matcher.collect()) {
        std::vector<subscription_id> sorted(ids.begin(), ids.end());
        std::sort(sorted.begin(), sorted.end());
        found.emplace_back(path.spell(), std::move(sorted));
    }
    std::sort(found.begin(), found.end());

    EXPECT_EQ(
        found, (std::vector<std::pair<std::string, std::vector<subscription_id>>> {
                   { "site",       { 2 }    },
                   { "site/line1", { 1, 3 } },
    })
    );
}

TEST(prefix_inspection_test, collect_follows_removals) {
    prefix_matcher matcher;
    ASSERT_TRUE(matcher.add("a/b", 1).has_value());
    ASSERT_TRUE(matcher.add("a", 2).has_value());
    ASSERT_TRUE(matcher.add("other", 3).has_value());

    EXPECT_TRUE(matcher.remove(1));
    EXPECT_TRUE(matcher.remove(3));

    ASSERT_EQ(matcher.collect().size(), 1u);
    EXPECT_EQ(matcher.collect()[0].first.spell(), "a");
}

// --- through the interface -------------------------------------------------------------------------

TEST(prefix_interface_test, a_prefix_matcher_drives_the_matcher_interface) {
    prefix_matcher concrete;
    matcher& erased = concrete;

    ASSERT_TRUE(erased.add("site/line1", 1).has_value());
    const auto topic = parse_topic("site/line1/device7/temp");
    ASSERT_TRUE(topic.has_value());

    EXPECT_EQ(erased.match(topic.value().texts()), (subscription_set { 1 }));
    EXPECT_TRUE(erased.remove(1));
    EXPECT_FALSE(erased.remove(1));
}

} // namespace

} // namespace ba7lya::msgbus
