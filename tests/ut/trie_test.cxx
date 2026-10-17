///
/// @file trie_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for the level-sequence prefix tree.
/// @version 0.1
/// @date 2026-10-15
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#include <algorithm>
#include <expected>
#include <gtest/gtest.h>
#include <initializer_list>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "trie.hxx"

namespace ba7lya::msgbus {

namespace {

///
/// @brief A level vocabulary the library itself never sees: `LITERAL`/`+`/`#` and nothing else.
/// @details The tree is supposed to know only this much about its keys, so the test drives it through a
/// vocabulary that is not a topic pattern at all. If a topic notion leaks into the tree, this stops
/// compiling.
///
struct probe_level {
    level_kind kind_ = level_kind::LITERAL;
    std::string text_;

    [[nodiscard]]
    level_kind kind() const noexcept {
        return kind_;
    }

    [[nodiscard]]
    std::string_view text() const noexcept {
        return text_;
    }

    [[nodiscard]]
    static probe_level literal(std::string_view text) {
        return probe_level { level_kind::LITERAL, std::string(text) };
    }

    [[nodiscard]]
    static probe_level single_level() {
        return probe_level { level_kind::SINGLE_LEVEL, "+" };
    }

    [[nodiscard]]
    static probe_level multi_level() {
        return probe_level { level_kind::MULTI_LEVEL, "#" };
    }

    friend bool operator==(const probe_level&, const probe_level&) = default;
};

/// @brief A sequence of levels, the thing the tree is indexed by.
struct probe_key {
    std::vector<probe_level> levels_;

    [[nodiscard]]
    std::span<const probe_level> levels() const noexcept {
        return levels_;
    }

    [[nodiscard]]
    static probe_key from_levels(std::span<const probe_level> levels) {
        return probe_key { std::vector<probe_level>(levels.begin(), levels.end()) };
    }

    friend bool operator==(const probe_key&, const probe_key&) = default;
};

/// @brief The payload the router will store: a set of subscription ids, so a pattern shared by two
/// subscriptions still occupies one node.
using id_set = std::unordered_set<int>;

using probe_tree = trie<id_set, probe_level>;

// The concept boundaries, checked where a compile error is cheapest to read.
static_assert(payload_container<id_set>);
static_assert(payload_container<std::vector<int>>);

struct no_empty_member {
    int size_ = 0;
};

static_assert(!payload_container<no_empty_member>);
static_assert(trie_level<probe_level>);
static_assert(trie_key<probe_key, probe_level>);
static_assert(!std::is_copy_constructible_v<probe_tree>);
static_assert(!std::is_copy_assignable_v<probe_tree>);

///
/// @brief What every case below builds on: a way to spell a path from text, to add and remove an id, and to
/// read the whole tree back as a comparable value.
///
class trie_test_fixture : public ::testing::Test {
protected:
    /// @brief Spells a path: `"+"` is the single-level level, `"#"` the multi-level one, anything else a
    /// literal.
    [[nodiscard]]
    static std::vector<probe_level> levels(std::initializer_list<const char*> texts) {
        std::vector<probe_level> out;
        for (const char* text : texts) {
            const std::string_view spelling(text);
            if (spelling == "+") { out.push_back(probe_level::single_level()); }
            else if (spelling == "#") { out.push_back(probe_level::multi_level()); }
            else { out.push_back(probe_level::literal(spelling)); }
        }
        return out;
    }

    /// @brief Adds `id` to the payload table of `path`, creating the levels it needs.
    static void add(probe_tree& tree, const std::vector<probe_level>& path, int id) { tree.table_for(path).insert(id); }

    /// @brief Removes `id` from the payload table of `path`, with the same prunings removals always take.
    [[nodiscard]]
    static std::expected<bool, trie_miss> remove(probe_tree& tree, const std::vector<probe_level>& path, int id) {
        return tree.update_node(std::span<const probe_level>(path), [id](id_set& ids) { ids.erase(id); });
    }

    /// @brief The whole tree as a sorted `(spelling, ids)` list, so one expectation covers every payload.
    [[nodiscard]]
    static std::vector<std::pair<std::string, std::vector<int>>> snapshot(const probe_tree& tree) {
        std::vector<std::pair<std::string, std::vector<int>>> out;
        for (const auto& [key, payload] : tree.collect_payloads<probe_key>()) {
            std::string spelling;
            for (const probe_level& level : key.levels_) {
                if (!spelling.empty()) { spelling += '/'; }
                spelling += level.text();
            }
            std::vector<int> ids(payload->begin(), payload->end());
            std::sort(ids.begin(), ids.end());
            out.emplace_back(std::move(spelling), std::move(ids));
        }
        std::sort(out.begin(), out.end());
        return out;
    }

    [[nodiscard]]
    static std::vector<bool> reachable(const probe_tree& tree, std::initializer_list<const char*> literal_levels) {
        std::vector<bool> found;
        const probe_tree* node = &tree;
        for (const char* level : literal_levels) {
            node = node->exact_child(level);
            found.push_back(node != nullptr);
            if (node == nullptr) { break; }
        }
        return found;
    }
};

TEST_F(trie_test_fixture, an_empty_tree_holds_nothing) {
    const probe_tree tree;

    EXPECT_TRUE(tree.is_empty());
    EXPECT_EQ(tree.exact_match_payload(), nullptr);
    EXPECT_EQ(tree.multi_level_wildcard_payload(), nullptr);
    EXPECT_EQ(tree.exact_child("device"), nullptr);
    EXPECT_EQ(tree.single_level_wildcard_child(), nullptr);
    EXPECT_TRUE(tree.collect_payloads<probe_key>().empty());
}

TEST_F(trie_test_fixture, a_path_creates_the_levels_it_needs) {
    probe_tree tree;
    add(tree, levels({ "device", "temp" }), 1);

    // A node counts a child as holding something, so `is_empty()` is about a subtree, not about a node's
    // own payload.
    EXPECT_FALSE(tree.is_empty());
    ASSERT_NE(tree.exact_child("device"), nullptr);
    EXPECT_FALSE(tree.exact_child("device")->is_empty());
    ASSERT_NE(tree.exact_child("device")->exact_child("temp"), nullptr);
    EXPECT_EQ(*tree.exact_child("device")->exact_child("temp")->exact_match_payload(), id_set { 1 });
}

TEST_F(trie_test_fixture, a_wildcard_level_gets_one_shared_subtree) {
    probe_tree tree;
    add(tree, levels({ "device", "+", "temp" }), 1);
    add(tree, levels({ "device", "+", "humidity" }), 2);

    ASSERT_NE(tree.exact_child("device"), nullptr);
    const probe_tree* wildcard = tree.exact_child("device")->single_level_wildcard_child();
    ASSERT_NE(wildcard, nullptr);
    EXPECT_EQ(tree.exact_child("device")->exact_child("+"), nullptr);
    ASSERT_NE(wildcard->exact_child("temp"), nullptr);
    ASSERT_NE(wildcard->exact_child("humidity"), nullptr);
    EXPECT_EQ(*wildcard->exact_child("temp")->exact_match_payload(), id_set { 1 });
    EXPECT_EQ(*wildcard->exact_child("humidity")->exact_match_payload(), id_set { 2 });
}

TEST_F(trie_test_fixture, a_multi_level_level_ends_the_path) {
    probe_tree tree;
    add(tree, levels({ "device", "#" }), 1);

    ASSERT_NE(tree.exact_child("device"), nullptr);
    const probe_tree* device = tree.exact_child("device");
    EXPECT_EQ(device->exact_match_payload(), nullptr);
    EXPECT_EQ(device->single_level_wildcard_child(), nullptr);
    ASSERT_NE(device->multi_level_wildcard_payload(), nullptr);
    EXPECT_EQ(*device->multi_level_wildcard_payload(), id_set { 1 });
    EXPECT_TRUE(device->exact_child("#") == nullptr);
}

TEST_F(trie_test_fixture, a_multi_level_level_stops_the_walk) {
    probe_tree tree;
    id_set& table = tree.table_for(levels({ "device", "#", "temp" }));

    // The walk ends at `#`, so the returned table is the one stored at `#` and `temp` was never visited.
    ASSERT_NE(tree.exact_child("device"), nullptr);
    const probe_tree* device = tree.exact_child("device");
    EXPECT_EQ(&table, device->multi_level_wildcard_payload());
    EXPECT_EQ(device->exact_child("temp"), nullptr);
}

TEST_F(trie_test_fixture, a_literal_level_is_not_a_prefix_match) {
    probe_tree tree;
    add(tree, levels({ "device" }), 1);
    add(tree, levels({ "devicex" }), 2);
    add(tree, levels({ "devi" }), 3);

    EXPECT_EQ(reachable(tree, { "device" }), (std::vector<bool> { true }));
    EXPECT_EQ(reachable(tree, { "devicex" }), (std::vector<bool> { true }));
    EXPECT_EQ(reachable(tree, { "devi" }), (std::vector<bool> { true }));
    EXPECT_EQ(reachable(tree, { "dev" }), (std::vector<bool> { false }));
    EXPECT_EQ(*tree.exact_child("device")->exact_match_payload(), id_set { 1 });
    EXPECT_EQ(*tree.exact_child("devicex")->exact_match_payload(), id_set { 2 });
    EXPECT_EQ(*tree.exact_child("devi")->exact_match_payload(), id_set { 3 });
}

TEST_F(trie_test_fixture, a_string_key_finds_the_same_node_as_a_view) {
    probe_tree tree;
    add(tree, levels({ "device" }), 1);

    const std::string owning = "device";
    EXPECT_EQ(tree.exact_child(owning), tree.exact_child(std::string_view("device")));
    EXPECT_NE(tree.exact_child(owning), nullptr);
}

TEST_F(trie_test_fixture, a_node_holds_both_an_exact_and_a_multi_level_payload) {
    probe_tree tree;
    add(tree, levels({ "device" }), 1);
    add(tree, levels({ "device", "#" }), 2);

    ASSERT_NE(tree.exact_child("device"), nullptr);
    const probe_tree* device = tree.exact_child("device");
    EXPECT_EQ(*device->exact_match_payload(), id_set { 1 });
    EXPECT_EQ(*device->multi_level_wildcard_payload(), id_set { 2 });
}

TEST_F(trie_test_fixture, a_node_carries_several_ids) {
    probe_tree tree;
    add(tree, levels({ "device", "temp" }), 1);
    add(tree, levels({ "device", "temp" }), 2);
    add(tree, levels({ "device", "temp" }), 3);

    EXPECT_EQ(*tree.exact_child("device")->exact_child("temp")->exact_match_payload(), (id_set { 1, 2, 3 }));
}

TEST_F(trie_test_fixture, a_repeated_id_is_stored_once) {
    probe_tree tree;
    add(tree, levels({ "device" }), 7);
    add(tree, levels({ "device" }), 7);

    EXPECT_EQ(tree.exact_child("device")->exact_match_payload()->size(), 1u);
}

TEST_F(trie_test_fixture, a_table_stays_put_while_its_siblings_are_created) {
    probe_tree tree;
    id_set& held = tree.table_for(levels({ "device", "temp" }));
    held.insert(1);

    // Creating siblings must not move the table: the handle is documented to survive until the subtree
    // changes, and a router keeps exactly this kind of handle across an insert.
    for (const char* level : { "a", "b", "c", "d", "e", "f", "g", "h" }) { add(tree, levels({ "device", level }), 2); }

    EXPECT_EQ(held, id_set { 1 });
    held.insert(3);
    EXPECT_EQ(*tree.exact_child("device")->exact_child("temp")->exact_match_payload(), (id_set { 1, 3 }));
}

TEST_F(trie_test_fixture, a_table_keeps_its_address_when_it_is_asked_for_twice) {
    probe_tree tree;
    id_set& first = tree.table_for(levels({ "device", "temp" }));
    id_set& second = tree.table_for(levels({ "device", "temp" }));

    EXPECT_EQ(&first, &second);
}

TEST_F(trie_test_fixture, is_empty_counts_a_child_that_holds_nothing) {
    probe_tree tree;
    const id_set& created = tree.table_for(levels({ "device" }));

    // Documents an edge that is deliberately inconsistent: an engaged-but-empty table makes the level
    // holding it read as empty while the level above it does not, because `exact_children_` — populated
    // regardless of what the child holds — is what the parent tests. Only a `table_for` with no insert
    // after it reaches this state; `update_node` resets a table the moment it empties it.
    EXPECT_TRUE(created.empty());
    EXPECT_NE(tree.exact_child("device")->exact_match_payload(), nullptr);
    EXPECT_TRUE(tree.exact_child("device")->is_empty());
    EXPECT_FALSE(tree.is_empty());
}

TEST_F(trie_test_fixture, update_node_reports_the_missing_level) {
    probe_tree tree;
    const auto outcome = remove(tree, levels({ "device", "temp" }), 1);

    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().level_, "device");
    EXPECT_EQ(outcome.error().depth_, 0u);
}

TEST_F(trie_test_fixture, update_node_reports_how_deep_the_miss_is) {
    probe_tree tree;
    add(tree, levels({ "a", "b", "c" }), 1);

    const auto outcome = remove(tree, levels({ "a", "b", "d" }), 1);

    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().level_, "d");
    EXPECT_EQ(outcome.error().depth_, 2u);
}

TEST_F(trie_test_fixture, update_node_reports_a_missing_wildcard_level) {
    probe_tree tree;
    add(tree, levels({ "a" }), 1);

    const auto missing_wildcard = remove(tree, levels({ "a", "+" }), 1);
    ASSERT_FALSE(missing_wildcard.has_value());
    EXPECT_EQ(missing_wildcard.error().level_, "+");
    EXPECT_EQ(missing_wildcard.error().depth_, 1u);

    const auto missing_multi = remove(tree, levels({ "a", "#" }), 1);
    ASSERT_FALSE(missing_multi.has_value());
    EXPECT_EQ(missing_multi.error().level_, "#");
    EXPECT_EQ(missing_multi.error().depth_, 1u);
}

TEST_F(trie_test_fixture, update_node_reports_an_empty_path_with_no_payload) {
    probe_tree tree;
    const auto outcome = tree.update_node(std::span<const probe_level>(), [](id_set&) {});

    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().level_, "");
    EXPECT_EQ(outcome.error().depth_, 0u);
}

TEST_F(trie_test_fixture, update_node_reaches_a_payload_at_an_empty_path) {
    probe_tree tree;
    add(tree, levels({ "device" }), 1);
    id_set& device = tree.table_for(levels({ "device" }));
    device.clear();

    // A table that exists and is empty is still a miss, so an update cannot revive it.
    const auto outcome = tree.update_node(std::span<const probe_level>(), [](id_set& ids) { ids.insert(9); });
    EXPECT_EQ(device, id_set {});
    EXPECT_FALSE(outcome.has_value());
}

TEST_F(trie_test_fixture, update_node_can_add_as_well_as_remove) {
    probe_tree tree;
    id_set& table = tree.table_for(levels({ "device" }));
    EXPECT_TRUE(table.empty());

    const auto outcome
        = tree.update_node(std::span<const probe_level>(levels({ "device" })), [](id_set& ids) { ids.insert(5); });

    ASSERT_TRUE(outcome.has_value());
    EXPECT_FALSE(outcome.value());
    EXPECT_EQ(*tree.exact_child("device")->exact_match_payload(), id_set { 5 });
}

TEST_F(trie_test_fixture, emptying_a_payload_prunes_its_whole_branch) {
    probe_tree tree;
    add(tree, levels({ "device", "temp" }), 1);

    const auto outcome = remove(tree, levels({ "device", "temp" }), 1);

    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome.value());
    EXPECT_TRUE(tree.is_empty());
    EXPECT_EQ(tree.exact_child("device"), nullptr);
}

TEST_F(trie_test_fixture, emptying_a_wildcard_payload_prunes_the_wildcard_branch) {
    probe_tree tree;
    add(tree, levels({ "device", "+", "temp" }), 1);

    const auto outcome = remove(tree, levels({ "device", "+", "temp" }), 1);

    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome.value());
    EXPECT_TRUE(tree.is_empty());
    EXPECT_EQ(tree.exact_child("device"), nullptr);
}

TEST_F(trie_test_fixture, emptying_a_multi_level_payload_prunes_it) {
    probe_tree tree;
    add(tree, levels({ "device", "#" }), 1);

    const auto outcome = remove(tree, levels({ "device", "#" }), 1);

    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome.value());
    EXPECT_TRUE(tree.is_empty());
    EXPECT_EQ(tree.exact_child("device"), nullptr);
}

TEST_F(trie_test_fixture, removing_one_id_keeps_the_node_and_the_rest) {
    probe_tree tree;
    add(tree, levels({ "device", "temp" }), 1);
    add(tree, levels({ "device", "temp" }), 2);

    const auto outcome = remove(tree, levels({ "device", "temp" }), 1);

    ASSERT_TRUE(outcome.has_value());
    EXPECT_FALSE(outcome.value());
    ASSERT_NE(tree.exact_child("device"), nullptr);
    EXPECT_EQ(*tree.exact_child("device")->exact_child("temp")->exact_match_payload(), id_set { 2 });
}

TEST_F(trie_test_fixture, pruning_spares_a_sibling_wildcard_under_the_same_level) {
    probe_tree tree;
    add(tree, levels({ "device", "temp" }), 1);
    add(tree, levels({ "device", "+" }), 2);

    const auto outcome = remove(tree, levels({ "device", "temp" }), 1);

    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome.value());
    ASSERT_NE(tree.exact_child("device"), nullptr);
    EXPECT_EQ(tree.exact_child("device")->exact_child("temp"), nullptr);
    ASSERT_NE(tree.exact_child("device")->single_level_wildcard_child(), nullptr);
    EXPECT_EQ(*tree.exact_child("device")->single_level_wildcard_child()->exact_match_payload(), id_set { 2 });
}

TEST_F(trie_test_fixture, pruning_spares_a_multi_level_payload_at_the_same_node) {
    probe_tree tree;
    add(tree, levels({ "device" }), 1);
    add(tree, levels({ "device", "#" }), 2);

    const auto outcome = remove(tree, levels({ "device" }), 1);

    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome.value());
    ASSERT_NE(tree.exact_child("device"), nullptr);
    EXPECT_EQ(tree.exact_child("device")->exact_match_payload(), nullptr);
    ASSERT_NE(tree.exact_child("device")->multi_level_wildcard_payload(), nullptr);
    EXPECT_EQ(*tree.exact_child("device")->multi_level_wildcard_payload(), id_set { 2 });
}

TEST_F(trie_test_fixture, pruning_spares_a_deeper_sequence) {
    probe_tree tree;
    add(tree, levels({ "a", "b", "c" }), 1);
    add(tree, levels({ "a", "b", "d" }), 2);

    const auto outcome = remove(tree, levels({ "a", "b", "c" }), 1);

    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome.value());
    // The `a/b` level outlives its own table, because a sibling sequence still hangs below it. The level's
    // payload is gone, so a lookup there finds nothing.
    ASSERT_NE(tree.exact_child("a"), nullptr);
    ASSERT_NE(tree.exact_child("a")->exact_child("b"), nullptr);
    EXPECT_EQ(tree.exact_child("a")->exact_child("b")->exact_match_payload(), nullptr);
    EXPECT_EQ(tree.exact_child("a")->exact_child("b")->exact_child("c"), nullptr);
    EXPECT_EQ(*tree.exact_child("a")->exact_child("b")->exact_child("d")->exact_match_payload(), id_set { 2 });
}

TEST_F(trie_test_fixture, pruning_a_shared_level_spares_the_deeper_sequence) {
    probe_tree tree;
    add(tree, levels({ "a" }), 1);
    add(tree, levels({ "a", "b" }), 2);

    const auto outcome = remove(tree, levels({ "a" }), 1);

    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome.value());
    ASSERT_NE(tree.exact_child("a"), nullptr);
    EXPECT_EQ(tree.exact_child("a")->exact_match_payload(), nullptr);
    EXPECT_EQ(*tree.exact_child("a")->exact_child("b")->exact_match_payload(), id_set { 2 });
}

TEST_F(trie_test_fixture, emptying_every_payload_leaves_an_empty_tree) {
    probe_tree tree;
    add(tree, levels({ "device", "temp" }), 1);
    add(tree, levels({ "device", "+" }), 2);
    add(tree, levels({ "device", "#" }), 3);

    EXPECT_TRUE(remove(tree, levels({ "device", "temp" }), 1).value());
    EXPECT_TRUE(remove(tree, levels({ "device", "+" }), 2).value());
    EXPECT_TRUE(remove(tree, levels({ "device", "#" }), 3).value());

    EXPECT_TRUE(tree.is_empty());
    EXPECT_TRUE(tree.collect_payloads<probe_key>().empty());
}

TEST_F(trie_test_fixture, the_walk_reports_every_branch) {
    probe_tree tree;
    add(tree, levels({ "device", "temp" }), 1);
    add(tree, levels({ "device", "+" }), 2);
    add(tree, levels({ "device", "#" }), 3);

    EXPECT_EQ(
        snapshot(tree), (std::vector<std::pair<std::string, std::vector<int>>> {
                            { "device/#",    { 3 } },
                            { "device/+",    { 2 } },
                            { "device/temp", { 1 } }
    })
    );
}

TEST_F(trie_test_fixture, the_walk_reports_nested_branches) {
    probe_tree tree;
    add(tree, levels({ "a", "b" }), 1);
    add(tree, levels({ "a", "+", "c" }), 2);
    add(tree, levels({ "a", "#" }), 3);
    add(tree, levels({ "x", "+", "+" }), 4);

    EXPECT_EQ(
        snapshot(tree), (std::vector<std::pair<std::string, std::vector<int>>> {
                            { "a/#",   { 3 } },
                            { "a/+/c", { 2 } },
                            { "a/b",   { 1 } },
                            { "x/+/+", { 4 } },
    })
    );
}

TEST_F(trie_test_fixture, the_walk_reports_the_ids_of_each_sequence) {
    probe_tree tree;
    add(tree, levels({ "device", "temp" }), 1);
    add(tree, levels({ "device", "temp" }), 2);
    add(tree, levels({ "device", "#" }), 2);

    EXPECT_EQ(
        snapshot(tree), (std::vector<std::pair<std::string, std::vector<int>>> {
                            { "device/#",    { 2 }    },
                            { "device/temp", { 1, 2 } },
    })
    );
}

TEST_F(trie_test_fixture, a_walk_finds_the_payloads_a_pruning_left) {
    probe_tree tree;
    add(tree, levels({ "device", "temp" }), 1);
    add(tree, levels({ "device", "#" }), 2);
    add(tree, levels({ "other", "+" }), 3);

    EXPECT_TRUE(remove(tree, levels({ "device", "temp" }), 1).value());

    EXPECT_EQ(
        snapshot(tree), (std::vector<std::pair<std::string, std::vector<int>>> {
                            { "device/#", { 2 } },
                            { "other/+",  { 3 } },
    })
    );
}

TEST_F(trie_test_fixture, a_moved_tree_keeps_its_payloads) {
    probe_tree source;
    add(source, levels({ "device", "+" }), 1);
    add(source, levels({ "device", "#" }), 2);

    const probe_tree moved(std::move(source));

    EXPECT_FALSE(moved.is_empty());
    EXPECT_EQ(*moved.exact_child("device")->multi_level_wildcard_payload(), id_set { 2 });
    EXPECT_EQ(*moved.exact_child("device")->single_level_wildcard_child()->exact_match_payload(), id_set { 1 });
}

TEST_F(trie_test_fixture, a_different_payload_container_works) {
    trie<std::vector<int>, probe_level> tree;
    tree.table_for(levels({ "device", "+" })).push_back(1);
    tree.table_for(levels({ "device", "+" })).push_back(2);

    ASSERT_NE(tree.exact_child("device"), nullptr);
    ASSERT_NE(tree.exact_child("device")->single_level_wildcard_child(), nullptr);
    EXPECT_EQ(
        *tree.exact_child("device")->single_level_wildcard_child()->exact_match_payload(), (std::vector<int> { 1, 2 })
    );

    const auto outcome
        = tree.update_node(std::span<const probe_level>(levels({ "device", "+" })), [](std::vector<int>& values) {
              values.clear();
          });
    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome.value());
    EXPECT_TRUE(tree.is_empty());
}

} // namespace

} // namespace ba7lya::msgbus
