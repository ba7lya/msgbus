///
/// @file trie.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief A prefix tree over sequences of levels: one node per level, and the structural algorithms that
/// insert into it, prune it and walk it.
/// @version 0.1
/// @date 2026-10-14
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <concepts>
#include <cstddef>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ba7lya::msgbus {

///
/// @brief The role a level plays in the sequence it belongs to.
/// @details The tree treats each role differently: a literal level gets a child of its own, a single-level
/// wildcard gets one shared child, and a multi-level wildcard gets a payload and no child — being the last
/// level by definition.
///
enum class level_kind {
    LITERAL,      ///< stands for itself
    SINGLE_LEVEL, ///< stands for exactly one level
    MULTI_LEVEL,  ///< stands for the remaining levels, and only ever ends a sequence
};

///
/// @brief What the tree needs of a level: which role it plays, the text it stands for, and the three roles'
/// constructors so that a walk can spell a path back out.
/// @details This is everything the tree knows about the levels it indexes, and deliberately the whole of it:
/// nothing here mentions topics, filters or matching. A level vocabulary opts in by providing these members,
/// which is what lets the tree stay a plain container.
/// @note `text()` is the level's own spelling — the literal text for a literal level, and the wildcard's
/// spelling otherwise.
///
template<typename L>
concept trie_level = requires(const L& level, std::string_view literal) {
    { level.kind() } -> std::convertible_to<level_kind>;
    { level.text() } -> std::convertible_to<std::string_view>;
    { L::literal(literal) } -> std::convertible_to<L>;
    { L::single_level() } -> std::convertible_to<L>;
    { L::multi_level() } -> std::convertible_to<L>;
};

///
/// @brief What the tree needs of the key it is indexed by: its levels, and a way to rebuild it from a run of
/// levels so that a walk can report which sequence a payload was stored under.
/// @note `from_levels` carries no error channel: every sequence already in the tree was validated before it
/// was inserted, so rebuilding one cannot fail.
///
template<typename K, typename L>
concept trie_key = requires(const K& key, std::span<const L> levels) {
    { key.levels() } -> std::convertible_to<std::span<const L>>;
    { K::from_levels(levels) } -> std::convertible_to<K>;
};

/// @brief What a node may hold as a payload: a container that can say whether it is empty.
/// @details The Rust original spells this as the `Len` trait, implemented for `HashSet` and `HashMap`. A
/// concept does the same job here without type erasure, so the payload type stays fully visible to the optimizer.
template<typename T>
concept payload_container = std::default_initializable<T> && requires(const T& container) {
    { container.empty() } -> std::convertible_to<bool>;
};

///
/// @brief Where a walk failed: the level it looked for and did not find, and how deep it had got when it
/// looked.
/// @details The tree reports a miss in its own terms rather than in a caller's error vocabulary. A level
/// vocabulary maps this onto whatever it publishes, or ignores it.
///
struct trie_miss {
    /// @brief The level that was looked for and not found; empty when the path itself was empty.
    std::string level_;
    /// @brief How many levels the walk descended before it failed.
    std::size_t depth_ = 0;

    /// @brief Whether two misses name the same level at the same depth.
    [[nodiscard]]
    friend bool operator==(const trie_miss&, const trie_miss&) = default;
};

///
/// @brief Hashes a key the way `std::hash<std::string_view>` would, so a `std::string_view` level can be
/// looked up in a `std::string`-keyed map without building a temporary string.
/// @details Matching runs once per delivered message per subscription, so the allocation a converting lookup would cost
/// is worth designing out. Both overloads agree on the value of a key, which is what makes the heterogeneous lookup
/// sound.
///
struct transparent_string_hash {
    /// @brief Marks the hash as usable for heterogeneous lookup.
    using is_transparent = void;

    /// @brief Hashes a view of the key.
    [[nodiscard]]
    std::size_t operator()(std::string_view text) const noexcept {
        return std::hash<std::string_view> {}(text);
    }

    /// @brief Hashes an owning key; identical to the view overload for the same characters.
    [[nodiscard]]
    std::size_t operator()(const std::string& text) const noexcept {
        return std::hash<std::string_view> {}(text);
    }
};

///
/// @brief One node of a level-sequence prefix tree.
/// @details Four slots, one per way a sequence can continue from here: a literal level's payload, the literal
/// levels' subtrees, the single-level wildcard's subtree, and the multi-level wildcard's payload.
/// @details The class knows nothing of matching: it stores sequences and hands back the payloads it holds.
/// Which of them a given input should reach is the caller's question, and it walks the tree through the four
/// read-only accessors below.
/// @tparam T the payload type, held per node
/// @tparam Level the level vocabulary, which supplies the whole of what the tree knows about its keys
/// @note Header-only by necessity: both the payload type and the level type are template parameters, so no
/// translation unit can instantiate the members for them.
///
template<payload_container T, trie_level Level>
class trie {
public:
    trie() = default;

    trie(const trie&) = delete;
    trie& operator=(const trie&) = delete;
    trie(trie&&) noexcept = default;
    trie& operator=(trie&&) noexcept = default;

    ~trie() = default;

    /// @brief Whether this node holds no payload and has no children, so its parent may drop it.
    [[nodiscard]]
    bool is_empty() const {
        return (!exact_match_data_.has_value() || exact_match_data_->empty()) && exact_children_.empty()
            && !single_level_wildcard_node_
            && (!multi_level_wildcard_data_.has_value() || multi_level_wildcard_data_->empty());
    }

    /// @brief Returns the payload table for `levels`, creating it and any missing levels.
    /// @details The returned reference stays valid until the node's subtree is modified: children live in
    /// `std::unique_ptr`s, so creating a sibling table cannot move this one.
    [[nodiscard]]
    T& table_for(std::span<const Level> levels) {
        trie* current = this;
        for (const Level& level : levels) {
            if (level.kind() == level_kind::LITERAL) {
                current = &current->exact_child_or_create(level.text());
                continue;
            }
            if (level.kind() == level_kind::SINGLE_LEVEL) {
                if (!current->single_level_wildcard_node_) {
                    current->single_level_wildcard_node_ = std::make_unique<trie>();
                }
                current = current->single_level_wildcard_node_.get();
                continue;
            }
            // A multi-level level ends a sequence by construction, so its payload belongs to this node and
            // the walk ends here.
            if (!current->multi_level_wildcard_data_.has_value()) { current->multi_level_wildcard_data_.emplace(); }
            return *current->multi_level_wildcard_data_;
        }

        if (!current->exact_match_data_.has_value()) { current->exact_match_data_.emplace(); }
        return *current->exact_match_data_;
    }

    /// @brief Convenience: the payload table stored under `key`'s levels.
    template<trie_key<Level> K>
    [[nodiscard]]
    T& table_for(const K& key) {
        return table_for(key.levels());
    }

    /// @brief Applies `update` to the payload table stored at `path`, pruning what it empties.
    /// @details Returns whether the payload table at `path` came out empty — the question a caller has to ask
    /// before it drops a subscription, since two distinct sequences may share one node. The node a table is
    /// dropped from is a separate matter: a callee prunes under itself whenever the caller reports the table
    /// gone, and this node then drops the branch only if that left it with nothing at all.
    /// @note A level of `path` the tree does not have is reported as a `trie_miss`.
    template<typename F>
        requires std::invocable<F&, T&>
    std::expected<bool, trie_miss> update_node(std::span<const Level> path, F update) {
        return update_node_at(path, update, 0);
    }

    /// @brief Every `(key, payload)` pair in the tree, by walking every branch.
    /// @details A diagnostic helper for asserting what a caller actually holds; production code that wants the
    /// active payloads should keep a flat table and not walk the tree. The order is unspecified — the levels
    /// live in a hash map — so compare the result as a set.
    template<trie_key<Level> K>
    [[nodiscard]]
    std::vector<std::pair<K, const T*>> collect_payloads() const {
        std::vector<std::pair<K, const T*>> collected;
        std::vector<Level> current_path;
        collect_payloads_internal<K>(current_path, collected);
        return collected;
    }

    /// @brief The payload of a sequence that ends at this node, if any.
    [[nodiscard]]
    const T* exact_match_payload() const noexcept {
        return exact_match_data_.has_value() ? &*exact_match_data_ : nullptr;
    }

    /// @brief The payload of a sequence that ends in the multi-level wildcard at this node, if any.
    [[nodiscard]]
    const T* multi_level_wildcard_payload() const noexcept {
        return multi_level_wildcard_data_.has_value() ? &*multi_level_wildcard_data_ : nullptr;
    }

    /// @brief The subtree for a literal level, or null when the level is absent.
    /// @note Not `noexcept`: the lookup hashes the key.
    [[nodiscard]]
    const trie* exact_child(std::string_view level) const {
        const auto child = exact_children_.find(level);
        return child == exact_children_.end() ? nullptr : child->second.get();
    }

    /// @brief The subtree for the single-level wildcard, or null when the level is absent.
    [[nodiscard]]
    const trie* single_level_wildcard_child() const noexcept {
        return single_level_wildcard_node_.get();
    }

private:
    /// @brief Returns the subtree for a literal level, creating it if absent.
    /// @note `find` is heterogeneous, so an already-present level is reached without building a temporary key.
    [[nodiscard]]
    trie& exact_child_or_create(std::string_view level) {
        const auto existing = exact_children_.find(level);
        if (existing != exact_children_.end()) {
            if (!existing->second) { existing->second = std::make_unique<trie>(); }
            return *existing->second;
        }
        auto& created = exact_children_[std::string(level)];
        created = std::make_unique<trie>();
        return *created;
    }

    /// @brief The pruning walk, carrying how deep it is so a miss can say where it happened.
    template<typename F>
        requires std::invocable<F&, T&>
    std::expected<bool, trie_miss> update_node_at(std::span<const Level> path, F& update, std::size_t depth) {
        if (path.empty()) {
            if (!exact_match_data_.has_value()) { return std::unexpected(trie_miss { "", depth }); }
            update(*exact_match_data_);
            if (!exact_match_data_->empty()) { return false; }
            exact_match_data_.reset();
            return true;
        }

        const Level& current_level = path.front();
        const std::span<const Level> rest = path.subspan(1);

        if (current_level.kind() == level_kind::LITERAL) {
            const auto child = exact_children_.find(current_level.text());
            if (child == exact_children_.end()) {
                return std::unexpected(trie_miss { std::string(current_level.text()), depth });
            }
            const auto pruned = child->second->update_node_at(rest, update, depth + 1);
            if (!pruned) { return std::unexpected(pruned.error()); }
            // The level's table is gone, so drop the level itself — but only if the subtree below it has
            // nothing left either, since a multi-level payload or a sibling wildcard may still live on.
            if (pruned.value() && child->second->is_empty()) { exact_children_.erase(child); }
            return pruned.value();
        }

        if (current_level.kind() == level_kind::SINGLE_LEVEL) {
            if (!single_level_wildcard_node_) {
                return std::unexpected(trie_miss { std::string(current_level.text()), depth });
            }
            const auto pruned = single_level_wildcard_node_->update_node_at(rest, update, depth + 1);
            if (!pruned) { return std::unexpected(pruned.error()); }
            if (pruned.value() && single_level_wildcard_node_->is_empty()) { single_level_wildcard_node_.reset(); }
            return pruned.value();
        }

        if (!multi_level_wildcard_data_.has_value()) {
            return std::unexpected(trie_miss { std::string(current_level.text()), depth });
        }
        update(*multi_level_wildcard_data_);
        if (!multi_level_wildcard_data_->empty()) { return false; }
        multi_level_wildcard_data_.reset();
        return true;
    }

    /// @brief Walks the subtree, rebuilding the sequence each payload was reached by.
    template<trie_key<Level> K>
    void collect_payloads_internal(
        std::vector<Level>& current_path, std::vector<std::pair<K, const T*>>& collected
    ) const {
        auto append = [&current_path, &collected](const T* payload) {
            collected.emplace_back(K::from_levels(current_path), payload);
        };

        if (exact_match_data_.has_value()) { append(&*exact_match_data_); }
        if (multi_level_wildcard_data_.has_value()) {
            current_path.push_back(Level::multi_level());
            append(&*multi_level_wildcard_data_);
            current_path.pop_back();
        }
        if (single_level_wildcard_node_) {
            current_path.push_back(Level::single_level());
            single_level_wildcard_node_->template collect_payloads_internal<K>(current_path, collected);
            current_path.pop_back();
        }
        for (const auto& [text, child] : exact_children_) {
            current_path.push_back(Level::literal(text));
            child->template collect_payloads_internal<K>(current_path, collected);
            current_path.pop_back();
        }
    }

    /// @brief Payload for a sequence that ends at this node.
    std::optional<T> exact_match_data_;
    /// @brief Subtrees for literal next levels.
    /// @details The subtree is held by pointer rather than by value: a node may not contain an incomplete type
    /// by value, and the indirection also keeps a payload table's address stable while its siblings are
    /// created.
    std::unordered_map<std::string, std::unique_ptr<trie>, transparent_string_hash, std::equal_to<>> exact_children_;
    /// @brief Subtree for the single-level wildcard.
    std::unique_ptr<trie> single_level_wildcard_node_;
    /// @brief Payload for a sequence that ends in the multi-level wildcard.
    std::optional<T> multi_level_wildcard_data_;
};

} // namespace ba7lya::msgbus
