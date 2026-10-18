///
/// @file publisher_test.cxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief Unit tests for `publisher`: the provenance it stamps, the two publishing paths, and the
/// lifetime it borrows its bus for.
/// @version 0.1
/// @date 2026-10-17
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#include <any>
#include <chrono>
#include <cstdint>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common.hxx"
#include "message.hxx"
#include "msgbus.hxx"
#include "publisher.hxx"

namespace ba7lya::msgbus {

namespace {

/// @brief A bus that only records what was published: enough for the publisher, and nothing else.
/// @details The facade's whole job is what it puts into the `message`, so a bus that stores the
/// messages verbatim is the narrowest thing that can show it. The subscription half is present because
/// the interface has it, not because these cases use it.
class recording_bus final : public msgbus {
public:
    result<subscription, error> subscribe(std::string_view, handler, dispatch_options, error_handler) override {
        return std::unexpected(error::make(error_code::EMPTY_INPUT, "the recording bus subscribes nothing"));
    }

    bool unsubscribe(subscription_id) override { return false; }

    void publish_sync(message msg) override { sync.push_back(std::move(msg)); }

    publish_status publish_async(message msg) override {
        if (full) { return publish_status::REJECTED; }
        async.push_back(std::move(msg));
        return publish_status::ACCEPTED;
    }

    bool flush(std::chrono::milliseconds) override {
        flushed += 1;
        return true;
    }

    std::vector<message> sync;
    std::vector<message> async;
    std::uint64_t flushed = 0;
    bool full = false;
};

} // namespace

// --- what the publisher promises as a type -----------------------------------------------------------

TEST(publisher_test, the_publisher_is_not_copyable) {
    // Two publishers named the same source would issue two overlapping sequence streams under one
    // name, which is the one thing `message_meta::seq_` cannot mean.
    static_assert(!std::is_copy_constructible_v<publisher>);
    static_assert(!std::is_copy_assignable_v<publisher>);
    static_assert(!std::is_move_constructible_v<publisher>);
    static_assert(!std::is_move_assignable_v<publisher>);
}

// --- provenance --------------------------------------------------------------------------------------

TEST(publisher_test, a_synchronous_publish_stamps_source_and_a_rising_sequence) {
    recording_bus bus;
    publisher sender(bus, "serial");

    sender.publish_sync("device/room1/temp", 21);
    sender.publish_sync("device/room1/temp", 22);

    ASSERT_EQ(bus.sync.size(), 2u);
    EXPECT_EQ(bus.sync[0].meta_.source_, "serial");
    EXPECT_EQ(bus.sync[1].meta_.source_, "serial");
    EXPECT_EQ(bus.sync[0].meta_.seq_, 1u);
    EXPECT_EQ(bus.sync[1].meta_.seq_, 2u);
    EXPECT_EQ(sender.sent(), 2u);
}

TEST(publisher_test, a_stamped_message_carries_the_topic_and_payload_it_was_given) {
    recording_bus bus;
    publisher sender(bus);

    sender.publish_sync("a/b", std::string("payload"));

    ASSERT_EQ(bus.sync.size(), 1u);
    EXPECT_EQ(bus.sync[0].topic_, "a/b");
    EXPECT_EQ(std::any_cast<std::string>(bus.sync[0].payload_), "payload");
}

TEST(publisher_test, an_empty_source_is_carried_as_empty_rather_than_refused) {
    recording_bus bus;
    publisher sender(bus);

    EXPECT_TRUE(sender.source().empty());
    sender.publish_sync("a", 1);

    ASSERT_EQ(bus.sync.size(), 1u);
    EXPECT_TRUE(bus.sync[0].meta_.source_.empty());
}

TEST(publisher_test, the_stamp_is_the_time_of_the_publish) {
    recording_bus bus;
    publisher sender(bus, "clock");

    const auto before = std::chrono::system_clock::now();
    sender.publish_sync("a", 1);
    const auto after = std::chrono::system_clock::now();

    ASSERT_EQ(bus.sync.size(), 1u);
    EXPECT_GE(bus.sync[0].meta_.stamp_, before);
    EXPECT_LE(bus.sync[0].meta_.stamp_, after);
}

TEST(publisher_test, an_asynchronous_publish_is_stamped_the_same_way) {
    recording_bus bus;
    publisher sender(bus, "async");

    EXPECT_EQ(sender.publish_async("a", 1), publish_status::ACCEPTED);
    EXPECT_EQ(sender.publish_async("a", 2), publish_status::ACCEPTED);

    ASSERT_EQ(bus.async.size(), 2u);
    EXPECT_EQ(bus.async[0].meta_.source_, "async");
    EXPECT_EQ(bus.async[0].meta_.seq_, 1u);
    EXPECT_EQ(bus.async[1].meta_.seq_, 2u);
}

TEST(publisher_test, a_rejected_asynchronous_publish_still_consumes_a_sequence_number) {
    // The counter records what the publisher emitted, not what the bus accepted: a rejection is a
    // gap in the stream, and the subscriber can see that gap rather than a silently renumbered message.
    recording_bus bus;
    publisher sender(bus, "commands");
    bus.full = true;

    EXPECT_EQ(sender.publish_async("a", 1), publish_status::REJECTED);
    EXPECT_EQ(sender.sent(), 1u);

    bus.full = false;
    EXPECT_EQ(sender.publish_async("a", 2), publish_status::ACCEPTED);
    ASSERT_EQ(bus.async.size(), 1u);
    EXPECT_EQ(bus.async[0].meta_.seq_, 2u);
}

TEST(publisher_test, a_message_handed_over_whole_keeps_its_own_provenance) {
    // The relay path: the stamp belongs to the original publish, and replacing it would destroy the
    // only record of when the message really arrived.
    recording_bus bus;
    publisher sender(bus, "relay");

    message relaying {
        "a/b", 7, message_meta { {}, 42u, "upstream" }
    };
    sender.publish_sync(std::move(relaying));

    ASSERT_EQ(bus.sync.size(), 1u);
    EXPECT_EQ(bus.sync[0].meta_.source_, "upstream");
    EXPECT_EQ(bus.sync[0].meta_.seq_, 42u);
    EXPECT_EQ(sender.sent(), 0u) << "nothing was stamped, so nothing was counted";
}

// --- forwarding --------------------------------------------------------------------------------------

TEST(publisher_test, flushing_waits_on_the_bus) {
    recording_bus bus;
    publisher sender(bus);

    EXPECT_TRUE(sender.flush());
    EXPECT_TRUE(sender.flush(std::chrono::milliseconds(1)));
    EXPECT_EQ(bus.flushed, 2u);
}

} // namespace ba7lya::msgbus
