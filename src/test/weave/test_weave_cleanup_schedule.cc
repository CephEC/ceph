// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "gtest/gtest.h"

#include <atomic>
#include <future>

#include "osd/weave/WeaveCleanupSchedule.h"
#include "osd/weave/detail/WeaveReclaimTimer.h"

using ceph::weave::WeaveCleanupSchedule;
using ceph::weave::WeaveReclaimTimer;

namespace {
constexpr int64_t DAY = 86400;
constexpr int64_t NOON = 12 * 3600;
}

TEST(WeaveReclaimTimer, FutureOccurrenceWithoutStartupCatchup)
{
  WeaveReclaimTimer schedule;
  ASSERT_TRUE(schedule.set_time("12:00", NOON + 1));
  EXPECT_FALSE(schedule.due(NOON + 2));
  EXPECT_FALSE(schedule.due(DAY + NOON - 1));
  EXPECT_TRUE(schedule.due(DAY + NOON));
  EXPECT_FALSE(schedule.due(DAY + NOON));
  EXPECT_FALSE(schedule.due(DAY + NOON + 1));
  EXPECT_TRUE(schedule.due(2 * DAY + NOON));

  WeaveReclaimTimer exact_start;
  ASSERT_TRUE(exact_start.set_time("12:00", NOON));
  EXPECT_FALSE(exact_start.due(NOON));
  EXPECT_TRUE(exact_start.due(DAY + NOON));
}

TEST(WeaveReclaimTimer, MidnightAndDelayedTick)
{
  WeaveReclaimTimer midnight;
  ASSERT_TRUE(midnight.set_time("00:00", DAY - 1));
  EXPECT_FALSE(midnight.due(DAY - 1));
  EXPECT_TRUE(midnight.due(DAY));
  EXPECT_FALSE(midnight.due(DAY + 1));

  WeaveReclaimTimer late_tick;
  ASSERT_TRUE(late_tick.set_time("23:59", 0));
  EXPECT_TRUE(late_tick.due(DAY + 1));
  EXPECT_FALSE(late_tick.due(DAY + 2));
  EXPECT_TRUE(late_tick.due(2 * DAY - 60));
}

TEST(WeaveReclaimTimer, ClockJumpsDoNotReplayMissedDays)
{
  WeaveReclaimTimer schedule;
  ASSERT_TRUE(schedule.set_time("12:00", 0));
  EXPECT_TRUE(schedule.due(5 * DAY + NOON));
  EXPECT_FALSE(schedule.due(5 * DAY + NOON + 1));
  EXPECT_FALSE(schedule.due(4 * DAY + NOON));
  EXPECT_FALSE(schedule.due(5 * DAY + NOON));
  EXPECT_TRUE(schedule.due(6 * DAY + NOON));
}

TEST(WeaveReclaimTimer, TimeChangesAndDisablePreserveConsumedDate)
{
  WeaveReclaimTimer schedule;
  ASSERT_TRUE(schedule.set_time("12:00", 0));
  EXPECT_TRUE(schedule.due(NOON));
  ASSERT_TRUE(schedule.set_time("13:00", NOON + 1));
  EXPECT_FALSE(schedule.due(13 * 3600));
  EXPECT_TRUE(schedule.due(DAY + 13 * 3600));
  ASSERT_TRUE(schedule.set_time("", DAY + 13 * 3600 + 1));
  EXPECT_FALSE(schedule.due(2 * DAY));
  // A backwards clock step followed by re-enabling cannot repeat either date.
  ASSERT_TRUE(schedule.set_time("12:00", 0));
  EXPECT_FALSE(schedule.due(NOON));
  EXPECT_FALSE(schedule.due(DAY + NOON));
  EXPECT_TRUE(schedule.due(2 * DAY + NOON));
}

TEST(WeaveReclaimTimer, EnablingOrChangingPastTimeDoesNotCatchUp)
{
  WeaveReclaimTimer schedule;
  EXPECT_FALSE(schedule.due(NOON));
  ASSERT_TRUE(schedule.set_time("13:00", NOON));
  ASSERT_TRUE(schedule.set_time("11:00", NOON + 1));
  EXPECT_FALSE(schedule.due(NOON + 2));
  EXPECT_TRUE(schedule.due(DAY + 11 * 3600));
}


TEST(WeaveReclaimTimer, CompletionTimeDoesNotShiftNextDeadline)
{
  WeaveReclaimTimer schedule;
  ASSERT_TRUE(schedule.set_time("01:00", 0));
  EXPECT_EQ(schedule.next_deadline(), 3600);
  EXPECT_TRUE(schedule.due(3600));
  EXPECT_FALSE(schedule.due(7200)); // job finishes at 02:00
  EXPECT_EQ(schedule.next_deadline(), DAY + 3600);
  EXPECT_TRUE(schedule.due(DAY + 3600));
}

TEST(WeaveReclaimTimer, ReactivationKeepsConsumedDate)
{
  WeaveReclaimTimer schedule;
  ASSERT_TRUE(schedule.set_time("01:00", 0));
  EXPECT_TRUE(schedule.due(3600));
  ASSERT_TRUE(schedule.set_time("01:00", 7200)); // same PG becomes active again
  EXPECT_EQ(schedule.next_deadline(), DAY + 3600);
  EXPECT_FALSE(schedule.due(7200));
}

namespace {
class WeaveCleanupScheduleTest : public ::testing::Test {
protected:
  using Ticket = WeaveCleanupSchedule::Ticket;
  ceph::timer<ceph::real_clock> timer{ceph::construct_suspended};
  WeaveCleanupSchedule schedule{timer};

  Ticket enqueue_cleanup() {
    auto queued = std::make_shared<std::promise<Ticket>>();
    auto future = queued->get_future();
    // A historical wall-clock deadline expires immediately on the real timer.
    EXPECT_TRUE(schedule.configure("01:00", 0));
    schedule.schedule([queued](Ticket ticket) {
      queued->set_value(std::move(ticket));
    });
    if (future.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
      ADD_FAILURE() << "timer did not enqueue cleanup";
      return {};
    }
    return future.get();
  }
};

TEST_F(WeaveCleanupScheduleTest, QueuedDeliveryIsConsumedOnlyOnce)
{
  timer.resume();
  auto ticket = enqueue_cleanup();
  ASSERT_NE(ticket, nullptr);
  auto duplicates = std::make_shared<std::atomic<unsigned>>(0);
  schedule.schedule([duplicates](Ticket) { ++*duplicates; });
  EXPECT_TRUE(schedule.consume_due_ticket(ticket, 3600));
  EXPECT_FALSE(schedule.consume_due_ticket(ticket, 3600));
  timer.suspend();
  EXPECT_EQ(*duplicates, 0u);
}

TEST_F(WeaveCleanupScheduleTest, ConfigChangeInvalidatesQueuedDelivery)
{
  timer.resume();
  auto old = enqueue_cleanup();
  ASSERT_NE(old, nullptr);
  ASSERT_TRUE(schedule.configure("02:00", 0));
  EXPECT_FALSE(old->valid);
  EXPECT_FALSE(schedule.consume_due_ticket(old, 7200));
}

TEST_F(WeaveCleanupScheduleTest, DisableCancelsDeadlineAndQueuedDelivery)
{
  auto queued = std::make_shared<std::atomic<unsigned>>(0);
  ASSERT_TRUE(schedule.configure("01:00", 0));
  schedule.schedule([queued](Ticket) { ++*queued; });
  ASSERT_TRUE(schedule.configure("", 0));
  schedule.schedule([queued](Ticket) { ++*queued; });
  timer.resume();
  auto ticket = enqueue_cleanup(); // also acts as a timer barrier
  ASSERT_NE(ticket, nullptr);
  EXPECT_EQ(*queued, 0u);
  ASSERT_TRUE(schedule.configure("", 0));
  EXPECT_FALSE(schedule.consume_due_ticket(ticket, 3600));
}

TEST_F(WeaveCleanupScheduleTest, OldRoleCannotConsumeReactivatedSchedule)
{
  timer.resume();
  auto old = enqueue_cleanup();
  ASSERT_NE(old, nullptr);
  schedule.cancel();
  auto current = enqueue_cleanup();
  ASSERT_NE(current, nullptr);
  EXPECT_FALSE(schedule.consume_due_ticket(old, 3600));
  EXPECT_TRUE(schedule.consume_due_ticket(current, 3600));
}

TEST_F(WeaveCleanupScheduleTest, ClockMovedBackBeforeExecutionRearmsDeadline)
{
  timer.resume();
  auto ticket = enqueue_cleanup();
  ASSERT_NE(ticket, nullptr);
  EXPECT_FALSE(schedule.consume_due_ticket(ticket, 3599));

  auto queued = std::make_shared<std::promise<Ticket>>();
  auto future = queued->get_future();
  schedule.schedule([queued](Ticket ticket) {
    queued->set_value(std::move(ticket));
  });
  ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_TRUE(schedule.consume_due_ticket(future.get(), 3600));
}

TEST_F(WeaveCleanupScheduleTest, ReplacementPGRejectsDestroyedPGTicket)
{
  timer.resume();
  auto queued = std::make_shared<std::promise<Ticket>>();
  auto future = queued->get_future();
  Ticket ticket;
  {
    WeaveCleanupSchedule removed{timer};
    ASSERT_TRUE(removed.configure("01:00", 0));
    removed.schedule([queued](Ticket ticket) {
      queued->set_value(std::move(ticket));
    });
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ticket = future.get();
  }
  EXPECT_FALSE(ticket->valid);
  EXPECT_FALSE(schedule.consume_due_ticket(ticket, 3600));
}
} // namespace
