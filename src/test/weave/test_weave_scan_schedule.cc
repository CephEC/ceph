// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "gtest/gtest.h"

#include <atomic>
#include <future>

#include "osd/weave/WeaveScanSchedule.h"

using ceph::weave::WeaveScanSchedule;

namespace {

class WeaveScanScheduleTest : public ::testing::Test {
protected:
  using Ticket = WeaveScanSchedule::Ticket;
  ceph::timer<ceph::mono_clock> timer{ceph::construct_suspended};
  WeaveScanSchedule schedule{timer};

  // Return the ticket a timer would enqueue, without executing a PG scan.
  Ticket enqueue_scan() {
    auto queued = std::make_shared<std::promise<Ticket>>();
    auto future = queued->get_future();
    schedule.schedule(ceph::timespan::zero(),
      [queued](Ticket ticket) { queued->set_value(std::move(ticket)); });
    if (future.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
      ADD_FAILURE() << "timer did not enqueue the scan";
      return {};
    }
    return future.get();
  }
};

TEST_F(WeaveScanScheduleTest, KeepsOneTicketUntilQueuedScanBegins)
{
  timer.resume();
  auto first = enqueue_scan();
  ASSERT_NE(first, nullptr);
  // A queued scan, not just a pending timer, suppresses another schedule.
  auto duplicate = std::make_shared<std::atomic<unsigned>>(0);
  schedule.schedule(ceph::timespan::zero(),
    [duplicate](Ticket) { ++*duplicate; });
  EXPECT_TRUE(schedule.begin_scan(first));
  EXPECT_FALSE(schedule.begin_scan(first));

  auto next = enqueue_scan();
  ASSERT_NE(next, nullptr);
  EXPECT_NE(first, next);
  EXPECT_FALSE(schedule.begin_scan(first));
  EXPECT_TRUE(schedule.begin_scan(next));
  timer.suspend();
  EXPECT_EQ(*duplicate, 0u);
}

TEST_F(WeaveScanScheduleTest, CancellationBeforeDeadlineRemovesTimer)
{
  auto cancelled = std::make_shared<std::atomic<unsigned>>(0);
  schedule.schedule(ceph::timespan::zero(),
    [cancelled](Ticket) { ++*cancelled; });
  schedule.cancel();
  timer.resume();
  auto next = enqueue_scan();
  ASSERT_NE(next, nullptr);
  EXPECT_TRUE(schedule.begin_scan(next));
  timer.suspend();
  EXPECT_EQ(*cancelled, 0u);
}

TEST_F(WeaveScanScheduleTest, QueuedOldRoleCannotConsumeRearmedScan)
{
  timer.resume();
  auto old = enqueue_scan();
  ASSERT_NE(old, nullptr);
  schedule.cancel();
  EXPECT_FALSE(old->valid);

  auto current = enqueue_scan();
  ASSERT_NE(current, nullptr);
  EXPECT_FALSE(schedule.begin_scan(old));
  EXPECT_TRUE(schedule.begin_scan(current));
}

TEST_F(WeaveScanScheduleTest, RearmingDoesNotWaitForOldLongInterval)
{
  auto cancelled = std::make_shared<std::atomic<unsigned>>(0);
  schedule.schedule(ceph::make_timespan(3600),
    [cancelled](Ticket) { ++*cancelled; });
  schedule.cancel();
  timer.resume();
  auto current = enqueue_scan();
  ASSERT_NE(current, nullptr);
  EXPECT_TRUE(schedule.begin_scan(current));
  EXPECT_EQ(*cancelled, 0u);
}

TEST_F(WeaveScanScheduleTest, DestroyedPGCannotRunItsQueuedScanOnReplacement)
{
  auto queued = std::make_shared<std::promise<Ticket>>();
  auto future = queued->get_future();
  Ticket old;
  timer.resume();
  {
    WeaveScanSchedule destroyed{timer};
    destroyed.schedule(ceph::timespan::zero(),
      [queued](Ticket ticket) { queued->set_value(std::move(ticket)); });
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    old = future.get();
  }
  EXPECT_FALSE(old->valid);
  WeaveScanSchedule replacement{timer};
  EXPECT_FALSE(replacement.begin_scan(old));
}

TEST_F(WeaveScanScheduleTest, CancellationWhileTimerIsEnqueuingInvalidatesTicket)
{
  auto entered = std::make_shared<std::promise<void>>();
  auto entered_future = entered->get_future();
  std::promise<void> release;
  auto released = release.get_future().share();
  auto queued = std::make_shared<std::promise<Ticket>>();
  auto queued_future = queued->get_future();
  schedule.schedule(ceph::timespan::zero(),
    [entered, released, queued](Ticket ticket) {
      entered->set_value();
      released.wait();
      queued->set_value(std::move(ticket));
    });
  timer.resume();
  const auto ready = entered_future.wait_for(std::chrono::seconds(5));
  if (ready != std::future_status::ready) {
    release.set_value();
    FAIL() << "timer did not enter the enqueue callback";
  }
  schedule.cancel();
  release.set_value();
  ASSERT_EQ(queued_future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  auto old = queued_future.get();
  EXPECT_FALSE(old->valid);
  EXPECT_FALSE(schedule.begin_scan(old));
  auto current = enqueue_scan();
  ASSERT_NE(current, nullptr);
  EXPECT_TRUE(schedule.begin_scan(current));
}

}  // namespace
