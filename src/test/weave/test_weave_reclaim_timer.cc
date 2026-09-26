// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "gtest/gtest.h"

#include <string>
#include <utility>

#include "common/config_proxy.h"
#include "common/errno.h"
#include "osd/weave/detail/WeaveReclaimTimer.h"

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

TEST(WeaveReclaimConfig, RejectsMalformedTimeWithoutChangingValue)
{
  ConfigProxy conf{false};
  ASSERT_EQ(0, conf.set_val("osd_weave_cleanup_time", "23:59"));
  for (const auto* invalid : {"1:00", "24:00", "00:60", "12:00Z", "12:0x"}) {
    EXPECT_EQ(-EINVAL, conf.set_val("osd_weave_cleanup_time", invalid));
    EXPECT_EQ("23:59", conf.get_val<std::string>("osd_weave_cleanup_time"));
  }
  EXPECT_EQ(0, conf.set_val("osd_weave_cleanup_time", "00:00"));
  EXPECT_EQ(0, conf.set_val("osd_weave_cleanup_time", ""));
  EXPECT_EQ("", conf.get_val<std::string>("osd_weave_cleanup_time"));
}

TEST(WeaveReclaimConfig, EnforcesInclusivePercentageBounds)
{
  ConfigProxy conf{false};
  ASSERT_EQ(0, conf.set_val("osd_weave_cleanup_live_percent", "0"));
  EXPECT_EQ(-EINVAL, conf.set_val("osd_weave_cleanup_live_percent", "-1"));
  EXPECT_EQ(0u, conf.get_val<uint64_t>("osd_weave_cleanup_live_percent"));
  ASSERT_EQ(0, conf.set_val("osd_weave_cleanup_live_percent", "100"));
  EXPECT_EQ(-EINVAL, conf.set_val("osd_weave_cleanup_live_percent", "101"));
  EXPECT_EQ(100u, conf.get_val<uint64_t>("osd_weave_cleanup_live_percent"));
}

TEST(WeaveConfig, RegistersOnlyWeaveOptions)
{
  ConfigProxy conf{false};
  EXPECT_EQ(0, conf.set_val("osd_weave_enabled", "true"));
  EXPECT_EQ(-ENOENT, conf.set_val("osd_aggregate_ec_enabled", "true"));
  const std::pair<const char*, const char*> options[] = {
    {"background_enabled", "true"},
    {"debug_crash_point", ""},
    {"debug_source_remove_error", "false"},
    {"min_object_size", "1"},
    {"quiet_period", "0"},
    {"scan_interval", "1"},
    {"max_volume_size", "67108864"},
    {"max_concurrent", "1"},
    {"max_padding_percent", "10"},
    {"cleanup_time", "12:00"},
    {"cleanup_live_percent", "50"},
    {"data_classes", "openssl_md5"},
    {"redirect_reads", "true"},
  };
  for (const auto& [suffix, value] : options) {
    SCOPED_TRACE(suffix);
    EXPECT_EQ(0, conf.set_val("osd_weave_" + std::string(suffix), value));
    EXPECT_EQ(-ENOENT,
              conf.set_val("osd_aggregate_" + std::string(suffix), value));
  }
}
