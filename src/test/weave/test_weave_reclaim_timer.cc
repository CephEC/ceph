// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "gtest/gtest.h"

#include <string>
#include <utility>

#include "common/config_proxy.h"
#include "common/errno.h"

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
  EXPECT_EQ(-ENOENT, conf.set_val("osd_weave_max_volume_size", "67108864"));
  const std::pair<const char*, const char*> options[] = {
    {"background_enabled", "true"},
    {"debug_crash_point", ""},
    {"debug_source_remove_error", "false"},
    {"min_object_size", "1"},
    {"quiet_period", "0"},
    {"scan_interval", "1"},
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
