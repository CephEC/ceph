// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

namespace ceph::weave {

// Wall-clock UTC schedule, serialized by the OSD lock. No startup/config-change
// catch-up: enabling or changing the time arms its next strictly future
// occurrence.
class WeaveReclaimTimer {
public:
  bool set_time(std::string_view time, int64_t now) {
    if (time.empty()) {
      next_.reset();
      return true;
    }

    int64_t seconds = 0;
    if (!parse_time_of_day(time, seconds)) {
      return false;
    }

    seconds_ = seconds;
    next_ = following(now);
    return true;
  }

  bool due(int64_t now) {
    if (!next_ || now < *next_) {
      return false;
    }

    // Collapse missed occurrences into one pass, including a tick crossing
    // midnight. Remember the scheduled date, not the tick's calendar date.
    auto day = utc_day(now);
    if (now < day * kDaySeconds + seconds_) {
      --day;
    }
    last_day_ = day;

    next_ = following(now);
    return true;
  }

private:
  static constexpr int64_t kDaySeconds = 24 * 60 * 60;

  // Accept exactly "HH:MM", with hour at most 23 and minute at most 59.
  static bool parse_time_of_day(std::string_view time, int64_t& seconds) {
    if (time.size() != 5 || time[2] != ':' ||
        time[0] < '0' || time[0] > '2' ||
        time[1] < '0' || time[1] > '9' ||
        (time[0] == '2' && time[1] > '3') ||
        time[3] < '0' || time[3] > '5' ||
        time[4] < '0' || time[4] > '9') {
      return false;
    }
    seconds = ((time[0] - '0') * 10 + time[1] - '0') * 3600 +
              ((time[3] - '0') * 10 + time[4] - '0') * 60;
    return true;
  }
  static int64_t utc_day(int64_t now) {
    return now / kDaySeconds - (now % kDaySeconds < 0);
  }
  int64_t following(int64_t now) const {
    auto day = utc_day(now);
    if (day * kDaySeconds + seconds_ <= now) {
      ++day;
    }

    // A backwards clock step or time edit must not replay a consumed UTC date.
    // Disabling/re-enabling the schedule preserves this high-water mark.
    if (last_day_ && day <= *last_day_) {
      day = *last_day_ + 1;
    }

    return day * kDaySeconds + seconds_;
  }

  int64_t seconds_ = 0;
  std::optional<int64_t> next_;
  std::optional<int64_t> last_day_;
};

}  // namespace ceph::weave
