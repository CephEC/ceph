// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <string>

#include "include/buffer.h"

namespace ceph::weave {

// The Volume is one WRITEFULL plus SETXATTR sub-ops. Candidate selection knows
// only its data size; submission also checks the exact attribute payload.
class WeaveWriteLimits {
public:
  WeaveWriteLimits(uint64_t max_object_bytes, uint64_t max_write_mib)
    : object_limit_(std::min(max_object_bytes, encoded_limit)),
      payload_limit_(encoded_limit) {
    // Zero disables the native request-size limit. Clamp before shifting so
    // large configured values cannot overflow the MiB-to-byte conversion.
    if (max_write_mib && max_write_mib <= (encoded_limit >> 20)) {
      payload_limit_ = max_write_mib << 20;
    }
  }

  uint64_t data_limit() const {
    return std::min(object_limit_, payload_limit_);
  }

  bool accepts(uint64_t size,
               const std::map<std::string, ceph::buffer::list>& attrs) const {
    if (size > data_limit()) return false;
    uint64_t remaining = payload_limit_ - size;
    for (const auto& [name, value] : attrs) {
      // ObjectOperation::setxattr puts both name and value in the data payload.
      if (name.size() > remaining) return false;
      remaining -= name.size();
      if (value.length() > remaining) return false;
      remaining -= value.length();
    }
    return true;
  }

private:
  static constexpr uint64_t encoded_limit = std::numeric_limits<uint32_t>::max();
  uint64_t object_limit_;
  uint64_t payload_limit_;
};

}  // namespace ceph::weave
