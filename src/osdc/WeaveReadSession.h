// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include "osd/weave/WeaveReadRoute.h"
#include <optional>

namespace ceph::weave {

// One logical client request can take one direct detour. Its object, operations
// and input buffers never change; every fallback resumes the original request.
class WeaveReadSession {
public:
  bool may_redirect() const { return !attempted_; }
  const std::optional<WeaveReadRoute>& route() const { return route_; }
  bool redirect(const WeaveReadRoute& route) {
    if (attempted_) return false;
    attempted_ = true;
    route_ = route;
    return true;
  }
  void fallback() { attempted_ = true; route_.reset(); }
  // Returns true when a previous direct destination has become invalid.
  bool refresh(epoch_t epoch, const std::vector<int>& acting, int primary) {
    if (!route_) return false;
    const int shard = route_->target.shard;
    if (route_->map_epoch == epoch && shard >= 0 &&
        static_cast<size_t>(shard) < acting.size() &&
        acting[shard] == route_->target.osd && route_->target.osd != primary) {
      return false;
    }
    fallback();
    return true;
  }
private:
  bool attempted_ = false;
  std::optional<WeaveReadRoute> route_;
};

} // namespace ceph::weave
