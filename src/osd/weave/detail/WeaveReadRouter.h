// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <memory>

#include "WeaveMemberAccess.h"
#include "osd/weave/WeavePGHost.h"

namespace ceph::weave {

/**
 * Owns server-side routing policy. The host supplies native placement and
 * durability checks; MemberAccess owns all logical-to-physical translation.
 */
class WeaveReadRouter {
public:
  WeaveReadRouter(WeavePGHost& host, WeaveMemberAccess& members)
    : host_(host), members_(members) {}
  bool redirect(const OpRequestRef&);
  int accept(OpRequestRef&);

private:
  bool eligible(const OpRequestRef&) const;
  bool may_redirect(const OpRequestRef&, const MOSDOp&) const;
  const WeaveMemberMeta* member_for(const OpRequestRef&) const;
  bool route_is_local(const MOSDOp&, const WeaveReadRoute&) const;
  std::shared_ptr<const WeaveVolumeMeta> load_route_metadata(
    const WeaveReadRoute&);
  bool assignment_matches(const WeaveReadRoute&, const WeaveMemberMeta&) const;

  WeavePGHost& host_;
  WeaveMemberAccess& members_;
};

}  // namespace ceph::weave
