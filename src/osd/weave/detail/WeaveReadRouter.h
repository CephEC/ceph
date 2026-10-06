// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <memory>

#include "WeaveMemberTranslator.h"
#include "osd/weave/WeavePGInterface.h"

namespace ceph::weave {

/**
 * Owns server-side routing policy. The PG interface supplies native placement
 * and durability checks; the member translator owns logical-to-physical translation.
 */
class WeaveReadRouter {
public:
  WeaveReadRouter(WeavePGInterface& pg_interface,
                  WeaveMemberTranslator& translator)
    : pg_interface_(pg_interface), translator_(translator) {}
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

  WeavePGInterface& pg_interface_;
  WeaveMemberTranslator& translator_;
};

}  // namespace ceph::weave
