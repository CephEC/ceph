// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <memory>

namespace ceph::weave {

// Shared lifetime of one OSD-wide cleanup pass. The queued dispatcher and each
// participating PG hold a Ref; the service only observes a weak reference.
// Finishing, rejecting or discarding work releases its Ref. The pass ends when
// the last owner releases it, including work discarded during shutdown.
struct WeaveReclaimPass {
  using Ref = std::shared_ptr<WeaveReclaimPass>;
};

}  // namespace ceph::weave
