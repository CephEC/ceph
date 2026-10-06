// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <functional>
#include <memory>
#include <string_view>

#include "include/common_fwd.h"
#include "osd/osd_types.h"

namespace ceph::weave {

class WeaveLease;
enum class WeaveRetryKind;

// One service per OSD. Config/tick/reclaim calls are serialized by the OSD
// lock. Capture snapshots under that lock; dispatched work runs without it.
class WeaveService {
public:
  enum class ReclaimResult { kAccepted, kAlreadyRunning, kStopping };
  // The dispatcher invokes one PG action at a time under that PG's lock.
  using Dispatch = std::function<void(unsigned, std::function<void()>)>;

  explicit WeaveService(CephContext*);
  ~WeaveService();

  bool update_reclaim_time(std::string_view, int64_t now);
  ReclaimResult request_reclaim(unsigned live_percent,
                                std::function<Dispatch()> snapshot);
  void tick(int64_t now, bool active, unsigned live_percent,
            std::function<Dispatch()> snapshot);
  void shutdown();

  // Used by the Ceph PG host, never by packing policy or native OSD callers.
  std::unique_ptr<WeaveLease> acquire(const spg_t&);
  void retry(const spg_t&, WeaveRetryKind, std::function<void()>);
  void cancel(const spg_t&);
  void post(std::function<void()>);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace ceph::weave
