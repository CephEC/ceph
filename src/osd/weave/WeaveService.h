// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <functional>
#include <memory>

#include "include/common_fwd.h"
#include "osd/osd_types.h"

namespace ceph::weave {

class WeaveLease;
enum class WeaveRetryKind;

// One service per OSD, sharing conversion slots, CPU work and retries across PGs.
class WeaveService {
public:
  explicit WeaveService(CephContext*);
  ~WeaveService();

  void shutdown();

  // Used by the native PG adapter, never by packing policy or native OSD callers.
  std::unique_ptr<WeaveLease> acquire(const spg_t&);
  void retry(const spg_t&, WeaveRetryKind, std::function<void()>);
  void cancel(const spg_t&);
  void post(std::function<void()>);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace ceph::weave
