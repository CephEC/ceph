// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <set>

#include "common/Thread.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "include/common_fwd.h"
#include "osd/osd_types.h"

namespace ceph::weave {

// One worker per OSD services delayed scans and CPU work. Objecter I/O must be
// asynchronous: callbacks must not wait for another callback on this worker.
class WeaveScheduler {
public:
  explicit WeaveScheduler(CephContext* cct);
  ~WeaveScheduler();

  // Coalesce the pending scan without postponing its earliest deadline.
  // Cancellation cannot recall a running callback; validate PG role/epoch.
  void schedule(const spg_t& pgid, double delay_seconds,
                std::function<void()> callback);
  void cancel(const spg_t& pgid);

  // Cancellation does not release an in-flight job's slot. Its completion owns
  // the release, preventing a late completion from releasing a newer job.
  bool try_acquire(const spg_t& pgid);
  void release(const spg_t& pgid);
  void post(std::function<void()> callback);

  // OSD lifecycle thread only, without PG/OSD locks. Reject new work, discard
  // pending callbacks, and join the running callback before returning.
  void shutdown();

private:
  using Deadlines = std::multimap<ceph::mono_time, spg_t>;
  struct Scan {
    Deadlines::iterator deadline;
    std::function<void()> callback;
  };
  struct Worker final : Thread {
    explicit Worker(WeaveScheduler& scheduler) : scheduler_(scheduler) {}
    void* entry() override {
      scheduler_.run();
      return nullptr;
    }
    WeaveScheduler& scheduler_;
  };

  // The worker loop and its two blocking steps; all require mutex_.
  void run();
  bool take_next_locked(std::function<void()>& callback);
  void wait_locked(std::unique_lock<ceph::mutex>& lock, ceph::mono_time now);
  ceph::mono_time deadline_for(double delay_seconds) const;
  void publish_locked(spg_t pgid, ceph::mono_time when,
                      std::function<void()> callback,
                      std::function<void()>& discarded);

  CephContext* const cct_;
  ceph::mutex mutex_ = ceph::make_mutex("weave::WeaveScheduler");
  ceph::condition_variable cond_;
  bool stopping_ = false;
  Deadlines deadlines_;
  std::map<spg_t, Scan> scans_;
  std::deque<std::function<void()>> work_;
  std::set<spg_t> active_;
  std::once_flag stopped_;
  Worker worker_;
};

}  // namespace ceph::weave
