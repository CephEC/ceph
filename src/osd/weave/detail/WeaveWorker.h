// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <utility>

#include "common/Thread.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "include/common_fwd.h"
#include "osd/osd_types.h"

namespace ceph::weave {

enum class WeaveRetryKind;

// One worker per OSD services conversion retries and CPU work. Objecter I/O
// must be asynchronous: callbacks must not wait for another callback here.
class WeaveWorker {
public:
  explicit WeaveWorker(CephContext* cct);
  ~WeaveWorker();

  // Coalesce retries of the same kind without postponing their deadline.
  // Different kinds keep independent callbacks, even for the same PG.
  // Cancellation cannot recall a running callback; validate PG role/epoch.
  // Retry after one second; background scan configuration is unrelated.
  void retry(const spg_t& pgid, WeaveRetryKind kind,
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
  void stop_worker();
  using RetryKey = std::pair<spg_t, WeaveRetryKind>;
  using Deadlines = std::multimap<ceph::mono_time, RetryKey>;
  struct PendingRetry {
    Deadlines::iterator deadline;
    std::function<void()> callback;
  };
  struct WorkerThread final : Thread {
    explicit WorkerThread(WeaveWorker& worker) : worker_(worker) {}
    void* entry() override {
      worker_.run();
      return nullptr;
    }
    WeaveWorker& worker_;
  };

  // The worker loop and its two blocking steps; all require mutex_.
  void run();
  bool take_next_locked(std::function<void()>& callback);
  void wait_locked(std::unique_lock<ceph::mutex>& lock, ceph::mono_time now);
  void publish_locked(RetryKey key, ceph::mono_time when,
                      std::function<void()> callback,
                      std::function<void()>& discarded);

  CephContext* const cct_;
  ceph::mutex mutex_ = ceph::make_mutex("weave::WeaveWorker");
  ceph::condition_variable cond_;
  bool stopping_ = false;
  Deadlines deadlines_;
  std::map<RetryKey, PendingRetry> retries_;
  std::deque<std::function<void()>> work_;
  std::set<spg_t> active_;
  std::once_flag stopped_;
  WorkerThread thread_;
};

}  // namespace ceph::weave
