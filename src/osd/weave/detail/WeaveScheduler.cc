// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "WeaveScheduler.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <utility>

#include "common/ceph_context.h"
#include "common/config.h"
#include "include/ceph_assert.h"

namespace ceph::weave {

WeaveScheduler::WeaveScheduler(CephContext* cct)
  : cct_(cct), worker_(*this)
{
  worker_.create("weave_worker");
}

WeaveScheduler::~WeaveScheduler()
{
  shutdown();
}

void WeaveScheduler::schedule(
  const spg_t& pgid, double delay_seconds, std::function<void()> callback)
{
  if (!callback || std::isnan(delay_seconds)) {
    return;
  }
  const auto when = deadline_for(delay_seconds);

  // Releasing a captured PGRef can run a destructor. Always do that outside
  // the scheduler lock, including replacement, cancellation, and shutdown.
  std::function<void()> discarded;
  {
    std::lock_guard l(mutex_);
    if (stopping_) {
      return;
    }

    publish_locked(pgid, when, std::move(callback), discarded);
  }
}

void WeaveScheduler::cancel(const spg_t& pgid)
{
  // Hold the cancelled callback until the lock is dropped; destroying it may
  // run a PGRef destructor.
  std::function<void()> discarded;
  {
    std::lock_guard l(mutex_);
    auto p = scans_.find(pgid);
    if (p != scans_.end()) {
      deadlines_.erase(p->second.deadline);
      discarded = std::move(p->second.callback);
      scans_.erase(p);
      cond_.notify_one();
    }
  }
}

bool WeaveScheduler::try_acquire(const spg_t& pgid)
{
  const auto limit =
    cct_->_conf.get_val<uint64_t>("osd_weave_max_concurrent");

  std::lock_guard l(mutex_);
  if (stopping_ || active_.size() >= limit) {
    return false;
  }

  // An already-active PG has no free slot to hand out.
  return active_.insert(pgid).second;
}

void WeaveScheduler::release(const spg_t& pgid)
{
  std::lock_guard l(mutex_);
  active_.erase(pgid);
}

void WeaveScheduler::post(std::function<void()> callback)
{
  if (!callback) {
    return;
  }

  std::lock_guard l(mutex_);
  if (!stopping_) {
    work_.push_back(std::move(callback));
    cond_.notify_one();
  }
}

void WeaveScheduler::run()
{
  std::unique_lock l(mutex_);
  while (!stopping_) {
    std::function<void()> callback;
    if (!take_next_locked(callback)) {
      wait_locked(l, ceph::mono_clock::now());
      continue;
    }

    // Run the callback with the lock dropped; it may re-enter the scheduler.
    l.unlock();
    callback();
    callback = {};
    l.lock();
  }
}

bool WeaveScheduler::take_next_locked(std::function<void()>& callback)
{
  // Give posted job progress precedence over scans, including a configured
  // zero scan interval that can continually enqueue immediately-due scans.
  const auto now = ceph::mono_clock::now();
  if (!work_.empty()) {
    callback = std::move(work_.front());
    work_.pop_front();
    return true;
  }

  if (deadlines_.empty() || deadlines_.begin()->first > now) {
    return false;
  }
  auto first = deadlines_.begin();
  auto p = scans_.find(first->second);
  ceph_assert(p != scans_.end());
  callback = std::move(p->second.callback);
  scans_.erase(p);
  deadlines_.erase(first);
  return true;
}

void WeaveScheduler::wait_locked(std::unique_lock<ceph::mutex>& l,
                                 ceph::mono_time now)
{
  if (deadlines_.empty()) {
    cond_.wait(l);
    return;
  }
  // Bound the OS wait duration even for saturated/infinite deadlines; chrono's
  // native timed wait may use a signed duration representation.
  const auto remaining = deadlines_.begin()->first.time_since_epoch() -
                         now.time_since_epoch();
  const auto delay = std::min(
    remaining,
    std::chrono::duration_cast<ceph::mono_clock::duration>(
      std::chrono::hours(1)));

  cond_.wait_for(l, std::chrono::duration_cast<ceph::signedspan>(delay));
}

ceph::mono_time WeaveScheduler::deadline_for(double delay_seconds) const
{
  const auto now = ceph::mono_clock::now();
  if (delay_seconds <= 0) {
    return now;
  }

  // Ceph time-point subtraction returns signedspan; max() - now can
  // overflow it. Subtract the underlying unsigned durations instead.
  const auto remaining = ceph::mono_time::max().time_since_epoch() -
                         now.time_since_epoch();
  if (static_cast<long double>(delay_seconds) >=
      std::chrono::duration<long double>(remaining).count()) {
    return ceph::mono_time::max();
  }

  auto when = now;
  when += std::chrono::duration_cast<ceph::mono_clock::duration>(
    std::chrono::duration<long double>(delay_seconds));
  return when;
}

void WeaveScheduler::publish_locked(spg_t pgid, ceph::mono_time when,
                                    std::function<void()> callback,
                                    std::function<void()>& discarded)
{
  auto p = scans_.find(pgid);
  if (p != scans_.end()) {
    // New foreground commits must not postpone a periodic scan forever.
    // Replace its callback, but retain the earliest outstanding wakeup.
    when = std::min(when, p->second.deadline->first);
    deadlines_.erase(p->second.deadline);
    discarded = std::move(p->second.callback);
    p->second = Scan{deadlines_.emplace(when, pgid), std::move(callback)};
  } else {
    // No pending scan for this PG: arm it at its own deadline.
    scans_.emplace(
      pgid, Scan{deadlines_.emplace(when, pgid), std::move(callback)});
  }

  cond_.notify_one();
}

void WeaveScheduler::shutdown()
{
  ceph_assert(!worker_.am_self());

  std::call_once(stopped_, [this] {
    decltype(scans_) discarded_scans;
    decltype(work_) discarded_work;
    {
      std::lock_guard l(mutex_);
      stopping_ = true;
      discarded_scans.swap(scans_);
      discarded_work.swap(work_);
      deadlines_.clear();
      active_.clear();
      cond_.notify_one();
    }

    // Destroy the discarded callbacks, and any PGRef they hold, unlocked.
    discarded_scans.clear();
    discarded_work.clear();
    worker_.join();
  });
}

}  // namespace ceph::weave
