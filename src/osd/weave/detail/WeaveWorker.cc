// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "WeaveWorker.h"

#include <algorithm>
#include <chrono>
#include <utility>
#include <vector>

#include "common/ceph_context.h"
#include "common/config.h"
#include "include/ceph_assert.h"
#include "osd/weave/WeavePGInterface.h"

namespace ceph::weave {

WeaveWorker::WeaveWorker(CephContext* cct)
  : cct_(cct), thread_(*this)
{
  thread_.create("weave_worker");
}

WeaveWorker::~WeaveWorker()
{
  shutdown();
}

void WeaveWorker::retry(
  const spg_t& pgid, WeaveRetryKind kind,
  std::function<void()> callback)
{
  if (!callback) return;
  const auto when = ceph::mono_clock::now() + std::chrono::seconds(1);

  // Releasing a captured PGRef can run a destructor. Always do that outside
  // the worker lock, including replacement, cancellation, and shutdown.
  std::function<void()> discarded;
  {
    std::lock_guard l(mutex_);
    if (stopping_) {
      return;
    }

    publish_locked({pgid, kind}, when, std::move(callback), discarded);
  }
}

void WeaveWorker::cancel(const spg_t& pgid)
{
  // Hold cancelled callbacks until the lock is dropped; destroying them may
  // run a PGRef destructor.
  std::vector<std::function<void()>> discarded;
  {
    std::lock_guard l(mutex_);
    auto p = retries_.lower_bound({pgid, WeaveRetryKind::kMaterialization});
    while (p != retries_.end() && p->first.first == pgid) {
      deadlines_.erase(p->second.deadline);
      discarded.push_back(std::move(p->second.callback));
      p = retries_.erase(p);
    }
    cond_.notify_one();
  }
}

bool WeaveWorker::try_acquire(const spg_t& pgid)
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

void WeaveWorker::release(const spg_t& pgid)
{
  std::lock_guard l(mutex_);
  active_.erase(pgid);
}

void WeaveWorker::post(std::function<void()> callback)
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

void WeaveWorker::run()
{
  std::unique_lock l(mutex_);
  while (!stopping_) {
    std::function<void()> callback;
    if (!take_next_locked(callback)) {
      wait_locked(l, ceph::mono_clock::now());
      continue;
    }

    // Run the callback with the lock dropped; it may re-enter the worker.
    l.unlock();
    callback();
    callback = {};
    l.lock();
  }
}

bool WeaveWorker::take_next_locked(std::function<void()>& callback)
{
  // Posted job progress runs before deferred retry callbacks.
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
  auto p = retries_.find(first->second);
  ceph_assert(p != retries_.end());
  callback = std::move(p->second.callback);
  retries_.erase(p);
  deadlines_.erase(first);
  return true;
}

void WeaveWorker::wait_locked(std::unique_lock<ceph::mutex>& l,
                                 ceph::mono_time now)
{
  if (deadlines_.empty()) {
    cond_.wait(l);
    return;
  }
  cond_.wait_for(l, std::chrono::duration_cast<ceph::signedspan>(
    deadlines_.begin()->first - now));
}

void WeaveWorker::publish_locked(RetryKey key, ceph::mono_time when,
                                    std::function<void()> callback,
                                    std::function<void()>& discarded)
{
  auto p = retries_.find(key);
  if (p != retries_.end()) {
    // A repeated request of the same kind retains its earliest retry.
    when = std::min(when, p->second.deadline->first);
    deadlines_.erase(p->second.deadline);
    discarded = std::move(p->second.callback);
    p->second = PendingRetry{deadlines_.emplace(when, key), std::move(callback)};
  } else {
    retries_.emplace(
      key, PendingRetry{deadlines_.emplace(when, key), std::move(callback)});
  }

  cond_.notify_one();
}

void WeaveWorker::shutdown()
{
  ceph_assert(!thread_.am_self());

  std::call_once(stopped_, [this] { stop_worker(); });
}

void WeaveWorker::stop_worker()
{
  decltype(retries_) discarded_retries;
  decltype(work_) discarded_work;
  {
    std::lock_guard l(mutex_);
    stopping_ = true;
    discarded_retries.swap(retries_);
    discarded_work.swap(work_);
    deadlines_.clear();
    active_.clear();
    cond_.notify_one();
  }

  // Destroy the discarded callbacks, and any PGRef they hold, unlocked.
  discarded_retries.clear();
  discarded_work.clear();
  thread_.join();
}

}  // namespace ceph::weave
