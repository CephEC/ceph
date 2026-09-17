#include "WeaveService.h"

#include <atomic>
#include <functional>
#include <memory>
#include <utility>

#include "WeavePGHost.h"
#include "detail/WeaveReclaimTimer.h"
#include "detail/WeaveScheduler.h"

namespace {

// Empty identity token for the one reclaim pass in flight. It stays weak, so a
// finished pass never keeps the service alive.
struct ReclaimPass {};

}  // namespace

namespace ceph::weave {

struct WeaveService::Impl {
  explicit Impl(CephContext* cct)
    : scheduler_(std::make_shared<WeaveScheduler>(cct)) {}

  // A pass is observable without taking a lock: request_reclaim() runs under
  // the OSD lock, but the pass ends on the scheduler worker.
  bool pass_running() const { return !pass_.expired(); }

  void launch_reclaim(std::function<Dispatch()> snapshot, unsigned percent) {
    // Snapshot the dispatcher while the OSD lock is still held.
    auto dispatch = snapshot();
    auto pass = std::make_shared<ReclaimPass>();
    pass_ = pass;

    // The posted work hands this token to every dispatched PG; the pass stays
    // observable until all of them have released it.
    scheduler_->post([dispatch = std::move(dispatch), percent, pass] {
      dispatch(percent, [pass] {});
    });
  }

  std::shared_ptr<WeaveScheduler> scheduler_;
  WeaveReclaimTimer timer_;
  std::weak_ptr<ReclaimPass> pass_;
  std::atomic<bool> stopping_{false};
};

WeaveService::WeaveService(CephContext* cct)
  : impl_(std::make_unique<Impl>(cct)) {}

WeaveService::~WeaveService() { shutdown(); }

bool WeaveService::update_reclaim_time(std::string_view value, int64_t now) {
  return impl_->timer_.set_time(value, now);
}

WeaveService::ReclaimResult WeaveService::request_reclaim(
  unsigned percent, std::function<Dispatch()> snapshot) {
  if (impl_->stopping_) return ReclaimResult::kStopping;

  // At most one pass may be in flight; a second request is refused until the
  // running one releases its token.
  if (impl_->pass_running()) return ReclaimResult::kAlreadyRunning;

  impl_->launch_reclaim(std::move(snapshot), percent);
  return ReclaimResult::kAccepted;
}

void WeaveService::tick(int64_t now, bool active, unsigned percent,
                        std::function<Dispatch()> snapshot) {
  // The timer only fires for an active PG, and only once the configured daily
  // cleanup time has arrived; request_reclaim() still owns admission.
  if (impl_->stopping_ || !impl_->timer_.due(now) || !active) return;

  request_reclaim(percent, std::move(snapshot));
}

void WeaveService::wake_candidates(std::function<void()> callback) {
  impl_->scheduler_->post(std::move(callback));
}

void WeaveService::shutdown() {
  // Idempotent: the scheduler guards its worker join with std::once_flag.
  impl_->stopping_ = true;

  // Stop admitting work before joining the worker.
  impl_->scheduler_->shutdown();
}

std::unique_ptr<WeaveLease> WeaveService::acquire(const spg_t& pgid) {
  // One conversion slot per PG: a busy slot or a stopping service yields no
  // lease, and the caller retries on a later wakeup.
  if (impl_->stopping_ || !impl_->scheduler_->try_acquire(pgid)) return {};

  // Release runs later on the scheduler worker, outside the OSD lock, so the
  // lease keeps the scheduler alive instead of the service.
  return std::make_unique<WeaveLease>([scheduler = impl_->scheduler_, pgid] {
    scheduler->release(pgid);
  });
}

void WeaveService::schedule(const spg_t& pgid, double delay,
                            std::function<void()> callback) {
  impl_->scheduler_->schedule(pgid, delay, std::move(callback));
}

void WeaveService::cancel(const spg_t& pgid) {
  impl_->scheduler_->cancel(pgid);
}

void WeaveService::post(std::function<void()> callback) {
  impl_->scheduler_->post(std::move(callback));
}

}  // namespace ceph::weave
