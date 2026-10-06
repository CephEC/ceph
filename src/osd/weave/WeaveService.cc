#include "WeaveService.h"

#include <atomic>
#include <chrono>
#include <cmath>
#include <functional>
#include <memory>
#include <optional>
#include <utility>

#include "WeavePGHost.h"
#include "detail/WeaveReclaimTimer.h"
#include "detail/WeaveWorker.h"

namespace {

// Empty identity token for the one reclaim pass in flight. It stays weak, so a
// finished pass never keeps the service alive.
struct ReclaimPass {};

}  // namespace

namespace ceph::weave {

struct WeaveService::Impl {
  explicit Impl(CephContext* cct)
    : worker_(std::make_shared<WeaveWorker>(cct)) {}

  // A pass is observable without taking a lock: request_reclaim() runs under
  // the OSD lock, but the pass ends on the background worker.
  bool pass_running() const { return !pass_.expired(); }

  void launch_reclaim(std::function<Dispatch()> snapshot, unsigned percent) {
    // Snapshot the dispatcher while the OSD lock is still held.
    auto dispatch = snapshot();
    auto pass = std::make_shared<ReclaimPass>();
    pass_ = pass;

    // The posted work hands this token to every dispatched PG; the pass stays
    // observable until all of them have released it.
    worker_->post([dispatch = std::move(dispatch), percent, pass] {
      dispatch(percent, [pass] {});
    });
  }

  std::shared_ptr<WeaveWorker> worker_;
  WeaveReclaimTimer timer_;
  std::weak_ptr<ReclaimPass> pass_;
  std::optional<ceph::mono_time> last_scan_;
  std::atomic<bool> scan_pending_{false};
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

void WeaveService::scan_candidates(double interval,
                                  std::function<Scan()> snapshot) {
  if (impl_->stopping_ || impl_->scan_pending_ ||
      !std::isfinite(interval) || interval < 0) return;
  const auto now = ceph::mono_clock::now();
  if (impl_->last_scan_ &&
      std::chrono::duration<double>(now - *impl_->last_scan_).count() < interval) {
    return;
  }

  auto scan = snapshot();
  impl_->last_scan_ = now;
  impl_->scan_pending_ = true;
  impl_->worker_->post([impl = impl_.get(), scan = std::move(scan)] {
    scan();
    impl->scan_pending_ = false;
  });
}

void WeaveService::shutdown() {
  // Idempotent: the worker guards its thread join with std::once_flag.
  impl_->stopping_ = true;

  // Stop admitting work before joining the worker.
  impl_->worker_->shutdown();
}

std::unique_ptr<WeaveLease> WeaveService::acquire(const spg_t& pgid) {
  // One conversion slot per PG: a busy slot or a stopping service yields no
  // lease, and the caller retries on a later wakeup.
  if (impl_->stopping_ || !impl_->worker_->try_acquire(pgid)) return {};

  // Release runs later on the background worker, outside the OSD lock, so the
  // lease keeps the worker alive instead of the service.
  return std::make_unique<WeaveLease>([worker = impl_->worker_, pgid] {
    worker->release(pgid);
  });
}

void WeaveService::retry(const spg_t& pgid, WeaveRetryKind kind,
                        std::function<void()> callback) {
  // Resource and I/O retries have their own cadence. Changing the candidate
  // scan interval must not delay a foreground materialization or job repair.
  impl_->worker_->retry(pgid, kind, std::move(callback));
}

void WeaveService::cancel(const spg_t& pgid) {
  impl_->worker_->cancel(pgid);
}

void WeaveService::post(std::function<void()> callback) {
  impl_->worker_->post(std::move(callback));
}

}  // namespace ceph::weave
