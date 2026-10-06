#include "WeaveService.h"

#include <atomic>
#include <functional>
#include <memory>
#include <utility>

#include "WeavePGInterface.h"
#include "detail/WeaveWorker.h"

namespace ceph::weave {

struct WeaveService::Impl {
  explicit Impl(CephContext* cct)
    : worker_(std::make_shared<WeaveWorker>(cct)) {}

  std::shared_ptr<WeaveWorker> worker_;
  std::atomic<bool> stopping_{false};
};

WeaveService::WeaveService(CephContext* cct)
  : impl_(std::make_unique<Impl>(cct)) {}

WeaveService::~WeaveService() { shutdown(); }

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

  // Release runs on the thread finishing the job. Keep the worker alive until
  // then; its mutex protects the slot independently of the service lifetime.
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
