#include "WeaveCleanupSchedule.h"

#include "include/random.h"
#include "detail/WeaveReclaimTimer.h"

namespace ceph::weave {

struct WeaveCleanupSchedule::Impl {
  explicit Impl(ceph::timer<ceph::real_clock>& timer) : wakeup(timer) {}

  WeaveReclaimTimer calendar;
  Timer wakeup;
};

WeaveCleanupSchedule::WeaveCleanupSchedule(
  ceph::timer<ceph::real_clock>& timer)
  : impl_(std::make_unique<Impl>(timer)) {}

WeaveCleanupSchedule::~WeaveCleanupSchedule() = default;

bool WeaveCleanupSchedule::configure(std::string_view time, int64_t now) {
  if (!impl_->calendar.set_time(time, now)) return false;
  impl_->wakeup.cancel();
  return true;
}

void WeaveCleanupSchedule::schedule(std::function<void(Ticket)> enqueue) {
  const auto deadline = impl_->calendar.next_deadline();
  if (!deadline) return;

  // Spread PG admission over five seconds without shifting tomorrow's time.
  const auto jitter = ceph::make_timespan(
    ceph::util::generate_random_number(0.0, 5.0));
  impl_->wakeup.schedule_at(
    ceph::real_clock::from_time_t(*deadline) + jitter, std::move(enqueue));
}

bool WeaveCleanupSchedule::consume_due_ticket(const Ticket& ticket, int64_t now) {
  return impl_->wakeup.consume_ticket(ticket) && impl_->calendar.due(now);
}

void WeaveCleanupSchedule::cancel() {
  impl_->wakeup.cancel();
}

}  // namespace ceph::weave
