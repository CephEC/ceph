// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <utility>

#include "common/ceph_time.h"
#include "common/ceph_timer.h"

namespace ceph::weave {

// Owned and accessed under the PG lock. The timer only enqueues a ticket;
// neither its callback nor a queued ticket retains a PG reference.
template <typename Clock>
class WeavePGTaskSchedule {
public:
  struct TaskTicket {
    std::atomic<bool> valid{true};
  };
  using Ticket = std::shared_ptr<TaskTicket>;

  explicit WeavePGTaskSchedule(ceph::timer<Clock>& timer)
    : timer_(timer) {}
  ~WeavePGTaskSchedule() { cancel(); }
  WeavePGTaskSchedule(const WeavePGTaskSchedule&) = delete;
  WeavePGTaskSchedule& operator=(const WeavePGTaskSchedule&) = delete;

  // Keep one ticket outstanding, including the time it spends in the PG queue.
  void schedule(ceph::timespan delay, std::function<void(Ticket)> enqueue) {
    schedule_at(Clock::now() + delay, std::move(enqueue));
  }

  void schedule_at(typename Clock::time_point when,
                   std::function<void(Ticket)> enqueue) {
    if (pending_) return;
    pending_ = std::make_shared<TaskTicket>();
    event_ = timer_.add_event(when,
      [ticket = pending_, enqueue = std::move(enqueue)] {
        if (ticket->valid) enqueue(ticket);
      });
  }

  // A cancelled or duplicate delivery cannot consume a newer task's ticket.
  bool consume_ticket(const Ticket& ticket) {
    if (!pending_ || ticket != pending_ || !ticket->valid) return false;
    ticket->valid = false;
    pending_.reset();
    event_ = 0;
    return true;
  }

  void cancel() {
    if (pending_) pending_->valid = false;
    pending_.reset();
    if (event_) timer_.cancel_event(std::exchange(event_, 0));
  }

private:
  ceph::timer<Clock>& timer_;
  uint64_t event_ = 0;
  Ticket pending_;
};

using WeaveScanSchedule = WeavePGTaskSchedule<ceph::mono_clock>;

}  // namespace ceph::weave
