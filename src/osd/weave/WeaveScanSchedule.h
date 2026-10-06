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
class WeaveScanSchedule {
public:
  struct ScanTicket {
    std::atomic<bool> valid{true};
  };
  using Ticket = std::shared_ptr<ScanTicket>;

  explicit WeaveScanSchedule(ceph::timer<ceph::mono_clock>& timer)
    : timer_(timer) {}
  ~WeaveScanSchedule() { cancel(); }
  WeaveScanSchedule(const WeaveScanSchedule&) = delete;
  WeaveScanSchedule& operator=(const WeaveScanSchedule&) = delete;

  // Keep one ticket outstanding, including the time it spends in the PG queue.
  void schedule(ceph::timespan delay, std::function<void(Ticket)> enqueue) {
    if (pending_) return;
    pending_ = std::make_shared<ScanTicket>();
    event_ = timer_.add_event(delay,
      [ticket = pending_, enqueue = std::move(enqueue)] {
        if (ticket->valid) enqueue(ticket);
      });
  }

  // A cancelled or duplicate delivery cannot consume a newer scan's ticket.
  bool begin_scan(const Ticket& ticket) {
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
  ceph::timer<ceph::mono_clock>& timer_;
  uint64_t event_ = 0;
  Ticket pending_;
};

}  // namespace ceph::weave
