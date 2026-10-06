// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <memory>
#include <string_view>

#include "WeavePGTaskSchedule.h"

namespace ceph::weave {

// Owned under the PG lock. The shared wall-clock timer only queues tickets;
// daily admission and rearming run in the PG queue, independently of job finish.
class WeaveCleanupSchedule {
public:
  using Timer = WeavePGTaskSchedule<ceph::real_clock>;
  using Ticket = Timer::Ticket;

  explicit WeaveCleanupSchedule(ceph::timer<ceph::real_clock>& timer);
  ~WeaveCleanupSchedule();

  bool configure(std::string_view time, int64_t now);
  void schedule(std::function<void(Ticket)> enqueue);
  bool consume_due_ticket(const Ticket& ticket, int64_t now);
  void cancel();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace ceph::weave
