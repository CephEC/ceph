// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <functional>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "WeaveReadRoute.h"
#include "osd/OpRequest.h"

namespace ceph::weave {

struct WeavePolicy {
  bool background = true;
  uint64_t min_size = 1 << 20;
  double quiet_seconds = 30;
  double scan_seconds = 5;
  uint64_t max_volume_size = 64 << 20;
  unsigned padding_percent = 10;
};

struct WeaveGeometry {
  uint32_t data_shards = 0;
  uint64_t unit = 0;
};

struct WeaveObjectState {
  enum class Access { kIdle, kReading, kBusy };
  object_info_t info;
  bool exists = false;
  // kReading means shared readers only, with no waiters or other blockers.
  // kBusy includes admitted writes through durable completion, even after an
  // Objecter timeout. Only packing may coexist with kReading.
  Access access = Access::kIdle;
  bool busy() const { return access != Access::kIdle; }
  bool blocks_pack() const { return access == Access::kBusy; }
  bool has_clones = false;
  snapid_t snap_sequence = 0;
};

using WeaveAttrs = std::map<std::string, ceph::buffer::list>;
using WeaveVolumeAttrs = std::vector<std::pair<hobject_t, ceph::buffer::list>>;
using WeaveCompletion = std::function<void(int)>;

// Move-only ownership of one conversion slot. Release is thread safe and
// idempotent; PG reservations are retired by the job's serialized completion.
class WeaveLease {
public:
  explicit WeaveLease(std::function<void()> release)
    : release_(std::move(release)) {}

  ~WeaveLease() { reset(); }
  WeaveLease(const WeaveLease&) = delete;
  WeaveLease& operator=(const WeaveLease&) = delete;

  void reset() {
    // Reset is idempotent and thread safe, so the callback is detached before
    // it runs: only one caller can ever see it.
    auto release = std::move(release_);
    release_ = {};

    if (release) release();
  }

private:
  std::function<void()> release_;
};

// Ceph implements this port without exposing PG/ObjectContext pointers.
// Unless specified otherwise, calls and completions run under the PG lock.
// I/O callbacks MUST NOT run inline: the returned tid authenticates the
// request before it is admitted to the PG. read/write buffers live through
// completion, including cancelled completions. pin() keeps the owner alive
// across callbacks.
class WeavePGHost {
public:
  virtual ~WeavePGHost() = default;
  virtual std::shared_ptr<void> pin() = 0;
  virtual WeavePolicy policy() const = 0;
  virtual WeaveGeometry geometry() const = 0;
  virtual bool primary() const = 0;
  virtual bool active() const = 0;
  virtual bool clean() const = 0;
  virtual bool has_missing() const = 0;
  virtual snapid_t snap_sequence() const = 0;
  virtual epoch_t epoch() const = 0;
  virtual bool current(epoch_t) const = 0;
  virtual int osd_id() const = 0;
  virtual WeaveObjectState inspect(const hobject_t&) = 0;
  virtual bool wait_for_available(const hobject_t&, OpRequestRef&) = 0;
  virtual int load_metadata(WeaveVolumeAttrs&) = 0;
  virtual hobject_t new_volume(const hobject_t& seed) = 0;
  virtual void requeue(std::list<OpRequestRef>&) = 0;
  virtual void reply_error(const OpRequestRef&, int) = 0;
  virtual std::optional<WeaveReadRoute> locate_read(
    const hobject_t&, unsigned member) = 0;
  virtual int load_read_route(const WeaveReadRoute&,
                              ceph::buffer::list&) = 0;
  virtual void reply_read_redirect(const OpRequestRef&,
                                   const WeaveReadRoute&) = 0;

  virtual std::unique_ptr<WeaveLease> acquire() = 0;
  virtual void schedule(double seconds, std::function<void()>) = 0;
  virtual void cancel_wakeup() = 0;
  // post() runs CPU work without the PG lock. serialized() reacquires it.
  virtual void post(std::function<void()>) = 0;
  virtual void serialized(std::function<void()>) = 0;

  // Optional crash-injection seam. Production hosts leave it inactive unless
  // explicitly configured; checkpoints always run under the PG lock.
  virtual void conversion_checkpoint(const char*, size_t = 0) {}

  virtual ceph_tid_t read(const hobject_t&, version_t, uint64_t size,
                          ceph::buffer::list*, WeaveAttrs*,
                          WeaveCompletion) = 0;
  virtual ceph_tid_t write(const hobject_t&, const ceph::buffer::list&,
                           const WeaveAttrs&, utime_t, bool replace,
                           WeaveCompletion) = 0;
  virtual ceph_tid_t remove(const hobject_t&, std::optional<version_t>,
                            WeaveCompletion) = 0;
  virtual void cancel_io(ceph_tid_t) = 0;
};

// Borrowed view of the native, locked transaction. Attribute names/encoding
// belong to Weave; the host reads projected attrs and commits the native txn.
struct WeaveTransaction {
  std::function<int(const char*, ceph::buffer::list&)> read_attribute;
  std::function<void(const char*, const ceph::buffer::list&)> set_attribute;
};

}  // namespace ceph::weave
