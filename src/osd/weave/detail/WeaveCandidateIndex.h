// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <set>
#include <vector>

#include "common/ceph_time.h"
#include "osd/osd_types.h"

namespace ceph::weave {

struct WeaveCandidate {
  hobject_t oid;
  uint64_t size;
  eversion_t version;
  version_t user_version;
  utime_t mtime;
  ceph::mono_time changed_at;
};

// The PG lock protects this index. Only committed ordinary objects belong here;
// the caller removes selected members while a conversion owns them.
class WeaveCandidateIndex {
public:
  // Bound per-PG retention even while conversion is disabled or cannot form
  // a group. Excluded objects are reconsidered on their next committed change.
  static constexpr size_t kMaxCandidates = 4096;

  WeaveCandidateIndex() = default;
  WeaveCandidateIndex(const WeaveCandidateIndex&) = delete;
  WeaveCandidateIndex& operator=(const WeaveCandidateIndex&) = delete;

  // Only pack object state representable by member metadata, which cannot
  // preserve native logical truncate history.
  static bool eligible(const object_info_t& oi) {
    return oi.truncate_seq == 0 && oi.truncate_size == 0;
  }

  size_t size() const { return candidates_.size(); }
  bool empty() const;
  std::vector<WeaveCandidate> select(
    uint32_t k, uint64_t unit, uint64_t min_size, double quiet_seconds,
    uint64_t max_volume_size, unsigned padding_percent,
    ceph::mono_time now,
    const std::function<bool(const WeaveCandidate&)>& available = {}) const;

  void upsert(const object_info_t& oi, ceph::mono_time now);
  void configure(uint32_t k, uint64_t unit, uint64_t min_size,
                 uint64_t max_volume_size);
  void erase(const hobject_t& oid);
  void clear();

private:
  struct BySize {
    bool operator()(const WeaveCandidate* a, const WeaveCandidate* b) const {
      return a->size != b->size ? a->size > b->size : a->oid < b->oid;
    }
  };

  bool admits(uint64_t size) const {
    return size && size >= minimum_ && size <= maximum_;
  }

  void prune_inadmissible();
  bool admissible(const WeaveCandidate& candidate, uint64_t max_slot,
                  ceph::mono_time now, double quiet_seconds) const;
  bool fits_slot(uint64_t size, uint64_t max_slot, uint64_t unit) const;

  void advance_window(std::vector<const WeaveCandidate*>& window,
                      size_t& head, uint64_t& sum,
                      const WeaveCandidate& candidate, uint32_t k) const;
  bool within_padding_budget(uint64_t volume_size, uint64_t sum,
                             unsigned padding_percent) const;
  std::vector<WeaveCandidate> window_snapshot(
    const std::vector<const WeaveCandidate*>& window, size_t head,
    uint32_t k) const;

  uint64_t minimum_ = 1;
  uint64_t maximum_ = std::numeric_limits<uint64_t>::max();
  std::map<hobject_t, WeaveCandidate> candidates_;
  // Map nodes own stable WeaveCandidate addresses; update size only while its
  // ordering node is extracted, and remove pointers before destroying owners.
  std::set<const WeaveCandidate*, BySize> by_size_;
};

}  // namespace ceph::weave
