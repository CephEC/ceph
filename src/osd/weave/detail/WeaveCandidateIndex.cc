// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "WeaveCandidateIndex.h"

#include <chrono>
#include <cmath>
#include <utility>

namespace ceph::weave {

void WeaveCandidateIndex::configure(uint32_t k, uint64_t unit,
  uint64_t min_size, uint64_t max_volume_size) {
  // A member must fit one slot of a k-member volume, so the upper bound is
  // the largest unit-aligned size that still does.
  const auto limit = k && unit ? (max_volume_size / k / unit) * unit : 0;
  if (minimum_ == min_size && maximum_ == limit) return;

  minimum_ = min_size;
  maximum_ = limit;
  prune_inadmissible();
}

void WeaveCandidateIndex::prune_inadmissible() {
  for (auto p = candidates_.begin(); p != candidates_.end();) {
    if (!admits(p->second.size)) {
      // Drop the size pointer before the candidate it points at.
      by_size_.erase(&p->second);
      p = candidates_.erase(p);
    } else {
      ++p;
    }
  }
}

void WeaveCandidateIndex::upsert(
  const object_info_t& oi, ceph::mono_time now) {
  auto p = candidates_.find(oi.soid);

  // A replay of an older object state must not undo a newer one.
  if (p != candidates_.end() && oi.version < p->second.version) {
    return;
  }

  // Ineligible or out-of-bounds objects must not stay in the index.
  if (!eligible(oi) || !admits(oi.size)) {
    erase(oi.soid);
    return;
  }

  if (p != candidates_.end()) {
    auto& old = p->second;
    // Identical state must not restart the quiet period.
    if (oi.version == old.version && oi.user_version == old.user_version &&
        oi.size == old.size && oi.mtime == old.mtime) {
      return;
    }
    if (oi.size != old.size) {
      // Reinsert at the new key: the ordering node itself is extracted first.
      auto node = by_size_.extract(&old);
      old.size = oi.size;
      by_size_.insert(std::move(node));
    }
    old.version = oi.version;
    old.user_version = oi.user_version;
    old.mtime = oi.mtime;
    old.changed_at = now;
  } else {
    if (candidates_.size() >= kMaxCandidates) return;
    auto inserted = candidates_.emplace(oi.soid, WeaveCandidate{
      oi.soid, oi.size, oi.version, oi.user_version, oi.mtime, now}).first;
    by_size_.insert(&inserted->second);
  }
}

void WeaveCandidateIndex::erase(const hobject_t& oid) {
  auto p = candidates_.find(oid);
  if (p != candidates_.end()) {
    // Drop the size pointer before the candidate it points at.
    by_size_.erase(&p->second);
    candidates_.erase(p);
  }
}

void WeaveCandidateIndex::clear() {
  // The size pointers must be dropped before their candidates.
  by_size_.clear();
  candidates_.clear();
}

bool WeaveCandidateIndex::empty() const {
  return candidates_.empty();
}

std::vector<WeaveCandidate> WeaveCandidateIndex::select(
  uint32_t k, uint64_t unit, uint64_t min_size, double quiet_seconds,
  uint64_t max_volume_size, unsigned padding_percent,
  ceph::mono_time now,
  const std::function<bool(const WeaveCandidate&)>& available) const {
  if (!k || !unit || candidates_.size() < k ||
      !std::isfinite(quiet_seconds) || quiet_seconds < 0) {
    return {};
  }

  // Check alignment against the per-slot limit before adding padding. This
  // also excludes oversized outliers without overflowing either L or k * L.
  const uint64_t max_slot = max_volume_size / k;
  if (max_slot < unit || max_slot < min_size) {
    return {};
  }

  // The persistent size ordering makes scans linear with only a k-pointer
  // circular window. Copy full object identities only for the accepted group.
  std::vector<const WeaveCandidate*> window;
  window.reserve(k);
  size_t head = 0;
  uint64_t sum = 0;

  for (const auto* candidate : by_size_) {
    // Sizes come in descending order, so the first small object ends the scan.
    if (!candidate->size || candidate->size < min_size) {
      break;
    }

    // Skip anything still settling, too large for a slot, or unavailable.
    if (!admissible(*candidate, max_slot, now, quiet_seconds)) {
      continue;
    }
    if (!fits_slot(candidate->size, max_slot, unit)) {
      continue;
    }
    if (available && !available(*candidate)) continue;

    // The window keeps the k most recently accepted candidates, largest at
    // head, and sum tracks their payload total for the padding check.
    advance_window(window, head, sum, *candidate, k);
    if (window.size() < k) {
      continue;
    }

    // One slot must hold the largest member at a whole number of units.
    const uint64_t largest = window[head]->size;
    const uint64_t largest_remainder = largest % unit;
    const uint64_t slot = largest +
      (largest_remainder ? unit - largest_remainder : 0);
    const uint64_t volume_size = slot * k;

    if (within_padding_budget(volume_size, sum, padding_percent)) {
      return window_snapshot(window, head, k);
    }
  }

  return {};
}

bool WeaveCandidateIndex::admissible(
  const WeaveCandidate& candidate, uint64_t max_slot, ceph::mono_time now,
  double quiet_seconds) const {
  if (candidate.size > max_slot) return false;

  // A changed_at in the future means the clock stepped back; not quiet yet.
  if (now < candidate.changed_at) return false;
  return std::chrono::duration<double>(now - candidate.changed_at).count() >=
    quiet_seconds;
}

bool WeaveCandidateIndex::fits_slot(
  uint64_t size, uint64_t max_slot, uint64_t unit) const {
  const uint64_t remainder = size % unit;
  const uint64_t padding = remainder ? unit - remainder : 0;
  // The padded slot, not the raw size, has to fit the per-slot limit.
  return padding <= max_slot - size;
}

void WeaveCandidateIndex::advance_window(
  std::vector<const WeaveCandidate*>& window, size_t& head, uint64_t& sum,
  const WeaveCandidate& candidate, uint32_t k) const {
  // The scan is size-descending, so a full window evicts its largest member
  // at head and reuses that slot for the new, smallest one.
  if (window.size() == k) {
    sum -= window[head]->size;
    window[head] = &candidate;
    head = (head + 1) % k;
  } else {
    window.push_back(&candidate);
  }

  // Each size is bounded by max_volume_size / k, so this rolling sum fits.
  sum += candidate.size;
}

bool WeaveCandidateIndex::within_padding_budget(
  uint64_t volume_size, uint64_t sum, unsigned padding_percent) const {
  // Widen before both multiplication and addition, including arbitrary
  // configured percentage values. Do not round a fractional byte allowance.
  return static_cast<unsigned __int128>(volume_size) * 100 <=
    static_cast<unsigned __int128>(sum) *
      (static_cast<uint64_t>(padding_percent) + 100);
}

std::vector<WeaveCandidate> WeaveCandidateIndex::window_snapshot(
  const std::vector<const WeaveCandidate*>& window, size_t head,
  uint32_t k) const {
  std::vector<WeaveCandidate> selected;
  selected.reserve(k);
  // Walk the circular window from its largest member.
  for (size_t i = 0; i < k; ++i) {
    selected.push_back(*window[(head + i) % k]);
  }

  return selected;
}

}  // namespace ceph::weave
