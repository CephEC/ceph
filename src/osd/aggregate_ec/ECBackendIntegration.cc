// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "ECBackendIntegration.h"

#include "include/ceph_assert.h"
#include "osd/PrimaryLogPG.h"

namespace ceph::aggregate_ec {

ECBackendIntegration::ECBackendIntegration(
  bool enabled, bool redirect_reads,
  const ECUtil::stripe_info_t &stripe_info,
  ceph::ErasureCodeInterfaceRef ec_impl)
  : enabled_(enabled),
    redirect_reads_(redirect_reads),
    stripe_info_(stripe_info),
    ec_impl_(std::move(ec_impl)) {}

std::pair<uint64_t, uint64_t>
ECBackendIntegration::shard_read_extent(
  uint64_t offset, uint64_t length) const {
  if (!enabled_) {
    return stripe_info_.aligned_offset_len_to_chunk({offset, length});
  }

  const uint64_t chunk_size = stripe_info_.get_chunk_size();
  if (length > chunk_size) {
    // Recovery and RMW read the complete shard, unlike a client partial read.
    return {0, chunk_size};
  }
  return {offset % chunk_size, length};
}

std::pair<uint64_t, uint64_t>
ECBackendIntegration::backend_read_extent(
  uint64_t offset, uint64_t length) const {
  if (enabled_) return {offset, length};
  return stripe_info_.offset_len_to_stripe_bounds({offset, length});
}

bool ECBackendIntegration::select_data_shards(
  const ReadExtents &extents, std::set<int> &want_to_read) const {
  if (!enabled_) return false;

  std::set<int> logical_shards;
  const uint64_t chunk_size = stripe_info_.get_chunk_size();
  for (const auto &extent : extents) {
    const uint64_t length = extent.get<1>();
    if (length == 0) continue;
    const uint64_t first = extent.get<0>() / chunk_size;
    const uint64_t last =
      (extent.get<0>() + length - 1) / chunk_size;
    for (uint64_t shard = first; shard <= last; ++shard) {
      logical_shards.insert(static_cast<int>(shard));
    }
  }
  map_logical_shards(logical_shards, want_to_read);
  return true;
}

void ECBackendIntegration::map_logical_shards(
  const std::set<int> &logical_shards,
  std::set<int> &want_to_read) const {
  ceph_assert(enabled_);
  const auto &chunk_mapping = ec_impl_->get_chunk_mapping();
  for (int logical : logical_shards) {
    const int physical = logical < static_cast<int>(chunk_mapping.size())
      ? chunk_mapping[logical]
      : logical;
    want_to_read.insert(physical);
  }
}

bool ECBackendIntegration::select_local_redirect_read(
  bool is_primary, pg_shard_t local_shard, int sub_chunk_count,
  const std::set<int> &want_to_read, ShardReads &shards) const {
  if (!enabled_ || !redirect_reads_ || is_primary) return false;

  ceph_assert(want_to_read.size() == 1);
  shards.emplace(
    local_shard,
    std::vector<std::pair<int, int>>{{0, sub_chunk_count}});
  return true;
}

bool ECBackendIntegration::can_return_without_decode(
  std::size_t shard_count) const {
  return enabled_ && shard_count == 1;
}

bool ECBackendIntegration::needs_full_reconstruction(
  std::size_t shard_count, int data_chunk_count) const {
  return enabled_ && shard_count == static_cast<std::size_t>(data_chunk_count);
}

void ECBackendIntegration::align_for_full_reconstruction(
  ObjectReads &reads) const {
  ceph_assert(enabled_);
  for (auto &entry : reads) {
    auto &extents = entry.second;
    extent_set aligned;
    uint32_t flags = 0;
    for (const auto &extent : extents) {
      auto range = stripe_info_.offset_len_to_stripe_bounds(
        {extent.get<0>(), extent.get<1>()});
      aligned.union_insert(range.first, range.second);
      flags |= extent.get<2>();
    }

    if (aligned.empty()) continue;
    extents.clear();
    for (auto extent = aligned.begin(); extent != aligned.end(); ++extent) {
      extents.push_back(boost::make_tuple(
        extent.get_start(), extent.get_len(), flags));
    }
  }
}

std::optional<std::map<int, std::size_t>>
ECBackendIntegration::storage_optimization_offsets(
  PrimaryLogPG *pg, const OpRequestRef &client_op,
  const hobject_t &volume_oid,
  int data_chunk_count, int coding_chunk_count) const {
  if (!client_op ||
      !client_op->need_aggregateEC_storage_optimize()) {
    return std::nullopt;
  }
  ceph_assert(enabled_ && pg != nullptr);
  return pg->get_aggregate_ec()->storage_optimization_offsets(
    client_op, volume_oid, ec_impl_->get_chunk_mapping(),
    data_chunk_count, coding_chunk_count);
}

bool ECBackendIntegration::read_cached_volume(
  PrimaryLogPG *pg, const hobject_t &volume_oid,
  extent_map &result) const {
  if (!enabled_ || pg == nullptr) return false;
  auto *integration = pg->get_aggregate_ec();
  if (!integration->is_volume_cached(volume_oid)) return false;
  integration->read_cached_volume(result);
  return true;
}

} // namespace ceph::aggregate_ec
