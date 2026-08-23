// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include "osd/ECUtil.h"
#include "osd/ExtentCache.h"
#include "osd/OpRequest.h"
#include "osd/osd_types.h"

#include <boost/tuple/tuple.hpp>

#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <optional>
#include <set>
#include <utility>
#include <vector>

class PrimaryLogPG;

namespace ceph::aggregate_ec {

// Contains the aggregateEC-specific policy injected into ECBackend's normal
// read/write algorithms.  ECBackend calls this boundary instead of embedding
// aggregate layout rules among the native stripe calculations.
class ECBackendIntegration {
public:
  using ReadExtent = boost::tuple<uint64_t, uint64_t, uint32_t>;
  using ReadExtents = std::list<ReadExtent>;
  using ObjectReads = std::map<hobject_t, ReadExtents>;
  using ShardReads =
    std::map<pg_shard_t, std::vector<std::pair<int, int>>>;

  ECBackendIntegration(
    bool enabled, bool redirect_reads,
    const ECUtil::stripe_info_t &stripe_info,
    ceph::ErasureCodeInterfaceRef ec_impl);

  bool enabled() const { return enabled_; }

  // Converts an object extent into the offset/length sent to one EC shard.
  std::pair<uint64_t, uint64_t> shard_read_extent(
    uint64_t offset, uint64_t length) const;

  // Native EC reads operate on stripe bounds; aggregateEC reads retain the
  // logical extent because each volume contains exactly one stripe.
  std::pair<uint64_t, uint64_t> backend_read_extent(
    uint64_t offset, uint64_t length) const;

  // Maps the logical data chunks covered by extents to physical EC shards.
  // Returns false in native mode so the caller can use Ceph's normal policy.
  bool select_data_shards(
    const ReadExtents &extents, std::set<int> &want_to_read) const;
  void map_logical_shards(
    const std::set<int> &logical_shards,
    std::set<int> &want_to_read) const;

  // A redirected replica read is known to target its local data shard, so it
  // must not consult primary-only missing-location state.
  bool select_local_redirect_read(
    bool is_primary, pg_shard_t local_shard, int sub_chunk_count,
    const std::set<int> &want_to_read, ShardReads &shards) const;

  bool can_return_without_decode(std::size_t shard_count) const;
  bool needs_full_reconstruction(
    std::size_t shard_count, int data_chunk_count) const;
  void align_for_full_reconstruction(ObjectReads &reads) const;

  std::optional<std::map<int, std::size_t>> storage_optimization_offsets(
    PrimaryLogPG *pg, const OpRequestRef &client_op,
    const hobject_t &volume_oid,
    int data_chunk_count, int coding_chunk_count) const;
  bool read_cached_volume(
    PrimaryLogPG *pg, const hobject_t &volume_oid,
    extent_map &result) const;

private:
  bool enabled_;
  bool redirect_reads_;
  const ECUtil::stripe_info_t &stripe_info_;
  ceph::ErasureCodeInterfaceRef ec_impl_;
};

} // namespace ceph::aggregate_ec
