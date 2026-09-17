// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <set>
#include <utility>
#include <vector>

#include <boost/tuple/tuple.hpp>

#include "osd/ECUtil.h"

struct ClsParmContext;

namespace ceph::weave {

// Data classes consume object bytes plus ClsParmContext, never a native PG
// operation context. Gathered and shard-local execution share this ABI.
//
// Private member extents use shard byte coordinates. All unmarked requests,
// including Volume recovery and RMW, retain ordinary EC stripe geometry.
class WeaveECAdapter {
public:
  using ReadExtent = boost::tuple<uint64_t, uint64_t, uint32_t>;
  using ReadExtents = std::list<ReadExtent>;

  WeaveECAdapter(const ECUtil::stripe_info_t &stripe_info,
                 ceph::ErasureCodeInterfaceRef ec_impl);

  static bool is_member_read(uint32_t);
  static uint32_t store_read_flags(uint32_t);
  static uint32_t with_reconstruction(uint32_t);
  static bool same_member_flags(uint32_t, uint32_t);
  static int execute_data_class(ClsParmContext&, ceph::buffer::list&,
                                ceph::buffer::list&);

  std::pair<uint64_t, uint64_t> shard_read_extent(
    uint64_t offset, uint64_t length, uint32_t flags) const;
  std::pair<uint64_t, uint64_t> backend_read_extent(
    uint64_t offset, uint64_t length, uint32_t flags) const;
  std::pair<uint64_t, uint64_t> reconstruction_extent(
    uint64_t offset, uint64_t length) const;

  int member_shard(uint32_t flags) const;
  int data_shard(unsigned logical) const;
  bool select_data_shards(
    const ReadExtents &extents, std::set<int> &want_to_read) const;

  // Full aligned shard buffers in, only the requested member's bytes out.
  // Pack plugin subchunks per stripe before using ECUtil's shard decoder.
  int decode_member(uint32_t flags, std::map<int, ceph::buffer::list> &shards,
                    ceph::buffer::list &output);

private:
  std::map<int, ceph::buffer::list> pack_stripe(
    uint64_t offset, uint64_t subchunk,
    const std::map<int, std::vector<std::pair<int, int>>> &minimum,
    const std::map<int, ceph::buffer::list> &shards) const;

  const ECUtil::stripe_info_t &stripe_info_;
  ceph::ErasureCodeInterfaceRef ec_impl_;
};

}  // namespace ceph::weave
