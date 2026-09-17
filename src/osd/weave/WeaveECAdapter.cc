// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "WeaveECAdapter.h"

#include <cerrno>

#include "detail/WeaveLayout.h"
#include "include/ceph_assert.h"
#include "include/intarith.h"
#include "osd/ClassHandler.h"
#include "osd/osd_types.h"

namespace ceph::weave {

bool WeaveECAdapter::is_member_read(uint32_t flags) {
  return ceph::weave::is_member_read(flags);
}

uint32_t WeaveECAdapter::store_read_flags(uint32_t flags) {
  return ceph::weave::store_read_flags(flags);
}

uint32_t WeaveECAdapter::with_reconstruction(uint32_t flags) {
  return flags | kMemberReconstruct;
}

bool WeaveECAdapter::same_member_flags(uint32_t a, uint32_t b) {
  return (a & kMemberFlags) == (b & kMemberFlags);
}

int WeaveECAdapter::execute_data_class(
  ClsParmContext& context, ceph::buffer::list& object_data,
  ceph::buffer::list& output) {
  std::string class_name, method_name;
  ceph::buffer::list arguments;
  auto p = context.parm_data.cbegin();

  // The parm blob is length prefixed; a short read means it is malformed.
  try {
    p.copy(context.class_len, class_name);
    p.copy(context.method_len, method_name);
    p.copy(context.indata_len, arguments);
  } catch (const ceph::buffer::error&) {
    return -EINVAL;
  }

  ClassHandler::ClassData* cls = nullptr;
  int r = ClassHandler::get_instance().open_class(class_name, &cls);
  // The executing shard can have a different class directory or allowlist
  // from the primary that validated the client operation.
  if (r < 0) return r;
  if (!cls) return -EIO;
  auto* method = cls->get_method(method_name);
  if (!method) return -EOPNOTSUPP;

  context.parm_data = std::move(arguments);
  return method->exec(static_cast<cls_method_context_t>(&context),
                      object_data, output);
}

WeaveECAdapter::WeaveECAdapter(
  const ECUtil::stripe_info_t &stripe_info,
  ceph::ErasureCodeInterfaceRef ec_impl)
  : stripe_info_(stripe_info), ec_impl_(std::move(ec_impl)) {}

std::pair<uint64_t, uint64_t>
WeaveECAdapter::shard_read_extent(
  uint64_t offset, uint64_t length, uint32_t flags) const {
  // Member reads already use shard byte coordinates; ordinary reads must be
  // widened to the stripe geometry the backend reads from.
  if (is_member_read(flags)) {
    // Only a reconstruction read needs the whole chunks around the request.
    return (flags & kMemberReconstruct)
      ? reconstruction_extent(offset, length)
      : std::make_pair(offset, length);
  }

  return stripe_info_.aligned_offset_len_to_chunk({offset, length});
}

std::pair<uint64_t, uint64_t>
WeaveECAdapter::backend_read_extent(
  uint64_t offset, uint64_t length, uint32_t flags) const {
  // A member read is a shard-local extent; anything else must cover whole
  // stripes for the backend to decode.
  if (is_member_read(flags)) return {offset, length};

  return stripe_info_.offset_len_to_stripe_bounds({offset, length});
}

std::pair<uint64_t, uint64_t>
WeaveECAdapter::reconstruction_extent(
  uint64_t offset, uint64_t length) const {
  const uint64_t unit = stripe_info_.get_chunk_size();
  // Expand to whole chunks on both sides: the decoder needs complete chunks.
  const uint64_t start = offset - offset % unit;
  const uint64_t end = round_up_to(offset + length, unit);

  return {start, end - start};
}

int WeaveECAdapter::member_shard(uint32_t flags) const {
  ceph_assert(is_member_read(flags));
  return data_shard(member_id(flags));
}

int WeaveECAdapter::data_shard(unsigned logical) const {
  ceph_assert(logical < ec_impl_->get_data_chunk_count());

  // A plugin may permute data shards; an empty mapping means identity.
  const auto &mapping = ec_impl_->get_chunk_mapping();
  return mapping.empty() ? logical : mapping.at(logical);
}

bool WeaveECAdapter::select_data_shards(
  const ReadExtents &extents, std::set<int> &want_to_read) const {
  if (extents.empty() || !is_member_read(extents.front().get<2>())) {
    return false;
  }

  // Every extent of the request must decode the same member shard, or the
  // gathered read has nothing to aim at.
  const int target = member_shard(extents.front().get<2>());
  for (const auto &extent : extents) {
    ceph_assert(is_member_read(extent.get<2>()));
    ceph_assert(member_shard(extent.get<2>()) == target);
  }

  want_to_read.insert(target);
  return true;
}

// One stripe's subchunk ranges, already restricted to the minimum set the
// plugin needs. Shards stay full aligned buffers; only these ranges are packed.
std::map<int, ceph::buffer::list> WeaveECAdapter::pack_stripe(
  uint64_t offset, uint64_t subchunk,
  const std::map<int, std::vector<std::pair<int, int>>> &minimum,
  const std::map<int, ceph::buffer::list> &shards) const {
  std::map<int, ceph::buffer::list> packed;
  for (const auto &[id, ranges] : minimum) {
    for (const auto &[start, count] : ranges) {
      ceph::buffer::list part;
      part.substr_of(shards.at(id), offset + start * subchunk,
                     count * subchunk);
      packed[id].claim_append(part);
    }
  }

  return packed;
}

int WeaveECAdapter::decode_member(
  uint32_t flags, std::map<int, ceph::buffer::list> &shards,
  ceph::buffer::list &output) {
  if (shards.empty()) return -EIO;

  // Phase one: every shard buffer must be the same chunk-aligned length, and
  // the plugin needs the set of shards it may draw on.
  const uint64_t unit = stripe_info_.get_chunk_size();
  const uint64_t length = shards.begin()->second.length();
  if (length % unit) return -EIO;
  std::set<int> available;
  for (const auto &[id, data] : shards) {
    if (data.length() != length) return -EIO;
    available.insert(id);
  }

  // Phase two: ask the plugin for the cheapest subchunk set that decodes the
  // target shard, then decode it one stripe at a time.
  const int target = member_shard(flags);
  std::map<int, std::vector<std::pair<int, int>>> minimum;
  int r = ec_impl_->minimum_to_decode({target}, available, &minimum);
  if (r < 0) return r;
  const uint64_t subchunk = unit / ec_impl_->get_sub_chunk_count();

  for (uint64_t offset = 0; offset < length; offset += unit) {
    auto packed = pack_stripe(offset, subchunk, minimum, shards);
    ceph::buffer::list decoded;
    std::map<int, ceph::buffer::list *> outputs{{target, &decoded}};
    r = ECUtil::decode(stripe_info_, ec_impl_, packed, outputs);
    if (r < 0) return r;
    if (decoded.length() != unit) return -EIO;

    output.claim_append(decoded);
  }
  return 0;
}

}  // namespace ceph::weave
