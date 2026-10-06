// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <vector>

#include "include/buffer.h"

namespace ceph::weave {

// Helper steps of interleave_members(). They stay inline in a named namespace
// rather than an unnamed one: unnamed namespaces in headers give every
// including translation unit its own copy.
inline uint64_t member_slot_length(const ceph::buffer::list& member,
                                   uint64_t offset, uint64_t unit) {
  return offset < member.length()
    ? std::min<uint64_t>(unit, member.length() - offset) : 0;
}

// Append one member slice. substr_of/claim_append retain references to the
// source data, so the slice is never copied.
inline void append_member_slice(const ceph::buffer::list& member,
                                uint64_t offset, uint64_t length,
                                ceph::buffer::list& output) {
  ceph::buffer::list part;
  part.substr_of(member, offset, length);
  output.claim_append(part);
}

// Zero-fill the remainder of a member's slot.
inline void append_slot_padding(uint64_t length, uint64_t unit,
                                ceph::buffer::list& output) {
  if (length < unit) output.append_zero(unit - length);
}

// Marks a server-side physical Objecter request. The adapter injects it on every
// sub-op it issues and the controller strips it before the native OSD inspects
// the operation, so both sides must agree on the bit.
constexpr uint32_t kInternalIo = 1u << 29;

// Flags for translated read extents. They are internal to Weave, never
// ObjectStore hints; offsets on such extents are member/shard-relative, not
// Volume logical offsets.
constexpr uint32_t kMemberRead = uint32_t{1} << 31;
constexpr uint32_t kMemberReconstruct = uint32_t{1} << 30;
constexpr uint32_t kMemberIdMask = uint32_t{0xff} << 16;
constexpr uint32_t kMemberFlags =
  kMemberRead | kMemberReconstruct | kMemberIdMask;

inline bool is_member_read(uint32_t flags) {
  return (flags & kMemberRead) != 0;
}

inline uint8_t member_id(uint32_t flags) {
  return static_cast<uint8_t>((flags & kMemberIdMask) >> 16);
}

inline uint32_t member_read_flags(uint32_t flags, uint8_t id) {
  return (flags & ~kMemberFlags) | kMemberRead | (uint32_t{id} << 16);
}

inline uint32_t store_read_flags(uint32_t flags) {
  return flags & ~kMemberFlags;
}

inline uint64_t member_volume_offset(
  uint64_t offset, uint32_t member, uint32_t data_shards, uint64_t unit) {
  return (offset / unit) * (uint64_t{data_shards} * unit) +
    uint64_t{member} * unit + offset % unit;
}

// Build a normal EC input stream whose i-th data shard contains members[i].
// substr_of/append retain references to source data; no full payload transpose
// copy is made.
inline bool interleave_members(
  const std::vector<ceph::buffer::list>& members,
  uint64_t unit, uint64_t slot_size, ceph::buffer::list& output) {
  // Reject geometry the loop cannot tile: unit must divide the slot.
  if (members.empty() || unit == 0 || slot_size == 0 || slot_size % unit ||
      slot_size > std::numeric_limits<uint32_t>::max() / members.size()) {
    return false;
  }

  for (const auto& member : members) {
    if (member.length() > slot_size) return false;
  }

  for (uint64_t offset = 0; offset < slot_size; offset += unit) {
    for (const auto& member : members) {
      const uint64_t length = member_slot_length(member, offset, unit);
      if (length) append_member_slice(member, offset, length, output);
      append_slot_padding(length, unit, output);
    }
  }
  return true;
}

}  // namespace ceph::weave
