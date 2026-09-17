// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "WeaveCatalog.h"

#include <algorithm>
#include <array>
#include <limits>
#include <mutex>

namespace {

bool valid_metadata(const ceph::weave::WeaveVolumeMeta &info) {
  const uint64_t max_slot = info.data_shards
    ? std::numeric_limits<uint64_t>::max() / info.data_shards : 0;
  if (info.data_shards == 0 || info.data_shards > 256 || info.slot_size == 0 ||
      info.slot_size > max_slot || info.members.size() > info.data_shards) {
    return false;
  }

  // Every member must claim a distinct shard of this Volume's geometry.
  std::array<bool, 256> seen{};
  for (const auto &[oid, member] : info.members) {
    if (oid == info.volume_oid || member.shard >= info.data_shards ||
        member.size > info.slot_size || seen[member.shard]) {
      return false;
    }
    seen[member.shard] = true;
  }
  return true;
}

}  // namespace

namespace ceph::weave {

void WeaveMemberMeta::encode(ceph::buffer::list &bl) const {
  using ceph::encode;
  encode(shard, bl);
  encode(size, bl);
  encode(mtime, bl);
  encode(user_version, bl);
  encode(snap_sequence, bl);
}

void WeaveMemberMeta::decode(ceph::buffer::list::const_iterator &p) {
  using ceph::decode;
  decode(shard, p);
  decode(size, p);
  decode(mtime, p);
  decode(user_version, p);
  decode(snap_sequence, p);
}

void WeaveVolumeMeta::encode(ceph::buffer::list &bl) const {
  using ceph::encode;
  ENCODE_START(4, 4, bl);
  encode(volume_oid, bl);
  encode(data_shards, bl);
  encode(slot_size, bl);
  encode(members, bl);
  ENCODE_FINISH(bl);
}

void WeaveVolumeMeta::decode(ceph::buffer::list::const_iterator &p) {
  using ceph::decode;
  DECODE_START(4, p);
  if (struct_v < 2 || struct_v > 4) {
    throw ceph::buffer::malformed_input(
      "unsupported aggregate metadata version");
  }

  decode(volume_oid, p);
  decode(data_shards, p);
  decode(slot_size, p);
  uint32_t count;
  decode(count, p);

  // v2 stored exactly one member per data shard; later versions allow fewer.
  if (data_shards == 0 || data_shards > 256 || count > data_shards ||
      (struct_v == 2 && count != data_shards)) {
    throw ceph::buffer::malformed_input("invalid aggregate member count");
  }
  members.clear();
  decode_members(count, struct_v, p);

  if (!valid_metadata(*this) || p.get_off() != struct_end) {
    throw ceph::buffer::malformed_input("invalid aggregate metadata");
  }
  DECODE_FINISH(p);
}

void WeaveVolumeMeta::decode_members(
  uint32_t count, __u8 struct_v, ceph::buffer::list::const_iterator &p) {
  using ceph::decode;
  for (uint32_t i = 0; i < count; ++i) {
    hobject_t oid;
    WeaveMemberMeta member;
    decode(oid, p);
    if (struct_v < 4) {
      member = decode_legacy_member(struct_v, p);
    } else {
      decode(member, p);
    }

    // A repeated member would leave the map smaller than the encoded count.
    if (!members.emplace(std::move(oid), member).second) {
      throw ceph::buffer::malformed_input("duplicate aggregate member");
    }
  }
}

WeaveMemberMeta WeaveVolumeMeta::decode_legacy_member(
  __u8 struct_v, ceph::buffer::list::const_iterator &p) {
  using ceph::decode;
  WeaveMemberMeta member;
  decode(member.shard, p);
  decode(member.size, p);
  decode(member.mtime, p);
  if (struct_v >= 3) decode(member.user_version, p);
  return member;
}

std::shared_ptr<const WeaveVolumeMeta> WeaveCatalog::lookup(
  const hobject_t &obj_oid) const {
  std::shared_lock lock(mutex_);
  auto it = index_.find(obj_oid);
  if (it == index_.end()) return nullptr;
  return it->second;
}

std::shared_ptr<const WeaveVolumeMeta> WeaveCatalog::lookup_volume(
  const hobject_t &volume_oid) const {
  std::shared_lock lock(mutex_);
  auto it = volumes_.find(volume_oid);
  return it == volumes_.end() ? nullptr : it->second;
}

std::vector<std::shared_ptr<const WeaveVolumeMeta>>
WeaveCatalog::list_volumes() const {
  std::shared_lock lock(mutex_);
  std::vector<std::shared_ptr<const WeaveVolumeMeta>> result;
  result.reserve(volumes_.size());

  for (const auto &[oid, metadata] : volumes_) {
    result.push_back(metadata);
  }
  return result;
}

bool WeaveCatalog::contains(const hobject_t &obj_oid) const {
  std::shared_lock lock(mutex_);
  return index_.find(obj_oid) != index_.end();
}

bool WeaveCatalog::contains_volume(const hobject_t &volume_oid) const {
  std::shared_lock lock(mutex_);
  return volumes_.find(volume_oid) != volumes_.end();
}

std::vector<hobject_t> WeaveCatalog::list_objects(
  const hobject_t &start, size_t limit,
  std::optional<hobject_t> &next) const {
  std::shared_lock lock(mutex_);
  next.reset();
  std::vector<hobject_t> result;
  result.reserve(std::min(limit, index_.size()));

  auto it = index_.lower_bound(start);
  while (it != index_.end() && result.size() < limit) {
    result.push_back(it->first);
    ++it;
  }

  // next is the first object not returned, so using it as the following
  // lower_bound neither repeats nor skips a logical object.
  if (it != index_.end()) next = it->first;
  return result;
}

size_t WeaveCatalog::size() const {
  std::shared_lock lock(mutex_);
  return index_.size();
}

void WeaveCatalog::upsert(const WeaveVolumeMeta &info) {
  ceph_assert(valid_metadata(info));
  auto meta = std::make_shared<const WeaveVolumeMeta>(info);

  std::unique_lock lock(mutex_);
  // A Volume update is authoritative for all of its logical objects. Remove
  // previous mappings, including membership in any other Volume.
  remove_volume_locked(meta->volume_oid);
  for (const auto &[oid, member] : meta->members) {
    remove_object_locked(oid);
  }

  // Unlink first, then publish: link_members() overwrites index_ entries, so
  // objects dropped from this Volume must already be gone by now.
  volumes_[meta->volume_oid] = meta;
  link_members(meta);
}

void WeaveCatalog::remove_member(
  const hobject_t &volume_oid, const hobject_t &member_oid) {
  std::unique_lock lock(mutex_);
  remove_member_locked(volume_oid, member_oid);
}

void WeaveCatalog::remove_volume(const hobject_t &volume_oid) {
  std::unique_lock lock(mutex_);
  remove_volume_locked(volume_oid);
}

void WeaveCatalog::clear() {
  std::unique_lock lock(mutex_);
  index_.clear();
  volumes_.clear();
}

int WeaveCatalog::load_from_disk(ceph::buffer::list &encoded) {
  if (encoded.length() == 0) return -EINVAL;

  WeaveVolumeMeta info;
  try {
    auto p = encoded.cbegin();
    info.decode(p);
    if (!p.end()) return -EINVAL;
  } catch (const ceph::buffer::error &) {
    return -EINVAL;
  }
  // Separate durable Volumes cannot both own a member. Without a persisted
  // handoff, neither scan order nor a logical version proves ownership.
  for (const auto &[oid, member] : info.members) {
    const auto owner = lookup(oid);
    if (owner && owner->volume_oid != info.volume_oid) return -EEXIST;
  }

  upsert(info);
  return 0;
}

int WeaveCatalog::replace_from_disk(std::vector<ceph::buffer::list> &encoded) {
  // Decode into a separate catalog so readers observe either the previous
  // complete snapshot or the replacement, never a partially loaded mixture.
  WeaveCatalog replacement;
  for (auto &entry : encoded) {
    const int r = replacement.load_from_disk(entry);
    if (r < 0) return r;
  }

  {
    std::unique_lock lock(mutex_);
    // Publish only a complete, unambiguous snapshot. On error keep the previous
    // snapshot intact; the controller closes admission until recovery succeeds.
    index_.swap(replacement.index_);
    volumes_.swap(replacement.volumes_);
  }
  return 0;
}

void WeaveCatalog::remove_volume_locked(const hobject_t &volume_oid) {
  // Caller holds mutex_ exclusively. Never erase a newer logical mapping.
  auto volume = volumes_.find(volume_oid);
  if (volume == volumes_.end()) return;

  for (const auto &[oid, member] : volume->second->members) {
    auto entry = index_.find(oid);
    if (entry != index_.end() && entry->second->volume_oid == volume_oid) {
      index_.erase(entry);
    }
  }

  volumes_.erase(volume);
}

void WeaveCatalog::remove_member_locked(
  const hobject_t &volume_oid, const hobject_t &member_oid) {
  auto volume = volumes_.find(volume_oid);
  if (volume == volumes_.end() ||
      !volume->second->members.count(member_oid)) return;

  // Published metadata may be pinned by in-flight readers. Replace it rather
  // than mutating it, preserving the original shard geometry even when empty.
  auto updated = std::make_shared<WeaveVolumeMeta>(*volume->second);
  updated->members.erase(member_oid);
  const std::shared_ptr<const WeaveVolumeMeta> metadata = std::move(updated);
  volume->second = metadata;

  // Repoint the surviving members, then drop the removed object if this Volume
  // still owns its logical mapping.
  relink_volume(metadata);
  auto entry = index_.find(member_oid);
  if (entry != index_.end() && entry->second->volume_oid == volume_oid) {
    index_.erase(entry);
  }
}

void WeaveCatalog::remove_object_locked(const hobject_t &obj_oid) {
  auto entry = index_.find(obj_oid);
  if (entry == index_.end()) return;

  // The index owns the current Volume, which need not be the caller's.
  const auto metadata = entry->second;
  remove_member_locked(metadata->volume_oid, obj_oid);
}

void WeaveCatalog::link_members(
  const std::shared_ptr<const WeaveVolumeMeta> &metadata) {
  // Caller holds mutex_ exclusively. Published metadata is immutable, so every
  // member of this Volume shares the one snapshot.
  for (const auto &[oid, member] : metadata->members) {
    index_[oid] = metadata;
  }
}

void WeaveCatalog::relink_volume(
  const std::shared_ptr<const WeaveVolumeMeta> &metadata) {
  // Caller holds mutex_ exclusively. Repoint only mappings this Volume still
  // owns; another Volume may have claimed the object since publication.
  for (const auto &[oid, member] : metadata->members) {
    auto entry = index_.find(oid);
    if (entry != index_.end() &&
        entry->second->volume_oid == metadata->volume_oid) {
      entry->second = metadata;
    }
  }
}

}  // namespace ceph::weave
