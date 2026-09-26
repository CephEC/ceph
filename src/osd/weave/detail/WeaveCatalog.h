// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "osd/osd_types.h"

namespace ceph::weave {

struct WeaveMemberMeta {
  uint8_t shard = 0;
  uint64_t size = 0;
  utime_t mtime;
  version_t user_version = 0;
  // Last snapshot context applied to the original native head, as recorded
  // when the member was packed.
  snapid_t snap_sequence = CEPH_NOSNAP;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &p);
};
WRITE_CLASS_ENCODER(WeaveMemberMeta)

struct WeaveVolumeMeta {
  hobject_t volume_oid;
  uint32_t data_shards = 0;
  uint64_t slot_size = 0;
  std::map<hobject_t, WeaveMemberMeta> members;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &p);

private:
  void decode_members(uint32_t count, ceph::buffer::list::const_iterator &p);
};
WRITE_CLASS_ENCODER(WeaveVolumeMeta)

class WeaveCatalog {
public:
  std::shared_ptr<const WeaveVolumeMeta> lookup(const hobject_t &obj_oid) const;
  std::shared_ptr<const WeaveVolumeMeta> lookup_volume(
    const hobject_t &volume_oid) const;
  std::vector<std::shared_ptr<const WeaveVolumeMeta>> list_volumes() const;
  bool contains(const hobject_t &obj_oid) const;
  std::vector<hobject_t> list_objects(
    const hobject_t &start, size_t limit,
    std::optional<hobject_t> &next) const;

  void upsert(const WeaveVolumeMeta &info);
  void remove_member(const hobject_t &volume_oid, const hobject_t &member_oid);
  void remove_volume(const hobject_t &volume_oid);
  // The source identity must match the encoded Volume; decode each row once.
  int load_from_disk(const hobject_t& source, const ceph::buffer::list& encoded);
  int replace_from_disk(
    const std::vector<std::pair<hobject_t, ceph::buffer::list>>& stored);
  void clear();

private:
  void remove_volume_locked(const hobject_t &volume_oid);
  void remove_member_locked(
    const hobject_t &volume_oid, const hobject_t &member_oid);
  void remove_object_locked(const hobject_t &obj_oid);
  void link_members(const std::shared_ptr<const WeaveVolumeMeta> &metadata);
  void relink_volume(const std::shared_ptr<const WeaveVolumeMeta> &metadata);

  mutable std::shared_mutex mutex_;
  // Logical object -> shared physical Volume metadata.
  std::map<hobject_t, std::shared_ptr<const WeaveVolumeMeta>> index_;
  // Physical Volume -> immutable published metadata.
  std::map<hobject_t, std::shared_ptr<const WeaveVolumeMeta>> volumes_;
};

}  // namespace ceph::weave
