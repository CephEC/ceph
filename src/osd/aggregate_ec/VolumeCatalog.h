// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include "osd/osd_types.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <shared_mutex>
#include <utility>
#include <vector>

namespace ceph::aggregate_ec {

struct VolumeMeta {
  hobject_t volume_oid;
  volume_t info;
};

class VolumeCatalog {
public:
  explicit VolumeCatalog(spg_t pgid = spg_t()) : pgid_(std::move(pgid)) {}

  std::shared_ptr<const VolumeMeta> lookup(const hobject_t &obj_oid) const;
  std::shared_ptr<const VolumeMeta> find_nonfull(
    uint32_t capacity,
    uint64_t chunk_size,
    const std::optional<hobject_t> &preferred_volume = std::nullopt,
    const std::set<hobject_t> *excluded_volumes = nullptr) const;
  bool contains(const hobject_t &obj_oid) const;
  bool contains_volume(const hobject_t &volume_oid) const;
  void upsert(const hobject_t &volume_oid, const volume_t &info);
  void remove_volume(const hobject_t &volume_oid);
  void remove_object(const hobject_t &obj_oid);
  int load_from_disk(ceph::buffer::list &encoded);
  int replace_from_disk(std::vector<ceph::buffer::list> &encoded);
  std::vector<hobject_t> list_objects(
    const hobject_t &start, size_t limit,
    std::optional<hobject_t> &next) const;
  void clear();
  size_t size() const;

private:
  void remove_volume_locked(const hobject_t &volume_oid);
  void remove_object_locked(const hobject_t &obj_oid);

  spg_t pgid_;
  mutable std::shared_mutex mutex_;
  // Logical object -> shared physical Volume metadata.
  std::map<hobject_t, std::shared_ptr<VolumeMeta>> index_;
  // Physical Volume -> metadata, used for updates and non-full reuse.
  std::map<hobject_t, std::shared_ptr<VolumeMeta>> volumes_;
  // Reverse membership used to remove every logical mapping atomically.
  std::map<hobject_t, std::set<hobject_t>> volume_objects_;
};

} // namespace ceph::aggregate_ec
