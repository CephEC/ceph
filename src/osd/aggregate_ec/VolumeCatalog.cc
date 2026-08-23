// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "VolumeCatalog.h"
#include <algorithm>
#include <mutex>

namespace ceph::aggregate_ec {

namespace {
bool valid_metadata(const volume_t &info) {
  // Treat persisted metadata as untrusted input.  The three catalog indexes
  // assume a one-to-one relationship between logical OIDs, chunk IDs, and set
  // bits, so reject any descriptor that would violate those invariants.
  const uint32_t capacity = info.get_cap();
  const auto &chunks = info.get_chunk_map();
  const auto &bitmap = info.get_chunk_bitmap();
  if (capacity == 0 || info.get_chunk_size() == 0 ||
      info.get_size() != chunks.size() || bitmap.size() != capacity) {
    return false;
  }
  std::vector<bool> seen(capacity, false);
  for (const auto &[oid, chunk] : chunks) {
    const uint32_t id = chunk.get_chunk_id().id;
    if (!chunk.is_valid() || chunk.get_oid() != oid || id >= capacity ||
        seen[id] || !bitmap[id]) {
      return false;
    }
    seen[id] = true;
  }
  return std::equal(seen.begin(), seen.end(), bitmap.begin(), bitmap.end());
}
} // anonymous namespace

std::shared_ptr<const VolumeMeta> VolumeCatalog::lookup(
  const hobject_t &obj_oid) const {
  std::shared_lock lock(mutex_);
  auto it = index_.find(obj_oid);
  if (it == index_.end()) return nullptr;
  return it->second;
}

std::shared_ptr<const VolumeMeta> VolumeCatalog::find_nonfull(
  uint32_t capacity,
  uint64_t chunk_size,
  const std::optional<hobject_t> &preferred_volume,
  const std::set<hobject_t> *excluded_volumes) const {
  // Capacity and chunk size are part of the physical layout and must match the
  // current EC profile.  excluded_volumes contains requests with stale-prone
  // metadata snapshots that must finish before the Volume can be reused.
  auto reusable = [capacity, chunk_size, excluded_volumes](
                    const auto &metadata) {
    return metadata->info.get_cap() == capacity &&
      metadata->info.get_chunk_size() == chunk_size &&
      metadata->info.get_size() < capacity &&
      (!excluded_volumes ||
       excluded_volumes->find(metadata->volume_oid) ==
         excluded_volumes->end());
  };
  std::shared_lock lock(mutex_);
  if (preferred_volume) {
    // Prefer the Volume whose physical OID matches the new logical object.  In
    // particular, this lets a previously deleted first object reclaim its
    // original container without creating an OID collision.
    auto preferred = volumes_.find(*preferred_volume);
    if (preferred != volumes_.end() && reusable(preferred->second)) {
      return preferred->second;
    }
  }
  for (const auto &[oid, metadata] : volumes_) {
    if (reusable(metadata)) return metadata;
  }
  return nullptr;
}

bool VolumeCatalog::contains(const hobject_t &obj_oid) const {
  std::shared_lock lock(mutex_);
  return index_.find(obj_oid) != index_.end();
}

bool VolumeCatalog::contains_volume(const hobject_t &volume_oid) const {
  std::shared_lock lock(mutex_);
  return volumes_.find(volume_oid) != volumes_.end();
}

void VolumeCatalog::upsert(const hobject_t &volume_oid, const volume_t &info) {
  std::unique_lock lock(mutex_);
  // A Volume update is authoritative for all of its logical objects.  Remove
  // the old reverse mappings first so deleted chunks do not remain visible.
  remove_volume_locked(volume_oid);

  auto meta = std::make_shared<VolumeMeta>();
  meta->volume_oid = volume_oid;
  meta->info = info;
  volumes_[volume_oid] = meta;
  auto &objects = volume_objects_[volume_oid];
  for (const auto &entry : info.get_chunk_map()) {
    // If corrupted/stale snapshots map an object to two Volumes, the newest
    // authoritative update owns it and is detached from the previous Volume.
    remove_object_locked(entry.first);
    index_[entry.first] = meta;
    objects.insert(entry.first);
  }
}

void VolumeCatalog::remove_volume(const hobject_t &volume_oid) {
  std::unique_lock lock(mutex_);
  remove_volume_locked(volume_oid);
}

void VolumeCatalog::remove_volume_locked(const hobject_t &volume_oid) {
  // Caller holds mutex_ exclusively.  Only erase index_ entries that still
  // point at this Volume; a newer upsert may already own the same logical OID.
  auto volume = volume_objects_.find(volume_oid);
  if (volume != volume_objects_.end()) {
    for (const auto &object_oid : volume->second) {
      auto entry = index_.find(object_oid);
      if (entry != index_.end() && entry->second->volume_oid == volume_oid) {
        index_.erase(entry);
      }
    }
    volume_objects_.erase(volume);
  }
  volumes_.erase(volume_oid);
}

void VolumeCatalog::remove_object(const hobject_t &obj_oid) {
  std::unique_lock lock(mutex_);
  remove_object_locked(obj_oid);
}

void VolumeCatalog::remove_object_locked(const hobject_t &obj_oid) {
  auto entry = index_.find(obj_oid);
  if (entry == index_.end()) return;
  auto volume = volume_objects_.find(entry->second->volume_oid);
  if (volume != volume_objects_.end()) {
    const auto volume_oid = entry->second->volume_oid;
    volume->second.erase(obj_oid);
    if (volume->second.empty()) {
      // No logical object references this physical Volume anymore, so remove
      // it from the reuse index as well.
      volume_objects_.erase(volume);
      volumes_.erase(volume_oid);
    }
  }
  index_.erase(entry);
}

int VolumeCatalog::load_from_disk(ceph::buffer::list &encoded) {
  if (encoded.length() == 0) return -EINVAL;
  volume_t info;
  try {
    auto p = encoded.cbegin();
    info.decode(p);
  } catch (const ceph::buffer::error &) {
    return -EINVAL;
  }
  // volume_t's persisted v1 format predates pg_id.  Restore the owning PG
  // from the catalog context before this metadata can be reused for writes.
  info.set_spg(pgid_);
  if (!valid_metadata(info)) return -EINVAL;
  upsert(info.get_oid(), info);
  return 0;
}

int VolumeCatalog::replace_from_disk(
  std::vector<ceph::buffer::list> &encoded) {
  // Decode into a separate catalog so readers observe either the previous
  // complete snapshot or the replacement, never a partially loaded mixture.
  VolumeCatalog replacement(pgid_);
  int result = 0;
  for (auto &entry : encoded) {
    if (replacement.load_from_disk(entry) < 0) result = -EINVAL;
  }
  {
    std::unique_lock lock(mutex_);
    // Invalid entries are omitted, but all valid entries are published in one
    // short critical section.  The negative result lets the caller log damage.
    index_.swap(replacement.index_);
    volumes_.swap(replacement.volumes_);
    volume_objects_.swap(replacement.volume_objects_);
  }
  return result;
}

std::vector<hobject_t> VolumeCatalog::list_objects(
  const hobject_t &start, size_t limit,
  std::optional<hobject_t> &next) const {
  std::shared_lock lock(mutex_);
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

void VolumeCatalog::clear() {
  std::unique_lock lock(mutex_);
  index_.clear();
  volumes_.clear();
  volume_objects_.clear();
}

size_t VolumeCatalog::size() const {
  std::shared_lock lock(mutex_);
  return index_.size();
}

} // namespace ceph::aggregate_ec
