// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "Volume.h"

#include "include/ceph_assert.h"

namespace ceph::aggregate_ec {

Volume::Volume(uint32_t capacity, uint64_t chunk_size, spg_t pgid)
  : capacity_(capacity), chunk_size_(chunk_size), pgid_(std::move(pgid)) {
  ceph_assert(capacity > 0 && chunk_size > 0);
  chunks_.resize(capacity);
}

Volume::Volume(const volume_t &metadata)
  : capacity_(metadata.get_cap()), occupied_(metadata.get_size()),
    chunk_size_(metadata.get_chunk_size()), pgid_(metadata.get_spg()),
    oid_(metadata.get_oid()), base_metadata_(metadata) {
  // This constructor reopens a persisted non-full Volume.  occupied_ includes
  // existing chunks, while buffered_ remains zero until this batch adds data.
  ceph_assert(capacity_ > 0 && chunk_size_ > 0 && occupied_ < capacity_);
  chunks_.resize(capacity_);
}

int Volume::find_free_slot() const {
  // chunks_ describes only this batch.  The persisted bitmap protects slots
  // already owned by objects from earlier aggregate commits.
  const auto *bitmap = base_metadata_
    ? &base_metadata_->get_chunk_bitmap()
    : nullptr;
  for (uint32_t i = 0; i < capacity_; ++i) {
    if (chunks_[i].empty() && (!bitmap || !(*bitmap)[i])) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

bool Volume::contains(const hobject_t &oid) const {
  // Check both persisted metadata and this batch so duplicate logical requests
  // cannot be assigned two slots before the first one commits.
  if (base_metadata_ &&
      base_metadata_->get_chunk_map().find(oid) !=
        base_metadata_->get_chunk_map().end()) {
    return true;
  }
  for (const auto &chunk : chunks_) {
    if (!chunk.empty() && chunk.origin_oid() == oid) return true;
  }
  return false;
}

int Volume::add(OpRequestRef op) {
  auto *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  if (contains(m->get_hobj())) return -EEXIST;
  int slot = find_free_slot();
  if (slot < 0) return -ENOSPC;
  int r = chunks_[slot].bind(static_cast<uint8_t>(slot), std::move(op), chunk_size_);
  if (r < 0) return r;
  // occupied_ controls fullness across persisted + buffered chunks;
  // buffered_ controls whether this batch has anything to flush.
  ++occupied_;
  ++buffered_;
  return slot;
}

void Volume::mark_written(uint8_t id) {
  ceph_assert(id < capacity_ && chunks_[id].pending());
  chunks_[id].mark_written();
}

void Volume::mark_failed(uint8_t id) {
  ceph_assert(id < capacity_ && chunks_[id].pending());
  chunks_[id].mark_failed();
}

MOSDOp* Volume::generate_write_op() const {
  // Use the first buffered client request as the transport template so epoch,
  // snap context, priority, features, and connection remain valid.
  const MOSDOp *seed = nullptr;
  for (const auto &chunk : chunks_) {
    if (!chunk.empty()) {
      seed = static_cast<const MOSDOp*>(chunk.request()->get_req());
      break;
    }
  }
  if (!seed) return nullptr;

  auto *volume_op = new MOSDOp(
    seed->get_client_inc(), seed->get_tid(), oid_, pgid_,
    seed->get_map_epoch(),
    seed->get_flags() | CEPH_OSD_FLAG_AGGREGATE,
    seed->get_features());
  volume_op->set_snapid(seed->get_snapid());
  volume_op->set_snap_seq(seed->get_snap_seq());
  volume_op->set_snaps(seed->get_snaps());
  volume_op->set_mtime(seed->get_mtime());
  volume_op->set_retry_attempt(seed->get_retry_attempt());
  if (seed->get_priority()) volume_op->set_priority(seed->get_priority());
  if (seed->get_reqid() != osd_reqid_t()) volume_op->set_reqid(seed->get_reqid());
  volume_op->set_header(seed->get_header());
  volume_op->set_footer(seed->get_footer());
  volume_op->set_connection(seed->get_connection());

  for (const auto &chunk : chunks_) {
    // Operations are emitted in slot order, matching their physical offsets.
    volume_op->ops.insert(
      volume_op->ops.end(), chunk.ops().begin(), chunk.ops().end());
  }
  auto info = metadata();
  // Persist metadata last so a successful transaction always describes all
  // chunk writes carried by this synthetic request.
  volume_op->ops.push_back(info.generate_write_meta_op());
  volume_op->encode_payload(volume_op->get_features());
  return volume_op;
}

volume_t Volume::metadata() const {
  // Start from persisted metadata when filling holes; a new Volume starts from
  // an empty descriptor.  Only chunks buffered in this batch are merged below.
  volume_t info = base_metadata_.value_or(
    volume_t(oid_, capacity_, pgid_, chunk_size_));
  for (const auto &chunk : chunks_) {
    if (chunk.empty()) continue;
    chunk_t meta(chunk.id(), pgid_);
    meta.set_from_op(chunk.id(), chunk.data_length(), chunk.origin_oid());
    meta.set_mtime(chunk.mtime());
    info.add_chunk(chunk.origin_oid(), meta);
  }
  return info;
}

} // namespace ceph::aggregate_ec
