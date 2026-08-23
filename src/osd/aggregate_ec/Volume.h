// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include "Chunk.h"
#include "osd/osd_types.h"

#include <cstdint>
#include <optional>
#include <vector>

namespace ceph::aggregate_ec {

class Volume {
public:
  Volume(uint32_t capacity, uint64_t chunk_size, spg_t pgid);
  explicit Volume(const volume_t &metadata);

  bool full() const { return occupied_ >= capacity_; }
  bool empty() const { return buffered_ == 0; }
  uint32_t capacity() const { return capacity_; }
  uint32_t occupied() const { return occupied_; }
  uint32_t buffered() const { return buffered_; }
  uint64_t chunk_size() const { return chunk_size_; }
  const spg_t& pgid() const { return pgid_; }

  bool contains(const hobject_t &oid) const;
  int add(OpRequestRef op);
  void mark_written(uint8_t id);
  void mark_failed(uint8_t id);

  void bind_oid(const hobject_t &oid) { oid_ = oid; }
  const hobject_t& oid() const { return oid_; }
  const Chunk& chunk(uint8_t id) const { return chunks_[id]; }
  Chunk& chunk(uint8_t id) { return chunks_[id]; }

  MOSDOp* generate_write_op() const;
  volume_t metadata() const;

private:
  int find_free_slot() const;

  uint32_t capacity_ = 0;
  // occupied_ includes persisted and buffered chunks; buffered_ counts only
  // requests owned by the current aggregate batch.
  uint32_t occupied_ = 0;
  uint32_t buffered_ = 0;
  uint64_t chunk_size_ = 0;
  spg_t pgid_;
  hobject_t oid_;
  // Present only when this batch is filling holes in a persisted Volume.
  std::optional<volume_t> base_metadata_;
  std::vector<Chunk> chunks_;
};

} // namespace ceph::aggregate_ec
