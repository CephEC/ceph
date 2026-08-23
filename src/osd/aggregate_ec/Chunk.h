// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include "messages/MOSDOp.h"
#include "osd/OpRequest.h"

#include <cstdint>
#include <vector>

namespace ceph::aggregate_ec {

enum class ChunkState : uint8_t { EMPTY, PENDING, WRITTEN, FAILED };

class Chunk {
public:
  int bind(uint8_t id, OpRequestRef op, uint64_t chunk_size);

  void mark_written() { state_ = ChunkState::WRITTEN; }
  void mark_failed() { state_ = ChunkState::FAILED; }

  bool empty() const { return state_ == ChunkState::EMPTY; }
  bool pending() const { return state_ == ChunkState::PENDING; }
  uint8_t id() const { return id_; }
  uint64_t data_length() const { return data_length_; }
  uint64_t data_offset() const { return data_offset_; }
  utime_t mtime() const { return mtime_; }
  const hobject_t& origin_oid() const { return origin_oid_; }
  const OpRequestRef& request() const { return op_; }
  const std::vector<OSDOp>& ops() const { return ops_; }

private:
  uint8_t id_ = 0;
  ChunkState state_ = ChunkState::EMPTY;
  // Keep both the client request (for split replies/requeue) and its rewritten
  // physical operations (for the synthetic Volume request).
  hobject_t origin_oid_;
  OpRequestRef op_;
  uint64_t data_offset_ = 0;
  uint64_t data_length_ = 0;
  utime_t mtime_;
  std::vector<OSDOp> ops_;
};

} // namespace ceph::aggregate_ec
