// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "Chunk.h"
#include "XAttr.h"

#include <utility>

namespace ceph::aggregate_ec {

namespace {
int rewrite_new_object_write(
  OSDOp &osd_op, uint8_t chunk_id, uint64_t chunk_size,
  uint64_t &data_length, unsigned &write_ops) {
  // A new logical object is materialized as one complete physical slot.  More
  // than one data write would require replaying compound file semantics while
  // the object has no catalog entry yet, so reject it explicitly.
  if (++write_ops > 1) return -EOPNOTSUPP;

  const uint64_t object_offset =
    osd_op.op.op == CEPH_OSD_OP_WRITE ? osd_op.op.extent.offset : 0;
  const uint64_t input_length = osd_op.indata.length();
  if (osd_op.op.extent.length != input_length) return -EINVAL;
  if (object_offset > chunk_size ||
      input_length > chunk_size - object_offset) {
    return -E2BIG;
  }

  data_length = object_offset + input_length;
  // Preserve a partial write's leading hole, then zero-fill through the end of
  // the slot so the aggregate request always writes a complete EC stripe unit.
  ceph::buffer::list padded;
  padded.append_zero(object_offset);
  padded.append(osd_op.indata);
  padded.append_zero(chunk_size - data_length);
  osd_op.indata = std::move(padded);
  osd_op.op.op = CEPH_OSD_OP_WRITE;
  osd_op.op.extent.offset = static_cast<uint64_t>(chunk_id) * chunk_size;
  osd_op.op.extent.length = chunk_size;
  return 0;
}

int rewrite_new_object_op(
  OSDOp &osd_op, const hobject_t &origin, uint8_t chunk_id,
  uint64_t chunk_size, uint64_t &data_length, unsigned &write_ops,
  bool &keep) {
  keep = true;
  switch (osd_op.op.op) {
  case CEPH_OSD_OP_WRITE:
  case CEPH_OSD_OP_WRITEFULL:
    return rewrite_new_object_write(
      osd_op, chunk_id, chunk_size, data_length, write_ops);
  case CEPH_OSD_OP_SETXATTR:
    return rewrite_xattr_op(osd_op, origin, true);
  case CEPH_OSD_OP_CREATE:
    // Creating the physical Volume itself satisfies logical CREATE; forwarding
    // CREATE for every chunk would repeatedly target the same container.
    keep = false;
    return 0;
  case CEPH_OSD_OP_DELETE:
    // A new logical object cannot safely delete its physical container in
    // the same compound request.
    return -EOPNOTSUPP;
  default:
    return 0;
  }
}
} // anonymous namespace

int Chunk::bind(uint8_t id, OpRequestRef op, uint64_t chunk_size) {
  auto *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  std::vector<OSDOp> translated_ops;
  uint64_t data_length = 0;
  unsigned write_ops = 0;

  // Build translated operations in temporary storage.  Chunk state is
  // published only after every sub-operation validates successfully.
  for (const auto &source : m->ops) {
    OSDOp translated = source;
    bool keep = true;
    int r = rewrite_new_object_op(
      translated, m->get_hobj(), id, chunk_size,
      data_length, write_ops, keep);
    if (r < 0) return r;
    if (!keep) continue;
    translated_ops.push_back(std::move(translated));
  }

  id_ = id;
  state_ = ChunkState::PENDING;
  origin_oid_ = m->get_hobj();
  op_ = std::move(op);
  data_offset_ = static_cast<uint64_t>(id) * chunk_size;
  data_length_ = data_length;
  mtime_ = m->get_mtime();
  // Some callers leave mtime unset; STAT still needs a stable logical value.
  if (mtime_ == utime_t()) mtime_ = ceph_clock_now();
  ops_ = std::move(translated_ops);
  return 0;
}

} // namespace ceph::aggregate_ec
