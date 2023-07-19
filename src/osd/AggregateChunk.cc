/*
 * AggregateChunk.cc
 * Copyright (C) 2023 root <root@ce1>
 *
 * Distributed under terms of the MIT license.
 */

#include "AggregateChunk.h"
#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

using std::ostream;
using std::ostringstream;

static ostream& _prefix(std::ostream *_dout, const Chunk *buf) {
  return *_dout << "aggregate chunk. ******* "; 
}

chunk_t Chunk::set_from_op(OpRequestRef _op, MOSDOp* _m, const uint8_t& seq, uint64_t chunk_size) {

  const hobject_t& oid = _m->get_hobj();
  const spg_t pg_id = _m->get_spg();
  if (pg_id.pgid != chunk_info.get_spg().pgid) {
    dout(4) << "set_from_op failed, request_pg_id = " << pg_id.pgid
      << " chunk_pg_id = " << pg_id.pgid << dendl;
    return chunk_t();
  }

  // 将对象填充到oid中
  chunk_info.set_from_op(seq, 0, oid);

  dout(20) << __func__ << ": ops.size=" << _m->ops.size() << dendl;

  this->op = _op;
  for (OSDOp &osd_op : _m->ops) {
    if (osd_op.indata.length() > chunk_size) {
      dout(4) << "set_from_op failed, data_len = " << osd_op.indata.length()
        << " max_chunk_size = " << chunk_size << dendl;
      clear();
      return chunk_t();
    }
    if (osd_op.op.op == CEPH_OSD_OP_WRITEFULL ||
        osd_op.op.op == CEPH_OSD_OP_WRITE) {
      // 将WRITEFULL改成WRITE，同时调整offset和length
      osd_op.op.op = CEPH_OSD_OP_WRITE;
      
      chunk_info.set_from_op(seq, osd_op.indata.length(), oid);
      dout(20) << __func__ << ": bufferlist before zerofilling " << osd_op.indata << dendl;
      size_t zero_to_fill = chunk_size - osd_op.indata.length();
      osd_op.indata.append_zero(zero_to_fill);
      dout(20) << __func__ << ": bufferlist after zerofilling " << osd_op.indata << dendl;

      osd_op.op.extent.offset = chunk_info.get_seq() * chunk_size;
      osd_op.op.extent.length = chunk_size;
    } else if (osd_op.op.op == CEPH_OSD_OP_SETXATTR) {
      std::string key;
      auto bp = osd_op.indata.cbegin();
      bp.copy(osd_op.op.xattr.name_len, key);

      bufferlist value;
      bp.copy(osd_op.op.xattr.value_len, value);

      key = oid.oid.name + "_" + key;
      osd_op.op.xattr.name_len = key.size();

      osd_op.indata.clear();
      osd_op.indata.append(key);
      osd_op.indata.append(value);
    } else if (osd_op.op.op == CEPH_OSD_OP_CREATE) {
      continue;
    }
    ops.push_back(osd_op);
  }
  dout(4) << " set ops, ops = " << ops << dendl;
  return chunk_info;
}


