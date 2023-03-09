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

using std::cout;
using std::ostream;
using std::ostringstream;

static ostream& _prefix(std::ostream *_dout, const Chunk *buf) {
  return *_dout << "aggregate chunk. ******* "; 
}

chunk_t Chunk::set_from_op(OpRequestRef _op, MOSDOp* _m, const uint8_t& seq) {
  
    const hobject_t& oid = _m->get_hobj();
    const spg_t pg_id = _m->get_spg();
    if (pg_id.pgid != chunk_info.get_spg().pgid) {
      dout(4) << "set_from_op failed, request_pg_id = " << pg_id.pgid
        << " chunk_pg_id = " << pg_id.pgid << dendl;
      return chunk_t();
    }
      
    uint64_t data_len = _m->get_data_len();
    // 检查data_len是否大于配置文件中的chunk_size 
    if(data_len > chunk_info.get_chunk_size() << 20) {
      dout(4) << "set_from_op failed, data_len = " << data_len
        << " max_chunk_size = " << (chunk_info.get_chunk_size() << 20) << dendl;
      return chunk_t();
    }
      
    chunk_info.set_from_op(seq, data_len, oid);
    
    // 根据元数据填0
    filled_with_zero(data_len, chunk_info.get_chunk_size());

    this->op = _op;
    for (OSDOp osd_op : _m->ops) {
      if (osd_op.op.op == CEPH_OSD_OP_WRITEFULL) {
        // 将WRITEFULL改成WRITE，同时调整offset和length
        osd_op.op.op = CEPH_OSD_OP_WRITE;
        osd_op.op.extent.offset = chunk_info.get_seq() * chunk_info.get_chunk_size();
        osd_op.op.extent.length = chunk_info.get_chunk_size();
      }
      ops.push_back(osd_op);
    }
    dout(4) << " set ops, ops = " << ops << dendl;
    return chunk_info;
  }


