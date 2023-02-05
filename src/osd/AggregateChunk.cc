/*
 * AggregateChunk.cc
 * Copyright (C) 2023 root <root@ce1>
 *
 * Distributed under terms of the MIT license.
 */

#include "AggregateChunk.h"


chunk_t Chunk::set_from_op(OpRequestRef _op, MOSDOp* _m, const uint8_t& seq) {
  
    const hobject_t& oid = _m->get_hobj();
    const spg_t pg_id = _m->get_spg();
    if (pg_id.pgid != chunk_info.get_spg().pgid) {
      return chunk_t();
    }
      
    uint64_t data_len = _m->get_data_len();
    // 检查data_len是否大于配置文件中的chunk_size 
    if(data_len > chunk_info.get_chunk_size() << 20) {
      return chunk_t();
    }
      
    chunk_info.set_from_op(seq, data_len, oid);
    
    // 根据元数据填0
    filled_with_zero(data_len, chunk_info.get_chunk_size());

    this->op = _op;
    this->m_op = _m;

    return chunk_info;
  }


