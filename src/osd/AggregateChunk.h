#ifndef CEPH_AGGREGATECHUNK_H
#define CEPH_AGGREGATECHUNK_H

#include "include/types.h"
#include "include/Context.h"
#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/hobject.h"

#include "osd/osd_types.h"
#include "osd/osd_internal_types.h"

#include "messages/MOSDOp.h"
#include "osd/OpRequest.h"

#include <sys/syscall.h>

#include "AggregateVolume.h"

class Volume;
/**
 * Chunk - 管理OpRequest
 *
 *
 */
class Chunk {
public:

  Chunk(uint8_t _seq, const spg_t& _pg_id, uint64_t _size, Volume* _vol): 
                        chunk_info(chunk_t(_seq, _pg_id, _size)), vol(_vol) {  }

  /**
   * @brief 根据OpRequest初始化，计算填0部分的偏移
   *
   * @param op chunk对应的OpRequest
   * @param seq chunk在volume内的索引
   * @return int
   */
  chunk_t set_from_op(OpRequestRef _op, MOSDOp* _m, const uint8_t& seq) {
  
    const hobject_t& oid = _m->get_hobj();
    const spg_t pg_id = _m->get_spg();
    if (pg_id.pgid != chunk_info.get_spg().pgid) {
      return chunk_t();
    }
      
    uint64_t data_len = _m->get_data_len();
    // 检查data_len是否大于配置文件中的chunk_size 
    if(data_len > chunk_info.get_chunk_size()) {
      return chunk_t();
    }
      
    chunk_info.set_from_op(seq, data_len, oid);
    
    // 根据元数据填0
    filled_with_zero(data_len, chunk_info.get_chunk_size());

    this->op = _op;
    this->m_op = _m;

    return chunk_info;
  }

  chunk_t get_chunk_info() { return chunk_info; }
  uint64_t get_chunk_size() { return chunk_info.get_chunk_size(); }
  OpRequestRef get_req() { return op; }
  MOSDOp* get_nonconst_message() { return m_op }

  bool is_empty() { return chunk_info.is_empty(); }
  bool is_valid() { return chunk_info.is_valid(); }
  bool is_invalid() { return chunk_info.is_invalid(); }

  void filled_with_zero(uint64_t _data_len, uint64_t _chunk_size) {
    // TODO: chunk填充0
    // ops里面每个op的indata填到128M
  }

  void clear() { 
    chunk_info.clear();
    // TODO: 处理request 
  }

private:
  chunk_t chunk_info;

  // 指向volume的指针
  Volume* vol;

  OpRequestRef op;
  MOSDOp* m_op;
};

#endif // !CEPH_AGGREGATECHUNK_H
