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

  Chunk(CephContext* _cct, uint8_t _seq, const spg_t& _pg_id, Volume* _vol): 
                        chunk_info(_seq, _pg_id), vol(_vol), cct(_cct) {  }

  /**
   * @brief 根据OpRequest初始化，计算填0部分的偏移
   *
   * @param op chunk对应的OpRequest
   * @param seq chunk在volume内的索引
   * @return int
   */
  chunk_t set_from_op(OpRequestRef _op, MOSDOp* _m, const uint8_t& seq, uint64_t chunk_size); 
  chunk_t get_chunk_info() { return chunk_info; }
  OpRequestRef get_req() { return op; }
  std::vector<OSDOp>& get_ops() { return ops; }

  bool is_empty() { return chunk_info.is_empty(); }
  bool is_valid() { return chunk_info.is_valid(); }
  bool is_invalid() { return chunk_info.is_invalid(); }

  void clear() {
    chunk_info.clear();
    op.reset();
    ops.clear();
  }

private:
  chunk_t chunk_info;

  // 指向volume的指针
  Volume* vol;

  OpRequestRef op;
  std::vector<OSDOp> ops;
  CephContext* cct;
};

#endif // !CEPH_AGGREGATECHUNK_H
