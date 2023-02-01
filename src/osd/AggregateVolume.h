#ifndef CEPH_AGGREGATEVOLUME_H
#define CEPH_AGGREGATEVOLUME_H

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

#include "AggregateChunk.h"
#include <sys/syscall.h>

class Chunk;
class Volume {

  volume_t volume_info;

public:
  Volume(uint32_t _cap, uint32_t _chunk_size, const spg_t& _pg_id);

  ~Volume();
  
  bool full() { return volume_info.full(); }
  bool empty() { return volume_info.empty(); }
  void set_cap(uint64_t _cap) { volume_info.set_cap(_cap); }

  spg_t get_spg() const { return volume_info.get_spg(); }
  uint32_t get_cap() const { return volume_info.get_cap(); }
  volume_t get_volume_info() const { return volume_info; }

  object_info_t find_object(hobject_t soid);

  void init(uint64_t _cap, uint64_t _chunk_size);
  /**
   * @brief chunk加进volume
   *
   * @param chunk
   * @return int
   */
  int add_chunk(OpRequestRef op, MOSDOp* m);

  void remove_chunk(hobject_t soid);
  
  void clear(); 

  void flush();

  /**
   * @brief 生成volume的op
   *
   * @return OpRequestRef
   */
  MOSDOp* generate_op();

  MOSDOp* _prepare_volume_op(MOSDOp *m);

  /*
   * @return free chunk index
   */
  uint32_t _find_free_chunk();

  /**
   * @brief 拼接数据
  */
  template<typename V>
  void _append_data(MOSDOp* d, MOSDOp* s)
  {
    V& dops = d->ops;
    V& sops = s->ops;
    // s中ops全部挂到d的ops后面
    dops.insert(dops.end(), sops.begin(), sops.end());
}

  void dump_op();
  // chunk

private:
  uint64_t chunk_size;

  std::vector<bool> bitmap;
  // chunk的顺序要与volume_info中chunk_set中chunk的顺序一致
  std::vector<Chunk*> chunks;
  // TODO: EC块缓存
  // std::vector<ECChunk*> ec_chunks;

  OpRequestRef vol_op;
};


#endif // !CEPH_AGGREGATEVOLUME_H
