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

#include "AggregateBuffer.h"
#include "AggregateChunk.h"
#include <sys/syscall.h>

class Volume {

  volume_t volume_info;

public:
  Volume(uint64_t _cap, uint64_t _chunk_size, spg_t _pg_id, AggregateBuffer* _buffer);

  ~Volume();
  
  bool full() { return volume_info.full(); }

  spg_t get_spg() { return volume_info.get_spg(); }
  uint64_t get_cap() { return volume_info.get_cap(); }
  volume_t get_volume_info() { return volume_info; }

  object_info_t find_object(hobject_t soid);
  /**
   * @brief chunk加进volume
   *
   * @param chunk
   * @return int
   */
  int add_chunk(OpRequestRef op);

  void remove_chunk(hobject_t soid);
  
  void clear(); 

  void flush();

  /*
   * @return free chunk index
   */
  int _find_free_chunk();

  void dump_op();
  // chunk

private:
  uint64_t chunk_size;

  std::vector<bool> bitmap;
  // chunk的顺序要与volume_info中chunk_set中chunk的顺序一致
  std::vector<Chunk*> chunks;
  // TODO: EC块缓存
  // std::vector<ECChunk*> ec_chunks;

  AggregateBuffer* volume_buffer;
  OpRequestRef vol_op;
};


#endif // !CEPH_AGGREGATEVOLUME_H