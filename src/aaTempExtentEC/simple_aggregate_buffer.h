/**
 * @brief 试作型聚合缓存
 * 
 */
#ifndef CEPH_SIMPLEAGGREGATEBUFFER_H
#define CEPH_SIMPLEAGGREGATEBUFFER_H

#include "include/types.h"
#include "messages/MOSDOp.h"

#include "common/ceph_context.h"
#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/Thread.h"

#include "simple_volume.h"

class SimpleAggregateBuffer
{
  constexpr int CHUNK_FAILED = -1;
  constexpr int VOLUME_FAILED = -2;

public:
  void init_with_cct(CephContext* _cct) { cct = _cct; }

/**
 * @brief 对象写入buffer，在add_chunk中执行对象元数据的创建操作
 * 
 * @param op 
 * @param osdmap 
 * @return int 
 */
  int write(MOSDOp* op, const OSDMap &osdmap);
  int read();
  int flush();

  SimpleAggregateBuffer(CephContext *_cct) : cct(_cct) { };
  SimpleAggregateBuffer() : cct(nullptr) { };

  bool may_batch_writing() { return false; };

private:
  // flush thread
    // ceph::condition_variable flusher_cond;
  // bool flusher_stop;
  // void flusher_entry();
  // class FlusherThread : public Thread {
  //   SimpleAggregateBuffer* buffer;
  // public:
  //   explicit FlusherThread(ObjectCacher *o) : oc(o) {}
  //   void *entry() override {
  //     oc->flusher_entry();
  //     return 0;
  //   }
  // } flush_thread;

  // Finisher finisher;

  friend class SimpleVolume;

  ceph::mutex flush_list_lock = ceph::make_mutex("AggregateBuffer::flush_list_lock");
  std::list<Volume*> pending_to_flush;

private:
  // PrimaryLogPG* pg;

  //bool is_batch = true;

  // flush时对整个volume list加锁肯定是不合理的 
  std::list<SimpleVolume*> volumes;

  // VolumeMeta
  std::vector<volume_t>* volume_meta_cache;
  // 又或者保存非空闲volume的freelist
  std::list<volume_t> volume_not_full; 
}

#endif // !CEPH_SIMPLEAGGREGATEBUFFER_H