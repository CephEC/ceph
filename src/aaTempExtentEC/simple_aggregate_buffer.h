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
  int write(MOSDOp* op, const OSDMap &osdmap);
  int read();
  int flush();

  SimpleAggregateBuffer(/*CephContext *cct_*/) { };

  bool may_batch_writing() { return false; };

private:
  ceph::condition_variable flusher_cond;
  bool flusher_stop;
  void flusher_entry();
  class FlusherThread : public Thread {
    SimpleAggregateBuffer* buffer;
  public:
    explicit FlusherThread(ObjectCacher *o) : oc(o) {}
    void *entry() override {
      oc->flusher_entry();
      return 0;
    }
  } flusher_thread;

  Finisher finisher;

private:
  bool is_batch = true;

  // flush时对整个volume list加锁肯定是不合理的
  std::list<SimpleVolume*> volumes;
}

#endif // !CEPH_SIMPLEAGGREGATEBUFFER_H