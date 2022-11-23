/**
 * @brief 试作型聚合缓存
 * 
 */
#ifndef CEPH_SIMPLEAGGREGATECACHE_H
#define CEPH_SIMPLEAGGREGATECACHE_H

#include "include/types.h"
#include "messages/MOSDOp.h"

#include "common/ceph_context.h"
#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/Thread.h"

class SimpleAggregationCache
{
public:
  int write(MOSDOp* op) {
    // is_empty
    // volume.add
  }
  int read();
  int flush();

  SimpleAggregationCache(CephContext *cct_)


  static SimpleAggregationCache& get_instance() {
    static SimpleAggregationCache instance;
    return instance;
  }

private:
  ceph::condition_variable flusher_cond;
  bool flusher_stop;
  void flusher_entry();
  class FlusherThread : public Thread {
    SimpleAggregationCache* cache;
  public:
    explicit FlusherThread(ObjectCacher *o) : oc(o) {}
    void *entry() override {
      oc->flusher_entry();
      return 0;
    }
  } flusher_thread;

  Finisher finisher;

private:
  std::vector<Volume*> volumes;
}

#endif // !CEPH_SIMPLEAGGREGATECACHE_H