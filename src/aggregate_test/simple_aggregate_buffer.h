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

using std::cout;

class volume_t;
class SimpleVolume;
class SimpleAggregateBuffer;

class SimpleAggregateBuffer
{
  friend class SimpleVolume;
  friend class FlushContext;
public:
  SimpleAggregateBuffer() :volume_buffer(nullptr) {
   
 };

  ~SimpleAggregateBuffer() { flush_timer->shutdown(); }

  void init(CephContext* _cct) { 
    cct = _cct; 
    flush_timer = new SafeTimer(cct, timer_lock);
    flush_timer->init();  
   
  }


  /**
   * @brief 对象写入buffer，在add_chunk中执行对象元数据的创建操作
   *
   * @param op
   * @param osdmap
   * @return int
   */
   int write(MOSDOp* op, const OSDMap &osdmap);
   // int write_list(MOSDOp*, const OSDMap &osdmap);
  /**
   * @brief 预留函数，用于根据请求到来的历史信息预测此时的IO模式，判断是否提前计算EC并缓存
   * 调用位置：volume flush函数中
   *
   * @return bool
   */
   bool may_batch_writing() { return false; };
   bool is_flush() { return is_flushing; }
private:
  ceph::mutex flush_lock = ceph::make_mutex("SimpleAggregateBuffer::flush_lock");
  ceph::mutex timer_lock = ceph::make_mutex("SimpleAggregateBuffer::timer_lock");
  ceph::condition_variable flush_cond;
  Context* flush_callback;
  SafeTimer* flush_timer = NULL;
  bool is_flushing = false;
 
  int flush();
  std::list<MOSDOp*> waiting_for_aggregate;
private:
  // PrimaryLogPG* pg;
  CephContext* cct;

  //std::shared_ptr<SimpleVolume*> volume_buffer;
  SimpleVolume* volume_buffer;

  // 属于PG的VolumeMeta
  std::vector<volume_t> volume_meta_cache;

  // 保存非空闲volume的meta
  std::list<volume_t/*, bufferlist*/> volume_not_full;

  // TODO: 缓存EC块
  //   std::map<volume_t, bufferlist*> ec_buffer;
};

class FlushContext
  : public Context
{
  SimpleAggregateBuffer* buffer;
public:
  explicit FlushContext(SimpleAggregateBuffer *_buffer): buffer(_buffer) {}
  void finish(int r) { 
    printf("volume timeout, flush\n");
    printf("thread tid is %d, pid is %d\n", gettid(), getpid());
    buffer->flush();
    printf("finish flush\n"); 
  }
};



#endif // !CEPH_SIMPLEAGGREGATEBUFFER_H
