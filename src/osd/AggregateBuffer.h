/**
 * @brief 聚合缓存
 *
 */
#ifndef CEPH_AGGREGATEBUFFER_H
#define CEPH_AGGREGATEBUFFER_H


#include "include/types.h"
#include "messages/MOSDOp.h"
#include "PrimaryLogPG.h"
#include "AggregateVolume.h"
#include "osd_types.h"

#include "OpRequest.h"
#include "OSD.h"
#include "common/ceph_context.h"
#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/Thread.h"

/* return code for aggregate buffer */
#define AGGREGATE_FAILED -1
#define AGGREGATE_SUCCESS 1
#define AGGREGATE_PENDING_OP 2
#define AGGREGATE_PENDING_REPLY 4

class Volume;
class PrimaryLogPG;

static constexpr uint64_t default_capacity = 3;
static constexpr uint64_t default_chunk_size = 128;
static constexpr double default_time_out = 200;

class AggregateBuffer
{
  Volume volume_buffer;

public:

  AggregateBuffer(CephContext* _cct, const spg_t& _pgid, PrimaryLogPG* _pg);

  ~AggregateBuffer() { 
    flush_timer->shutdown(); 
  }

  /**
   * @brief 初始化buffer(lazy)
   */
  void init(uint64_t _volume_cap, uint64_t _chunk_size, double _time_out);
  bool is_initialized() { return initialized; }


  /**
   * @brief 对象写入buffer，在add_chunk中执行对象元数据的创建操作
   *
   * @param op
   * @return int
   */
   int write(OpRequestRef op, MOSDOp* m);
   
   /**
    * @brief 加载元数据，未满volume加入volume_not_full，已满volume加入volume_meta_cache
    * 
    * 
   */
   void load_volume_attr();

  //  /**
  //   * @brief 调用osd的enqueue函数重排队op
  //   * 
  //  */
  // void requeue_op(OpRequestRef op);

  /**
   * @brief 把waiting_for_reply
   * 
  */
  void send_reply(Message* reply);

  
   /**
   * @brief 预留函数，用于根据请求到来的历史信息预测此时的IO模式，判断是否提前计算EC并缓存
   * 调用位置：volume flush函数中
   *
   * @return bool
   */
   bool may_batch_writing() { return false; };
   bool is_flush() { return is_flushing; }

private:
  ceph::mutex flush_lock = ceph::make_mutex("AggregateBuffer::flush_lock");
  ceph::mutex timer_lock = ceph::make_mutex("AggregateBuffer::timer_lock");
  ceph::condition_variable flush_cond;
  Context* flush_callback;
  SafeTimer* flush_timer = NULL;
  bool is_flushing = false;
  double flush_time_out;

  class FlushContext: public Context
  {
    AggregateBuffer* buffer;
  public:
    explicit FlushContext(AggregateBuffer *_buffer): buffer(_buffer) {}
    void finish(int r) { 
      buffer->flush();
    }
  };
 
  int flush();

  std::list<OpRequestRef> waiting_for_aggregate;
  std::list<OpRequestRef> waiting_for_reply;
  

private:
  CephContext* cct;
  PrimaryLogPG *pg;

  bool initialized = false;

  // 属于PG的VolumeMeta
  std::vector<volume_t> volume_meta_cache;
  // std::unordered_map<hobject_t, volume_t> volume_meta_cache;

  // 保存非空闲volume的meta
  std::list<volume_t> volume_not_full;

  // TODO: 缓存EC块
  // std::map<volume_t, bufferlist> ec_buffer;
};


#endif // !CEPH_AGGREGATEBUFFER_H
