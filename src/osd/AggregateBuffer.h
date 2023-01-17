/**
 * @brief 聚合缓存
 *
 */
#ifndef CEPH_AGGREGATEBUFFER_H
#define CEPH_AGGREGATEBUFFER_H


#include "include/types.h"
#include "messages/MOSDOp.h"
#include "PrimaryLogPG.h"
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

class AggregateBuffer
{
public:

  AggregateBuffer(CephContext* _cct, PrimaryLogPG* _pg) :cct(_cct), pg(_pg) {
    // init volume
    volume_buffer = new Volume(_cct->_conf.get_val<int>("osd_aggregate_buffer_capacity"),
                                _cct->_conf.get_val<Option::size_t>("osd_aggregate_buffer_chunk_size"),
                                pg->get_pgid(),
                                this);

    // init timer
    flush_time_out = _cct->_conf.get_val<std::chrono::seconds>("osd_aggregate_buffer_flush_timeout");
    flush_timer = new SafeTimer(cct, timer_lock);
    flush_timer->init(); 
 };

  ~AggregateBuffer() { 
    flush_timer->shutdown(); 
    delete volume_buffer;
  }


  /**
   * @brief 对象写入buffer，在add_chunk中执行对象元数据的创建操作
   *
   * @param op
   * @param osdmap
   * @return int
   */
   int write(OpRequestRef op);
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
  ceph::mutex flush_lock = ceph::make_mutex("AggregateBuffer::flush_lock");
  ceph::mutex timer_lock = ceph::make_mutex("AggregateBuffer::timer_lock");
  ceph::condition_variable flush_cond;
  Context* flush_callback;
  SafeTimer* flush_timer = NULL;
  bool is_flushing = false;
  std::chrono::seconds flush_time_out;

  class FlushContext: public Context
  {
    AggregateBuffer* buffer;
  public:
    explicit FlushContext(AggregateBuffer *_buffer): buffer(_buffer) {}
    void finish(int r) { 
      dout(4) << __func__ << "volume timeout, flush. "<< dendl;
      buffer->flush();
      dout(4) << "finish flush" << dendl;
    }
  };
 
  int flush();

  std::list<OpRequestRef> waiting_for_aggregate;
  std::list<OpRequestRef> waiting_for_reply;
  

private:
  CephContext* cct;
  PrimaryLogPG *pg;

  Volume* volume_buffer;

  // 属于PG的VolumeMeta
  std::vector<volume_t> volume_meta_cache;

  // 保存非空闲volume的meta
  std::list<volume_t/*, bufferlist*/> volume_not_full;

  // TODO: 缓存EC块
  // std::map<volume_t, bufferlist> ec_buffer;
};


#endif // !CEPH_AGGREGATEBUFFER_H
