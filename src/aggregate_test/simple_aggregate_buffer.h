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

class SimpleAggregateBuffer
{
public:
  SimpleAggregateBuffer() : flush_stop(false), flush_thread(this), volume_buffer(nullptr) { };

  /**
   * @brief 对象写入buffer，在add_chunk中执行对象元数据的创建操作
   *
   * @param op
   * @param osdmap
   * @return int
   */
   int write(MOSDOp* op/*, const OSDMap &osdmap*/);

  /**
   * @brief 预留函数，用于根据请求到来的历史信息预测此时的IO模式
   *
   * @return bool
   */
   bool may_batch_writing() { return false; };

  /**
   * @brief 开启flush线程
   *
   */
   void start() {
      flush_thread.create("volume_flusher");
      cout << "create flush thread." << std::endl;
   }
  /**
   * @brief 关闭flush线程
   *       *
   */
  void stop() {
    ceph_assert(flush_thread.is_started());
    flush_list_lock.lock();
    flush_stop = true;
    flush_cond.notify_all();
    flush_list_lock.unlock();
    flush_thread.join();
    cout << "stop flush thread" << std::endl;
  }

private:
  // flush thread
  ceph::condition_variable flush_cond;
  bool flush_stop;
  void flush_entry();
  class FlushThread : public Thread {
    SimpleAggregateBuffer* buffer;
  public:
    explicit FlushThread(SimpleAggregateBuffer* _buffer) : buffer(_buffer) {}
    void *entry() override {
      buffer->flush_entry();
      return 0;
    }
  } flush_thread;

  ceph::mutex flush_list_lock = ceph::make_mutex("SimpleAggregateBuffer::flush_list_lock");
  std::list<SimpleVolume*> pending_to_flush;

private:
  // PrimaryLogPG* pg;

  //std::shared_ptr<SimpleVolume*> volume_buffer;
  SimpleVolume* volume_buffer;

  // 属于PG的VolumeMeta
  std::vector<volume_t> volume_meta_cache;

  // 保存非空闲volume的meta
  std::list<volume_t/*, bufferlist*/> volume_not_full;

  // TODO: 缓存EC块
  //   std::map<volume_t, bufferlist*> ec_buffer;
};


#endif // !CEPH_SIMPLEAGGREGATEBUFFER_H
