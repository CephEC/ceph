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
#include "ExtentCache.h"

#include "OpRequest.h"
#include "OSD.h"
#include "common/ceph_context.h"
#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/Thread.h"
#include "osd/ClassHandler.h"

/* return code for aggregate buffer */
#define AGGREGATE_FAILED -1
#define NOT_TARGET 0
#define AGGREGATE_SUCCESS 1
#define AGGREGATE_PENDING_OP 2
#define AGGREGATE_PENDING_REPLY 4
#define AGGREGATE_CONTINUE 8

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
    flush_timer.shutdown(); 
    for (auto &cls_ctx : cls_ctx_map) {
      delete cls_ctx.second;
    }
  }

  /**
   * @brief 初始化buffer(lazy)
   */
  void init(uint64_t _volume_cap, uint64_t _chunk_size, bool _flush_timer_enabled, double _time_out);
  bool is_initialized() { return initialized; }

  /**
   * @brief volume_buffer需要与一个volume_info绑定
   * 如果volume_not_full非空，那么从中取出一个未满的volume绑定到volume_buffer
   * 如果volume_not_full为空，那么创建一个新的volume绑定到volume_buffer(新volume的oid直接取当前写入的RGW对象的Oid)
   */
  void bind(const hobject_t &first_oid);
  bool is_bind_volume() { return is_bind; }

  void finish_cls(const hobject_t& soid) {
    if (cls_ctx_map.find(soid) == cls_ctx_map.end())
      return;
    cls_ctx_map.erase(soid);
  }

  ClsParmContext* get_cls_ctx(const hobject_t& soid) {
    if (cls_ctx_map.find(soid) == cls_ctx_map.end())
      return nullptr;
    return cls_ctx_map[soid];
  }

  Volume& get_active_volume() { return volume_buffer; }

  volume_t& get_inflight_volume() { return inflight_volume_meta; }

  /**
   * volume对象写盘完成后，将waiting_for_aggregate_op中的RGW写请求重新投入OSD队列再次执行
  */
  void requeue_waiting_for_aggregate_op();

  /**
   * 从volume_meta_cache,volume_not_full中删除指定volume对象相关的元数据
  */
  void remove_volume_meta(const hobject_t& soid);

  /**
   * @brief 判断是否需要聚合该请求
   *
   */
  bool need_aggregate_op(MOSDOp* m);

  /**
   * @brief 判断是否为读请求
   * 
   * @param m 
   * @return true 
   * @return false 
   */
  bool need_translate_op(MOSDOp* m);

  /**
   * @brief 对象写入buffer，在add_chunk中执行对象元数据的创建操作
   *
   * @param op
   * @return int
   */
   int write(OpRequestRef op, MOSDOp* m);
   
  /**
   * @brief 访问rados对象，需要经过元数据转译成对 volume对象的访问(通过指针直接修改MOSDOp内容)
   *
   * @param m
   * @return int
   */
   int op_translate(OpRequestRef &op);

  /**
   * @brief 把waiting_for_reply
   * 
  */

  void send_reply(MOSDOpReply* reply, bool ignore_out_data);

  /**
   * @brief 一轮聚合完成，清空内部数据
   * 
   */
  void clear();
  /**
   * @brief 将已编码的元数据信息解码更新到volume_meta_cache中
   * 
  */
  void insert_to_meta_cache(std::shared_ptr<volume_t> meta_ptr);

  /**
   * @brief volume对象写盘完成后，将其元信息以及数据块更新到内存的缓存中
   * 
  */
  void update_cache(const hobject_t& soid, std::vector<OSDOp> *ops);

  void clear_ec_cache();

  // ECBackend调用该函数，将remote_read过程中读取到的数据块缓存到AggregateBuffer中
  void cache_data_chunk(extent_map& data);

  // 判断volume对象的数据块是否缓存
  bool is_volume_cached(const hobject_t& soid);

  // EC write过程中，读取已缓存的volume对象数据块，加速RMW过程
  void ec_cache_read(extent_map& read_result);

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
  SafeTimer flush_timer;
  std::atomic<bool> is_flushing = false;
  double flush_time_out;

  // class FlushContext: public Context
  // {
    // AggregateBuffer* buffer;
  // public:
    // explicit FlushContext(AggregateBuffer *_buffer): buffer(_buffer) {}
    // void finish(int r) { 
      // buffer->flush();
    // }
  // };

  template<typename T>
  class C_FlushContext : public LambdaContext<T> {
    public:
      C_FlushContext(const AggregateBuffer* _buffer, T&& f) :
  	LambdaContext<T>(std::forward<T>(f)),
	buffer(_buffer)
      {}
      void finish(int r) override {
        LambdaContext<T>::finish(r);
      } 
    private:
      const AggregateBuffer* buffer;
  };

  void cancel_flush_timeout();
  void reset_flush_timeout();

  int flush();

public:
  std::list<OpRequestRef> waiting_for_aggregate;
  std::list<OpRequestRef> waiting_for_reply;
  OpRequestRef volume_op;
  volume_t inflight_volume_meta; 
  // 与当前正在执行的请求相关联的volume元数据， 主要在ECBackEnd处做空间优化的时候被使用
private:
  CephContext* cct;
  PrimaryLogPG *pg;

  bool initialized = false;
  bool is_bind = false;
  bool flush_timer_enabled = false;

  // 属于PG的VolumeMeta
  // std::vector<volume_t> volume_meta_cache;
  std::map<hobject_t, std::shared_ptr<volume_t>> volume_meta_cache;

  // 保存非空闲volume的meta
  std::list<std::shared_ptr<volume_t>> volume_not_full;

  std::map<hobject_t, ClsParmContext*> cls_ctx_map;
  // TODO: 缓存EC块
  std::pair<std::shared_ptr<volume_t>, std::vector< bufferlist >> ec_cache;
};


#endif // !CEPH_AGGREGATEBUFFER_H
