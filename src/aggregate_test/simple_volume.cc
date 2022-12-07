#include "simple_volume.h"


/**
 * chunk
 */





/**
 * simple chunk
 */

// temp, for test
int SimpleChunk::_create_object_context(const hobject_t& oid/*, ObjectContextRef *pobc*/)
{
  // TODO: PrimaryLogPG::create_object_context
  object_info_t oi(oid);
  // ObjectContextRef obc = std::make_shared<ObjectContext>(new ObjectContext());
  // obc->destructor_callback = NULL;

  // cout << __func__ << ": " << obc << " " << oid << std::endl;

  // *pobc = ObjectContextRef();
  return 0;
}

int SimpleChunk::set_from_op(MOSDOp* op/*, OSDMap& osdmap*/)
{
  cout << __func__ << " " << op << std::endl;
  // TODO: 判断操作类型（读、写 etc）这里因为用的是MOSDOp没有获取接口，暂时跳过
  // bool can_create = op->may_write();
  // if (!can_create) {
    // 非写请求返回错误
    // cout << "misdirect" << std::endl;
    // return -ENOENT;
  // }

  const hobject_t& oid = op->get_hobj();
  int r = _create_object_context(oid/*, &obc*/);
  if (r == -EAGAIN || (r && (r != -ENOENT/* || !obc*/))) {
    // TODO：失败处理
    return r;
  }
  // TODO：通过obc和op初始化chunk_info中的信息
  // chunk_info.set_from_op()
  return 0;
}

/**
 * simple volume
 */


SimpleVolume::SimpleVolume(CephContext* _cct, uint64_t _cap, SimpleAggregateBuffer* _buffer)
  : cct(_cct),
    cap(_cap),
    size(0),
    volume_buffer(_buffer),
    vol_op(nullptr)
{
  // 效率不太高？
  bitmap.resize(cap);
  chunks.resize(cap);
  bitmap.assign(cap, false);
  chunks.assign(cap, nullptr);

  // TOREVIEW: 初始化定时器
  flush_timer = new SafeTimer(_cct, flush_lock);
  flush_timer->init();
} 



int SimpleVolume::add_chunk(MOSDOp* op, const OSDMap &osdmap) 
{
  static int index = 0;
  cout << "add chunk in volume " << index++ << std::endl;
  return 0;
}

void SimpleVolume::flush()
{
  std::lock_guard l{flush_lock};
  std::lock_guard l{cache->flush_list_lock};
  is_flushing = true;
  cache->pending_to_flush.push_back(this);
  //return true;
}