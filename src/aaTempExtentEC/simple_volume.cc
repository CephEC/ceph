#include "simple_volume.h"

class SimpleVolumeContext : public Context
{
public:
  explicit SimpleVolumeContext()
  {
    
  }

  void finish(int r) override
  {
    
    
  }

  ~SimpleVolumeContext() override
  {
  }

protected:
  
}

explicit SimpleChunk(Volume* _vol)
  : vol(_vol)
{

}

int SimpleChunk::set_from_op(MOSDOp* _request/*, OSDMap&*/)
{
  bool can_create = op->may_write();

  if (!can_create) {
    // 非写请求返回错误
    cout << "misdirect"
    return -ENOENT;
  }

  const hobject_t& oid =
    m->get_snapid() == CEPH_SNAPDIR ? head : m->get_hobj();

  int r = _create_object_context(oid, &obc);

  if (r == -EAGAIN || (r && (r != -ENOENT || !obc))) {
    // TODO：失败处理

    return r;
  }

  // TODO：通过obc初始化chunk_info中的oi等信息
  chunk_info.oi = obc->oi;


}

int SimpleChunk::_create_object_context(const hobject_t& oid, ObjectContextRef *pobc)
{
  // new object.
	object_info_t oi(oid);
	SnapSetContext *ssc = nullptr;
	ObjectContextRef obc = make_shared<ObjectContext>(new ObjectContext());

  obc->destructor_callback = NULL;
  obc->obs.oi = oi;
  obc->obs.exists = false;
  obc->ssc = ssc;

	cout << __func__ << ": " << obc << " " << oid
		 << " " << obc->rwstate
		 << " oi: " << obc->obs.oi
		 << " ssc: " << obc->ssc
		 << " snapset: " << obc->ssc->snapset << std::endl;
	
  *pobc = obc;
  return 0;
}





SimpleVolume::SimpleVolume(CephContext* _cct, uint64_t _cap, SimpleAggregationCache* _cache) 
        : flush_lock(ceph::make_mutex("SimpleVolume::flush_lock")),
          cct(_cct),
          cap(_cap), 
          size(0), 
          is_flushing(false), 
          vol_op(nullptr), 
          cache(_cache) 
{ 
    // 这里初始化的感觉怪怪的
    bitmap.resize(cap);
    chunks.resize(cap);
    bitmap.assign(cap, false);
    chunks.assign(cap, nullptr);

    flush_timer = new SafeTimer(_cct, flush_lock);
    flush_timer->init();
} 



int SimpleVolume::add_chunk(MOSDOp* op, const OSDMap &osdmap) {
      // 无所谓，PG会上锁
      // std::lock_guard locker{volume_lock};
      int ret = FULL_SIGNAL;
      
      if (full()) {
        cout << "full volume failed to add chunk. " << std::endl;
        return ret;
      } 

      // TODO：定时器flush事件取消
      flush_timer.cancel_event(flush_callback);

      // 构建chunk
      SimpleChunk* new_chunk = new SimpleChunk(this);
      // 根据op初始化chunk信息
      new_chunk->set_from_op(op);

      

      for (int i = 0; i < cap; i++)
      {
        if (bitmap[i]) continue;
        else {
          bitmap[i] = true;
          chunks[i] = new_chunk;
          size++;
          ret = 0;
          // 计时器清零

          break;
        }
      }

      
      // TODO：定时器清零重新开始计时      
      auto now = clock_t::now();  //记录现在的时间
      // 配置文件中规定客户端可容忍的超时时长
      /*
      auto when_flush = ceph::make_timespan(
         cct->_conf.get_val<double>("client_timeout_interval"));
      */
      // 没有配置文件，测试用
      auto when_flush = ceph::make_timespan(1.0);
      
      if (!flush_callback) {
	      flush_callback = timer.add_event_at(
        when,
        new LambdaContext([this](int r) {
            flush_callback = nullptr;
            flush();
	    }));
      }

      return ret;
}

void SimpleVolume::flush()
{
  std::lock_guard l{flush_list_lock};
  std::lock_guard l{cache->flush_list_lock};
  cache->pending_to_flush.push_back(this);
  //return true;
}