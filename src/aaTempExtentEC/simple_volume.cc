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


SimpleVolume::SimpleVolume(uint64_t _cap, SimpleAggregationCache* _cache) 
        : volume_lock(ceph::make_mutex("SimpleVolume::lock")),
          cap(_cap), 
          size(0), 
          is_flushed(false), 
          vol_op(nullptr), 
          cache(_cache) 
{ 
    // 这里初始化的感觉怪怪的
    bitmap.resize(cap);
    chunks.resize(cap);
    bitmap.assign(cap, false);
    chunks.assign(cap, nullptr);

    flush_timer = new SafeTimer(g_ceph_context, volume_lock);
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

      auto now = clock_t::now();  //记录现在的时间
      auto when_flush = ceph::make_timespan(
        cct->_conf.get_val<double>("client_timeout_interval"));

  //     if (!connect_retry_callback) {
	// connect_retry_callback = timer.add_event_at(
	//   when,
	//   new LambdaContext([this](int r){
	//       connect_retry_callback = nullptr;
	//       reconnect();
	//     }));

      for (int i = 0; i < cap; i++)
      {
        if (bitmap[i]) continue;
        else {
          bitmap[i] = true;
          chunks[i] = chunk;
          size++;
          ret = 0;
          // 计时器清零

          break;
        }
      }

      return ret;
    }