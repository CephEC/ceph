#include "simple_aggregate_buffer.h"

int SimpleAggregateBuffer::write(MOSDOp* op, const OSDMap &osdmap)
{
  int ret = 0;

  // volume满，未处于flushing状态，则等待flush（一般不会为真）
  if (volume_buffer && volume_buffer->full() && !is_flushing) {
     cout << "volume_buffer " << volume_buffer
          << "is full, waiting to flush "
          << std::endl;
     if (!is_flushing) {
       int r = flush();
       if (r < 0) {
         cout << "failed to flush pending full volume." << std::endl;
	 return r;
       }
       volume_buffer->clear();
     }
     else {
       waiting_for_aggregate.push_back(op);
       return ret;  
     }
  }


  if (volume_buffer == nullptr) {
    // volume_buffer = std::make_shared<SimpleVolume>(
    // new SimpleVolume(g_conf()->get_val<uint64_t>("aggregate_volume_size"), 4, this));
    cout << "create a new volume " << std::endl;
    volume_buffer = new SimpleVolume(cct, 4, this);
  }

  // ceph_assert(volume_buffer == nullptr);

  if (volume_buffer->add_chunk(op) < 0) {
    cout << "failed to add chunk in volume buffer." << std::endl;
    ret = -1;
  }

  if (volume_buffer->full()) {
    flush();
  } else {
    printf("thread tid is %d, pid is %d\n", gettid(), getpid());
    if (flush_callback != nullptr) flush_timer->cancel_event(flush_callback);
    flush_callback = new FlushContext(this);
    flush_timer->add_event_after(5, flush_callback);
  }

  return ret;
}

int SimpleAggregateBuffer::flush()
{
  // when start to flush, lock
  flush_lock.lock();
  // TODO: lock it, 整理volume op
  // TODO: requeue_op()
  // TODO: vol.dump;
  is_flushing = true;
  printf("lock and flush %x\n", volume_buffer);
  flush_lock.unlock();
  printf("thread tid is %d, pid is %d\n", gettid(), getpid());
  volume_buffer->clear();
  is_flushing = false;
  flush_timer->cancel_event(flush_callback);
  flush_callback = nullptr;
  return 0;
}


