#include "AggregateBuffer.h"

int AggregateBuffer::write(OpRequestRef op)
{
  // volume满，未处于flushing状态，则等待flush（一般不会为真）
  if (volume_buffer && volume_buffer->full() && !is_flushing) {
     dout(4) << "volume_buffer " << volume_buffer
          << "is full, waiting to flush "
          << dendl;
     if (!is_flushing) {
       int r = flush();
       if (r < 0) {
         dout(4) << "failed to flush full volume." << dendl;
	       return AGGREGATE_FAILED;
       }
     }
     else {
       dout(4) << "waiting to aggregate." << dendl;
       waiting_for_aggregate.push_back(op);
       return AGGREGATE_PENDING_OP;  
     }
  }

  // ceph_assert(volume_buffer == nullptr);

  if (volume_buffer->add_chunk(op) < 0) {
    dout(4) << "failed to add chunk in volume buffer." << dendl;
    return AGGREGATE_FAILED;
  }

  if (volume_buffer->full()) {
    flush();
  } else {
    if (flush_callback != nullptr) 
      flush_timer->cancel_event(flush_callback);
    flush_callback = new FlushContext(this);
    flush_timer->add_event_after(flush_time_out, flush_callback);
    
    waiting_for_reply.push_back(op);
    return AGGREGATE_PENDING_REPLY;
  }

  return AGGREGATE_SUCCESS;
}

int AggregateBuffer::flush()
{
  // when start to flush, lock
  flush_lock.lock();
  is_flushing = true;
  dout(4) << "lock and flush volume " << volume_buffer << dendl;
  volume_t vol_info = get_volume_info();
  // check full state and add in volume_not_full
  if (!volume_buffer->full()) {
    volume_not_full.push_back(vol_info);
  }
  // add in volume meta cache 
  volume_meta_cache.push_back(get_volume_info());
  
  // TODO: judge if cache ec chunk

  // TODO: generate new op for volume and requeue it
  

  flush_lock.unlock();
  
  volume_buffer->clear();
  is_flushing = false;
  flush_timer->cancel_event(flush_callback);
  flush_callback = nullptr;
  return 0;
}
