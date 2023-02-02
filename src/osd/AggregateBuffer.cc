#include "AggregateBuffer.h"

AggregateBuffer::AggregateBuffer(CephContext* _cct, const spg_t& _pgid, PrimaryLogPG* _pg) :
  // volume_buffer(_cct->_conf.get_val<std::uint64_t>("osd_aggregate_buffer_capacity"),
               // _cct->_conf.get_val<std::uint64_t>("osd_aggregate_buffer_chunk_size"),
                // _pgid),
  volume_buffer(0, 0, _pgid),
  flush_time_out(0),
  cct(_cct), 
  pg(_pg) {
    // init volume
    // volume_buffer = new Volume(_cct->_conf.get_val<std::uint64_t>("osd_aggregate_buffer_capacity"),
    //                            _cct->_conf.get_val<std::uint64_t>("osd_aggregate_buffer_chunk_size"),
    //                             _pgid);

    // init timer
    // flush_time_out = 5;
    // flush_time_out = _cct->_conf.get_val<double>("osd_aggregate_buffer_flush_timeout");
    flush_timer = new SafeTimer(cct, timer_lock);
    flush_timer->init(); 
 };

void AggregateBuffer::init(uint64_t _volume_cap, uint64_t _chunk_size, double _time_out)
{
  initialized = true; 
  volume_buffer.init(_volume_cap, _chunk_size);
  flush_time_out = _time_out; 
}

int AggregateBuffer::write(OpRequestRef op, MOSDOp* m)
{
  // volume满，未处于flushing状态，则等待flush（一般不会为真）
  if (volume_buffer.full() && !is_flushing) {
     if (!is_flushing) {
       int r = flush();
       if (r < 0) {
         return AGGREGATE_FAILED;
       }
     }
     else {
       waiting_for_aggregate.push_back(op);
       return AGGREGATE_PENDING_OP;  
     }
  }

  // ceph_assert(volume_buffer == nullptr);

  if (volume_buffer.add_chunk(op, m) < 0) {
    return AGGREGATE_FAILED;
  }

  if (volume_buffer.full()) {
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
  volume_t vol_info = volume_buffer.get_volume_info();
  // check full state and add in volume_not_full
  if (!volume_buffer.full()) {
    volume_not_full.push_back(vol_info);
  }
  // add in volume meta cache
  volume_meta_cache.push_back(vol_info);
  
  
  // TODO: judge if cache ec chunk

  // TODO: generate new op for volume and requeue it
  MOSDOp* m = volume_buffer.generate_op();
  // OpRequestRef op = pg->osd->op_tracker.create_request<OpRequest, Message*>(m);
  OpRequestRef op = pg->osd->osd->create_request(m);

  pg->requeue_op(op);

  flush_lock.unlock();
  
  volume_buffer.clear();
  is_flushing = false;
  flush_timer->cancel_event(flush_callback);
  flush_callback = nullptr;
  return 0;
}

void AggregateBuffer::send_reply(Message* reply)
{
  while (!waiting_for_reply.empty()) {
    auto op = waiting_for_reply.front();
    auto m = op->get_req<MOSDOp>();
    pg->osd->send_message_osd_client(reply, m->get_connection());
    waiting_for_reply.pop_front();
  }
}
