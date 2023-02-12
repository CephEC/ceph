#include "AggregateBuffer.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

using std::cout;
using std::ostream;
using std::ostringstream;

static ostream& _prefix(std::ostream *_dout, const AggregateBuffer *buf) {
  return *_dout << "aggregate buffer. ******* "; 
}


AggregateBuffer::AggregateBuffer(CephContext* _cct, const spg_t& _pgid, PrimaryLogPG* _pg) :
  // volume_buffer(_cct->_conf.get_val<std::uint64_t>("osd_aggregate_buffer_capacity"),
               // _cct->_conf.get_val<std::uint64_t>("osd_aggregate_buffer_chunk_size"),
                // _pgid),
  volume_buffer(0, 0, _pgid),
  flush_callback(NULL),
  flush_timer(_cct, timer_lock),
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
  };

void AggregateBuffer::init(uint64_t _volume_cap, uint64_t _chunk_size, double _time_out)
{
  initialized = true; 
  volume_buffer.init(_volume_cap, _chunk_size);
  flush_time_out = _time_out;
  flush_timer.init(); 

}

bool AggregateBuffer::may_aggregate(MOSDOp* m)
{
  bool ret = false;
  std::vector<OSDOp>::const_iterator iter;

  for (iter = m->ops.begin(); iter != m->ops.end(); ++iter) {
    if (iter->op.op == CEPH_OSD_OP_WRITEFULL) {
      ret = true;
      break;
    }
  }
  return ret;
}

bool AggregateBuffer::may_aggregate_read(MOSDOp* m)
{
  bool ret = false;
  std::vector<OSDOp>::const_iterator iter;

  for (iter = m->ops.begin(); iter != m->ops.end(); ++iter) {
    if (iter->op.op == CEPH_OSD_OP_READ ||
        iter->op.op == CEPH_OSD_OP_SPARSE_READ ||
        iter->op.op == CEPH_OSD_OP_SYNC_READ) {
      ret = true;
      break;
    }
  }
  return ret;
}

int AggregateBuffer::write(OpRequestRef op, MOSDOp* m)
{
  if (!may_aggregate(m))
    return NOT_TARGET;
	
  // volume满，未处于flushing状态，则等待flush（一般不会为真）
  if (volume_buffer.full() && !is_flushing) {
     if (!is_flushing) {
       int r = flush();
       if (r < 0) {
         return AGGREGATE_FAILED;
       }
     }
     else {
       op->get();
       waiting_for_aggregate.push_back(op);
       return AGGREGATE_PENDING_OP;  
     }
  }

  // ceph_assert(volume_buffer == nullptr);

  if (volume_buffer.add_chunk(op, m) < 0) {
    return AGGREGATE_FAILED;
  }

  // if (flush_callback) 
    // flush_timer->cancel_event(flush_callback);
  // flush_callback = new FlushContext(this);
  // flush_timer->add_event_after(flush_time_out, flush_callback);

   op->get();
   waiting_for_reply.push_back(op);


  reset_flush_timeout();

  if (volume_buffer.full()) {
    flush();
  }
  return AGGREGATE_PENDING_REPLY;

}


int AggregateBuffer::flush()
{
  if (!volume_buffer.empty()) {
  flush_lock.lock();
  // when start to flush, lock
  is_flushing = true;
  volume_t vol_info = volume_buffer.get_volume_info();

  // TODO: judge if cache ec chunk

  // TODO: generate new op for volume and requeue it
  MOSDOp* m = volume_buffer.generate_op();
  // OpRequestRef op = pg->osd->op_tracker.create_request<OpRequest, Message*>(m);
  OpRequestRef op = pg->osd->osd->create_request(m);
  op->set_requeued();

  pg->requeue_op(op);

  flush_lock.unlock();
  volume_buffer.clear();
  is_flushing = false;
  return AGGREGATE_PENDING_REPLY;
  } else {
    dout(4) << " timeout, but volume is empty. " << dendl;
    flush_callback = NULL;
    return NOT_TARGET;
  }

}

void AggregateBuffer::update_meta_cache(std::vector<OSDOp> *ops) {
  for (auto &osd_op : *ops) {
    if (osd_op.op.op == CEPH_OSD_OP_SETXATTR) {
      bufferlist bl;
      std::string aname;
      if (aname != "_volume_meta") {
        continue;
      }
      auto bp = osd_op.indata.cbegin();
      bp.copy(osd_op.op.xattr.name_len, aname);
      bp.copy(osd_op.op.xattr.value_len, bl);
      // TODO(zhengfuyu): 容量先写死为4了，后面再看需不需要改成配置参数
      auto meta_ptr = std::make_shared<volume_t>(4, pg->get_pgid());
      auto p = bl.cbegin();
      decode(*meta_ptr, p);
      insert_to_meta_cache(meta_ptr);
      if (!meta_ptr->full()) {
        // 未满的volume添加到volume_not_full
        volume_not_full.push_back(meta_ptr);
      }
    }
  }
}

void AggregateBuffer::send_reply(MOSDOpReply* reply, bool ignore_out_data)
{
  for (auto &op : waiting_for_reply) {
    auto m = op->get_req<MOSDOp>();
    MOSDOpReply* split_reply = new MOSDOpReply(m, reply, ignore_out_data);
    dout(4) << __func__ << ": send reply to " << m->get_connection()->get_peer_addr() << dendl;
    pg->osd->send_message_osd_client(split_reply, m->get_connection());
    op->mark_commit_sent(); 
  }
}

void AggregateBuffer::delete_request() {
  while (!waiting_for_reply.empty()) {
    auto op = waiting_for_reply.front();
    op->put();
    waiting_for_reply.pop_front();
  }
}

/************ timer *************/
void AggregateBuffer::cancel_flush_timeout()
{
  if(flush_callback) {
    dout(4) << " cancel flush timer. " << dendl;
    flush_timer.cancel_event(flush_callback);
    flush_callback = NULL;
  } else {
    dout(4) << " flush callback is null " << dendl;
  }
}


void AggregateBuffer::reset_flush_timeout()
{
  cancel_flush_timeout();
  dout(4) << "reset flush timer" << dendl;
  flush_callback =  new C_FlushContext{this, [this](int) {
          dout(4) << " time out, start to flush. " << dendl; 
    flush();	  
  }};  
  std::lock_guard l(timer_lock);    
    if(flush_timer.add_event_after(flush_time_out, flush_callback)) {
    dout(4) << " reset_flush_timeout " << flush_callback
      << " after " << flush_time_out << " seconds " << dendl;
  } else {
    flush_callback = nullptr;
  }
}

void AggregateBuffer::insert_to_meta_cache(std::shared_ptr<volume_t> meta_ptr) {
  auto soid_vec = meta_ptr->get_all_soid();
  for (auto soid : soid_vec) {
    volume_meta_cache[*soid] = meta_ptr;
  }
}

int AggregateBuffer::read(MOSDOp* m) {
  if (!may_aggregate_read(m))
    return NOT_TARGET;
  auto it = volume_meta_cache.find(m->get_hobj());
  if (it == volume_meta_cache.end()) {
    // rados对象不存在
    return -1;
  }
  auto volume_meta_ptr = volume_meta_cache[m->get_hobj()];
  auto chunk_meta = volume_meta_ptr->get_chunk(m->get_hobj());
  m->set_hobj(volume_meta_ptr->get_oid());
  for (auto &osd_op : m->ops) {
    if (osd_op.op.op == CEPH_OSD_OP_READ ||
        osd_op.op.op == CEPH_OSD_OP_SPARSE_READ ||
        osd_op.op.op == CEPH_OSD_OP_SYNC_READ) {
      // TODO(zhengfuyu): 目前默认所有对rados的读请求都是全量读取对象
      osd_op.op.extent.offset = uint8_t(chunk_meta.get_chunk_id()) * chunk_meta.get_chunk_size();
      osd_op.op.extent.length = chunk_meta.get_chunk_size();
    }
  }
  return 0;
}
