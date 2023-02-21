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

void AggregateBuffer::bind(const hobject_t &first_oid) {
  if (volume_not_full.empty()) {
    volume_buffer.get_volume_info().set_volume_id(first_oid);
  } else {
    volume_buffer.set_volume_info(volume_not_full.front());
    volume_not_full.pop_front();
  }
  is_bind = true;
  dout(4) << __func__ << " spg_t = " << volume_buffer.get_spg() <<
    " volume_buffer bind to volume,volume_oid = " << volume_buffer.get_volume_info().get_oid() << dendl;
}

bool AggregateBuffer::need_aggregate_op(MOSDOp* m)
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

bool AggregateBuffer::need_translate_op(MOSDOp* m)
{
  bool ret = false;
  std::vector<OSDOp>::const_iterator iter;

  for (iter = m->ops.begin(); iter != m->ops.end(); ++iter) {
    if (iter->op.op == CEPH_OSD_OP_READ ||
        iter->op.op == CEPH_OSD_OP_SPARSE_READ ||
        iter->op.op == CEPH_OSD_OP_SYNC_READ ||
        iter->op.op == CEPH_OSD_OP_CALL) {
      ret = true;
      break;
    }
  }
  return ret;
}

int AggregateBuffer::write(OpRequestRef op, MOSDOp* m)
{
  if (op_translate(m) == 0) {
    // RGW对象覆盖写，不需要聚合
    return AGGREGATE_OVERWRITE;
  }

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

  if (!is_bind_volume()) bind(m->get_hobj().get_head());
  // ceph_assert(volume_buffer == nullptr);

  if (volume_buffer.add_chunk(op, m) < 0) {
    return AGGREGATE_FAILED;
  }
  dout(4) << __func__ << " spg_t = " << volume_buffer.get_spg() << " write request(oid = " << m->get_hobj().get_head() << " aggregate into volume," <<
   "volume_oid = " << volume_buffer.get_volume_info().get_oid() << dendl;
  // if (flush_callback) 
    // flush_timer->cancel_event(flush_callback);
  // flush_callback = new FlushContext(this);
  // flush_timer->add_event_after(flush_time_out, flush_callback);
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
    volume_op = pg->osd->osd->create_request(m);
    volume_op->set_requeued();
    volume_op->set_write_volume();

    pg->requeue_op(volume_op);

    flush_lock.unlock();
    is_flushing = false;
    dout(4) << " aggregate finish, try to write volume, op = " << *m << dendl;
    return AGGREGATE_PENDING_REPLY;
  } else {
    dout(4) << " timeout, but volume is empty. " << dendl;
    flush_callback = NULL;
    return NOT_TARGET;
  }
}

void AggregateBuffer::update_meta_cache(std::vector<OSDOp> *ops) {
  dout(4) << __func__ << "try to update meta cache ops = " << *ops << dendl;
  for (auto &osd_op : *ops) {
    if (osd_op.op.op == CEPH_OSD_OP_SETXATTR) {
      bufferlist bl;
      std::string aname;
      auto bp = osd_op.indata.cbegin();
      bp.copy(osd_op.op.xattr.name_len, aname);
      if (aname != "volume_meta") {
        continue;
      }
      bp.copy(osd_op.op.xattr.value_len, bl);
      auto meta_ptr = std::make_shared<volume_t>(
        pg->get_pgbackend()->get_ec_data_chunk_count(),
        pg->get_pgid());
      auto p = bl.cbegin();
      decode(*meta_ptr, p);
      insert_to_meta_cache(meta_ptr);
      if (!meta_ptr->full()) {
        // 未满的volume添加到volume_not_full
        volume_not_full.push_back(meta_ptr);
        dout(4) << __func__ << " spg_t = " << volume_buffer.get_spg() << " add volume(oid = " <<
          meta_ptr->get_oid() << ") into volume_not_full" << dendl;
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
void AggregateBuffer::clear() {
  while (!waiting_for_reply.empty()) {
    waiting_for_reply.pop_front();
  }
  if (volume_op) {
    volume_op.reset();
  }
  volume_buffer.clear();
  is_bind = false;
}

/************ timer *************/
void AggregateBuffer::cancel_flush_timeout()
{
  if (flush_callback) {
    dout(4) << " cancel flush timer. " << dendl;
    timer_lock.lock();
    flush_timer.cancel_event(flush_callback);
    timer_lock.unlock();
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
  {
    std::lock_guard l(timer_lock);    
      if(flush_timer.add_event_after(flush_time_out, flush_callback)) {
      dout(4) << " reset_flush_timeout " << flush_callback
        << " after " << flush_time_out << " seconds " << dendl;
    } else {
      flush_callback = nullptr;
    }
  }
}

void AggregateBuffer::insert_to_meta_cache(std::shared_ptr<volume_t> meta_ptr) {
  auto soid_vec = meta_ptr->get_all_soid();
  for (auto soid : soid_vec) {
    volume_meta_cache[*soid] = meta_ptr;
  }
}



int AggregateBuffer::op_translate(MOSDOp* m) {
  auto it = volume_meta_cache.find(m->get_hobj());
  if (it == volume_meta_cache.end()) {
    // rados对象不存在
    return -ENOENT;
  }
  auto volume_meta_ptr = volume_meta_cache[m->get_hobj()];
  auto chunk_meta = volume_meta_ptr->get_chunk(m->get_hobj());
  for (auto &osd_op : m->ops) {
    if (osd_op.op.op == CEPH_OSD_OP_WRITEFULL) {
      // 为RGW对象覆盖写提供请求转译
      osd_op.op.op = CEPH_OSD_OP_WRITE;
    }
    if (osd_op.op.op == CEPH_OSD_OP_CALL) {
      // 为EC pool中的Cls请求提供转译
      osd_op.op.op = CEPH_OSD_OP_EC_CALL;
      ClsParmContext *cls_parm_ctx = 
        new ClsParmContext(osd_op.op.cls.class_len,
                           osd_op.op.cls.method_len,
                           osd_op.op.cls.argc,
                           osd_op.op.cls.indata_len,
                           osd_op.indata);
      cls_ctx_map[volume_meta_ptr->get_oid()] = cls_parm_ctx;
    }
    // 将待访问RGW对象的oid,off,len替换为volume对象的oid,off,len
    dout(4) << __func__ << " translate access_request(m= " << *m << ") to access_volume(oid = "
      << volume_meta_ptr->get_oid() << " off =  " << uint8_t(chunk_meta.get_chunk_id()) * chunk_meta.get_chunk_size()
      << " len = " << chunk_meta.get_chunk_size() << ")" << dendl;
    osd_op.op.extent.offset = uint8_t(chunk_meta.get_chunk_id()) * chunk_meta.get_chunk_size();
    osd_op.op.extent.length = chunk_meta.get_chunk_size();
  }
  m->set_hobj(volume_meta_ptr->get_oid());
  return 0;
}
