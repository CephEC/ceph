#include "AggregateBuffer.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

using std::ostream;
using std::ostringstream;

static ostream& _prefix(std::ostream *_dout, const AggregateBuffer *buf) {
  return *_dout << "aggregate buffer. ******* "; 
}


AggregateBuffer::AggregateBuffer(CephContext* _cct, const spg_t& _pgid, PrimaryLogPG* _pg) :
  volume_buffer(_cct, 0, _pgid),
  flush_callback(NULL),
  flush_timer(_cct, timer_lock),
  flush_time_out(0),
  cct(_cct), 
  pg(_pg) {};

void AggregateBuffer::init(uint64_t _volume_cap, uint64_t _chunk_size, 
  bool _flush_timer_enabled, double _time_out)
{
  initialized = true; 
  flush_timer_enabled = _flush_timer_enabled;
  volume_buffer.init(_volume_cap, _chunk_size);
  if (flush_timer_enabled) {
    flush_time_out = _time_out;
    flush_timer.init(); 
  }
  // 初始化ec_cache
  ec_cache.second.resize(_volume_cap);
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
    if (iter->op.op == CEPH_OSD_OP_WRITEFULL ||
        iter->op.op == CEPH_OSD_OP_WRITE ||
        iter->op.op == CEPH_OSD_OP_SETXATTR) {
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
        iter->op.op == CEPH_OSD_OP_CALL ||
        iter->op.op == CEPH_OSD_OP_DELETE ||
        iter->op.op == CEPH_OSD_OP_STAT ||
        iter->op.op == CEPH_OSD_OP_GETXATTR) {
      ret = true;
      break;
    }
  }
  return ret;
}

int AggregateBuffer::write(OpRequestRef op, MOSDOp* m)
{
  if (op_translate(op) == AGGREGATE_CONTINUE) {
    // RGW对象覆盖写，不需要聚合
    return AGGREGATE_CONTINUE;
  }

  // volume满，未处于flushing状态，则等待flush（一般不会为真）
  if (volume_buffer.full() || is_flushing.load()) {
    waiting_for_aggregate.push_back(op);
    return AGGREGATE_PENDING_OP;
  }

  if (!is_bind_volume()) bind(m->get_hobj().get_head());
  // ceph_assert(volume_buffer == nullptr);

  if (volume_buffer.add_chunk(op, m) < 0) {
    dout(4) << __func__ << " call add_chunk failed" << dendl;
    return AGGREGATE_FAILED;
  }
  dout(4) << __func__ << " spg_t = " << volume_buffer.get_spg() << " write request(oid = " << m->get_hobj().get_head() << " aggregate into volume," <<
   "volume_oid = " << volume_buffer.get_volume_info().get_oid() << dendl;
  // if (flush_callback) 
    // flush_timer->cancel_event(flush_callback);
  // flush_callback = new FlushContext(this);
  // flush_timer->add_event_after(flush_time_out, flush_callback);
  waiting_for_reply.push_back(op);

  if (flush_timer_enabled) {
    reset_flush_timeout();
  }

  if (volume_buffer.full() || !flush_timer_enabled) {
    flush();
  }
  return AGGREGATE_PENDING_REPLY;

}

bool AggregateBuffer::is_volume_cached(const hobject_t& soid) {
  if (!ec_cache.first) return false;
  if (soid != ec_cache.first->get_oid()) return false;
  auto chunk_bitmap = ec_cache.first->get_chunk_bitmap();
  uint64_t chunk_size = ec_cache.first->get_chunk_size();
  for (uint32_t i = 0; i < ec_cache.first->get_cap(); i++) {
    if (chunk_bitmap[i] && ec_cache.second[i].length() != chunk_size) {
      return false;
    }
  }
  return true;
}

// 针对"未缓存数据块的volume对象"做填充写时，需要将之前已落盘的数据块读入主OSD
// 于此同时，将这些数据块缓存起来，加速下一轮填充写
// 但注意，针对覆盖写的场景，无需缓存数据块
void AggregateBuffer::cache_data_chunk(extent_map& data) {
  if (ec_cache.first) return;
  dout(4) << "cache_data_chunk, data = " << data << dendl;
  for (auto &&extent: data) {
    uint64_t off = extent.get_off();
    uint64_t len = extent.get_len();
    uint64_t end = off + len;
    uint64_t chunk_size = volume_buffer.get_volume_info().get_chunk_size();
    ceph_assert(off == 0);
    ceph_assert(len % chunk_size == 0);
    while (off <end) {
      ec_cache.second[off / chunk_size].substr_of(extent.get_val(), off, chunk_size);
      off += chunk_size;
    }
  }
}

void AggregateBuffer::clear_ec_cache() {
  for (uint32_t i = 0; i < ec_cache.second.capacity(); i++) {
    dout(4) << __func__ << " update_cache clear ec_cache " << i << dendl;
    ec_cache.second[i].clear();
  }
  ec_cache.first.reset();
}

void AggregateBuffer::ec_cache_read(extent_map& read_result) {
  // TODO: 单个数据块内仅缓存有效数据
  for (uint32_t i = 0; i < ec_cache.second.size(); i++) {
    uint64_t chunk_size = volume_buffer.get_volume_info().get_chunk_size();
    uint64_t off = i * chunk_size;
    auto &bl = ec_cache.second[i];
    if (bl.length()) {
      ceph_assert(bl.length() == chunk_size);
      read_result.insert(off, chunk_size, bl);
    } else {
      bufferlist bl;
      bl.append_zero(chunk_size);
      read_result.insert(off, chunk_size, std::move(bl));
    }
  }
}

int AggregateBuffer::flush()
{
  if (!volume_buffer.empty()) {
    flush_lock.lock();
    // when start to flush, lock
    is_flushing.store(true);
    volume_t vol_info = volume_buffer.get_volume_info();

    // TODO: judge if cache ec chunk

    // TODO: generate new op for volume and requeue it
    MOSDOp* m = volume_buffer.generate_op();
    // OpRequestRef op = pg->osd->op_tracker.create_request<OpRequest, Message*>(m);
    volume_op = pg->osd->osd->create_request(m);
    volume_op->set_requeued();
    volume_op->set_write_volume();
    volume_op->set_cephec_storage_optimize();
    volume_op->set_cephec_translated_op();
    
    pg->requeue_op(volume_op);
    inflight_volume_meta = volume_buffer.get_volume_info();

    flush_lock.unlock();
    dout(4) << " aggregate finish, try to write volume, op = " << *m 
      << " aggregate_op_num = " << volume_buffer.get_volume_info().get_size() << dendl;
    return AGGREGATE_PENDING_REPLY;
  } else {
    dout(4) << " timeout, but volume is empty. " << dendl;
    flush_callback = NULL;
    return NOT_TARGET;
  }
}

void AggregateBuffer::remove_volume_meta(const hobject_t& soid) {
  for (auto it = volume_meta_cache.begin();
       it != volume_meta_cache.end();
       it++) {
    if (it->second->get_oid() == soid) {
      dout(4) << __func__ << " remove mapping: from " << it->first 
       << " to " << it->second->get_oid() << dendl;
      volume_meta_cache.erase(it);
    }
  }
  volume_not_full.remove_if([&](std::shared_ptr<volume_t> meta) {
    return meta->get_oid() == soid;
  });
}

void AggregateBuffer::requeue_waiting_for_aggregate_op() {
  for (auto &op : waiting_for_aggregate) {
    auto m = op->get_req<MOSDOp>();
    op->set_requeued();
    pg->requeue_op(op);
    dout(4) << __func__ << ": requeue write op " << *m << dendl;
  }
}

void AggregateBuffer::update_cache(const hobject_t& soid, std::vector<OSDOp> *ops) {
  dout(4) << __func__ << " try to update meta cache, object = " << soid << " ops = " << *ops << dendl;
  std::shared_ptr<volume_t> meta_ptr;
  for (auto i = ops->rbegin(); i != ops->rend(); i++) {
    auto &osd_op = *i;
    if (osd_op.op.op == CEPH_OSD_OP_DELETE) {
      // 删除与该volume对象相关的元数据
      remove_volume_meta(soid);
    } else if (osd_op.op.op == CEPH_OSD_OP_SETXATTR) {
      bufferlist bl;
      std::string aname;
      auto bp = osd_op.indata.cbegin();
      bp.copy(osd_op.op.xattr.name_len, aname);
      if (aname != "volume_meta") {
        continue;
      }
      // 如果volume_meta之前存在volume_meta_cache中，先清除它们，再写入
      remove_volume_meta(soid);
      bp.copy(osd_op.op.xattr.value_len, bl);
      meta_ptr = std::make_shared<volume_t>(
        pg->get_pgbackend()->get_ec_data_chunk_count(),
        pg->get_pgid());
      auto p = bl.cbegin();
      decode(*meta_ptr, p);
      insert_to_meta_cache(meta_ptr);
      if (meta_ptr->full()) {
        clear_ec_cache();
      } else {
        // 未满的volume添加到volume_not_full
        if (!ec_cache.first || (meta_ptr->get_oid() == ec_cache.first->get_oid())) {
          // ec_cache中已经缓存了该volume的数据块，那么下一轮聚合优先填充该volume
          volume_not_full.push_front(meta_ptr);
        } else {
          volume_not_full.push_back(meta_ptr);  // 一定对应删除操作(删除单个RGW对象,只需要更新volume元数据)
        }
        ec_cache.first = meta_ptr;
        dout(4) << __func__ << " update_cache spg_t = " << volume_buffer.get_spg() << " add volume( " <<
          *meta_ptr << ") into volume_not_full" << dendl;
      }
    } else if (osd_op.op.op == CEPH_OSD_OP_WRITE) {
      // 聚合逻辑下,所有request中一定包含一个WRITE OP和一个SETXATTR OP
      if (!meta_ptr || meta_ptr->full()) continue;
      // 将本轮聚合的数据缓存在内存中
      ceph_assert(ec_cache.first->get_oid() == soid);
      uint64_t chunk_size = volume_buffer.get_volume_info().get_chunk_size();
      ceph_assert(osd_op.op.extent.length == chunk_size);
      uint64_t chunk_id = osd_op.op.extent.offset / chunk_size;
      ceph_assert(ec_cache.second[chunk_id].length() == 0);
      ec_cache.second[chunk_id].append(std::move(osd_op.indata));
      ceph_assert(ec_cache.second[chunk_id].length() == chunk_size);
    }
  }
  if (!is_volume_cached(soid)) {
    // 判断ec_cache的volume_t元数据和实际缓存的数据块数据
    // 如果数据块没有完全缓存，那么就直接清空ec_cache
    clear_ec_cache();
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
  while (!waiting_for_aggregate.empty()) {
    waiting_for_aggregate.pop_front();
  }
  if (volume_op) {
    volume_op.reset();
  }
  volume_buffer.clear();
  is_bind = false;
  is_flushing.store(false);
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

OSDOp generate_zero_op(uint64_t off, uint64_t len) {
  OSDOp op;
  op.op.op = CEPH_OSD_OP_ZERO;
  op.op.extent.offset = off;
  op.op.extent.length = len;
  return op;
}

int AggregateBuffer::op_translate(OpRequestRef &op) {
  MOSDOp *m = static_cast<MOSDOp *>(op->get_nonconst_req());
  auto rgw_oid = m->get_hobj();
  auto it = volume_meta_cache.find(m->get_hobj());
  int r = 0;
  if (it == volume_meta_cache.end()) {
    // rados对象不存在
    dout(4) << __func__ << " object(oid = " << rgw_oid << ") not exists" << dendl;
    return -ENOENT;
  }
  // 这里采用深拷贝,内存中的volume_meta等待删除（或是写入）完成的回调请求来更新
  inflight_volume_meta = *(volume_meta_cache[rgw_oid]);
  auto chunk_meta = inflight_volume_meta.get_chunk(rgw_oid);
  // 将操作的对象oid由RGW对象oid替换为volume对象oid
  m->set_hobj(inflight_volume_meta.get_oid());
  for (uint64_t i = 0; i < m->ops.size(); i++) {
    auto &osd_op = m->ops[i];
    uint64_t vol_offset = uint8_t(chunk_meta.get_chunk_id()) * inflight_volume_meta.get_chunk_size();
    
    if (osd_op.op.op == CEPH_OSD_OP_ZERO) { continue; }
    if (osd_op.op.op == CEPH_OSD_OP_GETXATTR) {
      // 为了区分同一个volume内不同rgw对象的xattr,需要在xattr.key中追加对象的oid
      auto bp = osd_op.indata.cbegin();
      std::string key;
      bp.copy(osd_op.op.xattr.name_len, key);
      key = chunk_meta.get_oid().get_key() + "_" + key;
      osd_op.indata.clear();
      osd_op.op.xattr.name_len = key.size();
      osd_op.indata.append(key);
      continue;
    }

    if (osd_op.op.op == CEPH_OSD_OP_STAT) {
      encode(chunk_meta.get_offset(), osd_op.outdata);
      encode(chunk_meta.get_mtine(), osd_op.outdata);
      continue;
    }

    if (osd_op.op.op == CEPH_OSD_OP_DELETE) {
      ceph_assert(m->ops.size() == 1);
      if (!inflight_volume_meta.is_only_valid_object(rgw_oid)) {
        // volume中还有其他有效对象,更新元数据,并且生成zero请求挖洞
        m->ops.push_back(generate_zero_op(vol_offset, inflight_volume_meta.get_chunk_size()));
        inflight_volume_meta.remove_chunk(rgw_oid);
        inflight_volume_meta.generate_write_meta_op(m->ops[i]);
        op->set_cephec_storage_optimize();
        dout(4) << __func__ << " want delete object(oid = "
          << rgw_oid << "), but just update volume_meta. op = " << *m << dendl;
      } else {
        // volume中全部都是无效数据，直接删除整个volume对象
        dout(4) << __func__ << " want delete object(oid = " << rgw_oid 
          << "), but delete volume(oid = " << inflight_volume_meta.get_oid() << ")" << dendl;
      }
      if (ec_cache.first && inflight_volume_meta.get_oid() == ec_cache.first->get_oid()) {
        // 如果删除的对象在ec_cache中被缓存，刷新ec_cache
        for (uint32_t i = 0; i < ec_cache.second.capacity(); i++) {
          dout(4) << __func__ << " op_translate clear ec_cache " << i << dendl;
          ec_cache.second[i].clear();
        }
        ec_cache.first.reset();
      }
      // 删除整个volume对象和volume_t元数据(osd_op不用改，只需要把删除对象的oid替换成volume oid即可)
      // 回调时从内存中将该volume的信息清除
      continue;
    }

    {
      if (osd_op.op.op == CEPH_OSD_OP_WRITEFULL) {
        // 为RGW对象覆盖写提供请求转译
        osd_op.op.op = CEPH_OSD_OP_WRITE;
        r = AGGREGATE_CONTINUE;
      }
      if (osd_op.op.op == CEPH_OSD_OP_CALL) {
        std::string cname;
        try
        {
          osd_op.indata.begin().copy(osd_op.op.cls.class_len, cname);
        } catch (ceph::buffer::error &e)
        {
          dout(10) << "call unable to decode class" << dendl;
          dout(30) << "in dump: ";
          osd_op.indata.hexdump(*_dout);
          *_dout << dendl;
          ceph_assert(false);
        }
        // 只转译新构建的Cls算子（因为原生的cls算子和基于cephEC的cls算子的参数解析方式不同，所以需要区分）
        if (!ClassHandler::get_instance().in_class_list(cname, cct->_conf->osd_cephec_class_list)) {
          continue;
        }
        // 为EC pool中的Cls请求提供转译
        osd_op.op.op = CEPH_OSD_OP_EC_CALL;
        ClsParmContext *cls_parm_ctx = 
          new ClsParmContext(osd_op.op.cls.class_len,
                            osd_op.op.cls.method_len,
                            osd_op.op.cls.argc,
                            osd_op.op.cls.indata_len,
                            osd_op.indata);
        cls_ctx_map[inflight_volume_meta.get_oid()] = cls_parm_ctx;
        osd_op.op.extent.length = chunk_meta.get_offset();
      }
      // 如果是read请求，那就只需要执行下面这段公用的off,len转译代码就行
      // if (osd_op.op.op == CEPH_OSD_OP_READ) {}

      // READ, CALL, WRITEFULL公用的转译逻辑
      // 将待访问RGW对象的oid,off,len替换为volume对象的oid,off,len
      if (osd_op.op.extent.offset == inflight_volume_meta.get_chunk_size()) {
        // offset超出RGW对象本身的大小，本次不读取任何数据
        osd_op.op.extent.offset = inflight_volume_meta.get_cap() * inflight_volume_meta.get_chunk_size();
      } else {
        osd_op.op.extent.offset = vol_offset;
        osd_op.op.extent.length = osd_op.op.extent.length > chunk_meta.get_offset() ?
          chunk_meta.get_offset() : osd_op.op.extent.length;
      }
      dout(4) << __func__ << " translate access object(oid = " << rgw_oid << ") to access_volume(oid = "
        << inflight_volume_meta.get_oid() << " off =  " << osd_op.op.extent.offset
        << " len = " << osd_op.op.extent.length << ")" << dendl;
    }
  }
  op->set_cephec_translated_op();
  return r;
}
