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
        iter->op.op == CEPH_OSD_OP_SETXATTR ||
        iter->op.op == CEPH_OSD_OP_CREATE) {
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
        iter->op.op == CEPH_OSD_OP_GETXATTR ||
        iter->op.op == CEPH_OSD_OP_SETXATTR ||
        iter->op.op == CEPH_OSD_OP_GETXATTRS ||
        iter->op.op == CEPH_OSD_OP_CMPXATTR) {
      ret = true;
      break;
    }
  }
  return ret;
}

int AggregateBuffer::write(OpRequestRef op, MOSDOp* m)
{
  if (op_translate(op, m->ops) == AGGREGATE_CONTINUE) {
    // RGW对象覆盖写 or 对已存在的对象做setxattr操作,不需要聚合
    return AGGREGATE_CONTINUE;
  }

  // volume满，未处于flushing状态，则等待flush（一般不会为真）
  if (is_flushing.load() || volume_buffer.full()) {
    dout(4) << __func__ << " OP needs to wait for the current volume(" 
      << volume_buffer.get_volume_info().get_oid()
      << ") flush to finish. is_flushing =  " << is_flushing
      << " volume_buffer.full = " << volume_buffer.full() << dendl;
    waiting_for_aggregate.push_back(op);
    return AGGREGATE_PENDING_OP;
  }

  if (!is_bind_volume()) bind(m->get_hobj());

  if (volume_buffer.add_chunk(op, m) < 0) {
    dout(4) << __func__ << " call add_chunk failed" << dendl;
    return AGGREGATE_FAILED;
  }
  dout(4) << __func__ << " spg_t = " << volume_buffer.get_spg() << " write request(oid = " << m->get_hobj().get_head() << " aggregate into volume," <<
   "volume_oid = " << volume_buffer.get_volume_info().get_oid() << dendl;

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

void AggregateBuffer::clear_ec_cache() {
  for (uint32_t i = 0; i < ec_cache.second.capacity(); i++) {
    dout(4) << __func__ << " update_cache clear ec_cache " << i << dendl;
    ec_cache.second[i].clear();
  }
  ec_cache.first.reset();
}

void AggregateBuffer::ec_cache_read(extent_map& read_result) {
  dout(10) << __func__ << " read cache " << read_result << dendl;
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
  if (!pg) return 0;
  if (!volume_buffer.empty()) {
    flush_lock.lock();
    // when start to flush, lock
    is_flushing.store(true);
    volume_t vol_info = volume_buffer.get_volume_info();

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
    return 0;
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
  for (auto it = waiting_for_aggregate.rbegin(); it != waiting_for_aggregate.rend(); it++) {
    auto m = (*it)->get_req<MOSDOp>();
    (*it)->set_requeued();
    pg->requeue_op(*it);
    dout(4) << __func__ << ": requeue write op " << *m << dendl;
  }
}

void AggregateBuffer::update_cache(const hobject_t& soid, std::vector<OSDOp> *ops) {
  std::unique_lock<std::shared_mutex> lock(meta_mutex);
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
        volume_buffer.get_cap(),
        volume_buffer.get_spg());
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
      if (!meta_ptr || meta_ptr->full() || ec_cache.first->get_oid() != soid) continue;
            uint64_t chunk_size = volume_buffer.get_volume_info().get_chunk_size();
      dout(4) << __func__ << " cache data chunk, obj = " << soid << " off = " << osd_op.op.extent.offset
        << " len = " << osd_op.op.extent.length << " volume_chunk_size = " << chunk_size << dendl;
      // 将本轮聚合的数据缓存在内存中
      // ceph_assert(osd_op.op.extent.length == chunk_size);
      uint64_t chunk_id = osd_op.op.extent.offset / chunk_size;
      if (ec_cache.second[chunk_id].length() != 0) {
        // 覆盖写的对象刚好缓存在ec_cache中,更新ec_cache
        ec_cache.second[chunk_id].clear();
      }
      ec_cache.second[chunk_id].append(std::move(osd_op.indata));
      // ceph_assert(ec_cache.second[chunk_id].length() == chunk_size);
    }
    dout(4) << __func__ << " update meta cache complete, obj = " << soid << " ops = " << *ops << dendl;
  }
  if (!is_volume_cached(soid)) {
    // 判断ec_cache的volume_t元数据和实际缓存的数据块数据
    // 如果数据块没有完全缓存，那么就直接清空ec_cache
    clear_ec_cache();
  }
}

int AggregateBuffer::objects_list(pg_nls_response_t &response, unsigned list_size) {
  std::shared_lock<std::shared_mutex> lock(meta_mutex);
  hobject_t lower_bound = response.handle;
  auto iter = volume_meta_cache.lower_bound(lower_bound);
  if (iter == volume_meta_cache.end()) {
    response.handle = pg->info.pgid.pgid.get_hobj_end(pg->pool.info.get_pg_num());
    return 1;
  }
  for (unsigned i = 0; i < list_size && iter != volume_meta_cache.end(); i++, iter++) {
	  librados::ListObjectImpl item;
	  item.nspace = iter->first.get_namespace();
	  item.oid = iter->first.oid.name;
	  item.locator = iter->first.get_key();
	  response.entries.push_back(item);
  }
  response.handle = (iter != volume_meta_cache.end() ? iter->first : 
                            pg->info.pgid.pgid.get_hobj_end(pg->pool.info.get_pg_num()));
  return 0;
}

void AggregateBuffer::send_reply(MOSDOpReply* reply, bool ignore_out_data)
{
  for (auto &op : waiting_for_reply) {
    auto m = op->get_req<MOSDOp>();
    MOSDOpReply* split_reply = new MOSDOpReply(m, reply, ignore_out_data);
    dout(4) << __func__ << ": send reply to " << m->get_source_addr() << dendl;
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

void AggregateBuffer::object_off_to_volume_off(OSDOp &osd_op, chunk_t chunk_meta) {
  uint64_t vol_offset = uint8_t(chunk_meta.get_chunk_id()) * inflight_volume_meta.get_chunk_size();
  if (osd_op.op.extent.offset >= chunk_meta.get_offset()) {
    // offset超出RGW对象本身的大小，本次不读取任何数据
    osd_op.op.extent.offset = inflight_volume_meta.get_cap() * inflight_volume_meta.get_chunk_size();
  } else {
    osd_op.op.extent.length = osd_op.op.extent.offset + osd_op.op.extent.length > chunk_meta.get_offset() ?
      chunk_meta.get_offset() - osd_op.op.extent.offset : osd_op.op.extent.length;
    osd_op.op.extent.offset = vol_offset + osd_op.op.extent.offset;
  }
}

int AggregateBuffer::op_translate(OpRequestRef &op, std::vector<OSDOp> &ops) {
  std::shared_lock<std::shared_mutex> lock(meta_mutex);
  MOSDOp *m = static_cast<MOSDOp *>(op->get_nonconst_req());
  auto origin_oid = m->get_hobj();
  if (origin_oid_map.find(op) != origin_oid_map.end()) {
    origin_oid = origin_oid_map[op];
    dout(4) << __func__ << " origin_oid " << origin_oid << dendl;
  } else {
    origin_oid_map[op] = origin_oid;
  }
  auto it = volume_meta_cache.find(origin_oid);
  int r = AGGREGATE_CONTINUE;
  if (it == volume_meta_cache.end()) {
    // rados对象不存在
    dout(4) << __func__ << " object(oid = " << origin_oid << ") not exists" << dendl;
    purge_origin_obj(op);
    return -ENOENT;
  }
  // 这里采用深拷贝,内存中的volume_meta等待删除（或是写入）完成的回调请求来更新
  inflight_volume_meta = *(volume_meta_cache[origin_oid]);
  auto chunk_meta = inflight_volume_meta.get_chunk(origin_oid);
  // 将操作的对象oid由RGW对象oid替换为volume对象oid
  m->set_hobj(inflight_volume_meta.get_oid());
  for (int64_t i = ops.size() - 1; i >= 0; i--) {
    auto &osd_op = ops[i];
    
    uint64_t vol_offset = uint8_t(chunk_meta.get_chunk_id()) * inflight_volume_meta.get_chunk_size();
    
    switch (osd_op.op.op) {
    case CEPH_OSD_OP_CMPXATTR:
    case CEPH_OSD_OP_SETXATTR:
    {
      auto bp = osd_op.indata.cbegin();
      std::string key;
	    bufferlist value;
      bp.copy(osd_op.op.xattr.name_len, key);
      key = origin_oid.oid.name + "_" + key;
      bp.copy(osd_op.op.xattr.value_len, value);
      osd_op.indata.clear();
      osd_op.op.xattr.name_len = key.size();
      osd_op.indata.append(key);
      osd_op.indata.append(value);
      break;
    }
    case CEPH_OSD_OP_GETXATTR:
    {
      // 为了区分同一个volume内不同rgw对象的xattr,需要在xattr.key中追加对象的oid
      auto bp = osd_op.indata.cbegin();
      std::string key;
      bp.copy(osd_op.op.xattr.name_len, key);
      key = origin_oid.oid.name + "_" + key;
      osd_op.indata.clear();
      osd_op.op.xattr.name_len = key.size();
      osd_op.indata.append(key);
      break;
    }

    case CEPH_OSD_OP_GETXATTRS:
    {
      // 要获取rgw对象的所有扩展属性
      // 但是由于rgw对象的所有扩展属性都 增加了前缀并合并到volume对象中了
      // 所以我们需要获取volume对象的所有扩展属性，然后根据扩展属性的前缀筛选得到目标数据
      osd_op.indata.clear();
      osd_op.op.xattr.name_len = origin_oid.oid.name.length();
      osd_op.indata.append(origin_oid.oid.name);
      break;
    }
    case CEPH_OSD_OP_STAT:
    {
      encode(chunk_meta.get_offset(), osd_op.outdata);
      encode(chunk_meta.get_mtine(), osd_op.outdata);
      break;
    }
    case CEPH_OSD_OP_DELETE:
    {
      ceph_assert(ops.size() == 1);
      if (!inflight_volume_meta.is_only_valid_object(origin_oid)) {
        // volume中还有其他有效对象,更新元数据,并且生成zero请求挖洞
        ops.push_back(generate_zero_op(vol_offset, inflight_volume_meta.get_chunk_size()));
        inflight_volume_meta.remove_chunk(origin_oid);
        inflight_volume_meta.generate_write_meta_op(ops[i]);
        op->set_cephec_storage_optimize();
        dout(4) << __func__ << " want delete object(oid = "
          << origin_oid << "), but just update volume_meta. op = " << *m << dendl;
      } else {
        // volume中全部都是无效数据，直接删除整个volume对象
        dout(4) << __func__ << " want delete object(oid = " << origin_oid 
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
      break;
    }
    case CEPH_OSD_OP_WRITEFULL:
    {
      // 为RGW对象全量覆盖写提供请求转译
      osd_op.op.op = CEPH_OSD_OP_WRITE;
      osd_op.op.extent.offset = vol_offset;
      // 填0处理
      dout(4) << __func__ << ": bufferlist before zerofilling " << osd_op.indata << dendl;
      size_t zero_to_fill = inflight_volume_meta.get_chunk_size() - osd_op.indata.length();
      osd_op.indata.append_zero(zero_to_fill);
      dout(4) << __func__ << ": bufferlist after zerofilling " << osd_op.indata << dendl;
      // 全量覆盖写后，对象的len可能会变化，那么相应地也要修改volume元数据
      inflight_volume_meta.update_chunk(origin_oid, osd_op.op.extent.length);
      osd_op.op.extent.length = inflight_volume_meta.get_chunk_size();
      ops.push_back(inflight_volume_meta.generate_write_meta_op());
      break;
    }

    case CEPH_OSD_OP_CALL:
    {
      std::string cname;
      try
      {
        osd_op.indata.begin().copy(osd_op.op.cls.class_len, cname);
      } catch (ceph::buffer::error &e)
      {
        dout(10) << "call unable to decode class" << dendl;
        ceph_assert(false);
      }
      // 只转译新构建的Cls算子（因为原生的cls算子和基于aggregateEC的cls算子的参数解析方式不同，所以需要区分）
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
      // osd_op.op是union（包含cls字段和extent字段）结构
      // 为了避免cls字段残留的数据影响后续逻辑
      // 先对extent字段做初始化
      osd_op.op.extent.length = chunk_meta.get_offset();
      osd_op.op.extent.offset = 0;
      object_off_to_volume_off(osd_op, chunk_meta);
      break;
    }
    case CEPH_OSD_OP_READ:
    {
      object_off_to_volume_off(osd_op, chunk_meta);
      r = AGGREGATE_REDIRECT;
      break;
    }
    case CEPH_OSD_OP_ZERO:
    default:;
    }
    dout(4) << __func__ << " translate access object(oid = " << origin_oid << ") to access_volume(oid = "
      << inflight_volume_meta.get_oid() << " off =  " << osd_op.op.extent.offset
      << " len = " << osd_op.op.extent.length << ")" << dendl;
  }
  op->set_cephec_translated_op();
  return r;
}
