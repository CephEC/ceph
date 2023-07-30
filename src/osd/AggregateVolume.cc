/**
 * volume
 */

#include "AggregateVolume.h"
#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

using std::ostream;
using std::ostringstream;

static ostream& _prefix(std::ostream *_dout, const Volume *buf) {
  return *_dout << "aggregate volume. ******* "; 
}

Volume::Volume(CephContext* _cct, uint32_t _cap, const spg_t& _pg_id)
  : volume_info(_cap, _pg_id),
    vol_op(nullptr),
    cct(_cct)
{} 

Volume::~Volume()
{
  for (auto c: chunks)
      delete c;
}

void Volume::clear()
{
  for (auto i: chunks) {
    i->clear();
  }
  volume_info.clear();
}

void Volume::init(uint64_t _cap, uint64_t _chunk_size)
{
  volume_info.set_cap(_cap);
  volume_info.reset_chunk_bitmap();
  volume_info.set_chunk_size(_chunk_size);
  // 预分配Chunk
  for (uint8_t i = 0; i < _cap; i++) {
    Chunk* c = new Chunk(cct, i, get_spg(), this);
    chunks.push_back(c);
  }
}


int Volume::add_chunk(OpRequestRef op, MOSDOp* m) 
{
  uint32_t free_chunk_index = volume_info.find_free_chunk();
  // new Chunk
  Chunk* new_chunk = chunks[free_chunk_index];
  // init chunk & return its metadata
  chunk_t chunk_meta = new_chunk->set_from_op(op, m, free_chunk_index, volume_info.get_chunk_size());
  if (chunk_meta == chunk_t()) {
    dout(4) << " add_chunk failed" << " set_from_op failed " << dendl;
    return -1;
  }
  // 最后处理volume元数据
  volume_info.add_chunk(chunk_meta.get_oid(), chunk_meta);
  return 0;
}

MOSDOp* Volume::_prepare_volume_op(MOSDOp *m)
{
  auto volume_m = new MOSDOp(m->get_client_inc(), m->get_tid(),
		      volume_info.get_oid(), volume_info.get_spg(),
		      m->get_map_epoch(),
		      m->get_flags() | CEPH_OSD_FLAG_AGGREGATE, m->get_features());
  // oloc? ops?
  volume_m->set_snapid(m->get_snapid());
  volume_m->set_snap_seq(m->get_snap_seq());
  volume_m->set_snaps(m->get_snaps());

  volume_m->set_mtime(m->get_mtime());
  volume_m->set_retry_attempt(m->get_retry_attempt());

  // 优先级
  if (m->get_priority())
    volume_m->set_priority(m->get_priority());

  if (m->get_reqid() != osd_reqid_t()) {
    volume_m->set_reqid(m->get_reqid());
  }

  volume_m->set_header(m->get_header());
  volume_m->set_footer(m->get_footer());

  return volume_m;
  
}

MOSDOp* Volume::generate_op()
{
  // 生成新op然后将数据部分接在op的后面，并且要修改op的部分flag
  MOSDOp* newest_m = nullptr;
  MOSDOp* volume_m = nullptr;
  for (auto iter = chunks.begin(); iter != chunks.end(); iter++) {
    if (!(*iter)->is_valid()) continue;
    if(!newest_m) {
      // 使用第一个有效chunk中的Op来初始化Volume Op
      newest_m = static_cast<MOSDOp*>((*iter)->get_req()->get_nonconst_req());
      volume_m = _prepare_volume_op(newest_m);
    }
    auto &ops = (*iter)->get_ops();
    (volume_m->ops).insert((volume_m->ops).end(), ops.begin(), ops.end());
  }
  volume_m->set_connection(ConnectionRef());
    
  // 将volume_t元数据编码封装为一个写扩展属性的OSDOp
  // 这个OSDOp放置在MOSDOp中OSDOp数组的最末端，便于在on_commit回调中找到它
  OSDOp write_meta_op;
  volume_info.generate_write_meta_op(write_meta_op);
  (volume_m->ops).push_back(write_meta_op);

  volume_m->encode_payload(volume_m->get_features());
  return volume_m;
}
