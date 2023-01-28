/**
 * volume
 */

#include "AggregateVolume.h"


Volume::Volume(uint64_t _cap, uint64_t _chunk_size, const spg_t& _pg_id)
  : volume_info(_cap, _pg_id),
    vol_op(nullptr)
{
 
  bitmap.resize(_cap);
  bitmap.assign(_cap, false);

  // 预分配Chunk
  for (uint8_t i = 0; i < _cap; i++) {
    Chunk* c = new Chunk(i, _pg_id, _chunk_size, this);
    chunks.push_back(c);
  }

  // TODO: 预分配EC Chunk，这里需要获取ec pool的配置，m的值

} 

Volume::~Volume()
{
    for (auto c: chunks)
        delete c;
}

void Volume::clear()
{
  for (auto i: bitmap) {
    i = false;
  }
  for (auto i: chunks) {
    i->clear();
  }  
  // TODO: 处理vol_op
  volume_info.clear(); 
}



int Volume::_find_free_chunk()
{
  int free_index = 0;
  while (free_index < volume_info.get_cap() && bitmap[free_index]) 
    free_index++;
  return free_index;
}


int Volume::add_chunk(OpRequestRef op, MOSDOp* m) 
{
  int ret = 0;
  
  // TODO：查找oid是否存在（覆盖写情况）

  int free_chunk_index = _find_free_chunk();
  if (free_chunk_index >= volume_info.get_cap()) {
    return -1;
  }
  
  bitmap[free_chunk_index] = true;
  // new Chunk
  Chunk* new_chunk = chunks[free_chunk_index];
  // init chunk & return its metadata
  chunk_t chunk_meta = new_chunk->set_from_op(op, m, free_chunk_index);
  if (chunk_meta == chunk_t()) {
    return -1;
  }
  // 最后处理volume元数据
  volume_info.add_chunk(chunk_meta.get_oid(), chunk_meta);
  return 0;
}

MOSDOp* Volume::_prepare_volume_op(MOSDOp *m)
{
  // 这里直接用了其中一个写入的obj的名字
  volume_info.seg_volume_id(m->get_hobj()); 

  auto volume_m = new MOSDOp(m->get_client_inc(), newest_m->get_tid(),
		      m->get_hobj(), m->get_spg(),
		      m->get_map_epoch(),
		      m->get_flags(), m->get_features());
  // oloc? ops?
  volume_m->set_snapid(m->get_snapid());
  volume_m->set_snap_seq(m->get_snap_seq());
  volume_m->set_snaps(m->get_snaps());

  volume_m->ops = m->ops; // vector<OSDOp>
  volume_m->set_mtime(m->get_mtime());
  volume_m->set_retry_attempt(m->get_retry_attempt());

  // 优先级
  if (m->priority)
    volume_m->set_priority(m->priority);
  // else
  //   volume_m->set_priority(cct->_conf->osd_client_op_priority);

  if (m->reqid != osd_reqid_t()) {
    volume_m->set_reqid(m->reqid);
  }

  return volume_m;
  
}



// 倾向于用最后一个op的信息隐藏前面的op，保留了最新的OSDMap？
// 生成一个新op吧，不然很混乱
// 部分写的逻辑还需要完善，因为需要读过来再写回去
OpRequestRef Volume::generate_op()
{
  // TODO: 生成新op然后将数据部分接在op的后面，并且要修改op的部分flag
  MOSDOp* newest_m = nullptr;
  Chunks* newest_c = nullptr;
  // 逆序
  std::vector<Chunks*>::iterator c = chunks.begin();
  while (c != chunks.end()) {
    if ((*c)->is_valid()) {
      newest_m = (*c)->get_nonconst_message();
      newest_c = *c; 
      c++;
      break;
    }
    c++;
  }

  MOSDOp* volume_m;
  
  if (newest_m) {
    volume_m = _prepare_volume_op(newest_m);

    // TODO: 拼装&encode，如果不事先encode，message会被截断
    // encode的开销？
  
    while (c != chunks.end()) {
      if ((*c)->is_valid()) {
        MOSDOp* temp_m = (*c)->get_nonconst_message();
        append_data(newest_m, temp_m);
      }
      c++;
    }
  }
  
  

  // TODO： 生成OpRequest
  OpRequestRef volume_op = op_tracker.create_request<OpRequest, Message*>(m);
  
  return volume_op;
}