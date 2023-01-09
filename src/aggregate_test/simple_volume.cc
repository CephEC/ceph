#include "simple_volume.h"

/**
 * simple chunk
 */

chunk_t SimpleChunk::set_from_op(MOSDOp* _op, const uint8_t& seq)
{
  cout << __func__ << " " << _op << std::endl;
  const hobject_t& oid = _op->get_hobj();
  // spg_t pg_id = _op->get_spg();
  // if (pg_id != chunk_info.get_spg())
    // return chunk_t();
  uint64_t data_len = _op->get_data_len();
  // TODO: 检查data_len是否大于配置文件中的chunk_size 
  chunk_info.set_from_op(seq, data_len, oid);
 
  this->op = _op;

  return chunk_info;
}

/**
 * simple volume
 */


SimpleVolume::SimpleVolume(CephContext* _cct, uint64_t _cap, SimpleAggregateBuffer* _buffer)
  : cct(_cct),
    volume_info(0, _cap, spg_t()),
    volume_buffer(_buffer),
    vol_op(nullptr)
{
  // 有没有更好看的写法... 
  bitmap.resize(_cap);
  bitmap.assign(_cap, false);

  // 预分配Chunk
  for (uint8_t i = 0; i < _cap; i++) {
    // TOMODIFY: 加入OSDMap和pool的各种信息
    SimpleChunk* c = new SimpleChunk(i, spg_t(), 128, this);
    chunks.push_back(c);
  }

  // TODO: 预分配EC Chunk，这里需要获取ec pool的配置，m的值

} 

void SimpleVolume::clear()
{
  for (auto i: bitmap) {
    i = false;
  }
  for (auto i: chunks) {
    i->clear();
  }  
  // TODO: 处理vol_op
  volume_info.clear(); 
  std::cout << "clear buffer " << this << std::endl; 
}



int SimpleVolume::_find_free_chunk()
{
  int free_index = 0;
  while (free_index < volume_info.get_cap() && bitmap[free_index]) free_index++;
  return free_index;
}


int SimpleVolume::add_chunk(MOSDOp* op) 
{
  cout << "add chunk " << op << " in volume " <<  this << std::endl;
  int ret = 0;

  int free_chunk_index = _find_free_chunk();
  if (free_chunk_index >= volume_info.get_cap()) {
    cout << "failed to add chunk, volume is full." << std::endl;
    return -1;
  }
  
  bitmap[free_chunk_index] = true;
  // new SimpleChunk
  SimpleChunk* new_chunk = chunks[free_chunk_index];
  // init chunk & return its metadata
  chunk_t chunk_meta = new_chunk->set_from_op(op, free_chunk_index);

  cout << "allocate chunk index " << free_chunk_index << std::endl;
  // 最后处理volume元数据
  volume_info.add_chunk(op->get_hobj(), chunk_meta);
  return 0;
}

void SimpleVolume::flush()
{
  // volume_buffer->pending_to_flush.push_back(this);
  //return true;
}

void SimpleVolume::dump_op()
{
  OSDOp& osd_op = vol_op->ops.back();
  cout << "do_op " << vol_op << std::endl
	  << "opcode: " << osd_op.op.op << std::endl
	  << "length: " << osd_op.op.extent.length << std::endl;
}
