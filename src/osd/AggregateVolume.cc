/**
 * volume
 */

#include "AggregateVolume.h"


Volume::Volume(uint64_t _cap, uint64_t _chunk_size, const spg_t& _pg_id/*, AggregateBuffer* _buffer*/)
  : volume_info(_cap, _pg_id),
    /*volume_buffer(_buffer),*/
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


int Volume::add_chunk(OpRequestRef op) 
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
  chunk_t chunk_meta = new_chunk->set_from_op(op, free_chunk_index);
  if (chunk_meta == chunk_t()) {
    return -1;
  }
  // 最后处理volume元数据
  volume_info.add_chunk(chunk_meta.get_oid(), chunk_meta);
  return 0;
}
