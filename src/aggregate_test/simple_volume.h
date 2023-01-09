/**
 * @brief 试作型简易卷
 *
 * 和Volume区别在于chunk中保存的是MOSDOp还是OpRequest
 *
 */

#ifndef CEPH_SIMPLEVOLUME_H
#define CEPH_SIMPLEVOLUME_H

#include "include/types.h"
#include "include/Context.h"
#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/hobject.h"

#include "osd/osd_types.h"
#include "osd/osd_internal_types.h"

#include "messages/MOSDOp.h"
#include "osdc/Objecter.h"
#include "osd/OpRequest.h"

#include "simple_aggregate_buffer.h"
#include <iostream>
#include <sys/syscall.h>

//chunk id
struct chunk_id_t {
  std::string name;
  // 在volume中的位置编号
  int8_t id;

public:
  chunk_id_t() : id(0) {}
  chunk_id_t(int8_t _id) : name("temp"), id(_id) {}

  operator int8_t() const { return id; }

  // TODO: 加解码函数
  // void encode(ceph::buffer::list &bl) const {
  //    using ceph::encode;
  //    encode(id, bl);
  // }
  // void decode(ceph::buffer::list::const_iterator &bl) {
  //    using ceph::decode;
  //  decode(id, bl);
  //  }

  bool operator==(const chunk_id_t& c) const {
    return ((c.name == (*this).name) && (c.id == (*this).id));
  }
  auto operator!=(const chunk_id_t& c) const {
    return ((c.name != (*this).name) || (c.id != (*this).id));
  }
};
//WRITE_CLASS_ENCODER(chunk_id_t)

/**
 * @brief chunk实际上是来保存对象的元数据，将原生对象的元数据封装后存入rocksdb
 *
 */
class chunk_t {
public:
  typedef uint8_t state_t;

  // 暂时定为固定大小，通过配置文件来调整，缺的填0
  static const int CHUNK_SIZE = 128;
  static const int32_t NO_OSD = 0x7fffffff;
  // 全0
  static constexpr state_t EMPTY = 0;
  // 有效数据
  static constexpr state_t VALID = 1;
  // 填充但删除的数据
  static constexpr state_t INVALID = 2;

  chunk_t() : chunk_id(chunk_id_t()), chunk_state(EMPTY),
              chunk_fill_offset(0), chunk_size(CHUNK_SIZE), 
	      pg_id(spg_t()), soid(hobject_t()), is_erasure(false) {}
  
  chunk_t(uint8_t _id, spg_t _pg_id, uint64_t _size = CHUNK_SIZE) : chunk_id(_id), chunk_state(EMPTY),
              chunk_fill_offset(0), chunk_size(CHUNK_SIZE), 
	      pg_id(_pg_id), soid(hobject_t()), is_erasure(false) {}


  chunk_t(uint8_t _chunk_id, uint64_t _offset, uint64_t _chunk_size, hobject_t& _soid) :
          chunk_id(_chunk_id), chunk_state(EMPTY),
          chunk_fill_offset(_offset), chunk_size(_chunk_size),
	  pg_id(spg_t()), soid(_soid), is_erasure(false) {}

  void set_from_op(uint8_t _chunk_id, uint64_t _offset, const hobject_t& _soid,  bool _is_erasure = false, int64_t _chunk_size = CHUNK_SIZE)  
  {
    chunk_id = chunk_id_t(_chunk_id);
    chunk_fill_offset = _offset;
    chunk_size = _chunk_size;
    soid = _soid;
    chunk_state = VALID;
    is_erasure = _is_erasure;
  }

  uint8_t get_chunk_id() { return chunk_id; }
  spg_t get_spg() { return pg_id; }

  void set_seq(uint8_t seq) { chunk_id = chunk_id_t(seq); }
  void set_empty() { chunk_state = EMPTY; }
  void set_oid(hobject_t& _oid) { soid = _oid; }

   bool is_empty() { return chunk_state == EMPTY; }
   bool is_valid() { return chunk_state == VALID; }
   bool is_invalid() { return chunk_state == INVALID; }
   uint64_t offset() { return chunk_fill_offset; }
   hobject_t get_oid() { return soid; }

  void clear() 
  {
    chunk_state = (chunk_fill_offset != 0)? INVALID: EMPTY;
  }
   // TODO: 加解码函数
   //void encode(ceph::buffer::list &bl) const;
   //void decode(ceph::buffer::list::const_iterator &bl);
private:
  chunk_id_t chunk_id;    // chunk id

  state_t chunk_state;
  // 计算填0部分开始的偏移
  uint64_t chunk_fill_offset;
  uint64_t chunk_size;
  // pgid信息
  spg_t pg_id;
  // object元数据
  hobject_t soid;
  // ec chunk?
  bool is_erasure;
};
//WRITE_CLASS_ENCODER(chunk_t)


class SimpleVolume;
/**
 * @brief 这里姑且保存了对象对应的写入请求，MOSDOp到chunk_info的映射
 * 写入的时候作为一层封装便于管理，参考自do_osd_ops()中写入对象需要的组件
 *
 *
 */
class SimpleChunk {
public:

  SimpleChunk(uint8_t _seq, spg_t _pg_id, uint64_t _size, SimpleVolume* _vol): chunk_info(chunk_t(_seq, _pg_id, _size)), vol(_vol) {  }

  /**
   * @brief 根据MOSDOp，查找对象的obc，初始化chunk_info
   *
   * 对于写入请求其实是创建新的obc和oi，由chunk来保存，相当于把find_object_context拆开来
   *
   * @param _request
   * @return int
   */
  chunk_t set_from_op(MOSDOp* op, const uint8_t& seq);

  chunk_t get_chunk_info() { return chunk_info; }
  MOSDOp* get_req() { return op; }

  void clear()
  { 
    chunk_info.clear();
    // TODO: 处理request 
  }

private:
  chunk_t chunk_info;

  // 指向volume的指针
  SimpleVolume* vol;

  MOSDOp* op;

  // TODO: 集成到OSD中去的时候保存的不是MOSDOp指针而是OpRequestRef
  // OpRequestRef request;
};

/**
 * @brief volume元数据，oid与chunk的映射关系
 * 要在volume set里查oid
 *
 */
class volume_t {
public:
  volume_t(int _size, int _cap, spg_t _pg_id): size(_size), cap(_cap), pg_id(_pg_id) {}
  volume_t(): volume_id(hobject_t()), size(0), cap(4), pg_id(spg_t()) {}
  
  void set_volume_id(hobject_t& oid) { volume_id = oid; }

  bool full() { return size == cap; }
  bool empty() {return size == 0; }
  int get_size() { return size; }
  int get_cap() { return cap; }
  spg_t get_spg() { return pg_id; }

  // 对象是否存在
  bool exist(hobject_t& soid) { return chunks.count(soid); }
  // 获取指定soid所在chunk（元数据）
  chunk_t get_chunk(hobject_t& soid) { return chunks[soid]; }
  
  // chunk加入volume（元数据）
  void add_chunk(const hobject_t& soid, chunk_t& chunk) 
  {
    chunks[soid] = chunk;
    size++;
  }
  // 从volume中移除chunk（元数据）
  void remove_chunk(hobject_t& soid) 
  {
    auto o = chunks.find(soid);
    if(o != chunks.end())
      chunks.erase(o); 
    size--;
  }
  // 清空volume map, cap和pgid由pool配置决定，osd运行期间不改变
  void clear()
  {
    chunks.clear();
    size = 0;
  }

  // TODO: 加解码
  // void encode(ceph::buffer::list &bl) const;
  // void decode(ceph::buffer::list::const_iterator &bl);
private:
  // 通过oid索引，其顺序作为chunk id保存在chunk_t中，在chunk创建时赋值
  std::unordered_map<hobject_t, chunk_t> chunks;
  hobject_t volume_id;

  // volume现有的chunk数
  int size;
  // volume容量
  int cap;
  // 所属pg编号
  spg_t pg_id;
};
//WRITE_CLASS_ENCODER(volume_t)


class SimpleAggregateBuffer;


class SimpleVolume {
private:
  // read from config
  static constexpr uint64_t default_volume_capacity = 4;
  static constexpr int FULL_SIGNAL = -1;

  volume_t volume_info;

public:
  SimpleVolume(CephContext* _cct, uint64_t _cap, SimpleAggregateBuffer* _buffer);
  bool full() { return volume_info.full(); }

  spg_t get_spg() { return volume_info.get_spg(); }
  uint64_t get_cap() { return volume_info.get_cap(); }

  object_info_t find_object(hobject_t soid);
  /**
   * @brief chunk加进volume
   *
   * @param chunk
   * @return int
   */
  int add_chunk(MOSDOp* op);

  void remove_chunk(hobject_t soid);
  
  void clear(); 

  void flush();

  /*
   * @return free chunk index
   */
  int _find_free_chunk();

  void dump_op();
  // chunk

private:
  CephContext* cct;
  
 
  std::vector<bool> bitmap;
  // chunk的顺序要与volume_info中chunk_set中chunk的顺序一致
  std::vector<SimpleChunk*> chunks;
  // TODO: EC块缓存
  // std::vector<SimpleECChunk*> ec_chunks;

  SimpleAggregateBuffer* volume_buffer;
  //OpRequestRef vol_op;
  MOSDOp* vol_op;
};

#endif // !CEPH_SIMPLEVOLUME_H
