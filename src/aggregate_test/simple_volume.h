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
  static constexpr state_t CLEAN = 0;
  static constexpr state_t DIRTY = 1;
  static constexpr state_t GC = 2;

  chunk_t() : osd_id(NO_OSD), chunk_id(chunk_id_t()), chunk_state(CLEAN),
                 chunk_fill_offset(0), seq(0), pg_id(pg_t()), oi(object_info_t()), soid(hobject_t()) {}

  chunk_t(int32_t _osd_id, uint8_t _chunk_id,
                 uint64_t _offset, object_info_t& _oi, hobject_t& _soid) :
                 osd_id(_osd_id), chunk_id(_chunk_id), chunk_state(CLEAN),
                 chunk_fill_offset(_offset), seq(_chunk_id), pg_id(pg_t()), oi(_oi), soid(_soid) {}

  int32_t get_osd_id() { return osd_id; }
  chunk_id_t get_chunk_id() { return chunk_id; }

  void set_seq(uint8_t seq) { chunk_id = chunk_id_t(seq); }
  void set_object_info(object_info_t& _oi) { oi = _oi;  }

   bool is_clean() { return chunk_state == CLEAN; }
   bool is_dirty() { return chunk_state == DIRTY; }
   bool is_gc() { return chunk_state == GC; }
   uint64_t offset() { return chunk_fill_offset; }
   object_info_t get_object_info() { return oi; }
   hobject_t get_oid() { return soid; }

   void fill(uint64_t off) { chunk_fill_offset = off; }

   // TODO: 加解码函数
   //void encode(ceph::buffer::list &bl) const;
   //void decode(ceph::buffer::list::const_iterator &bl);
private:
  int32_t osd_id; // 分片所在osd
  chunk_id_t chunk_id;    // chunk id

  state_t chunk_state;
  // 计算填0部分开始的偏移
  uint64_t chunk_fill_offset;

  uint8_t seq;

  // pgid信息
  pg_t pg_id;
  // object元数据
  object_info_t oi;
  hobject_t soid;
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

  SimpleChunk(uint8_t _seq, SimpleVolume* _vol): seq(_seq), vol(_vol) {  }
  // temp create object context
  int _create_object_context(const hobject_t& oid/*, ObjectContextRef *pobc*/);

  /**
   * @brief 根据MOSDOp，查找对象的obc，初始化chunk_info
   *
   * 对于写入请求其实是创建新的obc和oi，由chunk来保存，相当于把find_object_context拆开来
   *
   * @param _request
   * @return int
   */
  int set_from_op(MOSDOp* op/*, OSDMap& osdmap*/);

  chunk_t get_chunk_info() { return chunk_info; }
  MOSDOp* get_req() { return op; }
  private:
  chunk_t chunk_info;
  // 在volume中的顺序编号
  uint8_t seq;

  // 指向volume的指针
  SimpleVolume* vol;

  // ObjectContextRef obc;
  MOSDOp* op;

  // TODO: 集成到OSD中去的时候保存的不是MOSDOp指针，以下是可能需要额外保存的上下文信息
  // OpRequestRef request;
  // OSDOp* op;
  // op_ctx在初始化的时候需要一个指向PrimaryLogPG的指针，需要在外部初始化之后
  // OpContext* op_ctx;
  // ObjectState new_obs;
  // SnapSetContext* snap_ctx;
};

/**
 * @brief volume元数据，oid与chunk的映射关系
 * 要在volume set里查oid
 *
 */
class volume_t {
public:
  volume_t(int _size, int _cap, pg_t _pg_id): size(_size), cap(_cap), pg_id(_pg_id) {}
  volume_t(): volume_id(hobject_t()), size(0), cap(4), pg_id(pg_t()) {}
  bool full() { return size == cap; }
  bool empty() {return size == 0; }

  // 对象是否存在
  bool exist(hobject_t soid) { return chunks.count(soid); }
  // 获取指定soid所在chunk（元数据）
  chunk_t get_chunk(hobject_t& soid) { return chunks[soid]; }
  // chunk加入volume（元数据）
  void add_chunk(hobject_t& soid, chunk_t& chunk) {
    chunks[soid] = chunk;
    size++;
  }
  // TODO: 从volume中移除chunk（元数据）
  void remove_chunk(hobject_t soid);
  // TODO: 清空volume
  void clear();

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
  pg_t pg_id;
};
//WRITE_CLASS_ENCODER(volume_t)

class SimpleAggregateBuffer;

class SimpleVolume {
public:
  // read from config
  static constexpr uint64_t default_volume_capacity = 4;
  static constexpr int FULL_SIGNAL = -1;

  volume_t volume_info;

public:
  SimpleVolume(CephContext* _cct, uint64_t _cap, SimpleAggregateBuffer* _buffer);
  bool full() { return size == cap; }
  bool empty() { return size == 0; }
  bool flushing() { return is_flushing; }

  object_info_t find_object(hobject_t soid);
  /**
   * @brief chunk加进volume
   *
   * @param chunk
   * @return int
   */
  int add_chunk(MOSDOp* op/*, const OSDMap& osdmap*/);

  void remove_chunk(hobject_t soid);
  void clear();

  bool flush();
  // chunk

private:
  CephContext* cct;
  ceph::mutex flush_lock = ceph::make_mutex("SimpleVolume::flush_lock");
  ceph::condition_variable flush_cond;
  SafeTimer* flush_timer = NULL;
  Context* flush_callback = nullptr;
  //Context *connect_retry_callback = nullptr;
    bool is_flushing = false;
  std::vector<bool> bitmap;
  // chunk的顺序要与volume_info中chunk_set中chunk的顺序一致
  std::vector<SimpleChunk*> chunks;
  // TODO: EC块缓存
  // std::vector<SimpleECChunk*> ec_chunks;
  uint64_t cap;
  uint64_t size;

  SimpleAggregateBuffer* volume_buffer;
  //OpRequestRef vol_op;
   MOSDOp* vol_op;
};

#endif // !CEPH_SIMPLEVOLUME_H
