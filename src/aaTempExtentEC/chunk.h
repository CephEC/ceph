

#ifndef CEPH_CHUNK_H
#define CEPH_CHUNK_H

#include <vector>
#include <list>
#include <set>
#include <map>
#include <memory>

#include <boost/smart_ptr/local_shared_ptr.hpp>

#include "include/common_fwd.h"
#include "include/types.h"
#include "common/ceph_releases.h"
#include "osd_types.h"
#include "include/ceph_assert.h"   // cpp-btree uses system assert, blech
#include "include/encoding.h"

#include "volume.h"

//chunk id
struct chunk_id_t {
  int8_t id;

  chunk_id_t() : id(chunk_id_t::NO_CHUNK) {}
  explicit chunk_id_t(int8_t _id) : id(_id) {}

  operator int8_t() const { return id; }

  //const static chunk_id_t NO_CHUNK;

  void encode(ceph::buffer::list &bl) const {
    using ceph::encode;
    encode(id, bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    using ceph::decode;
    decode(id, bl);
  }

  bool operator==(const chunk_id_t&) const = default;
  auto operator<=>(const chunk_id_t&) const = default;
};
WRITE_CLASS_ENCODER(chunk_id_t)
const chunk_id_t chunk_id_t::NO_CHUNK(-1);

/**
 * @brief chunk实际上是来保存对象的元数据，将原生对象的元数据封装后存入rocksdb
 * 
 */
class chunk_t {
    typedef uint8_t state_t;

    // chunk 固定大小，缺的填0
    const int CHUNK_SIZE = 128;
    
    static const int32_t NO_OSD = 0x7fffffff;
    static constexpr state_t CLEAN = 0;
    static constexpr state_t DIRTY = 1;
    // 正在进行GC
    static constexpr state_t GC = 2;

    chunk_id_t() : chunk_id(chunk_t()), osd_id(NO_OSD), chunk_state(CLEAN),
                        chunk_fill_offset(0), oi(object_info_t()), soid(hobject_t()) {}

    explicit chunk_id_t(uint8_t _chunk_id, int32_t _osd_id, 
                        state_t _state, uint64_t _offset, 
                        object_info_t _oi, hobject_t _soid) : 
                        chunk_id(_chunk_id), osd_id(_osd_id), chunk_state(_state),
                        chunk_fill_offset(offset), oi(_oi), soid(_soid) {}

    int32_t get_osd_id() { return osd_id; }

    chunk_id_t get_chunk_id() { return chunk_id; };

    bool is_clean() { return chunk_state == CLEAN; };
    bool is_dirty() { return chunk_state == DIRTY; };
    bool is_gc() { return chunk_state == GC; };

    uint64_t offset() { return chunk_fill_offset; };
    object_info_t get_object_info() { return oi; };
    hobject_t get_oid() { return soid; }

    void fill(uint64_t off) { chunk_fill_offset = off; }
    


    void encode(ceph::buffer::list &bl) const;
    void decode(ceph::buffer::list::const_iterator &bl);

    bool operator==(const chunk_t&) const = default;
    //auto operator<=>(const chunk_t&) const = default;


private:
    // 加个锁？
    //ceph::mutex chunk_mutex = ceph::make_mutex("Chunk::chunk_mutex");

    int32_t osd_id; // 分片所在osd
    chunk_id_t chunk_id;    // chunk id

    state_t chunk_state;
    // 计算填0部分开始的偏移
    uint64_t chunk_fill_offset;
    
    // object元数据
    object_info_t oi;
    hobject_t soid;
}
WRITE_CLASS_ENCODER(chunk_t)

/**
 * @brief 这里姑且保存了对象对应的写入请求，OSDOp到chunk_info的映射
 * 写入的时候作为一层封装便于管理，参考自do_osd_ops()中写入对象需要的组件
 * 
 * 
 */
class Chunk {
public:

    explicit Chunk(chunk_info_t _chunk_info, OpRequestRef _request, OSDOp* _op, OpContext* _op_ctx) : 
        chunk_info(_chunk_info), request(_request), op(_op), op_ctx(_op_ctx) { }

    Chunk() = delete;

    chunk_t get_chunk_info() { return chunk_info; }
    OpRequestRef get_req() { return request; }
    OSDOp* get_op() { return op; }
    OpContext* get_op_ctx() { return op_ctx; };

private:
    chunk_t chunk_info;

    // 指向volume的指针
    Volume* vol;

    OpRequestRef request;
    OSDOp* op;
    // op_ctx在初始化的时候需要一个指向PrimaryLogPG的指针，需要在外部初始化之后
    OpContext* op_ctx;
    // ObjectState new_obs;
    // SnapSetContext* snap_ctx;
}


#endif // ! CEPH_CHUNK_H