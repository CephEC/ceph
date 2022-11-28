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
#include "Objecter.h"
#include "OpRequest.h"

#include <iostream>

//chunk id
struct chunk_id_t {
  int8_t id;

  chunk_id_t() : id(0) {}
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
    
    // pgid信息
    pg_t pg_id; 
    // object元数据
    object_info_t oi;
    hobject_t soid;
}
WRITE_CLASS_ENCODER(chunk_t)

/**
 * @brief 这里姑且保存了对象对应的写入请求，MOSDOp到chunk_info的映射
 * 写入的时候作为一层封装便于管理，参考自do_osd_ops()中写入对象需要的组件
 * 
 * 
 */
class SimpleChunk {
public:

    explicit SimpleChunk(Volume* _vol);

    // temp create object context
    int _create_object_context(const hobject_t& oid, ObjectContextRef *pobc);

    /**
     * @brief 根据MOSDOp，查找对象的obc，初始化chunk_info
     * 
     * 对于写入请求其实是创建新的obc和oi，由chunk来保存，相当于把find_object_context拆开来
     * 
     * @param _request 
     * @return int 
     */
    int set_from_op(MOSDOp* _request/*, OSDMap&*/);


    chunk_t get_chunk_info() { return chunk_info; }
    MOSDOp* get_req() { return request; }

private:
    chunk_t chunk_info;

    // 指向volume的指针
    Volume* vol;

    ObjectContextRef obc;
    MOSDOp* op;
    // OpRequestRef request;
    // OSDOp* op;
    // op_ctx在初始化的时候需要一个指向PrimaryLogPG的指针，需要在外部初始化之后
    // OpContext* op_ctx;
    // ObjectState new_obs;
    // SnapSetContext* snap_ctx;
}


class SimpleVolume {
  public:
    // read from config
    constexpr uint64_t default_volume_capacity = 4;
    constexpr int FULL_SIGNAL = -1;

    volume_t volume_info;

public:
    SimpleVolume(CephContext* _cct, uint64_t _cap, SimpleAggregationCache* _cache);

    SimpleVolume() : 

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
    int add_chunk(MOSDOp* op, const OSDMap &osdmap);

    void remove_chunk(hobject_t soid);
    void clear();

    bool flush();

    

// chunk


private:
    CephContext* cct;

    ceph::mutex flush_lock;
    ceph::condition_variable flush_cond;
    SafeTimer flush_timer;
    Context* flush_callback = nullptr;

    //Context *connect_retry_callback = nullptr;

    bool is_flushing = false;

    std::vector<bool> bitmap;
    // chunk的顺序要与volume_info中chunk_set中chunk的顺序一致
    std::vector<Chunk*> chunks;
    uint64_t cap;
    uint64_t size;
    
    SimpleAggregationCache* cache;
    
    
    // create_request()
    //OpRequestRef vol_op
    MOSDOp* vol_op;
}

#endif // !CEPH_SIMPLEVOLUME_H