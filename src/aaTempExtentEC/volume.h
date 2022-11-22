#ifndef CEPH_VOLUME_H
#define CEPH_VOLUME_H

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

#include "chunk.h"

/**
 * @brief volume元数据，oid与chunk的映射关系
 * 要在volume set里查oid
 * 
 */
class volume_t {
public:
    

    volume_t(int _cap, pg_t _pg_id) : cap(_cap), size(_size), pg_id(_pg_id) {};
    // volume_t() = default, 

    bool exist(hobject_t oid);

    chunk_t get_chunk(hobject_t oid) { return chunk_set[oid]; }

    void add_chunk(hobject_t soid, chunk_t chunk);
    void remove_chunk(hobject_t soid);
    void clear();

    void encode(ceph::buffer::list &bl) const;
    void decode(ceph::buffer::list::const_iterator &bl);

private:
    // chunk是有序的
    std::vector<pair<hobject_t, chunk_t>> chunk_set;

    uint64_t volume_id;
    // volume现有的chunk数
    int size;
    // volume容量
    int cap;
    // 所属pg编号
    pg_t pg_id;
};
WRITE_CLASS_ENCODER(volume_t)
const volume_t volume_t::NO_VOL(-1);


/**
 * @brief 若干个chunk的集合
 * 
 */
class Volume {
    volume_t volume_info;

public:
    Volume(volume_t _volume) : volume_info(_volume) { }

    object_info_t find_object(hobject_t soid);
    void add_chunk(MOSDOp* op, chunk_info_t chunk_info, Chunk* chunk);

    void remove_chunk(hobject_t soid);
    void clear();


private:

    // chunk的顺序要与volume_info中chunk_set中chunk的顺序一致
    std::vector<Chunk*> chunks;
    
    // 计时器 safe timer

    

    // create_request()
    OpRequestRef vol_op

}

#endif // ! CEPH_VOLUME_H