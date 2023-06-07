#include "common/hobject.h"

struct opencv_thumbnail_op_t {
  hobject_t oid;
  bool is_ratio_shape = true;

  union {
    #pragma pack(push, 4)
    struct {
      uint32_t x;
      uint32_t y;
    } fixed;
    struct {
      float x;
      float y;
    } ratio;
    #pragma pack(pop)
  } shape;

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(oid, bl);
    encode(is_ratio_shape, bl);
    if(is_ratio_shape) {
      encode(shape.ratio.x, bl);
      encode(shape.ratio.y, bl);
    } else {
      encode(shape.fixed.x, bl);
      encode(shape.fixed.y, bl);
    }
    ENCODE_FINISH(bl);
  }


  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(oid, bl);
    decode(is_ratio_shape, bl);
    if(is_ratio_shape) {
      decode(shape.ratio.x, bl);
      decode(shape.ratio.y, bl);
    } else {
      decode(shape.fixed.x, bl);
      decode(shape.fixed.y, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(opencv_thumbnail_op_t)
