#include "common/hobject.h"

struct graph_op_t {
  std::vector<int> src_nodes;
  std::vector<int> sample_nums;

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(src_nodes, bl);
    encode(sample_nums, bl);
    ENCODE_FINISH(bl);
  }


  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(src_nodes, bl);
    decode(sample_nums, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(graph_op_t)
