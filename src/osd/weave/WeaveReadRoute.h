// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include "osd/osd_types.h"

namespace ceph::weave {

// A routing hint, not authority to access a physical object. The receiving OSD
// checks the original logical request against its own durable Volume metadata.
struct WeaveReadRoute {
  hobject_t volume;
  pg_shard_t target;
  epoch_t map_epoch = 0;
  eversion_t version;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(volume, bl);
    encode(target, bl);
    encode(map_epoch, bl);
    encode(version, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    DECODE_START(1, p);
    decode(volume, p);
    decode(target, p);
    decode(map_epoch, p);
    decode(version, p);
    DECODE_FINISH(p);
  }
};

WRITE_CLASS_ENCODER(WeaveReadRoute)

}  // namespace ceph::weave
