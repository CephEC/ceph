// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#ifndef CEPH_CLS_PARQUET_SCAN_TYPES_H
#define CEPH_CLS_PARQUET_SCAN_TYPES_H

#include <cstdint>
#include <string>

#include "include/buffer.h"
#include "include/encoding.h"

namespace ceph::parquet_scan {

inline constexpr uint32_t MAX_REQUEST_BYTES = 1024 * 1024;

// The request is raw JSON; only the response uses Ceph's versioned encoding.
struct ScanResponse {
  std::string stats_json;
  ceph::bufferlist ipc;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(stats_json, bl);
    encode(ipc, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(stats_json, bl);
    decode(ipc, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(ScanResponse)

} // namespace ceph::parquet_scan

#endif
