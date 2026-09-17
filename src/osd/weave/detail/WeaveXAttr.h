// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <cerrno>
#include <string>

#include "common/hobject.h"
#include "messages/MOSDOp.h"

namespace ceph::weave {

// Name of the encoded Volume metadata inside the Volume object. The ObjectStore
// projection carries the same attribute with a leading underscore.
constexpr const char* kVolumeMetaAttr = "volume_meta";
constexpr const char* kVolumeMetaXattr = "_volume_meta";

inline std::string xattr_prefix(const hobject_t &oid) {
  // hobject_t::to_str() includes pool/hash/snap/oid/key/namespace and escapes
  // separators, giving each logical object an exact prefix inside the Volume.
  return "aggregate_ec." + oid.to_str() + ".";
}

inline std::string xattr_name(const hobject_t &oid, const std::string &name) {
  return xattr_prefix(oid) + name;
}

inline int rewrite_xattr_op(
  OSDOp &osd_op, const hobject_t &oid, bool includes_value) {
  // Xattr payloads are encoded as name followed by an optional value. Rebuild
  // the payload because changing name_len shifts the value boundary.
  try {
    auto p = osd_op.indata.cbegin();
    std::string key;
    ceph::buffer::list value;
    p.copy(osd_op.op.xattr.name_len, key);
    if (includes_value) p.copy(osd_op.op.xattr.value_len, value);

    // Namespace the key under the prefix that getxattrs enumerates by.
    key = xattr_name(oid, key);
    osd_op.op.xattr.name_len = key.size();
    osd_op.indata.clear();
    osd_op.indata.append(key);
    if (includes_value) osd_op.indata.append(value);
  } catch (const ceph::buffer::error &) {
    return -EINVAL;
  }
  return 0;
}

}  // namespace ceph::weave
