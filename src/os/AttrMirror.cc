// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "AttrMirror.h"

#include <cerrno>

namespace ceph::os {

void AttrMirror::record(std::string_view key, const ceph::buffer::ptr* value,
                       KeyValueDB::Transaction& txn, bool& present) const
{
  if (value) {
    // Shallow list: the bytes stay owned by the record being written.
    ceph::buffer::list bytes;
    bytes.push_back(*value);
    txn->set(prefix_, key.data(), key.size(), bytes);
    present = true;
  } else if (present) {
    txn->rmkey(prefix_, key.data(), key.size());
    present = false;
  }
}

int AttrMirror::load(KeyValueDB* db, const std::string& lower,
                    const std::string& upper,
                    int (*decode_key)(const std::string&, ghobject_t*),
                    std::vector<std::pair<hobject_t, ceph::buffer::list>>& out) const
{
  // One iterator for the whole range: loads observe a single keyspace
  // snapshot, never a mixture of records and a later rewrite.
  auto it = db->get_iterator(prefix_, KeyValueDB::ITERATOR_NOCACHE);
  int r = it->lower_bound(lower);
  for (; r >= 0 && it->valid(); r = it->next()) {
    const auto key = it->key();
    if (key >= upper)
      break;
    ghobject_t oid;
    if (decode_key(key, &oid) < 0)
      return -EIO;
    // Recovery temporaries and rollback generations are not published heads.
    if (oid.generation == ghobject_t::NO_GEN &&
        oid.hobj.snap == CEPH_NOSNAP && oid.hobj.pool >= 0) {
      out.emplace_back(std::move(oid.hobj), it->value());
    }
  }
  return r < 0 ? r : it->status();
}

}  // namespace ceph::os
