// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/hobject.h"
#include "include/buffer.h"
#include "kv/KeyValueDB.h"

namespace ceph::os {

/**
 * Keep an opaque object attribute in a separate keyspace for range scans.
 * The onode attribute remains authoritative and follows normal object
 * replication, rollback and recovery. Both copies are maintained in the same
 * transaction from object creation; mounting never rebuilds missing rows.
 */
class AttrMirror {
public:
  AttrMirror(std::string attr, std::string prefix)
    : attr_(std::move(attr)), prefix_(std::move(prefix)) {}

  const std::string& attr() const { return attr_; }

  // nullptr removes the row. Track whether a row exists to avoid issuing
  // deletes for ordinary objects that never carried the attribute.
  void record(std::string_view key, const ceph::buffer::ptr* value,
              KeyValueDB::Transaction& txn, bool& present) const;

  // Append published head rows in [lower, upper), decoding only object keys.
  int load(KeyValueDB* db, const std::string& lower, const std::string& upper,
           int (*decode_key)(const std::string&, ghobject_t*),
           std::vector<std::pair<hobject_t, ceph::buffer::list>>& out) const;

private:
  std::string attr_;
  std::string prefix_;
};

}  // namespace ceph::os
