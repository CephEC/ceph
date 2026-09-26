// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/hobject.h"
#include "include/buffer.h"
#include "kv/KeyValueDB.h"

class CephContext;

namespace ceph::os {

/**
 * AttrMirror - a mirrored onode attribute.
 *
 * The attribute is authoritative and lives on the object, so it is replicated,
 * rolled back, recovered, exported and repaired together with the object's
 * data. This class maintains a second copy of exactly those bytes under a
 * separate keyspace, keyed like the primary record, so that a range scan can
 * enumerate the carriers of the attribute without decoding primary records.
 *
 * The mirror is derived state: it is always rebuilt from the attribute and
 * never the other way round. That is why the attribute name is the public
 * identity of a mirror, while its keyspace stays private to the store.
 */
class AttrMirror {
public:
  struct Spec {
    // Onode attribute holding the authoritative bytes.
    std::string attr;
    // Keyspace holding the mirror.
    std::string prefix;
    // Key under the store's super prefix recording that the mirror is
    // complete, valued with the layout version.
    std::string marker;
    uint32_t version = 1;
  };

  // Everything a mirror needs from the store that owns both keyspaces: how to
  // read the attribute out of one primary record, and what a mirror key names.
  struct Port {
    // Report the attribute bytes this primary record carries: *out holds them,
    // or is unset when the record is not a carrier. 0 on success; a negative
    // result aborts the rebuild instead of leaving a silently short mirror.
    std::function<int(const std::string& key, const ceph::buffer::list& value,
                      std::optional<ceph::buffer::list>& out)> read_attr;
    // Recover the object a mirror key names. Mirror keys are the primary
    // record's key bytes, so a range computed by the store selects exactly one
    // collection.
    std::function<int(const std::string& key, ghobject_t* oid)> decode_key;
  };

  // `bit` is the mask bit this mirror owns; the owner assigns one per mirror.
  AttrMirror(const Spec& spec, const Port& port, CephContext* cct, unsigned bit);

  const std::string& attr() const { return spec_.attr; }

  // Mirror one record in the same transaction that writes it. `value` is the
  // attribute as committed, or nullptr when the record no longer carries it
  // (attribute removed, or the record itself is gone). `mask` reports which
  // mirrors this record currently has, so a drop removes a row that exists
  // instead of deleting blindly on every write.
  void record(std::string_view key, const ceph::buffer::ptr* value,
              KeyValueDB::Transaction& txn, uint8_t& mask) const;

  // Mirror every primary record carrying the attribute. Gated by the
  // completion marker: whether the mirror is complete cannot be read off the
  // mirror itself, since an absent row is the normal state of most records, so
  // completeness is declared instead. An unrecognized marker version refuses
  // rather than guesses at a layout it does not know.
  int bootstrap(KeyValueDB* db, const std::string& primary_prefix,
                const std::string& super_prefix);

  // Append the mirror rows whose keys fall in [lower, upper).
  int load(KeyValueDB* db, const std::string& lower, const std::string& upper,
           std::vector<std::pair<hobject_t, ceph::buffer::list>>& out) const;

private:
  Spec spec_;
  Port port_;
  CephContext* cct_ = nullptr;
  uint8_t bit_ = 0;
};

}  // namespace ceph::os