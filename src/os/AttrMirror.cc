// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "AttrMirror.h"

#include "common/dout.h"
#include "common/errno.h"
#include "common/pretty_binary.h"
#include "include/denc.h"

// The mirror is maintained for a store that owns the keyspace, so its work is
// reported under that store's subsystem.
#define dout_context cct_
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "attrmirror " << spec_.attr << " "

namespace ceph::os {

namespace {

// A rebuild scans the primary keyspace, which cannot be one transaction, so
// rows are committed in batches. An interrupted pass leaves no marker and is
// redone from scratch.
constexpr uint64_t kBatchRows = 1024;

}  // namespace

AttrMirror::AttrMirror(const Spec& spec, const Port& port, CephContext* cct,
                     unsigned bit)
  : spec_(spec), port_(port), cct_(cct), bit_(static_cast<uint8_t>(1u << bit))
{
  ceph_assert(bit < 8);
}

void AttrMirror::record(std::string_view key, const ceph::buffer::ptr* value,
                       KeyValueDB::Transaction& txn, uint8_t& mask) const
{
  if (value) {
    // Shallow list: the bytes stay owned by the record being written.
    ceph::buffer::list bytes;
    bytes.push_back(*value);
    txn->set(spec_.prefix, key.data(), key.size(), bytes);
    mask |= bit_;
  } else if (mask & bit_) {
    txn->rmkey(spec_.prefix, key.data(), key.size());
    mask &= ~bit_;
  }
}

int AttrMirror::bootstrap(KeyValueDB* db, const std::string& primary_prefix,
                         const std::string& super_prefix)
{
  ceph::buffer::list marker;
  int r = db->get(super_prefix, spec_.marker, &marker);
  if (r == 0) {
    uint32_t stored_version = 0;
    auto p = marker.cbegin();
    try {
      decode(stored_version, p);
    } catch (const ceph::buffer::error&) {
      return -EIO;
    }
    return stored_version == spec_.version && p.end() ? 0 : -EOPNOTSUPP;
  }
  if (r != -ENOENT)
    return r;

  // Nothing else is writing yet, so the mirror is rebuilt rather than
  // reconciled with whatever an interrupted pass left behind.
  ldout(cct_, 1) << __func__ << " building mirror " << spec_.prefix
                 << " for " << spec_.attr << dendl;
  auto txn = db->get_transaction();
  txn->rmkeys_by_prefix(spec_.prefix);
  r = db->submit_transaction_sync(txn);
  if (r < 0)
    return r;

  txn = db->get_transaction();
  auto it = db->get_iterator(primary_prefix, KeyValueDB::ITERATOR_NOCACHE);
  uint64_t rows = 0;
  for (r = it->seek_to_first(); r >= 0 && it->valid(); r = it->next()) {
    const auto key = it->key();
    std::optional<ceph::buffer::list> value;
    r = port_.read_attr(key, it->value(), value);
    if (r < 0) {
      lderr(cct_) << __func__ << " cannot read " << spec_.attr << " from "
                  << pretty_binary_string(key) << ": " << cpp_strerror(r)
                  << dendl;
      return r;
    }
    if (!value)
      continue;
    txn->set(spec_.prefix, key, *value);
    if (++rows % kBatchRows == 0) {
      r = db->submit_transaction_sync(txn);
      if (r < 0)
        return r;
      txn = db->get_transaction();
    }
  }
  if (r < 0)
    return r;
  r = it->status();
  if (r < 0)
    return r;

  marker.clear();
  encode(spec_.version, marker);
  txn->set(super_prefix, spec_.marker, marker);
  r = db->submit_transaction_sync(txn);
  ldout(cct_, 1) << __func__ << " mirrored " << rows << " " << spec_.attr
                 << " records, result " << r << dendl;
  return r;
}

int AttrMirror::load(KeyValueDB* db, const std::string& lower,
                    const std::string& upper,
                    std::vector<std::pair<hobject_t, ceph::buffer::list>>& out) const
{
  // One iterator for the whole range: loads observe a single keyspace
  // snapshot, never a mixture of records and a later rewrite.
  auto it = db->get_iterator(spec_.prefix, KeyValueDB::ITERATOR_NOCACHE);
  int r = it->lower_bound(lower);
  for (; r >= 0 && it->valid(); r = it->next()) {
    const auto key = it->key();
    if (key >= upper)
      break;
    ghobject_t oid;
    if (port_.decode_key(key, &oid) < 0)
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