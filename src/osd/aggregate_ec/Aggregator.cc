// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "Aggregator.h"
#include "XAttr.h"

#include "common/debug.h"
#include "include/ceph_assert.h"
#include "objclass/objclass.h"
#include "osd/ClassHandler.h"
#include "osd/OSD.h"
#include "osd/PrimaryLogPG.h"

#include <algorithm>
#include <iterator>
#include <utility>

#define dout_context cct_
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "aggregate_ec "

namespace ceph::aggregate_ec {

namespace {
OSDOp make_zero_op(uint64_t offset, uint64_t length) {
  OSDOp op;
  op.op.op = CEPH_OSD_OP_ZERO;
  op.op.extent.offset = offset;
  op.op.extent.length = length;
  return op;
}

void rewrite_getxattrs(OSDOp &osd_op, const hobject_t &origin) {
  // GETXATTRS still runs against the physical Volume.  Pass the complete
  // logical-object prefix so PrimaryLogPG can filter and strip only this
  // object's attributes from the Volume-wide result.
  auto prefix = xattr_prefix(origin);
  osd_op.indata.clear();
  osd_op.op.xattr.name_len = prefix.size();
  osd_op.indata.append(prefix);
}

} // anonymous namespace

struct Aggregator::TranslationContext {
  TranslationContext(
    hobject_t origin, const VolumeMeta &metadata, chunk_t chunk,
    utime_t logical_mtime)
    : origin(std::move(origin)), volume_oid(metadata.volume_oid),
      volume_info(metadata.info), chunk(std::move(chunk)),
      logical_mtime(logical_mtime),
      slot_size(volume_info.get_chunk_size()),
      volume_offset(
        static_cast<uint64_t>(this->chunk.get_chunk_id()) * slot_size),
      final_logical_size(this->chunk.get_offset()) {}

  // Translation works on a private metadata snapshot.  The catalog is not
  // updated until the rewritten request completes successfully.
  hobject_t origin;
  hobject_t volume_oid;
  volume_t volume_info;
  chunk_t chunk;
  utime_t logical_mtime;
  uint64_t slot_size;
  uint64_t volume_offset;
  uint64_t final_logical_size;
  bool writes_metadata = false;
  bool deletes_object = false;
  int result = PREPROCESS_CONTINUE;
};

Aggregator::Aggregator(
  CephContext *cct, spg_t pgid, OSDService *osd, PrimaryLogPG *pg)
  : cct_(cct), pgid_(std::move(pgid)), osd_(osd), pg_(pg),
    flush_timer_(cct, timer_lock_), catalog_(pgid_) {}

Aggregator::~Aggregator() {
  shutdown();
}

void Aggregator::activate(
  uint8_t data_chunks, uint64_t chunk_size,
  bool flush_timer_enabled, double flush_timeout) {
  volume_capacity_ = data_chunks;
  chunk_size_ = chunk_size;
  flush_timer_enabled_ = flush_timer_enabled;
  flush_timeout_ = std::max(0.001, flush_timeout);
  cached_chunks_.resize(volume_capacity_);
  if (flush_timer_enabled_ && !timer_started_) {
    flush_timer_.init();
    timer_started_ = true;
  }
  initialized_ = true;
  dout(5) << "activated capacity=" << volume_capacity_
          << " chunk_size=" << chunk_size_
          << " timeout=" << flush_timeout_ << dendl;
}

void Aggregator::shutdown() {
  if (!timer_started_ && !initialized_) return;
  cancel_flush();
  if (timer_started_) {
    std::lock_guard lock(timer_lock_);
    flush_timer_.shutdown();
    timer_started_ = false;
  }
  flush_timer_enabled_ = false;
  initialized_ = false;
  {
    std::lock_guard lock(state_mutex_);
    active_volume_.reset();
    flushing_.clear();
    waiting_.clear();
    committed_.clear();
    inflight_volumes_.clear();
    original_oids_.clear();
    cls_contexts_.clear();
  }
  catalog_.clear();
  clear_ec_cache();
}

void Aggregator::load_metadata(
  std::vector<ceph::buffer::list> &encoded_metadata) {
  // Backend discovery is an authoritative snapshot, not an incremental
  // update.  Replacing it also drops Volumes that disappeared on disk.
  if (catalog_.replace_from_disk(encoded_metadata) < 0) {
    dout(1) << "one or more invalid volume metadata entries were ignored"
            << dendl;
  }
}

int Aggregator::preprocess(OpRequestRef op) {
  if (!initialized_) return PREPROCESS_CONTINUE;
  auto *m = static_cast<MOSDOp*>(op->get_nonconst_req());

  // Route the request before PrimaryLogPG opens the physical object context:
  //   1. serialize conflicting mutations with an active/reused Volume;
  //   2. aggregate writes for objects that do not exist in the catalog;
  //   3. translate reads and metadata operations for existing objects;
  //   4. leave unrelated operations on the normal OSD path.
  if (defer_conflicting_mutation(op, *m)) return PREPROCESS_CONSUMED;
  if (should_aggregate(*m)) return preprocess_aggregate(std::move(op), *m);

  if (should_translate(*m)) {
    int translated = translate(op, m->ops);
    if (translated < 0) purge_origin(op);
    return translated;
  }
  return PREPROCESS_CONTINUE;
}

bool Aggregator::defer_conflicting_mutation(
  OpRequestRef op, const MOSDOp &m) {
  if (!mutates_volume_metadata(m)) return false;

  auto metadata = catalog_.lookup(m.get_hobj());
  if (!metadata) return false;

  std::lock_guard lock(state_mutex_);
  const bool active_conflict =
    active_volume_ && active_volume_->oid() == metadata->volume_oid;
  const bool flushing_conflict =
    flushing_.find(metadata->volume_oid) != flushing_.end();
  if (!active_conflict && !flushing_conflict) return false;

  // A reused Volume was built from a metadata snapshot.  Letting a WRITE or
  // DELETE overtake it could make the aggregate commit restore stale
  // metadata, so retry the mutation after this Volume finishes.
  waiting_.push_back(std::move(op));
  return true;
}

int Aggregator::preprocess_aggregate(OpRequestRef op, MOSDOp &m) {
  // Existing logical objects must stay in their current Volume.  Only a
  // genuinely new object is eligible for a new aggregate write.
  int translated = translate(op, m.ops);
  if (translated >= 0) return translated;

  // translate() records the logical OID even on lookup failure.  A genuinely
  // new object must discard that temporary state before it is buffered.
  purge_origin(op);
  if (translated != -ENOENT) return translated;
  return buffer_new_object(std::move(op), m.get_hobj());
}

int Aggregator::buffer_new_object(OpRequestRef op, const hobject_t &oid) {
  bool volume_full = false;
  {
    std::lock_guard lock(state_mutex_);
    if (!flushing_.empty()) {
      // Only one aggregate Volume is flushed at a time.  This keeps client
      // ordering deterministic and prevents two metadata snapshots racing.
      waiting_.push_back(std::move(op));
      return PREPROCESS_CONSUMED;
    }

    int r = ensure_active_volume_locked(oid);
    if (r < 0) return r;
    if (active_volume_->oid() == hobject_t()) active_volume_->bind_oid(oid);
    if (active_volume_->contains(oid)) {
      // The first request for this logical OID has not committed yet.  Once
      // it does, this request will be requeued and translated as an overwrite.
      waiting_.push_back(std::move(op));
      return PREPROCESS_CONSUMED;
    }

    r = active_volume_->add(op);
    if (r < 0) {
      if (active_volume_->empty()) active_volume_.reset();
      return r;
    }
    volume_full = active_volume_->full();
  }

  // Timer operations and PG requeueing must happen outside state_mutex_;
  // their callbacks can re-enter Aggregator state transitions.
  if (volume_full) {
    cancel_flush();
    flush_now();
  } else if (flush_timer_enabled_) {
    schedule_flush();
  } else {
    flush_now();
  }
  return PREPROCESS_CONSUMED;
}

int Aggregator::ensure_active_volume_locked(const hobject_t &oid) {
  if (active_volume_) return 0;

  // Do not reuse a non-full Volume while a translated request still carries
  // a private snapshot for it.  A later aggregate commit could otherwise
  // overwrite that request's metadata update.
  std::set<hobject_t> excluded_volumes;
  for (const auto &entry : inflight_volumes_) {
    excluded_volumes.insert(entry.first);
  }
  auto reusable = catalog_.find_nonfull(
    volume_capacity_, chunk_size_, oid, &excluded_volumes);
  if (reusable) {
    // Volume(metadata) preserves occupied slots and only buffers new chunks
    // into holes represented by the persisted bitmap.
    active_volume_ = std::make_unique<Volume>(reusable->info);
    return 0;
  }
  // New Volumes use the first logical object as their physical OID.  Never
  // overwrite an existing physical Volume that happens to have that OID.
  if (catalog_.contains_volume(oid)) return -ENOSPC;

  active_volume_ = std::make_unique<Volume>(
    volume_capacity_, chunk_size_, pgid_);
  return 0;
}

int Aggregator::translate(OpRequestRef &op, std::vector<OSDOp> &ops) {
  auto *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  // A requeued translated request already contains the physical Volume OID.
  // original_oid() keeps the stable logical identity used by the catalog and
  // by xattr namespacing across retries.
  hobject_t origin = original_oid(op, *m);
  auto metadata = catalog_.lookup(origin);
  if (!metadata) return -ENOENT;

  auto chunk_it = metadata->info.get_chunk_map().find(origin);
  if (chunk_it == metadata->info.get_chunk_map().end()) return -ENOENT;

  utime_t logical_mtime = m->get_mtime();
  if (logical_mtime == utime_t()) logical_mtime = ceph_clock_now();

  TranslationContext ctx(
    std::move(origin), *metadata, chunk_it->second, logical_mtime);
  // Validate every mutation and calculate its final logical size before
  // changing the MOSDOp.  Errors therefore cannot publish partial metadata.
  int r = validate_mutations(ops, ctx);
  if (r < 0) return r;

  m->set_hobj(ctx.volume_oid);
  std::vector<OSDOp> appended_ops;
  appended_ops.reserve(2);
  // Preserve the historical reverse rewrite order (notably for multiple CLS
  // calls), but defer synthetic ZERO/metadata ops until iteration finishes so
  // vector growth cannot invalidate iterators.
  for (auto op_it = ops.rbegin(); op_it != ops.rend(); ++op_it) {
    r = translate_operation(op, *op_it, ctx, appended_ops);
    if (r < 0) return r;
  }
  ops.insert(
    ops.end(),
    std::make_move_iterator(appended_ops.begin()),
    std::make_move_iterator(appended_ops.end()));

  if (ctx.writes_metadata) {
    // Compound writes update the metadata snapshot once, using the final size
    // calculated in client operation order, then persist that final snapshot.
    ctx.volume_info.update_chunk(ctx.origin, ctx.final_logical_size);
    ctx.volume_info.get_chunk(ctx.origin).set_mtime(ctx.logical_mtime);
    ops.push_back(ctx.volume_info.generate_write_meta_op());
    clear_ec_cache();
  }
  {
    std::lock_guard lock(state_mutex_);
    // ECBackend consults this snapshot while reconstructing the translated
    // physical request.  purge_origin() removes it when the request finishes.
    inflight_volumes_[ctx.volume_oid] = std::move(ctx.volume_info);
  }
  op->set_aggregateEC_translated_op();
  return ctx.result;
}

int Aggregator::validate_mutations(
  const std::vector<OSDOp> &ops, TranslationContext &ctx) const {
  // Walk in client order to model WRITEFULL truncation followed by partial
  // writes correctly.  Mixing DELETE with writes is intentionally rejected:
  // one compound request cannot both remove and rebuild a logical slot safely.
  for (const auto &osd_op : ops) {
    switch (osd_op.op.op) {
    case CEPH_OSD_OP_WRITEFULL: {
      if (ctx.deletes_object) return -EOPNOTSUPP;
      const uint64_t data_length = osd_op.indata.length();
      if (data_length > ctx.slot_size) return -E2BIG;
      if (osd_op.op.extent.length != data_length) return -EINVAL;
      ctx.final_logical_size = data_length;
      ctx.writes_metadata = true;
      break;
    }
    case CEPH_OSD_OP_WRITE: {
      const uint64_t object_offset = osd_op.op.extent.offset;
      const uint64_t write_length = osd_op.op.extent.length;
      if (ctx.deletes_object) return -EOPNOTSUPP;
      if (osd_op.indata.length() != write_length) return -EINVAL;
      if (object_offset > ctx.slot_size ||
          write_length > ctx.slot_size - object_offset) {
        return -E2BIG;
      }
      ctx.final_logical_size = std::max(
        ctx.final_logical_size, object_offset + write_length);
      ctx.writes_metadata = true;
      break;
    }
    case CEPH_OSD_OP_DELETE:
      if (ctx.writes_metadata || ctx.deletes_object) return -EOPNOTSUPP;
      ctx.deletes_object = true;
      break;
    default:
      break;
    }
  }
  return 0;
}

int Aggregator::translate_operation(
  OpRequestRef &op, OSDOp &osd_op, TranslationContext &ctx,
  std::vector<OSDOp> &appended_ops) {
  switch (osd_op.op.op) {
    case CEPH_OSD_OP_CMPXATTR:
    case CEPH_OSD_OP_SETXATTR:
      // The physical Volume stores attributes for many logical objects.
      return rewrite_xattr_op(osd_op, ctx.origin, true);
    case CEPH_OSD_OP_GETXATTR:
      return rewrite_xattr_op(osd_op, ctx.origin, false);
    case CEPH_OSD_OP_GETXATTRS:
      rewrite_getxattrs(osd_op, ctx.origin);
      return 0;
    case CEPH_OSD_OP_STAT:
      // STAT must report logical size/mtime, never the physical Volume size.
      encode(ctx.chunk.get_offset(), osd_op.outdata);
      encode(ctx.chunk.get_mtime(), osd_op.outdata);
      return 0;
    case CEPH_OSD_OP_DELETE:
      translate_delete(op, osd_op, ctx, appended_ops);
      return 0;
    case CEPH_OSD_OP_WRITEFULL: {
      // WRITEFULL becomes a full-slot WRITE so stale bytes after the new
      // logical EOF are overwritten with zeros.
      osd_op.op.op = CEPH_OSD_OP_WRITE;
      osd_op.op.extent.offset = ctx.volume_offset;
      uint64_t data_length = osd_op.indata.length();
      osd_op.indata.append_zero(ctx.slot_size - data_length);
      osd_op.op.extent.length = ctx.slot_size;
      return 0;
    }
    case CEPH_OSD_OP_WRITE: {
      // Partial writes keep their object-relative offset inside the slot.
      const uint64_t object_offset = osd_op.op.extent.offset;
      osd_op.op.extent.offset = ctx.volume_offset + object_offset;
      return 0;
    }
    case CEPH_OSD_OP_CALL:
      return translate_call(osd_op, ctx);
    case CEPH_OSD_OP_READ:
    case CEPH_OSD_OP_SPARSE_READ:
    case CEPH_OSD_OP_SYNC_READ:
      translate_read(osd_op, ctx);
      ctx.result = PREPROCESS_REDIRECT;
      return 0;
    default:
      return 0;
  }
}

int Aggregator::translate_call(
  OSDOp &osd_op, const TranslationContext &ctx) {
  std::string class_name;
  try {
    osd_op.indata.cbegin().copy(osd_op.op.cls.class_len, class_name);
  } catch (const ceph::buffer::error &) {
    return -EINVAL;
  }
  if (!ClassHandler::get_instance().in_class_list(
        class_name, cct_->_conf->osd_aggregateEC_class_list)) {
    // Native classes keep their original CALL layout and normal OSD path.
    return 0;
  }

  // CEPH_OSD_OP_EC_CALL reuses the cls/extent union.  Preserve the original
  // class parameters separately before exposing the logical slot range.
  osd_op.op.op = CEPH_OSD_OP_EC_CALL;
  cls_contexts_[ctx.volume_oid] = std::make_unique<ClsParmContext>(
    osd_op.op.cls.class_len, osd_op.op.cls.method_len,
    osd_op.op.cls.argc, osd_op.op.cls.indata_len, osd_op.indata);
  osd_op.op.extent.length = ctx.chunk.get_offset();
  osd_op.op.extent.offset = ctx.volume_offset;
  return 0;
}

void Aggregator::translate_read(
  OSDOp &osd_op, const TranslationContext &ctx) const {
  const uint64_t object_offset = osd_op.op.extent.offset;
  const uint64_t object_length = ctx.chunk.get_offset();
  if (object_offset >= object_length) {
    // A zero-length OSD read means "to EOF", so issue a one-byte read
    // beyond the physical Volume to produce an empty result safely.
    osd_op.op.extent.offset =
      static_cast<uint64_t>(ctx.volume_info.get_cap()) * ctx.slot_size;
    osd_op.op.extent.length = 1;
    return;
  }

  const uint64_t remaining = object_length - object_offset;
  const uint64_t requested = osd_op.op.extent.length;
  // A zero requested length means "to logical EOF".  Otherwise never expose
  // zero padding or the next object's slot to the client.
  osd_op.op.extent.length =
    requested == 0 ? remaining : std::min(requested, remaining);
  osd_op.op.extent.offset = ctx.volume_offset + object_offset;
}

void Aggregator::translate_delete(
  OpRequestRef &op, OSDOp &osd_op, TranslationContext &ctx,
  std::vector<OSDOp> &appended_ops) {
  if (!ctx.volume_info.is_only_valid_object(ctx.origin)) {
    // Other logical objects still share the physical Volume: replace DELETE
    // with a metadata update and punch this complete slot to zeros.
    ctx.volume_info.remove_chunk(ctx.origin);
    ctx.volume_info.generate_write_meta_op(osd_op);
    appended_ops.push_back(make_zero_op(ctx.volume_offset, ctx.slot_size));
    op->set_aggregateEC_storage_optimize();
  }
  // If this was the last logical object, leave DELETE unchanged so the whole
  // physical Volume is removed.  Either form invalidates reconstructed data.
  clear_ec_cache();
}

bool Aggregator::owns_volume(const hobject_t &volume_oid) const {
  std::lock_guard lock(state_mutex_);
  return flushing_.find(volume_oid) != flushing_.end();
}

bool Aggregator::complete_volume(
  const hobject_t &volume_oid,
  const std::vector<OSDOp> &ops,
  MOSDOpReply *reply,
  bool ignore_out_data) {
  std::lock_guard lock(state_mutex_);
  auto it = flushing_.find(volume_oid);
  if (it == flushing_.end()) return false;

  // The physical write succeeded.  Publish its metadata before waking later
  // requests so they can resolve these logical OIDs through the catalog.
  auto &volume = *it->second;
  catalog_.upsert(volume_oid, volume.metadata());
  cache_volume(volume);
  dout(5) << "committed volume " << volume_oid
          << " objects=" << volume.occupied() << dendl;

  for (uint32_t i = 0; i < volume.capacity(); ++i) {
    const auto &chunk = volume.chunk(i);
    if (chunk.empty()) continue;
    auto request = chunk.request();
    auto *m = request->get_req<MOSDOp>();
    // One internal Volume request represents several client requests.  Clone
    // its result for each original message and connection.
    auto *split_reply = new MOSDOpReply(m, reply, ignore_out_data);
    osd_->send_message_osd_client(split_reply, m->get_connection());
    request->mark_commit_sent();
  }
  reply->put();
  // Keep flushing_ alive until the PG context's on_finish callback.  That
  // callback calls finish_volume(), releases waiters, and closes the state.
  committed_.insert(volume_oid);
  return true;
}

bool Aggregator::fail_volume(
  const hobject_t &volume_oid, int error,
  eversion_t version, version_t user_version,
  const std::vector<pg_log_op_return_item_t> &op_returns) {
  std::unique_ptr<Volume> volume;
  {
    // Remove failed state before sending replies: reply paths may immediately
    // re-enter the PG and must not observe this Volume as still in flight.
    std::lock_guard lock(state_mutex_);
    auto it = flushing_.find(volume_oid);
    if (it == flushing_.end()) return false;
    volume = std::move(it->second);
    flushing_.erase(it);
    committed_.erase(volume_oid);
    inflight_volumes_.erase(volume_oid);
  }

  const std::vector<pg_log_op_return_item_t> no_op_returns;
  for (uint32_t i = 0; i < volume->capacity(); ++i) {
    const auto &chunk = volume->chunk(i);
    if (!chunk.empty()) {
      auto *m = chunk.request()->get_req<MOSDOp>();
      // op_returns belongs to the internal combined MOSDOp.  Forward it only
      // when its shape also matches this original request.
      const auto &request_returns = op_returns.size() == m->ops.size()
        ? op_returns
        : no_op_returns;
      osd_->reply_op_error(
        chunk.request(), error, version, user_version, request_returns);
    }
  }
  requeue_waiting();
  return true;
}

bool Aggregator::fail_volume(
  const hobject_t &volume_oid,
  MOSDOpReply *reply,
  bool ignore_out_data) {
  std::unique_ptr<Volume> volume;
  {
    // record_write_error already built a reply for the internal request; take
    // ownership of the Volume and split that reply after dropping the lock.
    std::lock_guard lock(state_mutex_);
    auto it = flushing_.find(volume_oid);
    if (it == flushing_.end()) return false;
    volume = std::move(it->second);
    flushing_.erase(it);
    committed_.erase(volume_oid);
    inflight_volumes_.erase(volume_oid);
  }

  for (uint32_t i = 0; i < volume->capacity(); ++i) {
    const auto &chunk = volume->chunk(i);
    if (chunk.empty()) continue;
    auto request = chunk.request();
    auto *m = request->get_req<MOSDOp>();
    auto *split_reply = new MOSDOpReply(m, reply, ignore_out_data);
    osd_->send_message_osd_client(split_reply, m->get_connection());
    request->mark_commit_sent();
  }
  reply->put();
  requeue_waiting();
  return true;
}

int Aggregator::list_objects(pg_nls_response_t &response, unsigned limit) {
  std::optional<hobject_t> next;
  auto objects = catalog_.list_objects(response.handle, limit, next);
  for (const auto &oid : objects) {
    librados::ListObjectImpl item;
    item.nspace = oid.get_namespace();
    item.oid = oid.oid.name;
    item.locator = oid.get_key();
    response.entries.push_back(std::move(item));
  }
  response.handle = next.value_or(
    pg_->info.pgid.pgid.get_hobj_end(pg_->pool.info.get_pg_num()));
  return objects.empty() && !next ? 1 : 0;
}

void Aggregator::finish_cls(const hobject_t &volume_oid) {
  cls_contexts_.erase(volume_oid);
}

ClsParmContext *Aggregator::get_cls_ctx(const hobject_t &volume_oid) {
  auto it = cls_contexts_.find(volume_oid);
  return it == cls_contexts_.end() ? nullptr : it->second.get();
}

void Aggregator::purge_origin(OpRequestRef op) {
  std::lock_guard lock(state_mutex_);
  if (op && op->is_aggregateEC_translated_op()) {
    // The MOSDOp now carries the physical OID, which is also the key used by
    // ECBackend for its per-request metadata snapshot.
    auto *m = static_cast<MOSDOp*>(op->get_nonconst_req());
    inflight_volumes_.erase(m->get_hobj());
  }
  original_oids_.erase(op);
}

std::vector<OpRequestRef> Aggregator::buffered_requests(
  const hobject_t &volume_oid) const {
  std::lock_guard lock(state_mutex_);
  std::vector<OpRequestRef> result;
  auto it = flushing_.find(volume_oid);
  if (it == flushing_.end()) return result;
  for (uint32_t i = 0; i < it->second->capacity(); ++i) {
    const auto &chunk = it->second->chunk(i);
    if (!chunk.empty()) result.push_back(chunk.request());
  }
  return result;
}

void Aggregator::finish_volume(const hobject_t &volume_oid) {
  bool finished = false;
  {
    std::lock_guard lock(state_mutex_);
    if (committed_.erase(volume_oid)) {
      // Success replies were already sent by complete_volume(); this callback
      // only closes the internal state and makes queued requests runnable.
      flushing_.erase(volume_oid);
      inflight_volumes_.erase(volume_oid);
      finished = true;
    }
  }
  if (finished) {
    requeue_waiting();
  }
}

std::vector<OpRequestRef> Aggregator::cancel_volume(
  const hobject_t &volume_oid) {
  std::vector<OpRequestRef> requests;
  {
    std::lock_guard lock(state_mutex_);
    auto it = flushing_.find(volume_oid);
    if (it != flushing_.end()) {
      for (uint32_t i = 0; i < it->second->capacity(); ++i) {
        const auto &chunk = it->second->chunk(i);
        if (!chunk.empty()) {
          // A peering/map transition aborted the internal request.  Return
          // each original request to PrimaryLogPG instead of retrying the
          // synthetic Volume MOSDOp.
          auto request = chunk.request();
          request->set_requeued();
          requests.push_back(std::move(request));
        }
      }
      flushing_.erase(it);
    }
    committed_.erase(volume_oid);
    inflight_volumes_.erase(volume_oid);
  }
  requeue_waiting();
  return requests;
}

void Aggregator::update_cache(
  const hobject_t &volume_oid, const std::vector<OSDOp> &ops) {
  // Normal translated writes/deletes do not pass complete_volume().  Inspect
  // their final physical operations to keep the authoritative catalog in sync
  // only after PrimaryLogPG has successfully executed them.
  for (auto it = ops.rbegin(); it != ops.rend(); ++it) {
    if (it->op.op == CEPH_OSD_OP_DELETE) {
      catalog_.remove_volume(volume_oid);
      clear_ec_cache();
      continue;
    }
    if (it->op.op != CEPH_OSD_OP_SETXATTR) continue;
    try {
      auto p = it->indata.cbegin();
      std::string name;
      p.copy(it->op.xattr.name_len, name);
      if (name != "volume_meta") continue;
      ceph::buffer::list encoded;
      p.copy(it->op.xattr.value_len, encoded);
      if (catalog_.load_from_disk(encoded) < 0) {
        dout(1) << "invalid volume metadata update for " << volume_oid
                << dendl;
      }
    } catch (const ceph::buffer::error &) {
      dout(1) << "invalid volume metadata update for " << volume_oid << dendl;
    }
  }
}

bool Aggregator::is_volume_cached(const hobject_t &volume_oid) const {
  std::shared_lock lock(cache_mutex_);
  if (!cached_volume_meta_ || cached_volume_meta_->get_oid() != volume_oid) {
    return false;
  }
  const auto &bitmap = cached_volume_meta_->get_chunk_bitmap();
  const uint64_t size = cached_volume_meta_->get_chunk_size();
  // Reusing a persisted non-full Volume may leave old occupied slots uncached.
  // ECBackend may bypass reads only when every occupied slot is complete.
  for (uint32_t i = 0; i < cached_volume_meta_->get_cap(); ++i) {
    if (bitmap[i] && cached_chunks_[i].length() != size) return false;
  }
  return true;
}

volume_t Aggregator::inflight_volume(const hobject_t &volume_oid) const {
  std::lock_guard lock(state_mutex_);
  auto it = inflight_volumes_.find(volume_oid);
  ceph_assert(it != inflight_volumes_.end());
  return it->second;
}

std::optional<uint64_t> Aggregator::logical_offset(
  const OpRequestRef &op, uint64_t physical_offset) const {
  hobject_t origin;
  {
    std::lock_guard lock(state_mutex_);
    auto original = original_oids_.find(op);
    if (original == original_oids_.end()) return std::nullopt;
    origin = original->second;
  }
  auto metadata = catalog_.lookup(origin);
  if (!metadata) return std::nullopt;
  auto chunk = metadata->info.get_chunk_map().find(origin);
  if (chunk == metadata->info.get_chunk_map().end()) return std::nullopt;
  const uint64_t base =
    static_cast<uint64_t>(chunk->second.get_chunk_id()) *
    metadata->info.get_chunk_size();
  if (physical_offset < base) return std::nullopt;
  // Sparse-read extents are produced in physical coordinates; clients expect
  // offsets relative to the original logical object.
  return physical_offset - base;
}

void Aggregator::read_cached_volume(extent_map &result) const {
  std::shared_lock lock(cache_mutex_);
  ceph_assert(cached_volume_meta_);
  const uint64_t size = cached_volume_meta_->get_chunk_size();
  for (uint32_t i = 0; i < cached_chunks_.size(); ++i) {
    ceph::buffer::list data;
    if (cached_chunks_[i].length() == 0) {
      data.append_zero(size);
    } else {
      data = cached_chunks_[i];
    }
    result.insert(static_cast<uint64_t>(i) * size, size, std::move(data));
  }
}

bool Aggregator::should_aggregate(const MOSDOp &m) const {
  for (const auto &op : m.ops) {
    switch (op.op.op) {
    case CEPH_OSD_OP_WRITE:
    case CEPH_OSD_OP_WRITEFULL:
    case CEPH_OSD_OP_CREATE:
    case CEPH_OSD_OP_SETXATTR:
      return true;
    default:
      break;
    }
  }
  return false;
}

bool Aggregator::should_translate(const MOSDOp &m) const {
  for (const auto &op : m.ops) {
    switch (op.op.op) {
    case CEPH_OSD_OP_READ:
    case CEPH_OSD_OP_SPARSE_READ:
    case CEPH_OSD_OP_SYNC_READ:
    case CEPH_OSD_OP_CALL:
    case CEPH_OSD_OP_DELETE:
    case CEPH_OSD_OP_STAT:
    case CEPH_OSD_OP_GETXATTR:
    case CEPH_OSD_OP_SETXATTR:
    case CEPH_OSD_OP_GETXATTRS:
    case CEPH_OSD_OP_CMPXATTR:
      return true;
    default:
      break;
    }
  }
  return false;
}

bool Aggregator::mutates_volume_metadata(const MOSDOp &m) const {
  for (const auto &op : m.ops) {
    switch (op.op.op) {
    case CEPH_OSD_OP_WRITE:
    case CEPH_OSD_OP_WRITEFULL:
    case CEPH_OSD_OP_DELETE:
      return true;
    default:
      break;
    }
  }
  return false;
}

void Aggregator::schedule_flush() {
  if (!flush_timer_enabled_ || !timer_started_) return;
  // Every new chunk extends the batching window.  A full Volume bypasses this
  // path and flushes immediately from buffer_new_object().
  cancel_flush();
  auto *event = new LambdaContext([this](int) {
    flush_event_ = nullptr;
    flush_now();
  });
  std::lock_guard lock(timer_lock_);
  flush_event_ = flush_timer_.add_event_after(flush_timeout_, event);
}

void Aggregator::cancel_flush() {
  std::lock_guard lock(timer_lock_);
  if (!flush_event_) return;
  flush_timer_.cancel_event(flush_event_);
  flush_event_ = nullptr;
}

void Aggregator::flush_now() {
  OpRequestRef request;
  hobject_t oid;
  uint32_t object_count = 0;
  {
    std::lock_guard lock(state_mutex_);
    if (!active_volume_ || active_volume_->empty()) return;
    // Move the Volume out first so no later client request can append chunks
    // while its synthetic MOSDOp and metadata snapshot are being constructed.
    auto volume = std::move(active_volume_);
    MOSDOp *message = volume->generate_write_op();
    if (!message) {
      active_volume_ = std::move(volume);
      return;
    }

    request = osd_->osd->create_request(message);
    // The synthetic request re-enters the normal PG pipeline, but these flags
    // identify it as a physical aggregate write and suppress client semantics
    // that belong to the original logical requests.
    request->set_requeued();
    request->set_aggregateEC_translated_op();
    request->set_aggregateEC_storage_optimize();
    request->set_write_volume();
    oid = message->get_hobj();
    object_count = volume->occupied();
    inflight_volumes_[oid] = volume->metadata();
    flushing_[oid] = std::move(volume);
  }
  dout(5) << "flushing volume " << oid
          << " objects=" << object_count << dendl;
  pg_->requeue_op(request);
}

void Aggregator::requeue_waiting() {
  std::deque<OpRequestRef> waiting;
  {
    std::lock_guard lock(state_mutex_);
    waiting.swap(waiting_);
  }
  // PG::requeue_op() inserts at the scheduler front.  Consume this deque from
  // the back so repeated front insertion preserves original FIFO order.
  while (!waiting.empty()) {
    auto op = std::move(waiting.back());
    waiting.pop_back();
    op->set_requeued();
    pg_->requeue_op(op);
  }
}

void Aggregator::cache_volume(const Volume &volume) {
  std::unique_lock lock(cache_mutex_);
  // Cache data carried by this aggregate request.  is_volume_cached() later
  // rejects the cache if persisted occupied slots were not part of the batch.
  cached_volume_meta_ = std::make_shared<volume_t>(volume.metadata());
  cached_chunks_.assign(volume.capacity(), ceph::buffer::list{});
  for (uint32_t i = 0; i < volume.capacity(); ++i) {
    const auto &chunk = volume.chunk(i);
    if (chunk.empty()) continue;
    for (const auto &op : chunk.ops()) {
      if (op.op.op == CEPH_OSD_OP_WRITE) {
        cached_chunks_[i] = op.indata;
        break;
      }
    }
  }
}

void Aggregator::clear_ec_cache() {
  std::unique_lock lock(cache_mutex_);
  cached_volume_meta_.reset();
  for (auto &chunk : cached_chunks_) chunk.clear();
}

hobject_t Aggregator::original_oid(OpRequestRef op, const MOSDOp &m) {
  std::lock_guard lock(state_mutex_);
  // emplace intentionally keeps the first value.  After translation m.hobj is
  // the physical Volume OID, but retries still need the original logical OID.
  auto [it, inserted] = original_oids_.emplace(op, m.get_hobj());
  return it->second;
}

} // namespace ceph::aggregate_ec
