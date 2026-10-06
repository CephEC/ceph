// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "WeavePGControllerImpl.h"

#include <algorithm>

#include "WeaveLayout.h"
#include "WeaveRequestContext.h"
#include "WeaveXAttr.h"

namespace ceph::weave {

namespace {

bool has_pg_op(const std::vector<OSDOp>& ops)
{
  return std::any_of(ops.begin(), ops.end(), [](const auto& entry) {
    return ceph_osd_op_type_pg(entry.op.op);
  });
}

void sort_and_deduplicate(std::vector<hobject_t>& entries)
{
  std::sort(entries.begin(), entries.end());
  entries.erase(std::unique(entries.begin(), entries.end()), entries.end());
}

}  // namespace

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

WeavePGController::Impl::Impl(
  CephContext* cct, std::unique_ptr<WeavePGInterface> pg_interface, bool enabled)
  : pg_interface_(std::move(pg_interface)), enabled_(enabled),
    translator_(cct, catalog_), reads_(*pg_interface_, translator_) {}

WeavePGController::Impl::~Impl()
{
  pg_interface_->cancel_retries();
  // Native shutdown cancels active work before the PG can be destroyed.
  ceph_assert(!job_);
  finish_cleanup();
}

void WeavePGController::Impl::initialize()
{
  if (!enabled_ || translator_.initialized() || !pg_interface_->primary() ||
      !pg_interface_->active() || pg_interface_->has_missing()) {
    return;
  }
  if (!reload_metadata()) {
    fail_recovery_waiters();
    return;
  }
  // Geometry is only trusted once the PG is active and complete; activate()
  // revalidates it for the member translator.
  const auto geometry = pg_interface_->geometry();
  translator_.activate(geometry.data_shards, geometry.unit);

  pg_interface_->requeue(waiting_for_recovery_);
}

bool WeavePGController::Impl::reload_metadata()
{
  if (!enabled_ || job_) return false;
  WeaveVolumeAttrs stored;
  metadata_error_ = pg_interface_->load_metadata(stored);
  if (metadata_error_ < 0) {
    translator_.shutdown();
    return false;
  }
  WeaveVolumeAttrs metadata;
  metadata.reserve(stored.size());
  // Ordinary clients may store an attribute called volume_meta. Only physical
  // Volumes in the private namespace can publish member mappings.
  for (auto& entry : stored) {
    if (entry.first.nspace == kVolumeNamespace) {
      metadata.push_back(std::move(entry));
    }
  }
  // Publish the snapshot as a whole: readers observe either the previous
  // mapping set or the new one, never a partial mixture.
  if (catalog_.replace_from_disk(metadata) < 0) {
    metadata_error_ = -EIO;
    translator_.shutdown();
    return false;
  }
  return true;
}

void WeavePGController::Impl::fail_recovery_waiters()
{
  // Only a permanent load error ends the wait; a missing object is retried.
  if (metadata_error_ >= 0) return;

  for (const auto& op : waiting_for_recovery_) {
    pg_interface_->reply_error(op, metadata_error_);
  }
  waiting_for_recovery_.clear();
}

bool WeavePGController::Impl::can_work() const
{
  return enabled_ && translator_.initialized() && pg_interface_->primary() &&
    pg_interface_->active() && pg_interface_->clean();
}

bool WeavePGController::Impl::can_scan() const
{
  return can_work() && pg_interface_->policy().background;
}

void WeavePGController::Impl::on_recovery_progress()
{
  if (!enabled_ || !pg_interface_->primary()) return;
  // on_local_recover runs before submitting the recovery transaction.
  // initialize waits for the PG's missing/applied barrier before reloading.
  translator_.shutdown();
  initialize();
}

void WeavePGController::Impl::on_pg_change(bool requeue)
{
  if (!enabled_) return;

  // Invalidate every pending wakeup and the running job before the state it
  // depends on changes.
  ++generation_;
  pg_interface_->cancel_retries();
  cancel_job();

  // Per-role state: the cancelled job already released its reservations in
  // finish_job(), so only candidates are dropped here. They are rebuilt from
  // committed changes.
  candidates_.clear();

  // A replica can initialize the member translator for a direct request
  // without loading the primary catalog. Promotion must reload that catalog.
  translator_.shutdown();
  finish_cleanup();

  if (requeue) {
    pg_interface_->requeue(waiting_for_conversion_);
    pg_interface_->requeue(waiting_for_recovery_);
  } else {
    waiting_for_conversion_.clear();
    waiting_for_recovery_.clear();
  }
}

// ---------------------------------------------------------------------------
// Background work
// ---------------------------------------------------------------------------

void WeavePGController::Impl::configure_candidates()
{
  if (!enabled_) return;
  const auto geometry = pg_interface_->geometry();
  const auto policy = pg_interface_->policy();
  candidates_.configure(geometry.data_shards, geometry.unit,
                        policy.min_size, policy.max_volume_size);
}

void WeavePGController::Impl::scan_candidates()
{
  // Packing yields to foreground conversions and an existing cleanup pass.
  // This scan never resumes either of them; their own continuations do that.
  if (!can_scan() || job_ || cleanup_ || !waiting_for_conversion_.empty()) return;
  const auto geometry = pg_interface_->geometry();
  const auto policy = pg_interface_->policy();
  configure_candidates();
  std::vector<hobject_t> stale;
  auto members = select_packable(geometry, policy, stale);
  for (const auto& oid : stale) {
    candidates_.erase(oid);
  }
  if (members.empty()) return;
  auto lease = pg_interface_->acquire();
  if (!lease) return;
  auto volume = plan_volume(members, geometry);
  start_job(std::move(members), std::move(volume), std::move(lease), false);
}

std::vector<WeaveCandidate> WeavePGController::Impl::select_packable(
  const WeaveGeometry& geometry, const WeavePolicy& policy,
  std::vector<hobject_t>& stale)
{
  return candidates_.select(geometry.data_shards, geometry.unit,
    policy.min_size, policy.quiet_seconds, policy.max_volume_size,
    policy.padding_percent, ceph::mono_clock::now(),
    [this, &stale](const auto& member) {
      return candidate_available(member, stale);
    });
}

bool WeavePGController::Impl::candidate_available(
  const WeaveCandidate& member, std::vector<hobject_t>& stale)
{
  const auto object = pg_interface_->inspect(member.oid);
  // Anything the candidate index no longer describes exactly is
  // dropped here, so the next commit can reconsider it.
  if (catalog_.lookup(member.oid) ||
      !object.exists || object.info.version != member.version ||
      !WeaveCandidateIndex::eligible(object.info) ||
      !object.info.watchers.empty() || object.has_clones) {
    stale.push_back(member.oid);
    return false;
  }

  // Shared readers keep the source stable. Writers and queued native work
  // must drain before we reserve it against further modification.
  return !object.blocks_pack();
}

// Slot geometry comes from the largest member: every member then fits one
// aligned slot, and the Volume has the exact width a later read expects.
WeaveVolumeMeta WeavePGController::Impl::plan_volume(
  const std::vector<WeaveCandidate>& members, const WeaveGeometry& geometry)
{
  uint64_t largest = 0;
  for (const auto& member : members) {
    largest = std::max(largest, member.size);
  }
  const auto slot =
    ((largest + geometry.unit - 1) / geometry.unit) * geometry.unit;
  WeaveVolumeMeta volume{pg_interface_->new_volume(members.front().oid),
                         geometry.data_shards, slot, {}};

  // Shard order is the selection order; it is captured now, while the members
  // are still reserved against modification.
  for (size_t i = 0; i < members.size(); ++i) {
    const auto& member = members[i];
    volume.members.emplace(member.oid, WeaveMemberMeta{
      static_cast<uint8_t>(i), member.size, member.mtime, member.user_version,
      pg_interface_->inspect(member.oid).snap_sequence});
    candidates_.erase(member.oid);
  }
  return volume;
}

void WeavePGController::Impl::resume_cleanup()
{
  if (!cleanup_ || job_) return;
  if (!can_work()) {
    finish_cleanup();
    return;
  }
  // Let delayed client requests return to the native PG before reserving
  // another Volume for cleanup.
  if (!waiting_for_conversion_.empty()) {
    schedule_cleanup_retry();
    return;
  }
  scan_cleanup();
}

void WeavePGController::Impl::schedule_cleanup_retry()
{
  if (!cleanup_ || job_) return;
  auto ref = pg_interface_->pin();
  const auto generation = generation_;
  pg_interface_->retry(WeaveRetryKind::kCleanup,
    [this, ref = std::move(ref), generation] {
      if (generation == generation_) resume_cleanup();
    });
}

void WeavePGController::Impl::scan_cleanup()
{
  while (cleanup_->next < cleanup_->volumes.size()) {
    const auto& oid = cleanup_->volumes[cleanup_->next]->volume_oid;
    auto metadata = catalog_.lookup_volume(oid);
    if (!volume_needs_reclaim(metadata)) {
      ++cleanup_->next;
      continue;
    }
    // Contention on the source object retries on the next wakeup; a
    // started conversion defers the rest of the pass to its completion.
    if (!start_deaggregation(metadata)) {
      schedule_cleanup_retry();
      return;
    }
    ++cleanup_->next;
    if (job_) return;
  }
  finish_cleanup();
}

bool WeavePGController::Impl::volume_needs_reclaim(
  const std::shared_ptr<const WeaveVolumeMeta>& metadata) const
{
  return metadata && metadata->members.size() * 100 <=
    uint64_t{metadata->data_shards} * cleanup_->live_percent;
}

void WeavePGController::Impl::finish_cleanup()
{
  if (!cleanup_) return;
  auto on_finish = std::move(cleanup_->on_finish);
  cleanup_.reset();
  on_finish();
}

void WeavePGController::Impl::request_cleanup(
  unsigned live_percent, std::function<void()> on_finish)
{
  if (!enabled_ || !pg_interface_->primary() || !pg_interface_->active() ||
      !pg_interface_->clean() || cleanup_) {
    on_finish();
    return;
  }
  // A pass can only run against a loaded catalog, so try to load it now.
  initialize();
  if (!can_work()) {
    on_finish();
    return;
  }

  // Snapshot the Volumes once; every later step revalidates them against the
  // live catalog before acting.
  cleanup_.emplace(Cleanup{live_percent, catalog_.list_volumes(), 0,
                           std::move(on_finish)});
  // Reclaim empty containers before allocating copies of surviving members.
  std::partition(cleanup_->volumes.begin(), cleanup_->volumes.end(),
    [](const auto& metadata) { return metadata->members.empty(); });
  resume_cleanup();
}

// ---------------------------------------------------------------------------
// Conversion job
// ---------------------------------------------------------------------------

bool WeavePGController::Impl::start_deaggregation(
  const std::shared_ptr<const WeaveVolumeMeta>& volume)
{
  if (!geometry_matches(volume)) {
    fail_waiters(-EIO);
    return true;
  }
  // A missing Volume is not a retryable condition: the mapping is broken.
  const auto object = pg_interface_->inspect(volume->volume_oid);
  if (!object.exists) {
    fail_waiters(-ENOENT);
    return true;
  }

  // The caller owns its retry path: foreground admission or cleanup.
  if (object.busy()) return false;

  auto lease = pg_interface_->acquire();
  if (!lease) return false;

  auto members = unpack_candidates(volume, object);
  start_job(std::move(members), *volume, std::move(lease), true,
            object.info.user_version, object.info.size);
  return true;
}

// A Volume written by another EC profile or stripe unit can never be
// interpreted by this backend, so the mapping is failed rather than retried.
bool WeavePGController::Impl::geometry_matches(
  const std::shared_ptr<const WeaveVolumeMeta>& volume) const
{
  const auto geometry = pg_interface_->geometry();
  return geometry.data_shards && volume->data_shards == geometry.data_shards &&
    geometry.unit && volume->slot_size % geometry.unit == 0;
}

// The Volume is the only surviving copy of each member's bytes, so the
// restoration candidates carry its native version and per-member length.
std::vector<WeaveCandidate> WeavePGController::Impl::unpack_candidates(
  const std::shared_ptr<const WeaveVolumeMeta>& volume,
  const WeaveObjectState& object) const
{
  std::vector<WeaveCandidate> members;
  members.reserve(volume->members.size());
  for (const auto& [oid, member] : volume->members) {
    members.push_back({oid, member.size, object.info.version,
      member.user_version, member.mtime, ceph::mono_clock::now()});
  }
  return members;
}

void WeavePGController::Impl::start_job(std::vector<WeaveCandidate> members,
  WeaveVolumeMeta volume, std::unique_ptr<WeaveLease> lease, bool unpack,
  version_t version, uint64_t size)
{
  // Reserve every source under this identity before the job can run any
  // validation, so no later request can mutate them.
  const auto identity = ++sequence_;
  for (const auto& member : members) {
    reserved_[member.oid] = identity;
  }

  job_ = std::make_shared<WeaveConversionJob>(*pg_interface_, identity,
    pg_interface_->geometry().unit, std::move(members), std::move(volume),
    std::move(lease), make_job_hooks(identity, unpack), unpack, version, size);
  job_->start();
}

WeaveConversionJob::Hooks WeavePGController::Impl::make_job_hooks(
  uint64_t identity, bool unpack)
{
  return WeaveConversionJob::Hooks{
    [this, identity] { return job_is_valid(identity); },
    [this, identity] { publish_job(identity); },
    [this, identity] { detach_job(identity); },
    [this, identity, unpack](WeaveConversionJob::Result result) {
      finish_job(identity, unpack, result);
    }};
}

// Validation runs under the PG lock while the controller holds reservations,
// so no source can change between this check and the Volume write.
bool WeavePGController::Impl::job_is_valid(uint64_t identity) const
{
  // The job must still own every reservation it took when it started.
  if (!job_ || job_->identity() != identity) return false;

  for (const auto& member : job_->members()) {
    const auto object = pg_interface_->inspect(member.oid);
    const auto reservation = reserved_.find(member.oid);
    if (reservation == reserved_.end() || reservation->second != identity ||
        catalog_.lookup(member.oid) || !object.exists || object.blocks_pack() ||
        object.info.version != member.version ||
        object.snap_sequence !=
          job_->volume().members.at(member.oid).snap_sequence ||
        !WeaveCandidateIndex::eligible(object.info) ||
        !object.info.watchers.empty() || object.has_clones) {
      return false;
    }
  }
  return true;
}

// Publication only mirrors a Volume write that already committed.
void WeavePGController::Impl::publish_job(uint64_t identity)
{
  if (job_ && job_->identity() == identity) {
    catalog_.upsert(job_->volume());
  }
}

// Detachment only mirrors a durable Volume deletion.
void WeavePGController::Impl::detach_job(uint64_t identity)
{
  if (!job_ || job_->identity() != identity) return;
  catalog_.remove_volume(job_->volume().volume_oid);
}

void WeavePGController::Impl::finish_job(uint64_t identity, bool unpack,
                                         WeaveConversionJob::Result result)
{
  if (!job_ || job_->identity() != identity) return;
  release_reservations(identity, result.restore_candidates);
  job_.reset();

  // A cancelled job is only bookkeeping: the PG is changing role or is
  // shutting down, and the waiters were already released by on_pg_change().
  if (result.cancelled) return;

  // A failed Volume write may have committed before its reply was lost.
  // Resolve ownership before any waiter or new candidate can mutate a source.
  if (!unpack && result.error) translator_.shutdown();
  initialize();

  const bool had_waiters = !waiting_for_conversion_.empty();
  if (result.error) fail_waiters(result.error);
  else pg_interface_->requeue(waiting_for_conversion_);

  // Cleanup continues from this completion. Only yield when client requests
  // have just been handed back to the PG; scans need no completion wakeup.
  if (had_waiters && !result.error) schedule_cleanup_retry();
  else resume_cleanup();
}

void WeavePGController::Impl::release_reservations(uint64_t identity,
                                                   bool restore_candidates)
{
  if (restore_candidates) configure_candidates();
  for (const auto& member : job_->members()) {
    auto p = reserved_.find(member.oid);
    if (p != reserved_.end() && p->second == identity) {
      reserved_.erase(p);
    }
    // A rejected or cancelled job leaves its sources unchanged, so they are
    // candidates again.
    if (!restore_candidates) continue;

    const auto object = pg_interface_->inspect(member.oid);
    if (object.exists && !catalog_.lookup(member.oid)) {
      candidates_.upsert(object.info, ceph::mono_clock::now());
    }
  }
}

void WeavePGController::Impl::cancel_job()
{
  if (!job_) return;
  // Keep the job alive while it runs its own finish hook.
  const auto job = job_;
  job->cancel();
}

void WeavePGController::Impl::fail_waiters(int error)
{
  auto waiting = std::move(waiting_for_conversion_);
  waiting_for_conversion_.clear();
  for (auto& request : waiting) {
    pg_interface_->reply_error(request, error);
  }
}

void WeavePGController::Impl::schedule_materialization_retry()
{
  if (job_ || waiting_for_conversion_.empty()) return;
  auto ref = pg_interface_->pin();
  const auto generation = generation_;
  pg_interface_->retry(WeaveRetryKind::kMaterialization,
    [this, ref = std::move(ref), generation] {
      if (generation != generation_ || job_) return;
      // Retry the original client admission, which rechecks the catalog and
      // attempts materialization. Do not scan or start unrelated cleanup here.
      pg_interface_->requeue(waiting_for_conversion_);
    });
}

// ---------------------------------------------------------------------------
// Client request admission
// ---------------------------------------------------------------------------

RequestDisposition WeavePGController::Impl::reject(OpRequestRef& op, int error)
{
  pg_interface_->reply_error(op, error);
  return RequestDisposition::kRejected;
}

// Strips Weave's internal read flags and reports whether this is a server-side
// physical request. Returns the error that stops the request, or 0 when the
// operations may continue: EC_CALL is never a client request, it can only be a
// translated class call whose owning job is gone.
int WeavePGController::Impl::consume_internal_ops(MOSDOp& message,
                                                  bool& internal)
{
  for (auto& entry : message.ops) {
    // The native OSD must never see Weave's private read flags.
    internal |= entry.op.flags & kInternalIo;
    entry.op.flags = store_read_flags(entry.op.flags) & ~kInternalIo;
    // A translated class call is only meaningful to the job that issued it.
    if (entry.op.op == CEPH_OSD_OP_EC_CALL) return -EOPNOTSUPP;
  }
  return 0;
}

// A physical request is admitted only by the job that issued its tid. An
// Objecter retry can outlive a peering change and arrive without a job.
bool WeavePGController::Impl::background_io_unauthenticated(
  const OpRequestRef& op, const MOSDOp& message) const
{
  if (!op->is_background_weave_io()) return false;
  return !job_ || !job_->authenticates(message.get_tid());
}

// Clients never address the private namespace directly: only members translated
// from a logical request may reference it.
bool WeavePGController::Impl::private_object_access_denied(
  const OpRequestRef& op, const MOSDOp& message, bool internal) const
{
  if (internal || op->is_background_weave_io()) return false;
  return is_private_object(message.get_hobj());
}

std::optional<RequestDisposition>
WeavePGController::Impl::defer_for_metadata_recovery(OpRequestRef& op)
{
  if (op->is_background_weave_io() || !pg_interface_->primary() ||
      (translator_.initialized() && !pg_interface_->has_missing())) {
    return std::nullopt;
  }
  // Either this OSD cannot describe the objects yet (missing map entries) or
  // the last load failed permanently.
  translator_.shutdown();
  if (metadata_error_ < 0 && !pg_interface_->has_missing()) {
    return reject(op, metadata_error_);
  }

  // A missing local Volume is absent from the index, not a deleted logical
  // object. This gate also covers PG listing before native PG-op dispatch.
  waiting_for_recovery_.push_back(op);
  op->mark_delayed("waiting for Weave metadata recovery");
  return RequestDisposition::kDeferred;
}

// Filtered listing also consults Volume attributes. Keep it behind the
// deletion reply so it cannot observe a stale catalog over a missing Volume.
std::optional<RequestDisposition>
WeavePGController::Impl::defer_listing_during_retirement(
  const OpRequestRef& op, const MOSDOp& message)
{
  if (!job_ || job_->stage() != WeaveConversionJob::Stage::kRetiringVolume ||
      !has_pg_op(message.ops)) {
    return std::nullopt;
  }
  waiting_for_conversion_.push_back(op);
  op->mark_delayed("waiting for Weave retirement");
  return RequestDisposition::kDeferred;
}

RequestDisposition WeavePGController::Impl::prepare_request(OpRequestRef& op)
{
  auto* message = static_cast<MOSDOp*>(op->get_nonconst_req());
  // Native lock/recovery queues retain the translated request. Restore it
  // before do_op checks the logical namespace and object capability again.
  translator_.finish_request(op);

  const bool local_source = message->get_source().is_osd() &&
    message->get_source().num() == pg_interface_->osd_id();
  // Classify the request before anything else looks at its target.
  bool internal = false;
  if (const int unsupported = consume_internal_ops(*message, internal)) {
    return reject(op, unsupported);
  }

  // An Objecter retry can arrive at a different primary after peering. Never
  // reinterpret an old physical remove/write as an ordinary logical request.
  if (internal && !local_source) return reject(op, -ECANCELED);

  if (message->get_weave_read_route() && !enabled_) {
    return reject(op, -EAGAIN);
  }
  if (!enabled_) return RequestDisposition::kNative;

  // From here on the request is Weave's to admit.
  if (internal) {
    // Server-only physical Objecter request.
    op->set_background_weave_io();
  }
  if (background_io_unauthenticated(op, *message)) {
    return reject(op, -ECANCELED);
  }
  if (private_object_access_denied(op, *message, internal)) {
    return reject(op, -ENOENT);
  }

  // Requests that need a mapping, or a listing that reads one, wait until the
  // PG can answer them.
  if (auto deferred = defer_for_metadata_recovery(op)) return *deferred;
  if (auto deferred = defer_listing_during_retirement(op, *message)) {
    return *deferred;
  }
  return RequestDisposition::kNative;
}

RequestDisposition WeavePGController::Impl::accept_routed_read(OpRequestRef& op)
{
  const int result = reads_.accept(op);
  if (result < 0) {
    translator_.finish_request(op);
    return reject(op, result);
  }
  return RequestDisposition::kTranslated;
}

// Packing reads use the native source until publication, then the Volume.
// Existing native readers hold their object locks while source deletion waits;
// queued requests re-enter this routing decision. Mutations stay reserved.
// Unpacking retains its read barrier through the durable Volume deletion.
bool WeavePGController::Impl::defer_while_reserved(const hobject_t& head,
                                                   OpRequestRef& op)
{
  if (!reserved_.count(head) || can_read_during_pack(op)) return false;
  waiting_for_conversion_.push_back(op);
  op->mark_delayed("waiting for Weave publication");
  return true;
}

// A logical DELETE needs no snapshot context of its own; it only shrinks the
// mapping. Everything else must go through the native object first.
bool WeavePGController::Impl::is_logical_delete(
  const MOSDOp& message, bool snapshot,
  const std::shared_ptr<const WeaveVolumeMeta>& metadata) const
{
  // A snapshot context, on the request or in the pool, means the native
  // copy-on-write path has to run before the mapping shrinks.
  if (!metadata || snapshot || pg_interface_->snap_sequence() != 0 ||
      message.get_snap_seq() != 0) {
    return false;
  }

  // Only a trailing DELETE may update the mapping directly.
  return !message.ops.empty() &&
    message.ops.back().op.op == CEPH_OSD_OP_DELETE &&
    translator_.supports_member_ops(message.ops);
}

// Materialize snapshots using the original head's snapshot sequence. Native
// clone lookup and subsequent copy-on-write then preserve snapshot isolation.
bool WeavePGController::Impl::needs_native_transition(
  const OpRequestRef& op, const MOSDOp& message, bool snapshot,
  bool logical_delete) const
{
  if (snapshot) return true;
  if (op->may_write()) return !logical_delete;
  if (op->may_cache()) return true;
  // Operations Weave cannot translate keep their native meaning.
  return !translator_.supports_member_ops(message.ops);
}

// A cancelled publication/materialization can leave a native shadow.
// Drain that transition before deletion so removing the mapping cannot
// expose an obsolete native copy after restart.
std::optional<RequestDisposition>
WeavePGController::Impl::drain_shadow_before_delete(
  OpRequestRef& op, const MOSDOp& message, bool logical_delete,
  bool& needs_native)
{
  if (!logical_delete || op->is_weave_member_op()) return std::nullopt;
  const auto& oid = message.get_hobj();
  if (pg_interface_->wait_for_available(oid, op)) {
    return RequestDisposition::kDeferred;
  }
  needs_native = needs_native || pg_interface_->inspect(oid).exists;
  return std::nullopt;
}

std::optional<RequestDisposition>
WeavePGController::Impl::defer_for_materialization(
  OpRequestRef& op, const std::shared_ptr<const WeaveVolumeMeta>& metadata,
  bool needs_native)
{
  if (!metadata || !needs_native || op->is_weave_member_op()) {
    return std::nullopt;
  }
  if (pg_interface_->wait_for_available(metadata->volume_oid, op)) {
    return RequestDisposition::kDeferred;
  }
  waiting_for_conversion_.push_back(op);
  op->mark_delayed("waiting for Weave materialization");
  if (!job_ && !start_deaggregation(metadata)) schedule_materialization_retry();
  return RequestDisposition::kDeferred;
}

bool WeavePGController::Impl::can_read_during_pack(const OpRequestRef& op) const
{
  const auto* message = op->get_req<MOSDOp>();
  if (!job_ || !job_->packing()) return false;

  // Only a plain head read can share the source with packing; anything with
  // ordering, cache or PG semantics must wait for publication.
  return message->get_snapid() == CEPH_NOSNAP &&
    op->may_read() && !op->may_write() && !op->may_cache() &&
    !op->rwordered() && !op->includes_pg_op() &&
    !(message->get_flags() &
      (CEPH_OSD_FLAG_SKIPRWLOCKS | CEPH_OSD_FLAG_FLUSH)) &&
    translator_.supports_member_ops(message->ops);
}

RequestDisposition WeavePGController::Impl::preprocess_client_op(
  OpRequestRef& op)
{
  if (!enabled_ || op->is_background_weave_io()) {
    return RequestDisposition::kNative;
  }
  translator_.finish_request(op);

  // A request that arrives with a route was accepted here once already: it is
  // a member operation to translate, not a logical request to route.
  auto* message = static_cast<MOSDOp*>(op->get_nonconst_req());
  if (message->get_weave_read_route()) return accept_routed_read(op);
  if (!pg_interface_->primary()) return RequestDisposition::kNative;

  const bool snapshot = message->get_hobj().snap != CEPH_NOSNAP;
  const auto head = message->get_hobj().get_head();

  // A reserved head belongs to the running job until that job finishes.
  if (defer_while_reserved(head, op)) return RequestDisposition::kDeferred;

  auto metadata = catalog_.lookup(head);
  const bool logical_delete = is_logical_delete(*message, snapshot, metadata);
  bool needs_native =
    needs_native_transition(op, *message, snapshot, logical_delete);

  // Deletions and native-path transitions wait for the mapping to settle
  // before anything is translated.
  if (auto deferred = drain_shadow_before_delete(op, *message, logical_delete,
                                                 needs_native)) {
    return *deferred;
  }
  if (auto deferred = defer_for_materialization(op, metadata, needs_native)) {
    return *deferred;
  }

  // Translate in place: a published member is then served from its Volume.
  const int result = translator_.preprocess(op);
  if (result < 0) return reject(op, result);
  if (reads_.redirect(op)) return RequestDisposition::kReplied;
  return op->is_weave_member_op() ? RequestDisposition::kTranslated
                                      : RequestDisposition::kNative;
}

std::optional<version_t> WeavePGController::Impl::internal_copy_version(
  const OpRequestRef& op) const
{
  if (!job_ || !op || !op->is_background_weave_io()) return std::nullopt;
  auto* message = static_cast<MOSDOp*>(op->get_nonconst_req());
  return job_->copy_version(message->get_hobj());
}

std::optional<snapid_t> WeavePGController::Impl::internal_copy_snap_sequence(
  const OpRequestRef& op) const
{
  if (!job_ || !op || !op->is_background_weave_io()) return std::nullopt;
  return job_->copy_snap_sequence(op->get_req<MOSDOp>()->get_hobj());
}

// ---------------------------------------------------------------------------
// Commit notifications and member deletion
// ---------------------------------------------------------------------------

void WeavePGController::Impl::on_commit(const object_info_t& oi, bool exists,
                                        const OpRequestRef& op)
{
  // Physical writes of the Volume itself are not logical commits.
  if (!enabled_ || !pg_interface_->primary() ||
      (op && op->is_background_weave_io())) {
    return;
  }
  if (oi.soid.snap != CEPH_NOSNAP) return;

  // A committed member deletion shrinks the mapping; nothing else about the
  // object changed.
  const auto* context = op ? op->get_weave_context() : nullptr;
  if (context && context->member_deleted()) {
    apply_member_deletion(oi, *context->original_oid());
    return;
  }

  // A Volume object never becomes a candidate. Volumes only ever exist in the
  // private namespace (new_volume is their only creator), so this covers the
  // whole catalog; a refresh of an object that already has a mapping is dropped
  // by refresh_candidate() below.
  if (is_private_object(oi.soid)) return;

  refresh_candidate(oi, exists);
}

// Apply only this deletion, not an older full snapshot: later committed
// deletions or a metadata reload may already have removed other members.
void WeavePGController::Impl::apply_member_deletion(const object_info_t& oi,
                                                    const hobject_t& member)
{
  catalog_.remove_member(oi.soid, member);
  candidates_.erase(member);
}

void WeavePGController::Impl::refresh_candidate(const object_info_t& oi,
                                                bool exists)
{
  configure_candidates();
  if (!exists || catalog_.lookup(oi.soid)) candidates_.erase(oi.soid);
  else if (!reserved_.count(oi.soid))
    candidates_.upsert(oi, ceph::mono_clock::now());
}

int WeavePGController::Impl::prepare_member_delete(const OpRequestRef& op,
  WeaveTransaction& txn)
{
  // The caller already holds the native object lock, so the projected
  // attribute cache is the current membership.
  bufferlist encoded;
  int result = txn.read_attribute(kVolumeMetaXattr, encoded);
  if (result < 0) return result;

  bufferlist updated;
  result = translator_.prepare_member_delete(op, encoded, updated);
  if (result < 0) return result;

  // The shrunken membership is committed with the native transaction, but
  // the in-memory mapping is only updated later, by on_commit.
  txn.set_attribute(kVolumeMetaXattr, updated);
  return 0;
}

// ---------------------------------------------------------------------------
// Native query surface
// ---------------------------------------------------------------------------

void WeavePGController::Impl::finish_reply(const OpRequestRef& op,
                                           MOSDOpReply* reply)
{
  translator_.restore_client_reply_ops(op, reply);
  finish_request(op);
}

void WeavePGController::Impl::finish_request(const OpRequestRef& op)
{
  if (enabled_) translator_.finish_request(op);
}

ClsParmContext* WeavePGController::Impl::get_cls_ctx(const OpRequestRef& op,
                                                     std::size_t subop) const
{
  return enabled_ ? translator_.get_cls_ctx(op, subop) : nullptr;
}

bool WeavePGController::Impl::is_private_object(const hobject_t& oid) const
{
  return enabled_ && oid.nspace == kVolumeNamespace;
}

bool WeavePGController::Impl::is_logical_member(const hobject_t& oid) const
{
  return enabled_ && translator_.initialized() && catalog_.contains(oid);
}

std::pair<hobject_t, std::string> WeavePGController::Impl::listing_attribute(
  const hobject_t& oid, const std::string& key) const
{
  auto volume =
    enabled_ && translator_.initialized() ? catalog_.lookup(oid) : nullptr;
  if (!volume) return {oid, key};
  // PGLS supplies the ObjectStore name, including the user-xattr underscore.
  if (key.empty() || key.front() != '_') return {oid, key};
  return {volume->volume_oid, "_" + xattr_name(oid, key.substr(1))};
}

void WeavePGController::Impl::drop_private_entries(
  std::vector<hobject_t>& entries) const
{
  entries.erase(std::remove_if(entries.begin(), entries.end(),
    [this](const auto& oid) { return is_private_object(oid); }),
    entries.end());
}

void WeavePGController::Impl::merge_listing(const hobject_t& start,
  unsigned limit, std::vector<hobject_t>& entries, hobject_t& next) const
{
  if (!enabled_) return;

  // Merge the native page with the logical objects the catalog knows about.
  std::optional<hobject_t> logical_next;
  auto logical = catalog_.list_objects(start, limit, logical_next);
  drop_private_entries(entries);
  entries.insert(entries.end(), logical.begin(), logical.end());
  sort_and_deduplicate(entries);

  // Neither pager may skip or repeat an object, so the page ends at the
  // earlier of the two continuation points.
  if (logical_next && *logical_next < next) next = *logical_next;
  entries.erase(std::lower_bound(entries.begin(), entries.end(), next),
                entries.end());
}

bool WeavePGController::Impl::encode_logical_stat(const OpRequestRef& op,
                                                  bufferlist& out) const
{
  return enabled_ && translator_.encode_logical_stat(op, out);
}

uint64_t WeavePGController::Impl::logical_user_version(
  const OpRequestRef& op, uint64_t fallback) const
{
  return translator_.logical_user_version(op, fallback);
}

void WeavePGController::Impl::encode_getxattrs_result(
  const OpRequestRef& request, const OSDOp& op, XAttrs& attrs,
  bufferlist& encoded) const
{
  translator_.encode_getxattrs_result(request, op, attrs, encoded);
}

int WeavePGController::Impl::translate_native_class_ops(
  OpRequestRef& request, std::vector<OSDOp>& ops, uint64_t size)
{
  return enabled_ ? translator_.translate_native_class_ops(request, ops, size) : 0;
}

}  // namespace ceph::weave
