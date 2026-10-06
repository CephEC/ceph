// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <sstream>

#include "WeavePGInterface.h"
#include "WeaveService.h"
#include "common/Finisher.h"
#include "detail/WeaveLayout.h"
#include "detail/WeaveXAttr.h"
#include "detail/WeaveWriteLimits.h"
#include "osd/OSD.h"
#include "osd/PrimaryLogPG.h"
#include "osdc/Objecter.h"

namespace {

void mark_internal(ObjectOperation& op) {
  // Every sub-op of a Weave request carries the flag; the receiving OSD uses
  // it to tell server-side I/O from a client request.
  for (auto& entry : op.ops) {
    entry.op.flags = entry.op.flags | ceph::weave::kInternalIo;
  }
}

object_locator_t locator(const hobject_t& oid) {
  object_locator_t result(oid);

  // Only unkeyed objects need the explicit hash; keyed ones route by key.
  if (result.key.empty()) result.hash = oid.get_hash();
  return result;
}

// Both queued retries and I/O completions own a PG reference until after their
// callbacks return. The PG lock is acquired only when they execute.
class LockedPGCallback {
public:
  LockedPGCallback(PGRef pg, std::function<void()> callback)
    : pg_(std::move(pg)), callback_(std::move(callback)) {}

  void operator()() const {
    std::lock_guard lock(*pg_);
    callback_();
  }

private:
  PGRef pg_;
  std::function<void()> callback_;
};

class PGIoCompletion final : public Context {
public:
  PGIoCompletion(PGRef pg, ceph::weave::WeaveCompletion callback)
    : pg_(std::move(pg)), callback_(std::move(callback)) {}

private:
  void finish(int result) override {
    std::lock_guard lock(*pg_);
    callback_(result);
  }

  PGRef pg_;
  ceph::weave::WeaveCompletion callback_;
};

}  // namespace

// A nested native adapter has exactly the access of PrimaryLogPG. Weave's
// policy and state machine never receive this PG pointer or its object
// contexts.
class PrimaryLogPG::WeavePGAdapter final : public ceph::weave::WeavePGInterface {
public:
  WeavePGAdapter(PrimaryLogPG& pg, OSDService& osd) : pg_(pg), osd_(osd) {}

  std::shared_ptr<void> pin() override { return std::make_shared<PGRef>(&pg_); }

  ceph::weave::WeavePolicy policy() const override {
    const auto& conf = pg_.cct->_conf;
    return {conf->osd_weave_background_enabled,
      conf->osd_weave_min_object_size,
      conf->osd_weave_quiet_period,
      write_limits().data_limit(),
      static_cast<unsigned>(conf->osd_weave_max_padding_percent)};
  }

  ceph::weave::WeaveGeometry geometry() const override {
    return {
      static_cast<uint32_t>(pg_.get_pgbackend()->get_ec_data_chunk_count()),
      static_cast<uint64_t>(pg_.get_pgbackend()->get_ec_stripe_chunk_size())};
  }

  bool primary() const override { return pg_.is_primary(); }
  bool active() const override { return pg_.is_active(); }
  bool clean() const override { return pg_.is_clean(); }

  snapid_t snap_sequence() const override {
    return pg_.pool.info.get_snap_seq();
  }

  bool has_missing() const override {
    // recover_got clears the missing set before its transaction is submitted.
    return pg_.recovery_state.have_missing() || pg_.active_pushes > 0;
  }

  epoch_t epoch() const override { return pg_.get_last_peering_reset(); }

  bool current(epoch_t epoch) const override {
    return pg_.is_primary() && pg_.is_active() &&
      !pg_.pg_has_reset_since(epoch);
  }

  int osd_id() const override { return osd_.whoami; }

  ceph::weave::WeaveObjectState inspect(const hobject_t& oid) override {
    ceph::weave::WeaveObjectState result;
    auto obc = pg_.get_object_context(oid, false);
    if (obc) {
      result.info = obc->obs.oi;
      result.exists = obc->obs.exists;

      // A shared reader is kReading; a waiter, a blocker or an admitted writer
      // is kBusy. Only packing may coexist with kReading.
      using Access = ceph::weave::WeaveObjectState::Access;
      if (!obc->waiters.empty() || obc->is_blocked()) {
        result.access = Access::kBusy;
      } else if (obc->rwstate.state == RWState::RWREAD) {
        result.access = Access::kReading;
      } else if (!obc->empty()) {
        result.access = Access::kBusy;
      }

      // Clone and snap state comes from the snap context, if it is loaded.
      result.has_clones = obc->ssc && !obc->ssc->snapset.clones.empty();
      if (obc->ssc) result.snap_sequence = obc->ssc->snapset.seq;
    }
    return result;
  }

  bool wait_for_available(const hobject_t& oid, OpRequestRef& op) override {
    // The first condition the object meets decides which wait list takes it;
    // each wait returns true so the caller stops processing the request.
    if (pg_.is_unreadable_object(oid)) {
      pg_.wait_for_unreadable_object(oid, op);
      return true;
    }

    if (pg_.is_degraded_or_backfilling_object(oid)) {
      pg_.wait_for_degraded_object(oid, op);
      return true;
    }
    return false;
  }

  int load_metadata(ceph::weave::WeaveVolumeAttrs& out) override {
    return pg_.get_pgbackend()->load_attr_mirror(
      ceph::weave::kVolumeMetaXattr, out);
  }

  hobject_t new_volume(const hobject_t& seed) override {
    // A locally unique name keeps two concurrent creators from landing on the
    // same physical Volume object.
    std::ostringstream name;
    name << "volume_" << osd_.whoami << '_' << pg_.info.pgid << '_'
         << pg_.get_osdmap_epoch() << '_' << osd_.get_tid() << '_'
         << ++sequence_;

    return hobject_t(object_t(name.str()), std::string(), CEPH_NOSNAP,
                     seed.get_hash(), seed.pool,
                     ceph::weave::kVolumeNamespace);
  }

  void requeue(std::list<OpRequestRef>& requests) override {
    pg_.requeue_ops(requests);
  }

  void reply_error(const OpRequestRef& op, int r) override {
    pg_.reply_op_error(op, r);
  }

  std::optional<ceph::weave::WeaveReadRoute> locate_read(
    const hobject_t& volume, unsigned member) override {
    // Cheap local gates first: redirects must be enabled, this OSD must serve
    // reads, and the member must be inside the pool's data-chunk range.
    // The member bound is checked before the unreadable check below, so an
    // out-of-range member is rejected without consulting recovery state.
    if (!pg_.cct->_conf.get_val<bool>("osd_weave_redirect_reads") ||
        !pg_.is_active() || (pg_.is_primary() && !pg_.is_clean()) ||
        member >= static_cast<unsigned>(
          pg_.get_pgbackend()->get_ec_data_chunk_count()) ||
        pg_.is_unreadable_object(volume)) {
      return std::nullopt;
    }

    auto [found, target] = member_target(member);
    if (!found || !redirect_supported(target)) return std::nullopt;

    // The route is only a hint: it must name an object this OSD can actually
    // serve right now, at the version the receiver will re-check.
    auto object = inspect(volume);
    if (!object.exists || object.busy() ||
        (!pg_.is_primary() &&
         !pg_.recovery_state.can_serve_replica_read(volume))) {
      return std::nullopt;
    }

    return ceph::weave::WeaveReadRoute{
      volume, target, pg_.get_osdmap_epoch(), object.info.version};
  }

  int load_read_route(const ceph::weave::WeaveReadRoute& route,
                      bufferlist& out) override {
    if (!route_targets_local(route)) return -EAGAIN;

    // The hint is only usable while this shard still serves that version.
    auto object = inspect(route.volume);
    if (!object.exists || object.busy() ||
        object.info.version != route.version) {
      return -EAGAIN;
    }

    return pg_.get_pgbackend()->objects_get_attr(
      route.volume, ceph::weave::kVolumeMetaXattr, &out);
  }

  void reply_read_redirect(const OpRequestRef& op,
                           const ceph::weave::WeaveReadRoute& route) override {
    auto* m = static_cast<MOSDOp*>(op->get_nonconst_req());
    auto* reply = new MOSDOpReply(
      m, -EAGAIN, pg_.get_osdmap_epoch(),
      m->get_flags() & (CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK), true);
    reply->set_weave_read_route(route);

    // The client replays the same request against the named shard.
    lsubdout(pg_.cct, osd, 10) << "Weave redirect " << m->get_hobj() << " to "
      << route.target << " volume=" << route.volume << dendl;
    m->get_connection()->send_message(reply);
  }

  std::unique_ptr<ceph::weave::WeaveLease> acquire() override {
    return osd_.weave_service->acquire(pg_.info.pgid);
  }

  void retry(ceph::weave::WeaveRetryKind kind,
             std::function<void()> callback) override {
    osd_.weave_service->retry(pg_.info.pgid, kind,
      LockedPGCallback(PGRef(&pg_), std::move(callback)));
  }

  void cancel_retries() override { osd_.weave_service->cancel(pg_.info.pgid); }

  void post(std::function<void()> callback) override {
    osd_.weave_service->post(std::move(callback));
  }

  void serialized(std::function<void()> callback) override {
    // Reacquire the PG lock: work posted through post() runs without it.
    std::lock_guard lock(pg_);
    callback();
  }

  void conversion_checkpoint(const char* point, size_t member) override {
    const auto target =
      pg_.cct->_conf.get_val<std::string>("osd_weave_debug_crash_point");

    // Match "point:member" exactly; any other point is a no-op.
    if (target == std::string(point) + ":" + std::to_string(member)) {
      lderr(pg_.cct) << "Weave conversion crash at " << target << dendl;
      ceph_abort_msg("injected Weave conversion crash");
    }
  }

  ceph_tid_t read(const hobject_t& oid, version_t version, uint64_t size,
                  bufferlist* data, ceph::weave::WeaveAttrs* attrs,
                  ceph::weave::WeaveCompletion callback) override {
    ObjectOperation op;
    // Read exactly the validated version, plus the attributes the caller
    // asked for; both ride in one Objecter round trip.
    op.assert_version(version);
    op.read(0, size, data, nullptr, nullptr);
    op.getxattrs(attrs, nullptr);
    mark_internal(op);

    return osd_.objecter->read(oid.oid, locator(oid), op, CEPH_NOSNAP, nullptr,
                               CEPH_OSD_FLAG_IGNORE_OVERLAY,
                               make_io_completion(std::move(callback)));
  }

  ceph_tid_t write(const hobject_t& oid, const bufferlist& data,
                   const ceph::weave::WeaveAttrs& attrs, utime_t mtime,
                   bool replace,
                   ceph::weave::WeaveCompletion callback) override {
    if (!write_limits().accepts(data.length(), attrs)) {
      make_io_completion(std::move(callback))->complete(-EFBIG);
      return 0;
    }
    ObjectOperation op;
    // A replace drops the old object first, but a missing one must not fail
    // the write.
    if (replace) {
      op.remove();
      op.set_last_op_flags(CEPH_OSD_OP_FLAG_FAILOK);
    }
    op.create(true);

    // bufferlist shares payload storage; ObjectOperation consumes the list.
    auto payload = data;
    op.write_full(payload);
    for (const auto& [key, value] : attrs) op.setxattr(key, value);
    mark_internal(op);

    return mutate(oid, op, ceph::real_clock::from_ceph_timespec(mtime),
                  std::move(callback));
  }

  ceph_tid_t remove(const hobject_t& oid, std::optional<version_t> version,
                    ceph::weave::WeaveCompletion callback) override {
    if (version && pg_.cct->_conf.get_val<bool>(
          "osd_weave_debug_source_remove_error")) {
      // Keep fault delivery asynchronous, just like an Objecter completion.
      lsubdout(pg_.cct, osd, 10)
        << "Weave source retirement injected EIO for " << oid << dendl;
      make_io_completion(std::move(callback))->complete(-EIO);
      return 0;
    }

    ObjectOperation op;
    // A pinned version guards against retiring an object that was rewritten.
    if (version) op.assert_version(*version);
    op.remove();
    mark_internal(op);

    return mutate(oid, op, ceph::real_clock::now(), std::move(callback));
  }

  void cancel_io(ceph_tid_t tid) override {
    osd_.objecter->op_cancel(tid, -ECANCELED);
  }

private:
  ceph::weave::WeaveWriteLimits write_limits() const {
    const auto& conf = pg_.cct->_conf;
    return {conf.get_val<Option::size_t>("osd_max_object_size"),
            conf->osd_max_write_size};
  }

  // Always hop to the Objecter finisher, even for locally rejected I/O.
  // PGIoCompletion then holds the PG lock while advancing the job.
  Context* make_io_completion(ceph::weave::WeaveCompletion callback) {
    return new C_OnFinisher(
      new PGIoCompletion(PGRef(&pg_), std::move(callback)),
      osd_.get_objecter_finisher(pg_.get_pg_shard()));
  }

  // The primary redirects to another shard; the receiving replica validates
  // that the member belongs to its own shard.
  bool redirect_supported(const pg_shard_t& target) const {
    if (target.osd < 0 || !pg_.get_osdmap()->is_up(target.osd)) return false;

    // The target must understand the redirect reply this OSD is about to send.
    const auto& features = pg_.get_osdmap()->get_xinfo(target.osd).features;
    if (!HAVE_FEATURE(features, WEAVE_READ_REDIRECT) ||
        !HAVE_FEATURE(features, SERVER_QUINCY)) {
      return false;
    }
    if (pg_.is_primary()) {
      return target != pg_.whoami_shard();
    }
    return target == pg_.whoami_shard();
  }

  // The caller has already checked member against the data-chunk count.
  std::pair<bool, pg_shard_t> member_target(unsigned member) const {
    const int shard = pg_.get_pgbackend()->get_ec_data_shard(member);
    const auto acting = pg_.get_acting();
    // The mapping can point outside the acting set, e.g. while undersized.
    if (shard < 0 || static_cast<size_t>(shard) >= acting.size()) {
      return {false, pg_shard_t()};
    }

    return {true, pg_shard_t(acting[shard], shard_id_t(shard))};
  }

  // Admission checks in the order the receiving OSD applies them: role, epoch,
  // shard, pgid containment, readability, then servability.
  bool route_targets_local(const ceph::weave::WeaveReadRoute& route) const {
    if (pg_.is_primary() || !pg_.is_active()) return false;
    if (route.map_epoch != pg_.get_osdmap_epoch()) return false;

    // Only a replica of the routed shard of this very PG may serve the read.
    if (route.target != pg_.whoami_shard()) return false;
    if (route.volume.pool != pg_.info.pgid.pgid.pool()) return false;
    if (!pg_.info.pgid.pgid.contains(
          pg_.info.pgid.pgid.get_split_bits(pg_.pool.info.get_pg_num()),
          route.volume)) {
      return false;
    }

    // The member must be readable here, and actually servable by a replica.
    if (pg_.is_unreadable_object(route.volume)) return false;
    return pg_.recovery_state.can_serve_replica_read(route.volume);
  }

  // Shared Objecter path for write and remove: internal op flag, SnapContext
  // and completion framing are identical; only the op and mtime differ.
  ceph_tid_t mutate(const hobject_t& oid, ObjectOperation& op,
                    ceph::real_time mtime,
                    ceph::weave::WeaveCompletion callback) {
    return osd_.objecter->mutate(oid.oid, locator(oid), op, SnapContext(),
      mtime, CEPH_OSD_FLAG_IGNORE_OVERLAY |
        (oid.nspace == ceph::weave::kVolumeNamespace
           ? CEPH_OSD_FLAG_ENFORCE_SNAPC : 0),
      make_io_completion(std::move(callback)));
  }

  PrimaryLogPG& pg_;
  OSDService& osd_;
  uint64_t sequence_ = 0;
};

std::unique_ptr<ceph::weave::WeavePGInterface> PrimaryLogPG::make_weave_pg_adapter() {
  return std::make_unique<WeavePGAdapter>(*this, *osd);
}
