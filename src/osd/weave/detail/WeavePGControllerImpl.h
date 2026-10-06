// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <functional>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "osd/weave/WeavePGController.h"
#include "osd/weave/WeavePGInterface.h"

#include "WeaveCandidateIndex.h"
#include "WeaveCatalog.h"
#include "WeaveConversionJob.h"
#include "WeaveMemberTranslator.h"
#include "WeaveReadRouter.h"

namespace ceph::weave {

// PG-serialized implementation behind the WeavePGController facade. Owns the
// policy state (catalog, candidate index, reservations) and the single active
// conversion job. Every entry point below runs under the PG lock; the helpers
// are pure steps of those entry points and never take another lock.
class WeavePGController::Impl {
public:
  using XAttrs = std::map<std::string, ceph::buffer::list, std::less<>>;

  Impl(CephContext*, std::unique_ptr<WeavePGInterface>, bool);
  ~Impl();

  // Lifecycle and role changes. initialize() is the only place that loads the
  // catalog from disk; everything else assumes it has already run.
  void initialize();
  void on_recovery_progress();
  void on_pg_change(bool requeue = true);

  // Client request admission. Both calls may defer, reject or translate the
  // request; only preprocess_client_op() can send a reply itself.
  RequestDisposition prepare_request(OpRequestRef&);
  RequestDisposition preprocess_client_op(OpRequestRef&);

  // Native callbacks.
  void on_commit(const object_info_t&, bool exists, const OpRequestRef&);
  void request_cleanup(unsigned live_percent, std::function<void()> on_finish);
  int prepare_member_delete(const OpRequestRef&, WeaveTransaction&);
  void finish_reply(const OpRequestRef&, MOSDOpReply*);
  void finish_request(const OpRequestRef&);

  // Called by the PG's periodic queue item, never by a write completion.
  void scan_candidates();

  // Native query surface.
  ClsParmContext* get_cls_ctx(const OpRequestRef&, std::size_t) const;
  void merge_listing(const hobject_t&, unsigned, std::vector<hobject_t>&,
                     hobject_t& next) const;
  bool is_private_object(const hobject_t&) const;
  bool is_logical_member(const hobject_t&) const;
  std::pair<hobject_t, std::string> listing_attribute(
    const hobject_t&, const std::string&) const;
  bool encode_logical_stat(const OpRequestRef&, ceph::buffer::list&) const;
  uint64_t logical_user_version(const OpRequestRef&, uint64_t fallback) const;
  std::optional<version_t> internal_copy_version(const OpRequestRef&) const;
  std::optional<snapid_t> internal_copy_snap_sequence(
    const OpRequestRef&) const;
  void encode_getxattrs_result(const OpRequestRef&, const OSDOp&, XAttrs&,
                               ceph::buffer::list&) const;
  int translate_native_class_ops(OpRequestRef&, std::vector<OSDOp>&, uint64_t);

private:
  // One reclaim pass. Volumes are snapshotted at request time and revalidated
  // against the live catalog before each step, so a concurrent deletion only
  // shrinks the pass.
  struct Cleanup {
    unsigned live_percent;
    std::vector<std::shared_ptr<const WeaveVolumeMeta>> volumes;
    size_t next = 0;
    std::function<void()> on_finish;
  };

  // Lifecycle.
  bool reload_metadata();
  bool can_work() const;
  bool can_scan() const;
  void fail_recovery_waiters();

  // Background work.
  void configure_candidates();
  std::vector<WeaveCandidate> select_packable(
    const WeaveGeometry&, const WeavePolicy&, std::vector<hobject_t>& stale);
  bool candidate_available(const WeaveCandidate&, std::vector<hobject_t>& stale);
  WeaveVolumeMeta plan_volume(const std::vector<WeaveCandidate>&,
                              const WeaveGeometry&);
  // Cleanup progresses from its request and each conversion completion.
  void resume_cleanup();
  void schedule_cleanup_retry();
  void scan_cleanup();
  bool volume_needs_reclaim(
    const std::shared_ptr<const WeaveVolumeMeta>&) const;
  void finish_cleanup();

  // Conversion job.
  bool start_deaggregation(const std::shared_ptr<const WeaveVolumeMeta>&);
  bool geometry_matches(const std::shared_ptr<const WeaveVolumeMeta>&) const;
  std::vector<WeaveCandidate> unpack_candidates(
    const std::shared_ptr<const WeaveVolumeMeta>&,
    const WeaveObjectState&) const;
  void start_job(std::vector<WeaveCandidate>, WeaveVolumeMeta,
                 std::unique_ptr<WeaveLease>, WeaveConversionJob::Mode,
                 version_t = 0, uint64_t = 0);
  WeaveConversionJob::Hooks make_job_hooks(uint64_t identity,
                                          WeaveConversionJob::Mode mode);
  bool job_is_valid(uint64_t identity) const;
  void publish_job(uint64_t identity);
  void detach_job(uint64_t identity);
  void finish_job(uint64_t identity, WeaveConversionJob::Mode mode,
                  WeaveConversionJob::Result result);
  void release_reservations(uint64_t identity, bool restore_candidates);
  void cancel_job();
  void fail_waiters(int error);
  // A foreground materialization that could not start retries admission.
  void schedule_materialization_retry();

  // Request admission. Each predicate reports whether the request must be
  // rejected or deferred; the caller owns the reply.
  RequestDisposition reject(OpRequestRef&, int error);
  int consume_internal_ops(MOSDOp&, bool& internal);
  bool background_io_unauthenticated(const OpRequestRef&, const MOSDOp&) const;
  bool private_object_access_denied(const OpRequestRef&, const MOSDOp&,
                                    bool internal) const;
  std::optional<RequestDisposition> defer_for_metadata_recovery(
    OpRequestRef&);
  std::optional<RequestDisposition> defer_listing_during_retirement(
    const OpRequestRef&, const MOSDOp&);
  RequestDisposition accept_routed_read(OpRequestRef&);
  bool defer_while_reserved(const hobject_t& head, OpRequestRef&);
  bool is_logical_delete(const MOSDOp&, bool snapshot,
                         const std::shared_ptr<const WeaveVolumeMeta>&) const;
  bool needs_native_transition(const OpRequestRef&, const MOSDOp&,
                              bool snapshot, bool logical_delete) const;
  std::optional<RequestDisposition> drain_shadow_before_delete(
    OpRequestRef&, const MOSDOp&, bool logical_delete, bool& needs_native);
  std::optional<RequestDisposition> defer_for_materialization(
    OpRequestRef&, const std::shared_ptr<const WeaveVolumeMeta>&,
    bool needs_native);
  bool can_read_during_pack(const OpRequestRef&) const;

  // Commit notifications.
  void apply_member_deletion(const object_info_t&, const hobject_t& member);
  void refresh_candidate(const object_info_t&, bool exists);

  // Native query surface.
  void drop_private_entries(std::vector<hobject_t>&) const;

  std::unique_ptr<WeavePGInterface> pg_interface_;
  bool enabled_;
  // Permanent metadata error, reported to every waiting request.
  int metadata_error_ = 0;
  // Incremented on every role or peering change; stale wakeups compare it.
  uint64_t generation_ = 0;
  // Identity of the job that owns the current reservations.
  uint64_t sequence_ = 0;
  WeaveCatalog catalog_;
  WeaveMemberTranslator translator_;
  WeaveReadRouter reads_;
  WeaveCandidateIndex candidates_;
  std::map<hobject_t, uint64_t> reserved_;
  std::shared_ptr<WeaveConversionJob> job_;
  std::list<OpRequestRef> waiting_for_conversion_;
  std::list<OpRequestRef> waiting_for_recovery_;
  std::optional<Cleanup> cleanup_;
};

}  // namespace ceph::weave
