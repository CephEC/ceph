// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include "Volume.h"
#include "VolumeCatalog.h"
#include "common/Timer.h"
#include "include/Context.h"
#include "osd/ExtentCache.h"

#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <vector>

class OSDService;
class PrimaryLogPG;

namespace ceph::aggregate_ec {

class Aggregator {
public:
  static constexpr int PREPROCESS_CONTINUE = 0;
  static constexpr int PREPROCESS_REDIRECT = 1;
  static constexpr int PREPROCESS_CONSUMED = 2;

  Aggregator(CephContext *cct, spg_t pgid, OSDService *osd, PrimaryLogPG *pg);
  ~Aggregator();

  void activate(uint8_t data_chunks, uint64_t chunk_size,
                bool flush_timer_enabled, double flush_timeout);
  bool initialized() const { return initialized_; }
  void shutdown();

  void load_metadata(std::vector<ceph::buffer::list> &encoded_metadata);

  // Returns PREPROCESS_CONSUMED when the request is retained, REDIRECT when a
  // translated read may need redirecting, CONTINUE for normal execution, or a
  // negative errno when preprocessing fails.
  int preprocess(OpRequestRef op);
  int translate(OpRequestRef &op, std::vector<OSDOp> &ops);

  bool owns_volume(const hobject_t &volume_oid) const;
  bool complete_volume(const hobject_t &volume_oid,
                       const std::vector<OSDOp> &ops,
                       MOSDOpReply *reply,
                       bool ignore_out_data);
  bool fail_volume(
    const hobject_t &volume_oid, int error,
    eversion_t version = eversion_t(), version_t user_version = 0,
    const std::vector<pg_log_op_return_item_t> &op_returns = {});
  bool fail_volume(const hobject_t &volume_oid,
                   MOSDOpReply *reply,
                   bool ignore_out_data);

  int list_objects(pg_nls_response_t &response, unsigned limit);

  void finish_cls(const hobject_t &volume_oid);
  ClsParmContext *get_cls_ctx(const hobject_t &volume_oid);
  void purge_origin(OpRequestRef op);
  std::vector<OpRequestRef> buffered_requests(
    const hobject_t &volume_oid) const;
  void finish_volume(const hobject_t &volume_oid);
  std::vector<OpRequestRef> cancel_volume(const hobject_t &volume_oid);

  void update_cache(const hobject_t &volume_oid, const std::vector<OSDOp> &ops);
  bool is_volume_cached(const hobject_t &volume_oid) const;
  void read_cached_volume(extent_map &result) const;
  volume_t inflight_volume(const hobject_t &volume_oid) const;
  std::optional<uint64_t> logical_offset(
    const OpRequestRef &op, uint64_t physical_offset) const;

private:
  struct TranslationContext;

  bool defer_conflicting_mutation(OpRequestRef op, const MOSDOp &m);
  int preprocess_aggregate(OpRequestRef op, MOSDOp &m);
  int buffer_new_object(OpRequestRef op, const hobject_t &oid);
  int ensure_active_volume_locked(const hobject_t &oid);

  int validate_mutations(
    const std::vector<OSDOp> &ops, TranslationContext &ctx) const;
  int translate_operation(
    OpRequestRef &op, OSDOp &osd_op, TranslationContext &ctx,
    std::vector<OSDOp> &appended_ops);
  int translate_call(OSDOp &osd_op, const TranslationContext &ctx);
  void translate_read(OSDOp &osd_op, const TranslationContext &ctx) const;
  void translate_delete(
    OpRequestRef &op, OSDOp &osd_op, TranslationContext &ctx,
    std::vector<OSDOp> &appended_ops);

  bool should_aggregate(const MOSDOp &m) const;
  bool should_translate(const MOSDOp &m) const;
  bool mutates_volume_metadata(const MOSDOp &m) const;
  void schedule_flush();
  void cancel_flush();
  void flush_now();
  void requeue_waiting();
  void cache_volume(const Volume &volume);
  void clear_ec_cache();
  hobject_t original_oid(OpRequestRef op, const MOSDOp &m);

  CephContext *cct_;
  spg_t pgid_;
  OSDService *osd_;
  PrimaryLogPG *pg_;

  uint32_t volume_capacity_ = 0;
  uint64_t chunk_size_ = 0;
  double flush_timeout_ = 1.0;
  bool flush_timer_enabled_ = false;
  bool initialized_ = false;

  ceph::mutex timer_lock_ = ceph::make_mutex("aggregate_ec::timer");
  SafeTimer flush_timer_;
  Context *flush_event_ = nullptr;
  bool timer_started_ = false;
  std::set<hobject_t> committed_;

  mutable std::mutex state_mutex_;
  // Mutable state follows this lifecycle:
  // active_volume_ -> flushing_ -> committed_ -> removed by finish_volume().
  std::unique_ptr<Volume> active_volume_;
  std::map<hobject_t, std::unique_ptr<Volume>> flushing_;
  // Requests blocked by an active/flushing Volume are retried in FIFO order.
  std::deque<OpRequestRef> waiting_;
  // Private metadata snapshots consumed by ECBackend during translated I/O.
  std::map<hobject_t, volume_t> inflight_volumes_;

  VolumeCatalog catalog_;
  // Translation replaces MOSDOp::hobj with the physical OID; retain the first
  // logical OID here until the request finishes or fails.
  std::map<OpRequestRef, hobject_t> original_oids_;
  std::map<hobject_t, std::unique_ptr<ClsParmContext>> cls_contexts_;

  mutable std::shared_mutex cache_mutex_;
  std::shared_ptr<volume_t> cached_volume_meta_;
  std::vector<ceph::buffer::list> cached_chunks_;
};

} // namespace ceph::aggregate_ec
