// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include "Aggregator.h"

#include <cstddef>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

class PrimaryLogPG;

namespace ceph::aggregate_ec {

// Owns the aggregateEC hooks injected into PrimaryLogPG, ECBackend, and CLS.
// Aggregator remains the core state machine; this class translates between
// that state machine and the surrounding Ceph request lifecycle.
class PGIntegration {
public:
  using XAttrs =
    std::map<std::string, ceph::buffer::list, std::less<>>;

  PGIntegration(
    CephContext *cct, spg_t pgid, OSDService *osd, PrimaryLogPG *pg,
    bool enabled);
  ~PGIntegration();

  bool enabled() const { return enabled_; }

  // PG lifecycle hooks. Metadata is loaded only after the EC backend exists;
  // recovery waiters stay private to this integration boundary.
  void initialize();
  void reload_metadata();
  void on_recovery_progress();
  void on_pg_change();

  // Request-entry hook. true means the request was buffered, replied to, or
  // redirected, so PrimaryLogPG must stop its normal request path.
  bool preprocess_client_op(OpRequestRef &op);

  // Completion hooks fan one physical volume result back out to its logical
  // requests and release all request-translation state on every exit path.
  bool handle_error(
    OpRequestRef op, int error, eversion_t version, version_t user_version,
    const std::vector<pg_log_op_return_item_t> &op_returns);
  bool handle_write_error_reply(
    OpRequestRef op, MOSDOpReply *reply, bool ignore_out_data);
  bool handle_success_reply(
    const hobject_t &volume_oid, const std::vector<OSDOp> &ops,
    OpRequestRef op, MOSDOpReply *reply, bool ignore_out_data);
  void finish_context(const hobject_t &volume_oid, OpRequestRef op);

  // Write-side effect and cancellation hooks for a buffered volume write.
  bool handles_split_effects(const OpRequestRef &op) const;
  std::vector<OpRequestRef> buffered_requests(
    const hobject_t &volume_oid) const;
  bool requeue_aborted_volume(
    OpRequestRef &op, std::list<OpRequestRef> &requeue);

  // Read/CLS hooks that translate physical-volume results back to the logical
  // object view exposed to clients and object-class methods.
  void finish_call(const hobject_t &volume_oid, OpRequestRef op);
  void finish_request(OpRequestRef op);
  ClsParmContext *get_cls_ctx(const hobject_t &volume_oid);
  uint64_t sparse_response_offset(
    const OpRequestRef &op, uint64_t physical_offset) const;

  bool list_objects(
    pg_nls_response_t &response, unsigned limit, int &result);
  bool handles_logical_stat() const { return enabled_; }
  void encode_getxattrs_result(
    const OSDOp &osd_op, XAttrs &attrs,
    ceph::buffer::list &encoded) const;

  // Narrow backend/objclass bridge. Neither caller can reach Aggregator
  // directly, so aggregate state-machine details remain in this module.
  bool translate_class_ops(OpRequestRef &op, std::vector<OSDOp> &ops);
  bool is_volume_cached(const hobject_t &volume_oid) const;
  void read_cached_volume(extent_map &result) const;
  volume_t inflight_volume(const hobject_t &volume_oid) const;
  std::optional<std::map<int, std::size_t>> storage_optimization_offsets(
    const OpRequestRef &client_op, const hobject_t &volume_oid,
    const std::vector<int> &chunk_mapping,
    int data_chunk_count, int coding_chunk_count) const;

  bool should_check_debug_op_order() const { return !enabled_; }

private:
  PrimaryLogPG *pg_;
  bool enabled_;
  Aggregator aggregator_;
  std::list<OpRequestRef> waiting_for_recovery_;
};

} // namespace ceph::aggregate_ec
