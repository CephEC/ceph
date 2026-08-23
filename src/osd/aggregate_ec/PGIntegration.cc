// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "PGIntegration.h"

#include "common/debug.h"
#include "osd/PrimaryLogPG.h"

#include <algorithm>
#include <utility>

namespace ceph::aggregate_ec {

PGIntegration::PGIntegration(
  CephContext *cct, spg_t pgid, OSDService *osd, PrimaryLogPG *pg,
  bool enabled)
  : pg_(pg), enabled_(enabled),
    aggregator_(cct, std::move(pgid), osd, pg) {}

PGIntegration::~PGIntegration() {
  aggregator_.shutdown();
}

void PGIntegration::initialize() {
  if (!enabled_ || aggregator_.initialized() || !pg_->is_primary()) return;

  reload_metadata();
  auto *backend = pg_->get_pgbackend();
  aggregator_.activate(
    static_cast<uint8_t>(backend->get_ec_data_chunk_count()),
    backend->get_ec_stripe_chunk_size(),
    pg_->cct->_conf->osd_aggregate_flush_timer_enabled,
    pg_->cct->_conf->osd_aggregate_buffer_flush_timeout);
}

void PGIntegration::reload_metadata() {
  if (!enabled_) return;

  std::vector<ceph::buffer::list> metadata;
  int r = pg_->get_pgbackend()->load_volume_attrs(metadata);
  if (r < 0) {
    ldpp_dout(pg_, 1) << __func__ << " failed: " << cpp_strerror(r)
                      << dendl;
    return;
  }
  aggregator_.load_metadata(metadata);
}

bool PGIntegration::preprocess_client_op(OpRequestRef &op) {
  if (!enabled_ || !pg_->is_primary() || op->is_write_volume_op() ||
      op->is_aggregateEC_translated_op() ||
      !op->get_reqid().name.is_client()) {
    return false;
  }

  auto *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  int result = aggregator_.preprocess(op);
  if (result == Aggregator::PREPROCESS_CONSUMED) return true;
  if (result < 0) {
    if (result == -ENOENT && pg_->recovery_state.have_missing()) {
      waiting_for_recovery_.push_back(op);
    } else {
      pg_->reply_op_error(op, result);
    }
    return true;
  }

  if (result != Aggregator::PREPROCESS_REDIRECT || !pg_->is_active() ||
      !pg_->is_clean() ||
      static_cast<uint64_t>(m->get_retry_attempt()) >
        pg_->cct->_conf->aggregateEC_redirect_read_max_times) {
    return false;
  }

  pg_shard_t shard;
  int r = pg_->pgbackend->object_locate(m, shard);
  if (r || shard == pg_->whoami_shard()) return false;

  int flags = m->get_flags() & (CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
  auto *reply = new MOSDOpReply(
    m, -EAGAIN, pg_->get_osdmap_epoch(), flags, false, false);
  request_redirect_t redirect(
    m->get_object_locator(), m->get_hobj().oid.name,
    shard.osd, shard.shard);
  reply->set_redirect(redirect);
  m->get_connection()->send_message(reply);
  aggregator_.purge_origin(op);
  return true;
}

void PGIntegration::on_recovery_progress() {
  if (!enabled_ || pg_->recovery_state.have_missing()) return;
  reload_metadata();
  pg_->requeue_ops(waiting_for_recovery_);
}

void PGIntegration::on_pg_change() {
  if (enabled_) pg_->requeue_ops(waiting_for_recovery_);
}

bool PGIntegration::handle_error(
  OpRequestRef op, int error, eversion_t version, version_t user_version,
  const std::vector<pg_log_op_return_item_t> &op_returns) {
  if (!enabled_) return false;

  aggregator_.purge_origin(op);
  if (!op->is_write_volume_op()) return false;
  auto *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  return aggregator_.fail_volume(
    m->get_hobj(), error, version, user_version, op_returns);
}

bool PGIntegration::handle_write_error_reply(
  OpRequestRef op, MOSDOpReply *reply, bool ignore_out_data) {
  if (!enabled_) return false;

  if (op->is_write_volume_op()) {
    auto *m = static_cast<MOSDOp*>(op->get_nonconst_req());
    if (aggregator_.fail_volume(m->get_hobj(), reply, ignore_out_data)) {
      return true;
    }
  }
  aggregator_.purge_origin(op);
  return false;
}

bool PGIntegration::handle_success_reply(
  const hobject_t &volume_oid, const std::vector<OSDOp> &ops,
  OpRequestRef op, MOSDOpReply *reply, bool ignore_out_data) {
  if (!enabled_) return false;

  if (op && op->is_write_volume_op() &&
      aggregator_.complete_volume(
        volume_oid, ops, reply, ignore_out_data)) {
    return true;
  }
  aggregator_.update_cache(volume_oid, ops);
  aggregator_.purge_origin(op);
  return false;
}

void PGIntegration::finish_context(
  const hobject_t &volume_oid, OpRequestRef op) {
  if (enabled_ && op && op->is_write_volume_op()) {
    aggregator_.finish_volume(volume_oid);
  }
}

bool PGIntegration::handles_split_effects(const OpRequestRef &op) const {
  return enabled_ && op && op->is_write_volume_op();
}

std::vector<OpRequestRef> PGIntegration::buffered_requests(
  const hobject_t &volume_oid) const {
  if (!enabled_) return {};
  return aggregator_.buffered_requests(volume_oid);
}

bool PGIntegration::requeue_aborted_volume(
  OpRequestRef &op, std::list<OpRequestRef> &requeue) {
  if (!enabled_ || !op || !op->is_write_volume_op()) return false;

  auto *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  for (auto &request : aggregator_.cancel_volume(m->get_hobj())) {
    requeue.push_back(std::move(request));
  }
  op = OpRequestRef();
  return true;
}

void PGIntegration::finish_call(
  const hobject_t &volume_oid, OpRequestRef op) {
  if (!enabled_) return;
  aggregator_.finish_cls(volume_oid);
  aggregator_.purge_origin(op);
}

void PGIntegration::finish_request(OpRequestRef op) {
  if (enabled_) aggregator_.purge_origin(op);
}

ClsParmContext *PGIntegration::get_cls_ctx(const hobject_t &volume_oid) {
  return enabled_ ? aggregator_.get_cls_ctx(volume_oid) : nullptr;
}

uint64_t PGIntegration::sparse_response_offset(
  const OpRequestRef &op, uint64_t physical_offset) const {
  if (!enabled_ || !op || !op->is_aggregateEC_translated_op()) {
    return physical_offset;
  }
  return aggregator_.logical_offset(op, physical_offset)
    .value_or(physical_offset);
}

bool PGIntegration::list_objects(
  pg_nls_response_t &response, unsigned limit, int &result) {
  if (!enabled_) return false;
  result = aggregator_.list_objects(response, limit);
  return true;
}

void PGIntegration::encode_getxattrs_result(
  const OSDOp &osd_op, XAttrs &attrs,
  ceph::buffer::list &encoded) const {
  if (!enabled_) {
    encode(attrs, encoded);
    return;
  }

  std::string prefix;
  ceph_assert(osd_op.op.xattr.name_len > 0);
  auto p = osd_op.indata.cbegin();
  p.copy(osd_op.op.xattr.name_len, prefix);

  XAttrs logical_attrs;
  for (auto &entry : attrs) {
    if (entry.first.compare(0, prefix.length(), prefix) == 0) {
      logical_attrs[entry.first.substr(prefix.length())] =
        std::move(entry.second);
    }
  }
  encode(logical_attrs, encoded);
}

bool PGIntegration::translate_class_ops(
  OpRequestRef &op, std::vector<OSDOp> &ops) {
  if (!enabled_) return false;
  aggregator_.translate(op, ops);
  return true;
}

bool PGIntegration::is_volume_cached(const hobject_t &volume_oid) const {
  return enabled_ && aggregator_.is_volume_cached(volume_oid);
}

void PGIntegration::read_cached_volume(extent_map &result) const {
  ceph_assert(enabled_);
  aggregator_.read_cached_volume(result);
}

volume_t PGIntegration::inflight_volume(const hobject_t &volume_oid) const {
  ceph_assert(enabled_);
  return aggregator_.inflight_volume(volume_oid);
}

std::optional<std::map<int, std::size_t>>
PGIntegration::storage_optimization_offsets(
  const OpRequestRef &client_op, const hobject_t &volume_oid,
  const std::vector<int> &chunk_mapping,
  int data_chunk_count, int coding_chunk_count) const {
  if (!client_op ||
      !client_op->need_aggregateEC_storage_optimize()) {
    return std::nullopt;
  }
  ceph_assert(enabled_);

  const auto volume = aggregator_.inflight_volume(volume_oid);
  std::map<int, std::size_t> offsets;
  std::size_t max_data_length = 0;
  bool oid_match = false;

  auto physical_chunk = [&chunk_mapping](int logical) {
    return logical >= 0 &&
        static_cast<std::size_t>(logical) < chunk_mapping.size()
      ? chunk_mapping[logical]
      : logical;
  };

  for (const auto &chunk : volume.get_all_chunks()) {
    const int chunk_id = physical_chunk(chunk->get_chunk_id().id);
    offsets.emplace(chunk_id, chunk->get_offset());
    max_data_length = std::max(max_data_length, chunk->get_offset());
    oid_match |= chunk->get_oid() == volume_oid;
  }

  for (int logical = 0; logical < data_chunk_count; ++logical) {
    offsets.emplace(physical_chunk(logical), 0);
  }

  // The EC plugin's encoding granularity is not exposed here. Four bytes is
  // the largest granularity used by the supported implementations.
  constexpr std::size_t encode_unit = 4;
  max_data_length =
    (max_data_length + encode_unit - 1) & ~(encode_unit - 1);
  for (int code = 0; code < coding_chunk_count; ++code) {
    offsets.emplace(
      physical_chunk(data_chunk_count + code), max_data_length);
  }

  if (!oid_match) {
    ldpp_dout(pg_, 20)
      << __func__ << " warning: object " << volume_oid
      << " not found in volume " << volume.get_oid() << dendl;
  }
  return offsets;
}

} // namespace ceph::aggregate_ec
