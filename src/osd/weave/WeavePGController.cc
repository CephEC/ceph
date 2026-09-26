// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "WeavePGController.h"

#include "detail/WeavePGControllerImpl.h"

namespace ceph::weave {

WeavePGController::WeavePGController(
  CephContext* cct, std::unique_ptr<WeavePGHost> host, bool enabled)
  : impl_(std::make_unique<Impl>(cct, std::move(host), enabled)) {}

WeavePGController::~WeavePGController() = default;

void WeavePGController::initialize() { impl_->initialize(); }

void WeavePGController::reload_metadata() { impl_->reload_metadata(); }

void WeavePGController::on_recovery_progress() {
  impl_->on_recovery_progress();
}

void WeavePGController::on_pg_change(bool requeue) {
  impl_->on_pg_change(requeue);
}

void WeavePGController::schedule_work() { impl_->schedule_work(); }

RequestDisposition WeavePGController::prepare_request(OpRequestRef& op) {
  return impl_->prepare_request(op);
}

RequestDisposition WeavePGController::preprocess_client_op(OpRequestRef& op) {
  return impl_->preprocess_client_op(op);
}

void WeavePGController::on_commit(const object_info_t& info, bool exists,
                                  const OpRequestRef& op) {
  impl_->on_commit(info, exists, op);
}

void WeavePGController::request_cleanup(unsigned percent,
                                        std::function<void()> done) {
  impl_->request_cleanup(percent, std::move(done));
}

int WeavePGController::prepare_member_delete(const OpRequestRef& op,
                                             WeaveTransaction& txn) {
  // on_commit applies only this member's deletion, after the native
  // transaction.
  return impl_->prepare_member_delete(op, txn);
}

void WeavePGController::finish_reply(const OpRequestRef& op,
                                     MOSDOpReply* reply) {
  impl_->finish_reply(op, reply);
}

void WeavePGController::finish_request(const OpRequestRef& op) {
  impl_->finish_request(op);
}

ClsParmContext* WeavePGController::get_cls_ctx(const OpRequestRef& op,
                                               size_t n) const {
  return impl_->get_cls_ctx(op, n);
}

void WeavePGController::merge_listing(const hobject_t& start, unsigned limit,
                                      std::vector<hobject_t>& entries,
                                      hobject_t& next) const {
  impl_->merge_listing(start, limit, entries, next);
}

bool WeavePGController::is_private_object(const hobject_t& oid) const {
  return impl_->is_private_object(oid);
}

bool WeavePGController::is_logical_member(const hobject_t& oid) const {
  return impl_->is_logical_member(oid);
}

std::pair<hobject_t, std::string> WeavePGController::listing_attribute(
  const hobject_t& oid, const std::string& key) const {
  return impl_->listing_attribute(oid, key);
}

bool WeavePGController::encode_logical_stat(const OpRequestRef& op,
                                            bufferlist& out) const {
  return impl_->encode_logical_stat(op, out);
}

uint64_t WeavePGController::logical_user_version(const OpRequestRef& op,
                                                 uint64_t v) const {
  return impl_->logical_user_version(op, v);
}

std::optional<version_t> WeavePGController::internal_copy_version(
  const OpRequestRef& op) const {
  return impl_->internal_copy_version(op);
}

std::optional<snapid_t> WeavePGController::internal_copy_snap_sequence(
  const OpRequestRef& op) const {
  return impl_->internal_copy_snap_sequence(op);
}

void WeavePGController::encode_getxattrs_result(const OpRequestRef& op,
                                                const OSDOp& subop,
                                                XAttrs& attrs,
                                                bufferlist& encoded) const {
  impl_->encode_getxattrs_result(op, subop, attrs, encoded);
}

int WeavePGController::translate_native_class_ops(OpRequestRef& op,
                                                  std::vector<OSDOp>& ops,
                                                  uint64_t size) {
  return impl_->translate_native_class_ops(op, ops, size);
}

}  // namespace ceph::weave
