// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "WeaveMemberAccess.h"

#include <algorithm>
#include <utility>

#include "WeaveLayout.h"
#include "WeaveRequestContext.h"
#include "WeaveXAttr.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "include/ceph_assert.h"
#include "messages/MOSDOpReply.h"
#include "osd/ClassHandler.h"

namespace ceph::weave {

WeaveMemberAccess::WeaveMemberAccess(CephContext* cct,
                                     const WeaveCatalog& catalog)
  : cct_(cct), catalog_(catalog) {}

WeaveMemberAccess::~WeaveMemberAccess() {
  shutdown();
}

void WeaveMemberAccess::activate(uint8_t data_chunks, uint64_t stripe_unit) {
  data_chunks_ = data_chunks;
  stripe_unit_ = stripe_unit;
  initialized_ = data_chunks_ != 0 && stripe_unit_ != 0;
}

void WeaveMemberAccess::shutdown() {
  initialized_ = false;
}

int WeaveMemberAccess::preprocess(OpRequestRef op) {
  if (!initialized_ || op->is_aggregate_member_op()) {
    return kPreprocessContinue;
  }

  auto* message = static_cast<MOSDOp*>(op->get_nonconst_req());
  // Reads and a final logical DELETE run against the published Volume. Other
  // mutations and native class calls first materialize members into native EC.
  return translate(op, message->ops);
}

bool WeaveMemberAccess::supports_member_ops(
  const std::vector<OSDOp>& ops) const {
  // Malformed supported operations still go through translate() to return
  // their validation error without needlessly materializing a healthy Volume.
  return validate_member_ops(ops) != -EOPNOTSUPP;
}

int WeaveMemberAccess::validate_member_ops(
  const std::vector<OSDOp>& ops) const {
  for (std::size_t subop = 0; subop < ops.size(); ++subop) {
    const int result = validate_op(ops[subop], subop + 1 == ops.size());
    if (result < 0) return result;
  }

  return 0;
}

int WeaveMemberAccess::validate_op(const OSDOp& entry, bool is_last) const {
  switch (entry.op.op) {
  case CEPH_OSD_OP_READ:
  case CEPH_OSD_OP_SPARSE_READ:
  case CEPH_OSD_OP_SYNC_READ:
  case CEPH_OSD_OP_STAT:
  case CEPH_OSD_OP_GETXATTRS:
  case CEPH_OSD_OP_ASSERT_VER:
    return 0;
  case CEPH_OSD_OP_DELETE:
    return is_last ? 0 : -EOPNOTSUPP;
  case CEPH_OSD_OP_GETXATTR:
  case CEPH_OSD_OP_CMPXATTR:
    // The key, plus any comparison value, must fit in the input payload.
    if (uint64_t{entry.op.xattr.name_len} +
        (entry.op.op == CEPH_OSD_OP_CMPXATTR
          ? uint64_t{entry.op.xattr.value_len} : 0) > entry.indata.length()) {
      return -EINVAL;
    }
    return 0;
  case CEPH_OSD_OP_CALL: {
    std::string name;
    const int result = aggregate_class_name(entry, name);
    if (result < 0) return result;

    return class_is_allowed(name) ? 0 : -EOPNOTSUPP;
  }
  default:
    return -EOPNOTSUPP;
  }
}

int WeaveMemberAccess::aggregate_class_name(const OSDOp& entry,
                                            std::string& name) const {
  if (uint64_t{entry.op.cls.class_len} + entry.op.cls.method_len +
      entry.op.cls.indata_len > entry.indata.length()) {
    return -EINVAL;
  }

  entry.indata.cbegin().copy(entry.op.cls.class_len, name);
  return 0;
}

bool WeaveMemberAccess::class_is_allowed(const std::string& name) const {
  return ClassHandler::get_instance().in_class_list(
    name, cct_->_conf->osd_aggregate_data_classes);
}

std::optional<WeaveMemberAccess::Target> WeaveMemberAccess::resolve_target(
  const OpRequestRef& op, const MOSDOp& message, int& error) const {
  error = 0;
  // A weave context that already resolved wins over the catalog: it names the
  // published Volume that owns this member, while the catalog only serves
  // fresh requests.
  const auto* existing = op->get_weave_context();
  const hobject_t origin = existing && existing->original_oid()
    ? *existing->original_oid() : message.get_hobj();
  auto metadata = existing && existing->volume_metadata()
    ? existing->volume_metadata() : catalog_.lookup(origin);
  if (!metadata) return std::nullopt;

  // Published metadata that does not list this object cannot describe it.
  const auto found = metadata->members.find(origin);
  if (found == metadata->members.end()) {
    error = -EIO;
    return std::nullopt;
  }

  // The live geometry must match the one the member was written with, since
  // every split below assumes it.
  if (metadata->data_shards != data_chunks_ || !stripe_unit_ ||
      metadata->slot_size % stripe_unit_) {
    error = -EIO;
    return std::nullopt;
  }

  return Target{origin, metadata, &found->second};
}

int WeaveMemberAccess::translate(OpRequestRef& op, std::vector<OSDOp>& ops) {
  if (!initialized_) return kPreprocessContinue;
  auto* message = static_cast<MOSDOp*>(op->get_nonconst_req());

  // An -EIO out of resolve_target() beats "not ours": metadata that cannot
  // describe a published object must not fall back to the native path.
  int error = 0;
  auto target = resolve_target(op, *message, error);
  if (error < 0) return error;
  if (!target) return kPreprocessContinue;

  const int result = validate_member_ops(ops);
  if (result < 0) return result;

  // The logical origin and the client's ops are recorded before the rewrite,
  // so the reply path can still recover the client's view.
  auto& ctx = op->ensure_weave_context();
  ctx.remember_original_oid(target->origin);
  ctx.remember_client_ops(ops);
  rewrite_ops(ops, *target, ctx);

  // The rewritten sub-ops now address the Volume that stores the member.
  ctx.set_volume_metadata(target->volume);
  message->set_hobj(target->volume->volume_oid);
  return kPreprocessContinue;
}

void WeaveMemberAccess::rewrite_ops(std::vector<OSDOp>& ops,
                                   const Target& target,
                                   WeaveRequestContext& ctx) const {
  // Sub-ops are rewritten in place: reply results are merged back by index.
  for (std::size_t subop = 0; subop < ops.size(); ++subop) {
    auto& entry = ops[subop];
    switch (entry.op.op) {
    case CEPH_OSD_OP_GETXATTR:
    case CEPH_OSD_OP_CMPXATTR: {
      const int rewritten = rewrite_xattr_key_op(entry, target);
      ceph_assert(rewritten == 0);  // Payload bounds checked above.
      break;
    }
    case CEPH_OSD_OP_GETXATTRS:
      rewrite_getxattrs_op(entry, target);
      break;
    case CEPH_OSD_OP_STAT:
      // The logical size and mtime are re-encoded from metadata on the reply.
      entry.outdata.clear();
      break;
    case CEPH_OSD_OP_CALL:
      rewrite_class_call_op(entry, target, ctx, subop);
      break;
    case CEPH_OSD_OP_SYNC_READ:
    case CEPH_OSD_OP_READ:
    case CEPH_OSD_OP_SPARSE_READ:
      rewrite_read_op(entry, target);
      break;
    default:
      break;
    }
  }
}

int WeaveMemberAccess::rewrite_xattr_key_op(OSDOp& entry,
                                            const Target& target) const {
  // Delegates to the namespace-scope helper, which rebuilds the payload
  // because the renamed key shifts the value boundary.
  return ceph::weave::rewrite_xattr_op(
    entry, target.origin, entry.op.op == CEPH_OSD_OP_CMPXATTR);
}

void WeaveMemberAccess::rewrite_getxattrs_op(OSDOp& entry,
                                             const Target& target) const {
  // The reply is filtered by this prefix, so the physical read must use it.
  const auto prefix = xattr_prefix(target.origin);
  entry.indata.clear();
  entry.indata.append(prefix);
  entry.op.xattr.name_len = prefix.size();
}

void WeaveMemberAccess::rewrite_read_op(OSDOp& entry,
                                        const Target& target) const {
  if (entry.op.op == CEPH_OSD_OP_SYNC_READ) {
    entry.op.op = CEPH_OSD_OP_READ;
  }

  const uint64_t offset = entry.op.extent.offset;
  const uint64_t logical_size = target.member->size;
  const uint64_t remaining = offset < logical_size ? logical_size - offset : 0;
  const uint64_t requested = entry.op.extent.length;
  // An unset length means "to end of member"; otherwise the physical read is
  // clamped to what is left, so it never crosses the logical size.
  entry.op.extent.length =
    requested ? std::min(requested, remaining) : remaining;

  entry.op.flags = member_read_flags(entry.op.flags, target.member->shard);
  // Offset remains in member coordinates, even at EOF. The primary handles
  // this marked zero-length extent without expanding it to container EOF.
}

void WeaveMemberAccess::rewrite_class_call_op(OSDOp& entry,
                                              const Target& target,
                                              WeaveRequestContext& ctx,
                                              std::size_t subop) const {
  // The call runs against the whole member, so its EC extent covers it.
  ctx.save_cls_context(subop, std::make_unique<ClsParmContext>(
    entry.op.cls.class_len, entry.op.cls.method_len, entry.op.cls.argc,
    entry.op.cls.indata_len, entry.indata));

  entry.op.op = CEPH_OSD_OP_EC_CALL;
  entry.op.extent.offset = 0;
  entry.op.extent.length = target.member->size;
  entry.op.flags = member_read_flags(entry.op.flags, target.member->shard);
}

void WeaveMemberAccess::finish_request(const OpRequestRef& op) {
  if (!op) return;
  const auto* ctx = op->get_weave_context();
  if (!ctx) return;

  auto* message = static_cast<MOSDOp*>(op->get_nonconst_req());
  // Peering and deferred requests replay the original logical operation,
  // never a stale physical target or an already-rewritten input payload.
  if (ctx->original_oid()) message->set_hobj(*ctx->original_oid());
  if (const auto* client = ctx->client_ops()) message->ops = *client;

  // The context is spent; nothing downstream may reuse the physical target.
  op->clear_weave_context();
}

ClsParmContext* WeaveMemberAccess::get_cls_ctx(
  const OpRequestRef& op, std::size_t subop) const {
  const auto* ctx = op ? op->get_weave_context() : nullptr;
  return ctx ? ctx->cls_context(subop) : nullptr;
}

int WeaveMemberAccess::prepare_member_delete(
  const OpRequestRef& op, const bufferlist& encoded, version_t fallback,
  bufferlist& updated) {
  auto* context = op ? op->get_weave_context() : nullptr;
  if (!context || !context->original_oid() || !context->volume_metadata()) {
    return -EINVAL;
  }

  WeaveVolumeMeta metadata;
  try {
    auto p = encoded.cbegin();
    decode(metadata, p);
    if (!p.end() ||
        metadata.volume_oid != context->volume_metadata()->volume_oid) {
      return -EIO;
    }
  } catch (const buffer::error&) {
    return -EIO;
  }

  // The caller reads the projected EC attribute cache while holding the native
  // object lock. Concurrent deletes cannot overwrite each other's membership.
  if (!metadata.members.erase(*context->original_oid())) return -ENOENT;

  // Members without a recorded version inherit the caller's fallback.
  for (auto& [oid, member] : metadata.members) {
    if (!member.user_version) member.user_version = fallback;
  }

  encode(metadata, updated);

  // The PG controller applies this deletion once the request completes.
  context->mark_member_deleted();
  return 0;
}

void WeaveMemberAccess::restore_client_reply_ops(const OpRequestRef& op,
                                                 MOSDOpReply* reply) const {
  if (!op || !reply) return;
  const auto* ctx = op->get_weave_context();
  const auto* client = ctx ? ctx->client_ops() : nullptr;
  if (!client) return;

  std::vector<OSDOp> physical;
  reply->claim_ops(physical);

  // Client inputs are dropped; the results are merged back in below.
  auto restored = *client;
  for (auto& entry : restored) {
    entry.indata.clear();
  }
  merge_reply_ops(restored, physical);

  reply->claim_ops(restored);
}

void WeaveMemberAccess::merge_reply_ops(std::vector<OSDOp>& restored,
                                        std::vector<OSDOp>& physical) const {
  const auto count = std::min(restored.size(), physical.size());
  // Results line up positionally, so only the shared prefix is merged.
  for (std::size_t i = 0; i < count; ++i) {
    restored[i].rval = physical[i].rval;
    restored[i].outdata = std::move(physical[i].outdata);
  }
}

bool WeaveMemberAccess::handles_logical_stat(const OpRequestRef& op) const {
  return op && op->is_aggregate_member_op();
}

bool WeaveMemberAccess::encode_logical_stat(const OpRequestRef& op,
                                            bufferlist& out) const {
  using ceph::encode;
  if (!handles_logical_stat(op)) return false;
  const auto* context = op->get_weave_context();
  const auto& member =
    context->volume_metadata()->members.at(*context->original_oid());

  // The logical size and mtime replace what the physical object holds.
  out.clear();
  encode(member.size, out);
  encode(member.mtime, out);
  return true;
}

uint64_t WeaveMemberAccess::logical_user_version(const OpRequestRef& op,
                                                 uint64_t fallback) const {
  if (!handles_logical_stat(op)) return fallback;
  const auto* ctx = op->get_weave_context();
  if (!ctx || !ctx->original_oid()) return fallback;

  // Missing membership or version means the caller's value stands.
  const auto& metadata = ctx->volume_metadata();
  auto it = metadata->members.find(*ctx->original_oid());
  return it == metadata->members.end() || !it->second.user_version
    ? fallback : it->second.user_version;
}

void WeaveMemberAccess::encode_getxattrs_result(const OpRequestRef& request,
                                                const OSDOp& op, XAttrs& attrs,
                                                bufferlist& encoded) const {
  using ceph::encode;
  if (!handles_logical_stat(request)) {
    encode(attrs, encoded);
    return;
  }

  // The rewritten read was scoped to this prefix, so the reply strips it.
  std::string prefix;
  auto p = op.indata.cbegin();
  p.copy(op.op.xattr.name_len, prefix);
  encode(filter_logical_attrs(attrs, prefix), encoded);
}

// Values are moved out: the caller no longer needs the physical attributes.
WeaveMemberAccess::XAttrs WeaveMemberAccess::filter_logical_attrs(
  XAttrs& attrs, const std::string& prefix) {
  XAttrs logical;
  for (auto& [key, value] : attrs) {
    if (key.compare(0, prefix.length(), prefix) == 0) {
      logical.emplace(key.substr(prefix.length()), std::move(value));
    }
  }
  return logical;
}

int WeaveMemberAccess::translate_native_class_ops(OpRequestRef& request,
                                                  std::vector<OSDOp>& ops,
                                                  uint64_t size) {
  if (!request || request->is_background_aggregate_io() ||
      request->is_aggregate_member_op()) return 0;

  for (std::size_t i = 0; i < ops.size(); ++i) {
    auto& op = ops[i];
    if (op.op.op != CEPH_OSD_OP_CALL) continue;
    std::string name;
    try {
      op.indata.cbegin().copy(op.op.cls.class_len, name);
    } catch (const buffer::error&) {
      return -EINVAL;
    }
    if (!class_is_allowed(name)) continue;

    // The reply path needs the client's ops and class arguments back.
    auto& ctx = request->ensure_weave_context();
    ctx.remember_client_ops(ops);
    ctx.save_cls_context(i, std::make_unique<ClsParmContext>(
      op.op.cls.class_len, op.op.cls.method_len, op.op.cls.argc,
      op.op.cls.indata_len, op.indata));

    op.op.op = CEPH_OSD_OP_EC_CALL;
    op.op.extent.offset = 0;
    op.op.extent.length = size;
  }

  return 0;
}

}  // namespace ceph::weave
