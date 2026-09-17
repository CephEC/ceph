// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "WeaveReadRouter.h"

#include "WeaveRequestContext.h"

namespace ceph::weave {

bool WeaveReadRouter::eligible(const OpRequestRef& op) const {
  const auto* m = op->get_req<MOSDOp>();
  // Any write, cache, PG-level or ordering flag keeps the request here.
  return op->may_read() && !op->may_write() && !op->may_cache() &&
    !op->includes_pg_op() && m->get_snapid() == CEPH_NOSNAP &&
    !(m->get_flags() & (CEPH_OSD_FLAG_RWORDERED | CEPH_OSD_FLAG_SKIPRWLOCKS |
                        CEPH_OSD_FLAG_FLUSH | CEPH_OSD_FLAG_IGNORE_REDIRECT));
}

bool WeaveReadRouter::may_redirect(const OpRequestRef& op,
                                   const MOSDOp& message) const {
  const auto* ctx = op->get_weave_context();
  // The context is required: the redirect needs a Volume and a logical member.
  return message.allows_weave_redirect() && eligible(op) && ctx &&
    ctx->volume_metadata() && ctx->original_oid();
}

const WeaveMemberMeta* WeaveReadRouter::member_for(
  const OpRequestRef& op) const {
  const auto* ctx = op->get_weave_context();
  const auto& metadata = *ctx->volume_metadata();
  return &metadata.members.at(*ctx->original_oid());
}

bool WeaveReadRouter::redirect(const OpRequestRef& op) {
  auto* message = static_cast<MOSDOp*>(op->get_nonconst_req());
  if (!may_redirect(op, *message)) return false;

  const auto& metadata = *op->get_weave_context()->volume_metadata();
  const auto* member = member_for(op);
  // Legacy metadata cannot yet supply an authoritative logical version.
  if (!member->user_version) return false;

  auto route = host_.locate_read(metadata.volume_oid, member->shard);
  if (!route) return false;

  // Undo the local translation before handing the client to the member owner.
  members_.finish_request(op);
  host_.reply_read_redirect(op, *route);
  return true;
}

bool WeaveReadRouter::route_is_local(const MOSDOp& message,
                                     const WeaveReadRoute& route) const {
  return members_.supports_member_ops(message.ops) &&
    route.volume.pool == message.get_hobj().pool &&
    route.volume.nspace == ".ceph-internal-aggregate";
}

std::shared_ptr<const WeaveVolumeMeta> WeaveReadRouter::load_route_metadata(
  const WeaveReadRoute& route) {
  bufferlist encoded;
  const int result = host_.load_read_route(route, encoded);
  if (result < 0) return nullptr;

  auto metadata = std::make_shared<WeaveVolumeMeta>();
  try {
    auto p = encoded.cbegin();
    decode(*metadata, p);
    if (!p.end()) return nullptr;
  } catch (const buffer::error&) {
    return nullptr;
  }

  // The route must name the Volume this OSD has actually published.
  if (metadata->volume_oid != route.volume) return nullptr;
  return metadata;
}

// Validate the member-to-physical-shard assignment using the same native
// placement check as the primary. No client extent flags are trusted.
bool WeaveReadRouter::assignment_matches(const WeaveReadRoute& route,
                                         const WeaveMemberMeta& member) const {
  auto expected = host_.locate_read(route.volume, member.shard);
  return expected && expected->target == route.target &&
    expected->version == route.version;
}

int WeaveReadRouter::accept(OpRequestRef& op) {
  auto* message = static_cast<MOSDOp*>(op->get_nonconst_req());
  const auto& route = *message->get_weave_read_route();
  if (!eligible(op) || !route_is_local(*message, route)) return -EAGAIN;

  // The route is only as good as the metadata it names, so the local Volume
  // is loaded and cross-checked before anything is rewritten.
  auto metadata = load_route_metadata(route);
  if (!metadata) return -EAGAIN;

  const auto member = metadata->members.find(message->get_hobj());
  if (member == metadata->members.end() || !member->second.user_version) {
    return -EAGAIN;
  }

  // The sender's member-to-shard assignment must match local placement.
  if (!assignment_matches(route, member->second)) return -EAGAIN;

  // Only now is the request adopted: every failure above leaves it untouched
  // for a native retry.
  auto geometry = host_.geometry();
  members_.activate(geometry.data_shards, geometry.unit);
  op->ensure_weave_context().set_volume_metadata(std::move(metadata));
  return members_.translate(op, message->ops);
}

}  // namespace ceph::weave
