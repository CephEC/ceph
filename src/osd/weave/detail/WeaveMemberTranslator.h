// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "WeaveCatalog.h"
#include "osd/OpRequest.h"

namespace ceph::weave {

class WeaveRequestContext;

/**
 * Ceph-facing logical member requests. No PG, Objecter, scheduler or write jobs:
 * the PG interface supplies native placement and durability, while this class owns
 * every logical-to-physical translation and the matching reply rewrite.
 */
class WeaveMemberTranslator {
public:
  using XAttrs = std::map<std::string, ceph::buffer::list, std::less<>>;
  static constexpr int kPreprocessContinue = 0;

  WeaveMemberTranslator(CephContext* cct, const WeaveCatalog& catalog);
  ~WeaveMemberTranslator();
  void activate(uint8_t data_chunks, uint64_t stripe_unit);
  bool initialized() const { return initialized_; }
  void shutdown();

  // Catalog misses use ordinary foreground EC. Only published members
  // translate.
  int preprocess(OpRequestRef op);
  int translate(OpRequestRef& op, std::vector<OSDOp>& ops);
  bool supports_member_ops(const std::vector<OSDOp>& ops) const;
  void finish_request(const OpRequestRef& op);
  ClsParmContext* get_cls_ctx(const OpRequestRef& op, std::size_t subop) const;
  int prepare_member_delete(const OpRequestRef&, const ceph::buffer::list&,
                            ceph::buffer::list&);
  void restore_client_reply_ops(const OpRequestRef&, MOSDOpReply*) const;
  bool handles_logical_stat(const OpRequestRef&) const;
  bool encode_logical_stat(const OpRequestRef&, ceph::buffer::list&) const;
  uint64_t logical_user_version(const OpRequestRef&, uint64_t) const;
  void encode_getxattrs_result(const OpRequestRef&, const OSDOp&, XAttrs&,
                               ceph::buffer::list&) const;
  int translate_native_class_ops(OpRequestRef&, std::vector<OSDOp>&, uint64_t);

private:
  // One logical member request: the original object, the published Volume that
  // still owns it, and the member's own slot inside that Volume.
  struct Target {
    hobject_t origin;
    std::shared_ptr<const WeaveVolumeMeta> volume;
    const WeaveMemberMeta* member;
  };

  // Resolves the request to its published member. Returns nullopt when the
  // request is not ours to translate, and reports -EIO through error when
  // published metadata cannot describe an object on this OSD.
  std::optional<Target> resolve_target(const OpRequestRef&, const MOSDOp&,
                                       int& error) const;
  int validate_member_ops(const std::vector<OSDOp>&) const;
  int validate_op(const OSDOp& entry, bool is_last) const;
  int data_class_name(const OSDOp&, std::string& name) const;
  bool class_is_allowed(const std::string& name) const;
  void rewrite_ops(std::vector<OSDOp>& ops, const Target& target,
                  WeaveRequestContext& ctx) const;
  int rewrite_xattr_key_op(OSDOp& entry, const Target& target) const;
  void rewrite_getxattrs_op(OSDOp& entry, const Target& target) const;
  void rewrite_read_op(OSDOp& entry, const Target& target) const;
  void rewrite_class_call_op(OSDOp& entry, const Target& target,
                             WeaveRequestContext& ctx, std::size_t subop) const;
  void merge_reply_ops(std::vector<OSDOp>& restored,
                       std::vector<OSDOp>& physical) const;
  static XAttrs filter_logical_attrs(XAttrs& attrs, const std::string& prefix);

  CephContext* cct_;
  uint32_t data_chunks_ = 0;
  uint64_t stripe_unit_ = 0;
  bool initialized_ = false;

  const WeaveCatalog& catalog_;
};

}  // namespace ceph::weave
