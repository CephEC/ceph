// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <functional>
#include <memory>
#include <optional>

#include "osd/OpRequest.h"

namespace ceph::weave {

class WeavePGHost;
struct WeaveTransaction;

enum class RequestDisposition {
  kNative,
  kTranslated,
  kDeferred,
  kRejected,
  kReplied,
};

inline bool stops_native_processing(RequestDisposition result) {
  return result == RequestDisposition::kDeferred ||
    result == RequestDisposition::kRejected ||
    result == RequestDisposition::kReplied;
}

// PG-serialized facade. The native owner supplies lifecycle and transaction
// hooks; policy, mapping state and conversion progress remain private.
class WeavePGController {
public:
  using XAttrs = std::map<std::string, ceph::buffer::list, std::less<>>;

  WeavePGController(CephContext*, std::unique_ptr<WeavePGHost>, bool);
  ~WeavePGController();

  void initialize();
  void on_recovery_progress();
  void on_pg_change(bool requeue = true);
  // Periodic background scan; foreground commits only update candidates.
  void scan_candidates();
  RequestDisposition prepare_request(OpRequestRef&);
  RequestDisposition preprocess_client_op(OpRequestRef&);
  void on_commit(const object_info_t&, bool exists, const OpRequestRef&);
  void request_cleanup(unsigned live_percent, std::function<void()> on_finish);
  int prepare_member_delete(const OpRequestRef&, WeaveTransaction&);
  void finish_reply(const OpRequestRef&, MOSDOpReply*);
  void finish_request(const OpRequestRef&);
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
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace ceph::weave
