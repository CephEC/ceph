// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "WeaveCatalog.h"

namespace ceph::weave {

// Carries original reply shape for member operations and native data-class
// calls. Only member operations pin volume metadata and a logical object
// identity.
class WeaveRequestContext {
public:
  const std::optional<hobject_t>& original_oid() const { return original_oid_; }
  const std::vector<OSDOp>* client_ops() const {
    return client_ops_ ? &*client_ops_ : nullptr;
  }
  const std::shared_ptr<const WeaveVolumeMeta>& volume_metadata() const {
    return volume_metadata_;
  }
  bool member_deleted() const { return member_deleted_; }
  ClsParmContext* cls_context(std::size_t subop) const {
    auto it = cls_contexts_.find(subop);
    return it == cls_contexts_.end() ? nullptr : it->second.get();
  }

  const hobject_t& remember_original_oid(const hobject_t &oid) {
    if (!original_oid_) original_oid_ = oid;
    return *original_oid_;
  }
  void remember_client_ops(const std::vector<OSDOp> &ops) {
    if (client_ops_) return;

    client_ops_ = ops;
    // Keep a clean reply template: the replay supplies outdata from the
    // physical ops that actually ran.
    for (auto &op : *client_ops_) {
      op.outdata.clear();
    }
  }
  void set_volume_metadata(std::shared_ptr<const WeaveVolumeMeta> metadata) {
    volume_metadata_ = std::move(metadata);
  }
  void mark_member_deleted() { member_deleted_ = true; }

  void save_cls_context(std::size_t subop,
                        std::unique_ptr<ClsParmContext> context) {
    cls_contexts_[subop] = std::move(context);
  }

private:
  std::optional<hobject_t> original_oid_;
  std::optional<std::vector<OSDOp>> client_ops_;
  std::shared_ptr<const WeaveVolumeMeta> volume_metadata_;
  bool member_deleted_ = false;
  std::map<std::size_t, std::unique_ptr<ClsParmContext>> cls_contexts_;
};

}  // namespace ceph::weave
