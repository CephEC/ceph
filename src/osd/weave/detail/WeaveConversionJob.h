// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include <variant>

#include "WeaveCandidateIndex.h"
#include "WeaveCatalog.h"
#include "osd/weave/WeavePGHost.h"

namespace ceph::weave {

class WeaveConversionJob
  : public std::enable_shared_from_this<WeaveConversionJob> {
public:
  enum class Stage {
    kReady, kReadingMembers, kBuildingVolume, kWritingVolume, kResolvingVolume,
    kPublishing, kRetiringMembers, kReadingVolume, kMaterializing,
    kWritingMembers, kDetaching, kRetiringVolume, kCompleted, kCancelled
  };

  struct Result {
    bool restore_candidates;
    int error = 0;
    bool cancelled = false;
  };

  struct Hooks {
    // Validation runs under the PG lock while the controller holds
    // reservations. Publication/detachment only mirror a successfully
    // committed disk change.
    std::function<bool()> validate;
    std::function<void()> publish;
    std::function<void()> detach;
    std::function<void(Result)> finish;
  };

  WeaveConversionJob(WeavePGHost&, uint64_t identity, uint64_t unit,
                    std::vector<WeaveCandidate>, WeaveVolumeMeta,
                    std::unique_ptr<WeaveLease>, Hooks, bool unpack,
                    version_t volume_version = 0, uint64_t volume_size = 0);
  ~WeaveConversionJob();

  void start();
  void cancel();

  uint64_t identity() const { return identity_; }
  Stage stage() const { return stage_; }
  bool packing() const { return std::holds_alternative<PackState>(state_); }
  bool authenticates(ceph_tid_t tid) const;
  std::optional<version_t> copy_version(const hobject_t&) const;
  std::optional<snapid_t> copy_snap_sequence(const hobject_t&) const;
  const std::vector<WeaveCandidate>& members() const { return members_; }
  const WeaveVolumeMeta& volume() const { return volume_; }

private:
  struct ObjectData { ceph::buffer::list data; WeaveAttrs attrs; };
  struct PackState { ObjectData volume; };
  struct UnpackState { ObjectData volume; version_t version; uint64_t size; };

  bool current() const;
  bool terminal() const;
  void finish(Result);
  WeaveCompletion completion(std::function<void(int)>);
  bool accept_completion(uint64_t sequence);
  void retry(std::function<void()>);

  void read_member(size_t);
  void read_volume();
  void volume_read(int);
  void member_read(size_t, int);
  void build_volume();
  void volume_built(bool valid);
  bool compose_volume(PackState&);
  void collect_member_attrs(WeaveAttrs&) const;
  void submit_volume();
  void volume_written(int);
  void resolve_volume(int error);
  void retire_member(size_t);
  void member_retired(size_t, int);

  void materialize();
  void members_materialized(bool valid);
  bool extract_member(size_t index, const ObjectData& volume);
  bool extract_member_data(size_t index, const ceph::buffer::list& data);
  void extract_member_attrs(size_t index, const WeaveAttrs& attrs);
  void write_member(size_t);
  void member_written(size_t, int);
  void retire_volume();
  void volume_retired(int);

  WeavePGHost& host_;
  std::shared_ptr<void> owner_;
  const uint64_t identity_;
  const epoch_t epoch_;
  const uint64_t unit_;
  const std::vector<WeaveCandidate> members_;
  const WeaveVolumeMeta volume_;
  std::vector<ObjectData> data_;
  std::variant<PackState, UnpackState> state_;
  std::unique_ptr<WeaveLease> lease_;
  Hooks hooks_;
  Stage stage_ = Stage::kReady;
  ceph_tid_t tid_ = 0;
  uint64_t io_sequence_ = 0;
};
}  // namespace ceph::weave
