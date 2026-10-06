// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#pragma once

#include "WeaveCandidateIndex.h"
#include "WeaveCatalog.h"
#include "osd/weave/WeavePGInterface.h"

namespace ceph::weave {

class WeaveConversionJob
  : public std::enable_shared_from_this<WeaveConversionJob> {
public:
  // Fixed for the lifetime of a job; Stage tracks its execution progress.
  enum class Mode { kPack, kUnpack };

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

  WeaveConversionJob(WeavePGInterface&, uint64_t identity, uint64_t unit,
                    std::vector<WeaveCandidate>, WeaveVolumeMeta,
                    std::unique_ptr<WeaveLease>, Hooks, Mode mode,
                    version_t volume_version = 0, uint64_t volume_size = 0);
  ~WeaveConversionJob();

  void start();
  void cancel();

  uint64_t identity() const { return identity_; }
  Mode mode() const { return mode_; }
  Stage stage() const { return stage_; }
  bool packing() const { return mode_ == Mode::kPack; }
  bool authenticates(ceph_tid_t tid) const;
  std::optional<version_t> copy_version(const hobject_t&) const;
  std::optional<snapid_t> copy_snap_sequence(const hobject_t&) const;
  const std::vector<WeaveCandidate>& members() const { return members_; }
  const WeaveVolumeMeta& volume() const { return volume_; }

private:
  struct ObjectData { ceph::buffer::list data; WeaveAttrs attrs; };

  bool current() const;
  bool terminal() const;
  void finish(Result);
  struct IoCompletion;
  WeaveCompletion make_io_completion(WeaveCompletion);
  bool accept_io_completion(uint64_t sequence);
  void retry(std::function<void()>);

  void submit_member_read(size_t);
  void submit_volume_read();
  void on_volume_read_complete(int);
  void on_member_read_complete(size_t, int);
  void build_volume();
  void on_volume_build_complete(bool valid);
  bool compose_volume();
  void collect_member_attrs(WeaveAttrs&) const;
  void submit_volume_write();
  void on_volume_write_complete(int);
  void resolve_volume(int error);
  void submit_member_remove(size_t);
  void on_member_remove_complete(size_t, int);

  void materialize_members();
  void on_members_materialization_complete(bool valid);
  bool extract_member(size_t index, const ObjectData& volume);
  bool extract_member_data(size_t index, const ceph::buffer::list& data);
  void extract_member_attrs(size_t index, const WeaveAttrs& attrs);
  void submit_member_write(size_t);
  void on_member_write_complete(size_t, int);
  void submit_volume_remove();
  void on_volume_remove_complete(int);

  WeavePGInterface& pg_interface_;
  std::shared_ptr<void> owner_;
  const uint64_t identity_;
  const epoch_t epoch_;
  const uint64_t unit_;
  const Mode mode_;
  const std::vector<WeaveCandidate> members_;
  const WeaveVolumeMeta volume_;
  // Source volume identity/length used when unpacking.
  const version_t volume_version_;
  const uint64_t volume_size_;
  std::vector<ObjectData> data_;
  ObjectData volume_data_;
  std::unique_ptr<WeaveLease> lease_;
  Hooks hooks_;
  Stage stage_ = Stage::kReady;
  ceph_tid_t tid_ = 0;
  uint64_t io_sequence_ = 0;
};
}  // namespace ceph::weave
