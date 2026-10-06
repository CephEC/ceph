// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "WeaveConversionJob.h"

#include <algorithm>
#include <cerrno>

#include "WeaveLayout.h"
#include "WeaveXAttr.h"

namespace ceph::weave {

WeaveConversionJob::WeaveConversionJob(
  WeavePGInterface& pg_interface, uint64_t identity, uint64_t unit,
  std::vector<WeaveCandidate> members, WeaveVolumeMeta volume,
  std::unique_ptr<WeaveLease> lease, Hooks hooks, bool unpack,
  version_t version, uint64_t size)
  : pg_interface_(pg_interface), owner_(pg_interface.pin()), identity_(identity),
    epoch_(pg_interface.epoch()), unit_(unit), members_(std::move(members)),
    volume_(std::move(volume)),
    data_(members_.size()), state_(PackState{}), lease_(std::move(lease)),
    hooks_(std::move(hooks)) {
  if (unpack) state_ = UnpackState{{}, version, size};
}

WeaveConversionJob::~WeaveConversionJob() = default;

bool WeaveConversionJob::terminal() const {
  return stage_ == Stage::kCompleted || stage_ == Stage::kCancelled;
}

bool WeaveConversionJob::current() const {
  return !terminal() && pg_interface_.current(epoch_);
}

bool WeaveConversionJob::authenticates(ceph_tid_t tid) const {
  return current() && tid_ != 0 && tid == tid_;
}

std::optional<version_t> WeaveConversionJob::copy_version(
  const hobject_t& oid) const {
  // Only an unpack job rewrites its members, and each must keep the
  // user_version it carried while packed.
  if (!current() || !std::holds_alternative<UnpackState>(state_)) {
    return std::nullopt;
  }

  for (const auto& member : members_) {
    if (member.oid == oid) return member.user_version;
  }
  return std::nullopt;
}

std::optional<snapid_t> WeaveConversionJob::copy_snap_sequence(
  const hobject_t& oid) const {
  if (!copy_version(oid)) return std::nullopt;

  const auto seq = volume_.members.at(oid).snap_sequence;
  // A member packed from the head has no snapset sequence to restore.
  return seq == CEPH_NOSNAP ? std::nullopt : std::optional<snapid_t>(seq);
}

// Enters kCompleted, or kCancelled when the caller cancelled the job.
void WeaveConversionJob::finish(Result result) {
  // The first caller wins; a late reply or a racing cancel must not run the
  // epilogue a second time.
  if (terminal()) return;

  stage_ = result.cancelled ? Stage::kCancelled : Stage::kCompleted;
  tid_ = 0;
  lease_.reset();

  // Detach the callback before invoking it, so it cannot re-enter finish().
  auto done = std::move(hooks_.finish);
  hooks_ = {};
  if (done) done(result);
}

void WeaveConversionJob::cancel() {
  // finish() is idempotent and clears tid_, so a terminal job has nothing left
  // to cancel. The caller keeps the job alive across this call.
  const auto tid = tid_;
  finish({false, 0, true});

  // finish() cleared tid_, so cancel with the value captured above.
  if (tid) pg_interface_.cancel_io(tid);
}

WeaveCompletion WeaveConversionJob::completion(
  std::function<void(int)> callback) {
  return [self = shared_from_this(), sequence = ++io_sequence_,
          callback = std::move(callback)](int r) {
    if (!self->accept_completion(sequence)) return;

    callback(r);
  };
}

bool WeaveConversionJob::accept_completion(uint64_t sequence) {
  // Reject replies from cancelled, superseded or timed-out attempts.
  if (!current() || sequence != io_sequence_) return false;

  // Consume the sequence so duplicate deliveries cannot advance the job.
  ++io_sequence_;
  tid_ = 0;
  return true;
}

void WeaveConversionJob::retry(std::function<void()> callback) {
  // Retry this job's failed step, independently of scans and cleanup retries.
  pg_interface_.retry(WeaveRetryKind::kConversion,
    [self = shared_from_this(), callback = std::move(callback)] {
      if (self->current()) callback();
    });
}

// Enters kReadingVolume (unpack) or kReadingMembers (pack).
void WeaveConversionJob::start() {
  ceph_assert(stage_ == Stage::kReady);

  if (packing()) {
    ceph_assert(!members_.empty());
    submit_member_read(0);
  } else if (members_.empty()) {
    // A volume with no members has nothing left to restore.
    submit_volume_remove();
  } else {
    submit_volume_read();
  }
}

void WeaveConversionJob::submit_volume_read() {
  auto& unpack = std::get<UnpackState>(state_);
  stage_ = Stage::kReadingVolume;
  tid_ = pg_interface_.read(volume_.volume_oid, unpack.version, unpack.size,
    &unpack.volume.data, &unpack.volume.attrs, completion([this](int r) {
      on_volume_read_complete(r);
    }));
}

void WeaveConversionJob::on_volume_read_complete(int r) {
  if (r < 0) {
    finish({false, r});
    return;
  }

  stage_ = Stage::kMaterializing;
  // materialize_members() is CPU work, so it is posted outside the lock.
  pg_interface_.post([self = shared_from_this()] { self->materialize_members(); });
}

// Enters kReadingMembers, one member per round trip.
void WeaveConversionJob::submit_member_read(size_t index) {
  stage_ = Stage::kReadingMembers;
  const auto& member = members_[index];

  // One member per round trip; the next read starts from this reply.
  tid_ = pg_interface_.read(member.oid, member.user_version, member.size,
    &data_[index].data, &data_[index].attrs, completion([this, index](int r) {
      on_member_read_complete(index, r);
    }));
}

void WeaveConversionJob::on_member_read_complete(size_t index, int r) {
  // A short read would truncate the interleave, so fail the job instead.
  if (r < 0 || data_[index].data.length() != members_[index].size) {
    finish({true});
    return;
  }

  if (index + 1 < members_.size()) submit_member_read(index + 1);
  else {
    stage_ = Stage::kBuildingVolume;
    pg_interface_.post([self = shared_from_this()] { self->build_volume(); });
  }
}

// Runs the kBuildingVolume CPU phase, then submits under the PG lock.
void WeaveConversionJob::build_volume() {
  // CPU work owns its buffers and never touches PG state without serialized().
  auto& pack = std::get<PackState>(state_);
  const bool valid = compose_volume(pack);

  // Submit only under the PG lock, and only while the epoch is still ours.
  pg_interface_.serialized([self = shared_from_this(), valid] {
    self->on_volume_build_complete(valid);
  });
}

void WeaveConversionJob::on_volume_build_complete(bool valid) {
  if (!current()) return;
  if (!valid) {
    finish({true});
    return;
  }
  submit_volume_write();
}

bool WeaveConversionJob::compose_volume(PackState& pack) {
  std::vector<bufferlist> input;
  for (const auto& member : data_) input.push_back(member.data);

  auto& volume = pack.volume;
  // The interleave refuses a member that cannot fit its computed slot.
  const bool valid = interleave_members(
    input, unit_, volume_.slot_size, volume.data);
  if (!valid) return false;

  collect_member_attrs(volume.attrs);
  encode(volume_, volume.attrs[kVolumeMetaAttr]);
  return true;
}

void WeaveConversionJob::collect_member_attrs(WeaveAttrs& attrs) const {
  // Prefix each attribute with its member's object id to avoid key collisions.
  for (size_t i = 0; i < members_.size(); ++i) {
    const auto prefix = xattr_prefix(members_[i].oid);
    for (const auto& [key, value] : data_[i].attrs) {
      attrs.emplace(prefix + key, value);
    }
  }
}

// Enters kWritingVolume, then kPublishing once the volume is durable.
void WeaveConversionJob::submit_volume_write() {
  // Called under the PG lock, while the controller still holds the
  // reservations this write depends on.
  if (!hooks_.validate()) {
    finish({true});
    return;
  }

  auto& volume = std::get<PackState>(state_).volume;
  stage_ = Stage::kWritingVolume;

  pg_interface_.conversion_checkpoint("pack_before_write");
  tid_ = pg_interface_.write(volume_.volume_oid, volume.data, volume.attrs,
    utime_t(ceph::real_clock::now()), false, completion([this](int r) {
      on_volume_write_complete(r);
    }));
}

void WeaveConversionJob::on_volume_write_complete(int r) {
  if (r < 0) {
    resolve_volume(r);
    return;
  }

  pg_interface_.conversion_checkpoint("pack_committed");
  stage_ = Stage::kPublishing;
  // Data and volume_meta were committed in one object transaction. A
  // cancelled callback cannot undo that authority; reload uses the disk.
  hooks_.publish();
  pg_interface_.conversion_checkpoint("pack_published");

  // Sources are only removed once the packed volume can be reloaded.
  submit_member_remove(0);
}

// Enters kResolvingVolume.
void WeaveConversionJob::resolve_volume(int error) {
  stage_ = Stage::kResolvingVolume;

  // Objecter timeouts can lose a successful write's reply. The old tid is no
  // longer admitted; wait for any already admitted transaction to release its
  // native lock before the controller reloads the authoritative disk metadata.
  if (pg_interface_.inspect(volume_.volume_oid).busy()) {
    retry([this, error] { resolve_volume(error); });
    return;
  }

  finish({true, error});
}

// Enters kRetiringMembers, one member per round trip.
void WeaveConversionJob::submit_member_remove(size_t index) {
  stage_ = Stage::kRetiringMembers;
  const auto& member = members_[index];

  pg_interface_.conversion_checkpoint("source_before_remove", index);
  tid_ = pg_interface_.remove(member.oid, member.user_version,
    completion([this, index](int r) {
      on_member_remove_complete(index, r);
    }));
}

void WeaveConversionJob::on_member_remove_complete(size_t index, int r) {
  // A member already gone counts as retired; anything else is retried.
  if (r < 0 && r != -ENOENT) {
    retry([this, index] { submit_member_remove(index); });
    return;
  }

  pg_interface_.conversion_checkpoint("source_removed", index);
  if (index + 1 < members_.size()) submit_member_remove(index + 1);
  else finish({false});
}

// Runs the kMaterializing CPU phase, then submits the restored members.
void WeaveConversionJob::materialize_members() {
  const auto& volume = std::get<UnpackState>(state_).volume;
  // Restore every member from the packed volume before touching PG state.
  bool valid = true;
  for (size_t i = 0; i < members_.size() && valid; ++i) {
    valid = extract_member(i, volume);
  }

  // Writing restored members requires the PG lock.
  pg_interface_.serialized([self = shared_from_this(), valid] {
    self->on_members_materialization_complete(valid);
  });
}

void WeaveConversionJob::on_members_materialization_complete(bool valid) {
  if (!current()) return;
  if (!valid) {
    finish({false, -EIO});
    return;
  }
  submit_member_write(0);
}

bool WeaveConversionJob::extract_member(
  size_t index, const ObjectData& volume) {
  if (!extract_member_data(index, volume.data)) return false;
  extract_member_attrs(index, volume.attrs);
  return true;
}

bool WeaveConversionJob::extract_member_data(size_t index, const bufferlist& data) {
  const auto& member = members_[index];
  const auto shard = volume_.members.at(member.oid).shard;

  // Read the member back out of the slots it was interleaved into.
  for (uint64_t off = 0; off < member.size; off += unit_) {
    const uint64_t len = std::min(unit_, member.size - off);
    const auto source = member_volume_offset(
      off, shard, volume_.data_shards, unit_);
    if (source > data.length() || len > data.length() - source) {
      return false;
    }

    bufferlist part;
    part.substr_of(data, source, len);
    data_[index].data.claim_append(part);
  }

  return true;
}

void WeaveConversionJob::extract_member_attrs(size_t index, const WeaveAttrs& attrs) {
  // Lift the member's own attributes back out of the volume namespace.
  const auto prefix = xattr_prefix(members_[index].oid);
  for (auto p = attrs.lower_bound(prefix); p != attrs.end() &&
       p->first.compare(0, prefix.size(), prefix) == 0; ++p) {
    data_[index].attrs.emplace(p->first.substr(prefix.size()), p->second);
  }
}

// Enters kWritingMembers, one member per round trip.
void WeaveConversionJob::submit_member_write(size_t index) {
  stage_ = Stage::kWritingMembers;
  const auto& member = members_[index];

  pg_interface_.conversion_checkpoint("member_before_write", index);
  tid_ = pg_interface_.write(member.oid, data_[index].data, data_[index].attrs,
    member.mtime, true, completion([this, index](int r) {
      on_member_write_complete(index, r);
    }));
}

void WeaveConversionJob::on_member_write_complete(size_t index, int r) {
  if (r < 0) {
    retry([this, index] { submit_member_write(index); });
    return;
  }

  pg_interface_.conversion_checkpoint("member_written", index);
  // Every member must be back natively before the volume is retired.
  if (index + 1 < members_.size()) submit_member_write(index + 1);
  else submit_volume_remove();
}

// Enters kRetiringVolume, then kDetaching once the volume is gone.
void WeaveConversionJob::submit_volume_remove() {
  stage_ = Stage::kRetiringVolume;

  pg_interface_.conversion_checkpoint("volume_before_remove");
  tid_ = pg_interface_.remove(volume_.volume_oid, std::nullopt,
    completion([this](int r) {
      on_volume_remove_complete(r);
    }));
}

void WeaveConversionJob::on_volume_remove_complete(int r) {
  // A volume that is already gone needs no further removal attempt.
  if (r < 0 && r != -ENOENT) {
    retry([this] { submit_volume_remove(); });
    return;
  }

  pg_interface_.conversion_checkpoint("volume_removed");
  // Until the Volume deletion commits, native copies remain shadows and the
  // mapping remains authoritative, including after a reset or restart.
  stage_ = Stage::kDetaching;
  hooks_.detach();
  pg_interface_.conversion_checkpoint("volume_detached");

  finish({true});
}
}  // namespace ceph::weave
