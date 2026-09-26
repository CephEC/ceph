// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "WeaveConversionJob.h"

#include <algorithm>
#include <cerrno>

#include "WeaveLayout.h"
#include "WeaveXAttr.h"

namespace ceph::weave {

WeaveConversionJob::WeaveConversionJob(
  WeavePGHost& host, uint64_t identity, uint64_t unit,
  std::vector<WeaveCandidate> members, WeaveVolumeMeta volume,
  std::unique_ptr<WeaveLease> lease, Hooks hooks, bool unpack,
  version_t version, uint64_t size)
  : host_(host), owner_(host.pin()), identity_(identity), epoch_(host.epoch()),
    unit_(unit), members_(std::move(members)), volume_(std::move(volume)),
    data_(members_.size()), state_(PackState{}), lease_(std::move(lease)),
    hooks_(std::move(hooks)) {
  if (unpack) state_ = UnpackState{{}, version, size};
}

WeaveConversionJob::~WeaveConversionJob() = default;

bool WeaveConversionJob::terminal() const {
  return stage_ == Stage::kCompleted || stage_ == Stage::kCancelled;
}

bool WeaveConversionJob::current() const {
  return !terminal() && host_.current(epoch_);
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
  if (tid) host_.cancel_io(tid);
}

WeaveCompletion WeaveConversionJob::completion(
  std::function<void(int)> callback) {
  return [self = shared_from_this(), sequence = ++io_sequence_,
          callback = std::move(callback)](int r) {
    // Every attempt takes a fresh sequence, so a reply from a superseded or
    // timed-out attempt can no longer act on the job.
    if (!self->current() || sequence != self->io_sequence_) return;

    // Consume the sequence too, so a duplicate delivery is ignored as well.
    ++self->io_sequence_;
    self->tid_ = 0;

    callback(r);
  };
}

void WeaveConversionJob::retry(std::function<void()> callback) {
  // Retry one scan interval later, and only while the epoch is still ours.
  host_.schedule(host_.policy().scan_seconds,
    [self = shared_from_this(), callback = std::move(callback)] {
      if (self->current()) callback();
    });
}

// Enters kReadingVolume (unpack) or kReadingMembers (pack).
void WeaveConversionJob::start() {
  ceph_assert(stage_ == Stage::kReady);

  // Unpack reads the packed volume once; pack reads every source member.
  if (auto* unpack = std::get_if<UnpackState>(&state_)) {
    // A volume with no members has nothing left to restore.
    if (members_.empty()) { retire_volume(); return; }

    stage_ = Stage::kReadingVolume;
    tid_ = host_.read(volume_.volume_oid, unpack->version, unpack->size,
      &unpack->volume.data, &unpack->volume.attrs, completion([this](int r) {
        if (r < 0) { finish({false, r}); return; }

        stage_ = Stage::kMaterializing;
        // materialize() is CPU work, so it is posted outside the lock.
        host_.post([self = shared_from_this()] { self->materialize(); });
      }));
  } else {
    ceph_assert(!members_.empty());
    read_member(0);
  }
}

// Enters kReadingMembers, one member per round trip.
void WeaveConversionJob::read_member(size_t index) {
  stage_ = Stage::kReadingMembers;
  const auto& member = members_[index];

  // One member per round trip; the next read starts from this reply.
  tid_ = host_.read(member.oid, member.user_version, member.size,
    &data_[index].data, &data_[index].attrs, completion([this, index](int r) {
      // A short read would truncate the interleave, so fail the job instead.
      if (r < 0 || data_[index].data.length() != members_[index].size) {
        finish({true}); return;
      }

      if (index + 1 < members_.size()) read_member(index + 1);
      else {
        stage_ = Stage::kBuildingVolume;
        host_.post([self = shared_from_this()] { self->build_volume(); });
      }
    }));
}

// Runs the kBuildingVolume CPU phase, then submits under the PG lock.
void WeaveConversionJob::build_volume() {
  // CPU work owns its buffers and never touches PG state without serialized().
  auto& pack = std::get<PackState>(state_);
  const bool valid = compose_volume(pack);

  // Submit only under the PG lock, and only while the epoch is still ours.
  host_.serialized([self = shared_from_this(), valid] {
    if (!self->current()) return;
    if (!valid) { self->finish({true}); return; }

    self->submit_volume();
  });
}

bool WeaveConversionJob::compose_volume(PackState& pack) {
  std::vector<bufferlist> input;
  for (const auto& member : data_) input.push_back(member.data);

  auto& volume = pack.volume;
  // The interleave refuses a member that cannot fit its computed slot.
  const bool valid = interleave_members(
    input, unit_, volume_.slot_size, volume.data);
  if (!valid) return false;

  // Prefix each attribute with its member's object id, so that two members
  // of the volume cannot collide on a key.
  for (size_t i = 0; i < members_.size(); ++i) {
    for (const auto& [key, value] : data_[i].attrs) {
      volume.attrs.emplace(xattr_prefix(members_[i].oid) + key, value);
    }
  }

  // The volume's own metadata travels as one of its attributes.
  encode(volume_, volume.attrs[kVolumeMetaAttr]);
  return true;
}

// Enters kWritingVolume, then kPublishing once the volume is durable.
void WeaveConversionJob::submit_volume() {
  // Called under the PG lock, while the controller still holds the
  // reservations this write depends on.
  if (!hooks_.validate()) { finish({true}); return; }

  auto& volume = std::get<PackState>(state_).volume;
  stage_ = Stage::kWritingVolume;

  host_.conversion_checkpoint("pack_before_write");
  tid_ = host_.write(volume_.volume_oid, volume.data, volume.attrs,
    utime_t(ceph::real_clock::now()), false, completion([this](int r) {
      if (r < 0) { resolve_volume(r); return; }

      host_.conversion_checkpoint("pack_committed");
      stage_ = Stage::kPublishing;
      // Data and volume_meta were committed in one object transaction. A
      // cancelled callback cannot undo that authority; reload uses the disk.
      hooks_.publish();
      host_.conversion_checkpoint("pack_published");

      // Sources are only removed once the packed volume can be reloaded.
      retire_member(0);
    }));
}

// Enters kResolvingVolume.
void WeaveConversionJob::resolve_volume(int error) {
  stage_ = Stage::kResolvingVolume;

  // Objecter timeouts can lose a successful write's reply. The old tid is no
  // longer admitted; wait for any already admitted transaction to release its
  // native lock before the controller reloads the authoritative disk metadata.
  if (host_.inspect(volume_.volume_oid).busy()) {
    retry([this, error] { resolve_volume(error); });
    return;
  }

  finish({true, error});
}

// Enters kRetiringMembers, one member per round trip.
void WeaveConversionJob::retire_member(size_t index) {
  stage_ = Stage::kRetiringMembers;
  const auto& member = members_[index];

  host_.conversion_checkpoint("source_before_remove", index);
  tid_ = host_.remove(member.oid, member.user_version,
    completion([this, index](int r) {
      // A member already gone counts as retired; anything else is retried.
      if (r < 0 && r != -ENOENT) {
        retry([this, index] { retire_member(index); }); return;
      }

      host_.conversion_checkpoint("source_removed", index);
      if (index + 1 < members_.size()) retire_member(index + 1);
      else finish({false});
    }));
}

// Runs the kMaterializing CPU phase, then submits the restored members.
void WeaveConversionJob::materialize() {
  const auto& volume = std::get<UnpackState>(state_).volume;
  // Restore every member from the packed volume before touching host state.
  bool valid = true;
  for (size_t i = 0; i < members_.size() && valid; ++i) {
    valid = extract_member(i, volume);
  }

  // submit_members() writes objects, so it needs the PG lock back.
  host_.serialized([self = shared_from_this(), valid] {
    if (!self->current()) return;
    if (!valid) { self->finish({false, -EIO}); return; }

    self->submit_members();
  });
}

bool WeaveConversionJob::extract_member(
  size_t index, const ObjectData& volume) {
  const auto& member = members_[index];
  const auto shard = volume_.members.at(member.oid).shard;

  // Read the member back out of the slots it was interleaved into.
  for (uint64_t off = 0; off < member.size; off += unit_) {
    const uint64_t len = std::min(unit_, member.size - off);
    const auto source = member_volume_offset(
      off, shard, volume_.data_shards, unit_);
    if (source > volume.data.length() || len > volume.data.length() - source) {
      return false;
    }

    bufferlist part;
    part.substr_of(volume.data, source, len);
    data_[index].data.claim_append(part);
  }

  // Lift the member's own attributes back out of the volume namespace.
  const auto prefix = xattr_prefix(member.oid);
  for (auto p = volume.attrs.lower_bound(prefix); p != volume.attrs.end() &&
       p->first.compare(0, prefix.size(), prefix) == 0; ++p) {
    data_[index].attrs.emplace(p->first.substr(prefix.size()), p->second);
  }
  return true;
}

void WeaveConversionJob::submit_members() {
  write_member(0);
}

// Enters kWritingMembers, one member per round trip.
void WeaveConversionJob::write_member(size_t index) {
  stage_ = Stage::kWritingMembers;
  const auto& member = members_[index];

  host_.conversion_checkpoint("member_before_write", index);
  tid_ = host_.write(member.oid, data_[index].data, data_[index].attrs,
    member.mtime, true, completion([this, index](int r) {
      if (r < 0) { retry([this, index] { write_member(index); }); return; }

      host_.conversion_checkpoint("member_written", index);
      // Every member must be back natively before the volume is retired.
      if (index + 1 < members_.size()) write_member(index + 1);
      else retire_volume();
    }));
}

// Enters kRetiringVolume, then kDetaching once the volume is gone.
void WeaveConversionJob::retire_volume() {
  stage_ = Stage::kRetiringVolume;

  host_.conversion_checkpoint("volume_before_remove");
  tid_ = host_.remove(volume_.volume_oid, std::nullopt,
    completion([this](int r) {
      // A volume that is already gone needs no further removal attempt.
      if (r < 0 && r != -ENOENT) { retry([this] { retire_volume(); }); return; }

      host_.conversion_checkpoint("volume_removed");
      // Until the Volume deletion commits, native copies remain shadows and the
      // mapping remains authoritative, including after a reset or restart.
      stage_ = Stage::kDetaching;
      hooks_.detach();
      host_.conversion_checkpoint("volume_detached");

      finish({true});
    }));
}
}  // namespace ceph::weave
