#include "gtest/gtest.h"
#include "global/global_context.h"
#include "common/TrackedOp.h"
#include "osd/OSDMap.h"
#include "osd/osd_internal_types.h"
#include "osd/OSDCap.h"
#include "osd/weave/WeaveService.h"
#include <future>
#include "osd/weave/WeavePGController.h"
#include "osd/weave/detail/WeaveConversionJob.h"
#include "osd/weave/detail/WeaveLayout.h"
#include "osd/weave/detail/WeaveReadRouter.h"
#include "osd/weave/detail/WeaveXAttr.h"
#include <deque>
#include <set>

using namespace ceph::weave;
namespace {
hobject_t oid(const char* name) {
  return hobject_t(sobject_t(object_t(name), CEPH_NOSNAP));
}
class FakeWeavePG final : public WeavePGInterface {
public:
  struct Object { WeaveObjectState state; bufferlist data; WeaveAttrs attrs; };
  struct IO { ceph_tid_t tid; std::string kind; hobject_t oid;
              std::function<int(int)> apply; WeaveCompletion complete; };
  std::map<hobject_t, Object> objects;
  std::deque<IO> io;
  std::deque<std::function<void()>> cpu;
  std::map<WeaveRetryKind, std::function<void()>> retries;
  std::vector<std::string> events;
  std::function<void(Object&)> restore_copy_state;
  epoch_t generation = 1;
  unsigned acquired = 0, released = 0;
  size_t requeued = 0;
  ceph_tid_t sequence = 0;
  std::set<ceph_tid_t> cancelled;
  bool allow_acquire = true;
  bool primary_role = true;
  bool missing = false;
  bool clean_state = true;
  snapid_t pool_snap_sequence = 0;
  int metadata_result = 0;
  int last_error = 0;
  std::optional<WeaveReadRoute> read_route;
  bufferlist read_metadata;
  int read_route_result = 0;
  unsigned redirects = 0;
  WeavePolicy settings{true, 1, 0, 1024, 100};
  void put(const hobject_t& id, const char* bytes) {
    auto& o = objects[id];
    o.data.clear(); o.data.append(bytes);
    o.state.exists = true; o.state.info.soid = id;
    o.state.info.size = o.data.length();
    o.state.info.version = eversion_t(1, 1); o.state.info.user_version = 1;
  }
  std::shared_ptr<void> pin() override { return {}; }
  WeavePolicy policy() const override { return settings; }
  WeaveGeometry geometry() const override { return {2, 4}; }
  bool primary() const override { return primary_role; }
  bool active() const override { return true; }
  bool clean() const override { return clean_state; }
  snapid_t snap_sequence() const override { return pool_snap_sequence; }
  bool has_missing() const override { return missing; }
  epoch_t epoch() const override { return generation; }
  bool current(epoch_t e) const override { return e == generation; }
  int osd_id() const override { return 0; }
  std::optional<WeaveReadRoute> locate_read(const hobject_t&, unsigned) override { return read_route; }
  int load_read_route(const WeaveReadRoute&, bufferlist& out) override {
    out = read_metadata; return read_route_result;
  }
  void reply_read_redirect(const OpRequestRef& op, const WeaveReadRoute&) override {
    EXPECT_FALSE(op->is_weave_member_op());
    ++redirects;
  }
  WeaveObjectState inspect(const hobject_t& id) override { return objects[id].state; }
  bool wait_for_available(const hobject_t&, OpRequestRef&) override { return false; }
  int load_metadata(WeaveVolumeAttrs& out) override {
    if (metadata_result < 0) return metadata_result;
    for (const auto& [id, object] : objects) {
      auto p = object.attrs.find("volume_meta");
      if (object.state.exists && p != object.attrs.end()) out.emplace_back(id, p->second);
    }
    return 0;
  }
  hobject_t new_volume(const hobject_t&) override {
    auto id = oid("volume");
    id.nspace = ".ceph-internal-weave";
    return id;
  }
  void requeue(std::list<OpRequestRef>& requests) override {
    requeued += requests.size();
    requests.clear();
  }
  void reply_error(const OpRequestRef&, int r) override {
    last_error = r;
    events.push_back("reply_error");
  }
  std::unique_ptr<WeaveLease> acquire() override {
    if (!allow_acquire) return {};
    ++acquired;
    return std::make_unique<WeaveLease>([this] { ++released; });
  }
  void retry(WeaveRetryKind kind, std::function<void()> cb) override {
    retries[kind] = std::move(cb);
  }
  void cancel_retries() override { retries.clear(); }
  void post(std::function<void()> cb) override { cpu.push_back(std::move(cb)); }
  void serialized(std::function<void()> cb) override { cb(); }
  ceph_tid_t read(const hobject_t& id, version_t version, uint64_t size,
    bufferlist* data, WeaveAttrs* attrs, WeaveCompletion cb) override {
    const auto tid = ++sequence;
    io.push_back({tid, "read", id, [=](int r) {
      if (!r) {
        auto& object = objects[id];
        if (!object.state.exists) r = -ENOENT;
        else if (version != object.state.info.user_version) r = -ERANGE;
        else {
          data->substr_of(object.data, 0, std::min<uint64_t>(size, object.data.length()));
          *attrs = object.attrs;
        }
      }
      return r;
    }, std::move(cb)});
    return tid;
  }
  ceph_tid_t write(const hobject_t& id, const bufferlist& data, const WeaveAttrs& attrs,
    utime_t mtime, bool replace, WeaveCompletion cb) override {
    const auto tid = ++sequence;
    Object restored;
    restored.state.info.soid = id;
    restored.state.info.user_version = 1;
    if (replace && restore_copy_state) restore_copy_state(restored);
    io.push_back({tid, "write", id, [=](int r) {
      if (!r) {
        auto& object = objects[id];
        if (object.state.exists && !replace) r = -EEXIST;
        else {
          object.data = data; object.attrs = attrs;
          object.state.exists = true; object.state.info.soid = id;
          object.state.info.size = data.length(); object.state.info.mtime = mtime;
          object.state.info.version = eversion_t(1, 1); object.state.info.user_version = 1;
          if (replace) {
            object.state.info.user_version = restored.state.info.user_version;
            object.state.snap_sequence = restored.state.snap_sequence;
          }
          events.push_back("durable:" + id.oid.name);
        }
      }
      return r;
    }, std::move(cb)});
    return tid;
  }
  ceph_tid_t remove(const hobject_t& id, std::optional<version_t> version,
    WeaveCompletion cb) override {
    const auto tid = ++sequence;
    io.push_back({tid, "remove", id, [=](int r) {
      if (!r) {
        auto& object = objects[id];
        if (!object.state.exists) r = -ENOENT;
        else if (version && *version != object.state.info.user_version) r = -ERANGE;
        else { object.state.exists = false; events.push_back("removed:" + id.oid.name); }
      }
      return r;
    }, std::move(cb)});
    return tid;
  }
  void cancel_io(ceph_tid_t tid) override { cancelled.insert(tid); }
  IO complete(int r = 0) {
    auto pending = std::move(io.front()); io.pop_front();
    pending.complete(pending.apply(r));
    return pending;
  }
  void run_cpu() { auto cb = std::move(cpu.front()); cpu.pop_front(); cb(); }
  void tick(WeaveRetryKind kind = WeaveRetryKind::kConversion) {
    auto p = retries.find(kind);
    if (p == retries.end()) return;
    auto cb = std::move(p->second);
    retries.erase(p);
    cb();
  }
};

class WeaveConversion : public ::testing::Test {
protected:
  using Mode = WeaveConversionJob::Mode;

  FakeWeavePG pg_interface;
  const hobject_t a = oid("a"), b = oid("b"), v = oid("volume");
  WeaveVolumeMeta volume{v, 2, 4,
    {{a, WeaveMemberMeta{0, 4, {}, 1}}, {b, WeaveMemberMeta{1, 4, {}, 1}}}};
  std::vector<WeaveCandidate> members;
  std::shared_ptr<WeaveConversionJob> job;
  bool published = false, detached = false, accept_validation = true;
  unsigned finishes = 0;
  WeaveConversionJob::Result result{};
  void SetUp() override {
    pg_interface.put(a, "AAAA"); pg_interface.put(b, "BBBB");
    for (auto id : {a, b}) {
      auto state = pg_interface.inspect(id);
      members.push_back({id, 4, state.info.version, 1, {}, {}});
    }
  }
  void create(Mode mode = Mode::kPack) {
    WeaveConversionJob::Hooks hooks{
      [this] { return accept_validation; },
      [this] {
        EXPECT_TRUE(pg_interface.objects[v].state.exists);
        pg_interface.events.push_back("publish");
        published = true;
      },
      [this] { detached = true; pg_interface.events.push_back("detach"); },
      [this](auto r) { ++finishes; result = r; }};
    job = std::make_shared<WeaveConversionJob>(pg_interface, 1, 4, members, volume,
      pg_interface.acquire(), std::move(hooks), mode, 1, 8);
    job->start();
  }
  void ready_to_publish() {
    create(); pg_interface.complete(); pg_interface.complete(); pg_interface.run_cpu();
    ASSERT_EQ(job->stage(), WeaveConversionJob::Stage::kWritingVolume);
  }
  void prepare_unpack() {
    pg_interface.put(v, "AAAABBBB");
    pg_interface.objects[a].state.exists = false; pg_interface.objects[b].state.exists = false;
    create(Mode::kUnpack);
  }
  void TearDown() override {
    if (job) job->cancel();
    pg_interface.io.clear(); pg_interface.cpu.clear(); pg_interface.retries.clear(); job.reset();
    EXPECT_EQ(pg_interface.acquired, pg_interface.released);
  }
};

TEST_F(WeaveConversion, PublishesOnlyAfterDurableVolumeAndRetiresSourcesLast) {
  ready_to_publish();
  EXPECT_FALSE(published); EXPECT_EQ(pg_interface.released, 0u);
  pg_interface.complete();
  EXPECT_TRUE(published); EXPECT_TRUE(pg_interface.objects[a].state.exists);
  pg_interface.complete(); pg_interface.complete();
  EXPECT_EQ(job->stage(), WeaveConversionJob::Stage::kCompleted);
  EXPECT_EQ(finishes, 1u); EXPECT_EQ(pg_interface.released, 1u);
  EXPECT_EQ(pg_interface.events, (std::vector<std::string>{"durable:volume", "publish", "removed:a", "removed:b"}));
}
TEST_F(WeaveConversion, PackModeReadsMemberWithNonemptyMembers) {
  create(Mode::kPack);
  ASSERT_EQ(pg_interface.io.size(), 1u);
  EXPECT_EQ(pg_interface.io.front().kind, "read");
  EXPECT_EQ(pg_interface.io.front().oid, a);
  EXPECT_FALSE(job->copy_version(a));
}
TEST_F(WeaveConversion, UnpackModeReadsVolumeWithNonemptyMembers) {
  prepare_unpack();
  ASSERT_EQ(pg_interface.io.size(), 1u);
  EXPECT_EQ(pg_interface.io.front().kind, "read");
  EXPECT_EQ(pg_interface.io.front().oid, v);
  EXPECT_EQ(job->copy_version(a), std::optional<version_t>(1));
}
TEST_F(WeaveConversion, ReadFailureRetainsSourcesAndReleasesOnce) {
  create(); pg_interface.complete(-EIO);
  EXPECT_EQ(finishes, 1u); EXPECT_TRUE(result.restore_candidates);
  EXPECT_FALSE(published); EXPECT_TRUE(pg_interface.objects[a].state.exists);
  job->cancel(); EXPECT_EQ(pg_interface.released, 1u);
}
TEST_F(WeaveConversion, FailedValidationNeverSubmitsMetadataOrRetiresSources) {
  accept_validation = false;
  create(); pg_interface.complete(); pg_interface.complete(); pg_interface.run_cpu();
  EXPECT_TRUE(pg_interface.io.empty());
  EXPECT_TRUE(pg_interface.objects[a].state.exists); EXPECT_FALSE(pg_interface.objects[v].state.exists);
  EXPECT_TRUE(result.restore_candidates); EXPECT_EQ(finishes, 1u);
}
TEST_F(WeaveConversion, VolumeWriteFailureNeverPublishes) {
  ready_to_publish(); pg_interface.complete(-EIO);
  EXPECT_FALSE(published); EXPECT_EQ(finishes, 1u); EXPECT_TRUE(result.restore_candidates);
}
TEST_F(WeaveConversion, CancelledReadAndDuplicateCallbackCannotAdvanceNewJob) {
  create(); const auto tid = pg_interface.io.front().tid;
  EXPECT_TRUE(job->authenticates(tid));
  job->cancel(); job->cancel();
  EXPECT_TRUE(pg_interface.cancelled.count(tid)); EXPECT_EQ(finishes, 1u);
  auto next_lease = pg_interface.acquire();
  auto stale = pg_interface.complete(); stale.complete(0);
  EXPECT_TRUE(pg_interface.io.empty()); EXPECT_FALSE(published);
  EXPECT_EQ(pg_interface.acquired, 2u); EXPECT_EQ(pg_interface.released, 1u);
  next_lease.reset();
}
TEST_F(WeaveConversion, PendingReadKeepsCancelledJobAliveUntilCallbackIsReleased) {
  create();
  std::weak_ptr<WeaveConversionJob> pending_job = job;
  job->cancel();
  job.reset();
  EXPECT_FALSE(pending_job.expired());

  // Objecter may still fill the job's read buffers after cancellation.
  auto late = pg_interface.complete();
  EXPECT_FALSE(pending_job.expired());
  EXPECT_TRUE(pg_interface.io.empty());
  EXPECT_EQ(finishes, 1u);
  EXPECT_EQ(pg_interface.released, 1u);
  late.complete = {};
  EXPECT_TRUE(pending_job.expired());
}
TEST_F(WeaveConversion, DuplicateSuccessfulReadCannotReadNextMemberTwice) {
  create(); auto first = pg_interface.complete();
  ASSERT_EQ(pg_interface.io.size(), 1u);
  first.complete(0);
  EXPECT_EQ(pg_interface.io.size(), 1u);
}
TEST_F(WeaveConversion, CancelledCPUWorkCannotSubmitVolume) {
  create(); pg_interface.complete(); pg_interface.complete(); job->cancel(); pg_interface.run_cpu();
  EXPECT_TRUE(pg_interface.io.empty()); EXPECT_FALSE(published); EXPECT_EQ(finishes, 1u);
}
TEST_F(WeaveConversion, CancelledDurableWriteCannotPublish) {
  ready_to_publish(); job->cancel(); pg_interface.complete();
  EXPECT_TRUE(pg_interface.objects[v].state.exists); EXPECT_FALSE(published);
  EXPECT_TRUE(pg_interface.objects[a].state.exists); EXPECT_EQ(finishes, 1u);
}
TEST_F(WeaveConversion, RetirementFailureKeepsLeaseAndRetries) {
  ready_to_publish(); pg_interface.complete(); pg_interface.complete(-EIO);
  EXPECT_EQ(pg_interface.released, 0u); EXPECT_EQ(finishes, 0u);
  pg_interface.tick(); pg_interface.complete(); pg_interface.complete();
  EXPECT_EQ(finishes, 1u); EXPECT_EQ(pg_interface.released, 1u);
}
TEST_F(WeaveConversion, CancelledRetryCannotRetireSourcesAfterReset) {
  ready_to_publish(); pg_interface.complete(); pg_interface.complete(-EIO);
  job->cancel(); ++pg_interface.generation;
  pg_interface.tick();
  EXPECT_TRUE(pg_interface.io.empty()); EXPECT_TRUE(pg_interface.objects[a].state.exists);
  EXPECT_EQ(finishes, 1u); EXPECT_EQ(pg_interface.released, 1u);
}
TEST_F(WeaveConversion, CancelledRestoreKeepsPublishedMapping) {
  prepare_unpack(); pg_interface.complete(); pg_interface.run_cpu(); pg_interface.complete();
  EXPECT_TRUE(pg_interface.objects[a].state.exists); EXPECT_FALSE(detached);
  job->cancel(); pg_interface.complete();
  EXPECT_FALSE(detached); EXPECT_TRUE(pg_interface.objects[v].state.exists);
  EXPECT_EQ(finishes, 1u);
}
TEST_F(WeaveConversion, RestoresAllMembersBeforeDetachingMapping) {
  prepare_unpack(); pg_interface.complete(); pg_interface.run_cpu();
  pg_interface.complete(); EXPECT_FALSE(detached);
  pg_interface.complete(); EXPECT_FALSE(detached); EXPECT_TRUE(pg_interface.objects[v].state.exists);
  pg_interface.complete(); EXPECT_TRUE(detached); EXPECT_FALSE(pg_interface.objects[v].state.exists);
  EXPECT_TRUE(pg_interface.objects[a].state.exists); EXPECT_TRUE(pg_interface.objects[b].state.exists);
  EXPECT_EQ(pg_interface.objects[a].data.to_str(), "AAAA"); EXPECT_EQ(pg_interface.objects[b].data.to_str(), "BBBB");
  EXPECT_EQ(finishes, 1u);
}
TEST_F(WeaveConversion, RestoreFailureKeepsMappingAndRetries) {
  prepare_unpack(); pg_interface.complete(); pg_interface.run_cpu(); pg_interface.complete(-ENOSPC);
  EXPECT_FALSE(detached); EXPECT_EQ(pg_interface.released, 0u);
  pg_interface.tick(); pg_interface.complete(); pg_interface.complete(); pg_interface.complete();
  EXPECT_EQ(finishes, 1u);
}
TEST_F(WeaveConversion, VolumeReadFailureDoesNotDetach) {
  prepare_unpack(); pg_interface.complete(-EIO);
  EXPECT_FALSE(detached); EXPECT_EQ(result.error, -EIO); EXPECT_EQ(finishes, 1u);
}
TEST_F(WeaveConversion, ShortVolumeFailsMaterializationWithoutNativeWrites) {
  prepare_unpack(); pg_interface.objects[v].data.clear(); pg_interface.objects[v].data.append("A");
  pg_interface.complete(); pg_interface.run_cpu();
  EXPECT_FALSE(detached); EXPECT_TRUE(pg_interface.io.empty()); EXPECT_EQ(result.error, -EIO);
}
TEST_F(WeaveConversion, EmptyVolumeDeletionDetachesOnlyAfterCommit) {
  pg_interface.put(v, "AAAABBBB"); members.clear(); volume.members.clear();
  create(Mode::kUnpack);
  ASSERT_EQ(pg_interface.io.size(), 1u);
  EXPECT_EQ(pg_interface.io.front().kind, "remove");
  EXPECT_EQ(pg_interface.io.front().oid, v);
  EXPECT_FALSE(detached); pg_interface.complete(); EXPECT_TRUE(detached); EXPECT_EQ(finishes, 1u);
}

TEST_F(WeaveConversion, FailedVolumeDeletionKeepsAuthorityUntilDurableRetry) {
  prepare_unpack(); pg_interface.complete(); pg_interface.run_cpu();
  pg_interface.complete(); pg_interface.complete(); pg_interface.complete(-EIO);
  EXPECT_FALSE(detached);
  EXPECT_TRUE(pg_interface.objects[v].state.exists);
  EXPECT_EQ(pg_interface.released, 0u);
  pg_interface.tick(); pg_interface.complete();
  EXPECT_TRUE(detached);
  EXPECT_EQ(finishes, 1u);
}

// Each restart keeps only durable objects/attributes, discarding the job,
// reservations and catalog. I/O commit and callback delivery are independent.
class WeaveDurableRecovery : public ::testing::TestWithParam<unsigned> {
protected:
  OpTracker tracker{g_ceph_context, false, 1};
  std::unique_ptr<WeavePGController> controller;
  FakeWeavePG* pg_interface = nullptr;
  const hobject_t a = oid("a"), b = oid("b");
  hobject_t v;
  std::vector<OpRequestRef> requests;

  void open(std::map<hobject_t, FakeWeavePG::Object> disk = {}) {
    auto owner = std::make_unique<FakeWeavePG>();
    pg_interface = owner.get();
    pg_interface->objects = std::move(disk);
    pg_interface->settings.background = false;
    pg_interface->restore_copy_state = [this](auto& object) {
      auto op = request(object.state.info.soid, CEPH_OSD_OP_WRITEFULL);
      op->set_background_weave_io();
      const auto version = controller->internal_copy_version(op);
      const auto sequence = controller->internal_copy_snap_sequence(op);
      ASSERT_TRUE(version);
      ASSERT_TRUE(sequence);
      object.state.info.user_version = *version;
      object.state.snap_sequence = *sequence;
    };
    v = pg_interface->new_volume(a);
    controller = std::make_unique<WeavePGController>(
      g_ceph_context, std::move(owner), true);
    controller->initialize();
  }
  void SetUp() override {
    open();
    for (auto id : {a, b}) {
      pg_interface->put(id, id == a ? "AAAA" : "BBBB");
      auto& object = pg_interface->objects[id];
      object.state.info.user_version = 7;
      object.state.info.version = eversion_t(1, 7);
      object.state.info.mtime = utime_t(123, 456);
      object.state.snap_sequence = 3;
      object.attrs["tag"].append(id == a ? "alpha" : "beta");
    }
  }
  OpRequestRef request(const hobject_t& id, int opcode, bool init_info = true,
                       int flags = 0) {
    auto* message = new MOSDOp(0, 1, id, spg_t(), 1, CEPH_OSD_FLAG_ONDISK | flags,
                              CEPH_FEATURES_SUPPORTED_DEFAULT);
    message->ops.resize(1);
    message->ops[0].op.op = opcode;
    if (opcode == CEPH_OSD_OP_READ) message->ops[0].op.extent.length = 4;
    auto op = tracker.create_request<OpRequest, Message*>(message);
    if (init_info) {
      EXPECT_EQ(op->maybe_init_op_info(OSDMap()), 0);
    }
    requests.push_back(op);
    return op;
  }
  void step() {
    if (!pg_interface->io.empty()) pg_interface->complete();
    else if (!pg_interface->cpu.empty()) pg_interface->run_cpu();
    else FAIL() << "no pending conversion step";
  }
  void drain() {
    unsigned limit = 32;
    while ((!pg_interface->io.empty() || !pg_interface->cpu.empty()) && limit--) step();
    ASSERT_TRUE(pg_interface->io.empty());
    ASSERT_TRUE(pg_interface->cpu.empty());
  }
  void pack() {
    pg_interface->settings.background = true;
    for (auto id : {a, b}) controller->on_commit(pg_interface->inspect(id).info, true, {});
    controller->scan_candidates();
    pg_interface->settings.background = false;
    ASSERT_FALSE(pg_interface->io.empty());
  }
  void restart(bool pending_commits, bool rebuild = true) {
    for (const auto& op : requests) controller->finish_request(op);
    controller->on_pg_change(false);
    ++pg_interface->generation;
    if (!pg_interface->io.empty()) pg_interface->complete(pending_commits ? 0 : -ECANCELED);
    while (!pg_interface->cpu.empty()) pg_interface->run_cpu();
    ASSERT_TRUE(pg_interface->io.empty());
    EXPECT_EQ(pg_interface->acquired, pg_interface->released);
    if (rebuild) {
      auto disk = std::move(pg_interface->objects);
      controller.reset();
      open(std::move(disk));
    } else {
      controller->initialize();
    }
  }
  void expect_original(const hobject_t& id) {
    auto op = request(id, CEPH_OSD_OP_READ);
    ASSERT_EQ(controller->prepare_request(op), RequestDisposition::kNative);
    const auto disposition = controller->preprocess_client_op(op);
    const auto& object = pg_interface->objects[op->get_req<MOSDOp>()->get_hobj()];
    ASSERT_TRUE(object.state.exists);
    EXPECT_EQ(controller->logical_user_version(op, object.state.info.user_version), 7u);
    if (disposition == RequestDisposition::kTranslated) {
      WeaveVolumeMeta metadata;
      auto p = object.attrs.at("volume_meta").cbegin();
      decode(metadata, p);
      const auto& member = metadata.members.at(id);
      EXPECT_EQ(object.data.to_str().substr(member.shard * 4, 4), id == a ? "AAAA" : "BBBB");
      EXPECT_EQ(member.snap_sequence, 3u);
      EXPECT_EQ(member.mtime, utime_t(123, 456));
      EXPECT_EQ(object.attrs.at(xattr_name(id, "tag")).to_str(), id == a ? "alpha" : "beta");
    } else {
      EXPECT_EQ(disposition, RequestDisposition::kNative);
      EXPECT_EQ(object.data.to_str(), id == a ? "AAAA" : "BBBB");
      EXPECT_EQ(object.state.snap_sequence, 3u);
      EXPECT_EQ(object.state.info.mtime, utime_t(123, 456));
      EXPECT_EQ(object.attrs.at("tag").to_str(), id == a ? "alpha" : "beta");
    }
    controller->finish_request(op);
  }
  void mutate_after_recovery() {
    auto write = request(a, CEPH_OSD_OP_WRITEFULL);
    const auto disposition = controller->preprocess_client_op(write);
    if (disposition == RequestDisposition::kDeferred) {
      drain();
      ASSERT_EQ(controller->preprocess_client_op(write), RequestDisposition::kNative);
    } else ASSERT_EQ(disposition, RequestDisposition::kNative);
    // The acknowledged new version must survive another complete reconstruction.
    pg_interface->put(a, "NEW!");
    pg_interface->objects[a].state.info.user_version = 8;
    controller->on_commit(pg_interface->inspect(a).info, true, write);
    auto remove = request(b, CEPH_OSD_OP_DELETE);
    ASSERT_EQ(controller->preprocess_client_op(remove), RequestDisposition::kNative);
    pg_interface->objects[b].state.exists = false;
    controller->on_commit(pg_interface->inspect(b).info, false, remove);
    restart(false);
    EXPECT_FALSE(controller->is_logical_member(a));
    EXPECT_EQ(pg_interface->objects[a].data.to_str(), "NEW!");
    EXPECT_EQ(pg_interface->objects[a].state.info.user_version, 8u);
    EXPECT_FALSE(controller->is_logical_member(b));
    EXPECT_FALSE(pg_interface->objects[b].state.exists);
    auto recreate = request(b, CEPH_OSD_OP_WRITEFULL);
    ASSERT_EQ(controller->preprocess_client_op(recreate), RequestDisposition::kNative);
    pg_interface->put(b, "BNEW"); // recreated objects may reuse a user_version
    controller->on_commit(pg_interface->inspect(b).info, true, recreate);
    controller->request_cleanup(100, std::make_shared<WeaveReclaimPass>());
    drain();
    restart(false);
    EXPECT_EQ(pg_interface->objects[b].data.to_str(), "BNEW");
    EXPECT_TRUE(pg_interface->objects[b].state.exists);
    EXPECT_EQ(pg_interface->objects[a].data.to_str(), "NEW!");
  }
  void TearDown() override {
    if (controller) {
      for (const auto& op : requests) controller->finish_request(op);
      controller->on_pg_change(false);
      controller.reset();
    }
    requests.clear();
    tracker.on_shutdown();
  }
};

TEST_P(WeaveDurableRecovery, PackCommitAndLostCompletionNeverExposeNewNativeWrites) {
  pack();
  for (unsigned i = 0; i < GetParam() / 2; ++i) step();
  restart(GetParam() % 2);
  expect_original(a); expect_original(b);
  mutate_after_recovery();
}

TEST_P(WeaveDurableRecovery, MaterializationCommitAndLostCompletionNeverReviveOldVolume) {
  pack(); drain();
  auto write = request(a, CEPH_OSD_OP_WRITEFULL);
  ASSERT_EQ(controller->preprocess_client_op(write), RequestDisposition::kDeferred);
  // Unpacking has five transitions, packing six; include repeated terminal cases.
  for (unsigned i = 0; i < std::min(GetParam() / 2, 5u); ++i) step();
  restart(GetParam() % 2);
  expect_original(a); expect_original(b);
  mutate_after_recovery();
}

INSTANTIATE_TEST_SUITE_P(EveryBoundary, WeaveDurableRecovery, ::testing::Range(0u, 14u));

TEST_F(WeaveDurableRecovery, ResetReloadHonorsCommittedVolumeEvenWithoutPublicationCallback) {
  pack(); step(); step(); step(); // Volume write queued
  restart(true, false); // same controller, committed I/O with cancelled callback
  ASSERT_TRUE(controller->is_logical_member(a));
  expect_original(a);
  mutate_after_recovery();
}

TEST_F(WeaveDurableRecovery, TimedOutCommittedVolumeWriteReloadsAuthority) {
  pack(); step(); step(); step();
  auto pending = std::move(pg_interface->io.front()); pg_interface->io.pop_front();
  ASSERT_EQ(pending.apply(0), 0);
  pending.complete(-ETIMEDOUT);
  EXPECT_TRUE(controller->is_logical_member(a));
  expect_original(a); expect_original(b);
  mutate_after_recovery();
}

TEST_F(WeaveDurableRecovery, TimedOutWriteWaitsForAdmittedTransactionBeforeReload) {
  pack(); step(); step(); step();
  auto pending = std::move(pg_interface->io.front()); pg_interface->io.pop_front();
  pg_interface->objects[v].state.access = WeaveObjectState::Access::kBusy;
  pending.complete(-ETIMEDOUT);
  EXPECT_EQ(pg_interface->released, 0u);
  auto write = request(a, CEPH_OSD_OP_WRITEFULL);
  EXPECT_EQ(controller->preprocess_client_op(write), RequestDisposition::kDeferred);
  ASSERT_EQ(pending.apply(0), 0);
  pg_interface->objects[v].state.access = WeaveObjectState::Access::kIdle;
  pg_interface->tick();
  EXPECT_EQ(pg_interface->released, 1u);
  EXPECT_TRUE(controller->is_logical_member(a));
  expect_original(a); expect_original(b);
  mutate_after_recovery();
}

TEST_F(WeaveDurableRecovery, ConflictingOwnersCloseRequestAdmissionUntilDiskIsRepaired) {
  pack(); drain();
  auto other = v; other.oid = object_t("other-volume");
  auto duplicate = pg_interface->objects[v];
  WeaveVolumeMeta metadata;
  auto p = duplicate.attrs.at("volume_meta").cbegin(); decode(metadata, p);
  metadata.volume_oid = other;
  duplicate.attrs["volume_meta"].clear();
  encode(metadata, duplicate.attrs["volume_meta"]);
  pg_interface->objects[other] = std::move(duplicate);
  restart(false);
  auto read = request(a, CEPH_OSD_OP_READ);
  EXPECT_EQ(controller->prepare_request(read), RequestDisposition::kRejected);
  EXPECT_EQ(pg_interface->last_error, -EIO);
  pg_interface->objects[other].state.exists = false;
  controller->initialize();
  expect_original(a);
}

TEST_F(WeaveDurableRecovery, SourceSnapshotChangeBeforeCommitNeverPersistsMapping) {
  pack(); step(); step();
  pg_interface->objects[a].state.snap_sequence = 5;
  step();
  EXPECT_TRUE(pg_interface->io.empty());
  EXPECT_FALSE(pg_interface->objects[v].state.exists);
  EXPECT_FALSE(controller->is_logical_member(a));
}

TEST_F(WeaveDurableRecovery, OldPhysicalRequestsCannotDeleteOrOverwriteRecreatedMembers) {
  pack(); step(); step(); step(); step();
  const auto old_tid = pg_interface->io.front().tid;
  restart(false);
  mutate_after_recovery();
  for (const auto source : {0, 1}) {
    for (auto opcode : {CEPH_OSD_OP_DELETE, CEPH_OSD_OP_WRITEFULL}) {
      auto op = request(b, opcode);
      auto* message = static_cast<MOSDOp*>(op->get_nonconst_req());
      message->get_header().src.type = CEPH_ENTITY_TYPE_OSD;
      message->get_header().src.num = source;
      message->set_tid(old_tid);
      message->ops[0].op.flags = 1u << 29;
      EXPECT_EQ(controller->prepare_request(op), RequestDisposition::kRejected);
      EXPECT_EQ(pg_interface->last_error, -ECANCELED);
    }
  }
  EXPECT_TRUE(pg_interface->objects[b].state.exists);
  EXPECT_EQ(pg_interface->objects[b].data.to_str(), "BNEW");
}

TEST_F(WeaveDurableRecovery, ReservationsHoldReadsWritesDeletesAndSnapshotsThroughRetirement) {
  pack(); drain();
  auto write = request(a, CEPH_OSD_OP_WRITEFULL);
  ASSERT_EQ(controller->preprocess_client_op(write), RequestDisposition::kDeferred);
  step(); step(); step(); step(); // both native copies durable; Volume still owns them
  ASSERT_TRUE(controller->is_logical_member(a));
  for (auto opcode : {CEPH_OSD_OP_READ, CEPH_OSD_OP_WRITEFULL, CEPH_OSD_OP_DELETE}) {
    auto op = request(a, opcode);
    EXPECT_EQ(controller->preprocess_client_op(op), RequestDisposition::kDeferred);
  }
  auto snap = a; snap.snap = 5;
  auto read = request(snap, CEPH_OSD_OP_READ);
  EXPECT_EQ(controller->preprocess_client_op(read), RequestDisposition::kDeferred);
  // Commit deletion without delivering the reply: reservations must still hold.
  auto pending = std::move(pg_interface->io.front()); pg_interface->io.pop_front();
  ASSERT_EQ(pending.apply(0), 0);
  auto late = request(a, CEPH_OSD_OP_READ);
  EXPECT_EQ(controller->preprocess_client_op(late), RequestDisposition::kDeferred);
  auto listing = request(oid(""), CEPH_OSD_OP_PGLS, false);
  EXPECT_EQ(controller->prepare_request(listing), RequestDisposition::kDeferred);
  pending.complete(0);
  EXPECT_EQ(controller->preprocess_client_op(late), RequestDisposition::kNative);
  EXPECT_EQ(controller->prepare_request(listing), RequestDisposition::kNative);
  expect_original(a); expect_original(b);
}

class WeavePackingReads : public WeaveDurableRecovery {};

TEST_F(WeavePackingReads, ReadsSucceedAcrossEveryPackStage) {
  pack();
  for (unsigned phase = 0; phase <= 6; ++phase) {
    SCOPED_TRACE(phase);
    expect_original(a); expect_original(b);
    if (phase < 6) step();
  }
}

TEST_F(WeavePackingReads, DurableVolumeBeforeCallbackStillReadsNative) {
  pack(); step(); step(); step();
  auto pending = std::move(pg_interface->io.front()); pg_interface->io.pop_front();
  ASSERT_EQ(pending.apply(0), 0);
  ASSERT_FALSE(controller->is_logical_member(a));
  expect_original(a); expect_original(b);
  pending.complete(0);
  ASSERT_TRUE(controller->is_logical_member(a));
  expect_original(a); expect_original(b);
  drain();
}

TEST_F(WeavePackingReads, CleanupFailureDoesNotBlockReadsButMutationsStayFenced) {
  pack(); step(); step(); step(); step();
  for (int retry = 0; retry < 4; ++retry) {
    pg_interface->complete(-EIO);
    expect_original(a); expect_original(b);
    for (auto opcode : {CEPH_OSD_OP_WRITEFULL, CEPH_OSD_OP_DELETE, CEPH_OSD_OP_SETXATTR}) {
      auto op = request(a, opcode);
      EXPECT_EQ(controller->preprocess_client_op(op), RequestDisposition::kDeferred);
    }
    EXPECT_EQ(pg_interface->released, 0u);
    pg_interface->tick();
  }
  drain();
}

TEST_F(WeavePackingReads, FailedVolumeWriteLeavesNativeReadable) {
  pack(); step(); step(); step();
  pg_interface->complete(-EIO);
  EXPECT_FALSE(controller->is_logical_member(a));
  expect_original(a); expect_original(b);
}

TEST_F(WeavePackingReads, TimeoutResolutionKeepsNativeReadsAvailable) {
  pack(); step(); step(); step();
  auto pending = std::move(pg_interface->io.front()); pg_interface->io.pop_front();
  pg_interface->objects[v].state.access = WeaveObjectState::Access::kBusy;
  pending.complete(-ETIMEDOUT);
  expect_original(a); expect_original(b);
  ASSERT_EQ(pending.apply(0), 0);
  expect_original(a); expect_original(b);
  auto write = request(a, CEPH_OSD_OP_WRITEFULL);
  EXPECT_EQ(controller->preprocess_client_op(write), RequestDisposition::kDeferred);
  pg_interface->objects[v].state.access = WeaveObjectState::Access::kIdle;
  pg_interface->tick();
  ASSERT_TRUE(controller->is_logical_member(a));
  expect_original(a); expect_original(b);
}

TEST_F(WeavePackingReads, NativeRequestRequeuedAfterPublicationUsesVolume) {
  pack();
  auto read = request(a, CEPH_OSD_OP_READ);
  ASSERT_EQ(controller->preprocess_client_op(read), RequestDisposition::kNative);
  step(); step(); step(); step();
  ASSERT_EQ(controller->prepare_request(read), RequestDisposition::kNative);
  ASSERT_EQ(controller->preprocess_client_op(read), RequestDisposition::kTranslated);
  EXPECT_EQ(read->get_req<MOSDOp>()->get_hobj(), v);
  controller->finish_request(read);
  drain();
}

TEST_F(WeavePackingReads, SharedReadersDoNotPreventSelectionOrPublication) {
  pg_interface->objects[a].state.access = WeaveObjectState::Access::kReading;
  pg_interface->objects[b].state.access = WeaveObjectState::Access::kReading;
  pack();
  expect_original(a); expect_original(b);
  step(); step(); step(); step();
  EXPECT_TRUE(controller->is_logical_member(a));
  expect_original(a); expect_original(b);
  pg_interface->objects[a].state.access = WeaveObjectState::Access::kIdle;
  pg_interface->objects[b].state.access = WeaveObjectState::Access::kIdle;
  drain();
}

TEST_F(WeavePackingReads, NativeConflictBeforeCommitStillCancelsPacking) {
  pack(); step(); step();
  pg_interface->objects[a].state.access = WeaveObjectState::Access::kBusy;
  step();
  EXPECT_TRUE(pg_interface->io.empty());
  EXPECT_FALSE(controller->is_logical_member(a));
  EXPECT_FALSE(pg_interface->objects[v].state.exists);
  pg_interface->objects[a].state.access = WeaveObjectState::Access::kIdle;
  expect_original(a);
}

TEST_F(WeavePackingReads, OrderedAndMixedRequestsRemainFenced) {
  pack();
  for (auto flag : {CEPH_OSD_FLAG_RWORDERED, CEPH_OSD_FLAG_SKIPRWLOCKS,
                    CEPH_OSD_FLAG_FLUSH}) {
    auto op = request(a, CEPH_OSD_OP_READ, true, flag);
    EXPECT_EQ(controller->preprocess_client_op(op), RequestDisposition::kDeferred);
  }
  auto mixed = request(a, CEPH_OSD_OP_READ, false);
  auto* message = static_cast<MOSDOp*>(mixed->get_nonconst_req());
  message->ops.resize(2);
  message->ops.back().op.op = CEPH_OSD_OP_WRITEFULL;
  ASSERT_EQ(mixed->maybe_init_op_info(OSDMap()), 0);
  EXPECT_EQ(controller->preprocess_client_op(mixed), RequestDisposition::kDeferred);
  drain();
}

TEST_F(WeavePackingReads, PublishedReadsCanRedirectWhileSourceCleanupIsRetrying) {
  pack(); step(); step(); step(); step();
  pg_interface->complete(-EIO);
  pg_interface->read_route = WeaveReadRoute{v, pg_shard_t(1, shard_id_t(0)), 1, eversion_t(1, 1)};
  auto read = request(a, CEPH_OSD_OP_READ);
  auto* message = static_cast<MOSDOp*>(read->get_nonconst_req());
  message->allow_weave_redirect(true);
  EXPECT_EQ(controller->preprocess_client_op(read), RequestDisposition::kReplied);
  EXPECT_EQ(pg_interface->redirects, 1u);
  EXPECT_EQ(message->get_hobj(), a);
  drain();
}

TEST_F(WeavePackingReads, UnsupportedAndSnapshotReadsKeepExistingNativeBarrier) {
  pack();
  auto snap = a; snap.snap = 5;
  auto snapshot = request(snap, CEPH_OSD_OP_READ);
  EXPECT_EQ(controller->preprocess_client_op(snapshot), RequestDisposition::kDeferred);
  auto checksum = request(a, CEPH_OSD_OP_CHECKSUM);
  EXPECT_EQ(controller->preprocess_client_op(checksum), RequestDisposition::kDeferred);
  drain();
}

TEST(WeavePackingLocks, OldReadersHoldDeletionAndWakeQueuedWork) {
  ObjectContext native;
  OpRequestRef no_request;
  ASSERT_TRUE(native.get_read(no_request));
  ASSERT_TRUE(native.get_read(no_request));
  ASSERT_FALSE(native.get_write(no_request));
  EXPECT_FALSE(native.rwstate.empty());
  std::list<OpRequestRef> requeue;
  native.put_read(&requeue);
  EXPECT_FALSE(native.get_write(no_request));
  native.put_read(&requeue);
  ASSERT_TRUE(native.get_write(no_request));
  EXPECT_FALSE(native.get_read(no_request));
  native.put_write(&requeue);
  EXPECT_TRUE(native.rwstate.empty());
}

TEST(WeavePGController, RevalidatesSourcesBeforePublicationAndCancelsOnReset) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  const auto a = oid("a"), b = oid("b"), v = pg_interface.new_volume(a);
  pg_interface.put(a, "AAAA"); pg_interface.put(b, "BBBB");
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  controller.initialize();
  controller.on_commit(pg_interface.inspect(a).info, true, {});
  controller.on_commit(pg_interface.inspect(b).info, true, {});
  controller.scan_candidates();
  ASSERT_EQ(pg_interface.io.size(), 1u);
  pg_interface.complete(); pg_interface.complete();
  pg_interface.objects[a].state.info.version = eversion_t(1, 2);
  pg_interface.run_cpu();
  EXPECT_TRUE(pg_interface.io.empty());
  EXPECT_FALSE(pg_interface.objects[v].state.exists);
  EXPECT_TRUE(pg_interface.objects[a].state.exists); EXPECT_EQ(pg_interface.released, 1u);
  // The restored candidates can run again; a PG reset retires their lease.
  controller.scan_candidates();
  ASSERT_FALSE(pg_interface.io.empty());
  controller.on_pg_change(false);
  EXPECT_EQ(pg_interface.released, 2u);
  pg_interface.complete();
  EXPECT_TRUE(pg_interface.io.empty());
}

TEST(WeavePGController, BusyFirstCandidateDoesNotStarveColdGroup) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  // Size ordering puts the hot object first, independent of hobject hashes.
  pg_interface.put(oid("a-hot"), "HHHHH");
  pg_interface.put(oid("b-cold"), "BBBB");
  pg_interface.put(oid("c-cold"), "CCCC");
  pg_interface.objects[oid("a-hot")].state.access = WeaveObjectState::Access::kBusy;
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  controller.initialize();
  for (const auto& [id, object] : pg_interface.objects)
    controller.on_commit(object.state.info, true, {});
  controller.scan_candidates();
  ASSERT_EQ(pg_interface.io.size(), 1u);
  EXPECT_NE(pg_interface.io.front().oid, oid("a-hot"));
  pg_interface.complete(); pg_interface.complete(); pg_interface.run_cpu();
  pg_interface.complete(); pg_interface.complete(); pg_interface.complete();
  EXPECT_TRUE(pg_interface.objects[oid("a-hot")].state.exists);
  EXPECT_FALSE(pg_interface.objects[oid("b-cold")].state.exists);
  EXPECT_FALSE(pg_interface.objects[oid("c-cold")].state.exists);
  controller.on_pg_change(false);
}

TEST(WeavePGController, ForegroundCommitsOnlyRecordCandidatesForPeriodicScan) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  pg_interface.settings.quiet_seconds = 3600;
  pg_interface.put(oid("a"), "AAAA"); pg_interface.put(oid("b"), "BBBB");
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  controller.initialize();
  controller.on_commit(pg_interface.inspect(oid("a")).info, true, {});
  controller.on_commit(pg_interface.inspect(oid("b")).info, true, {});
  EXPECT_TRUE(pg_interface.retries.empty());
  EXPECT_TRUE(pg_interface.io.empty());

  controller.scan_candidates();
  EXPECT_TRUE(pg_interface.io.empty());
  EXPECT_TRUE(pg_interface.retries.empty());
  pg_interface.settings.quiet_seconds = 0;
  controller.scan_candidates();
  ASSERT_EQ(pg_interface.io.size(), 1u);
  controller.on_pg_change(false);
  pg_interface.complete();
}

TEST(WeavePGController, PeriodicScanPacksAfterCleanWithoutAnotherCommit) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  pg_interface.clean_state = false;
  pg_interface.put(oid("a"), "AAAA"); pg_interface.put(oid("b"), "BBBB");
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  controller.initialize();
  controller.on_commit(pg_interface.inspect(oid("a")).info, true, {});
  controller.on_commit(pg_interface.inspect(oid("b")).info, true, {});
  EXPECT_TRUE(pg_interface.retries.empty());
  controller.scan_candidates();
  EXPECT_TRUE(pg_interface.io.empty());
  pg_interface.clean_state = true;
  controller.scan_candidates();
  ASSERT_EQ(pg_interface.io.size(), 1u);
  controller.on_pg_change(false);
  pg_interface.complete();
}

TEST(WeavePGController, ListingUsesCurrentMemberAttributesAndDeletionIdentity) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  const auto a = oid("a"), b = oid("b"), v = pg_interface.new_volume(a);
  pg_interface.put(v, "AAAABBBB");
  WeaveVolumeMeta layout{v, 2, 4,
    {{a, WeaveMemberMeta{0, 4, {}, 1}}, {b, WeaveMemberMeta{1, 4, {}, 1}}}};
  encode(layout, pg_interface.objects[v].attrs["volume_meta"]);
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  controller.initialize();
  EXPECT_TRUE(controller.is_logical_member(a));
  EXPECT_EQ(controller.listing_attribute(a, "_tag"),
            std::make_pair(v, "_" + xattr_name(a, "tag")));
  EXPECT_EQ(controller.listing_attribute(oid("native"), "_tag"),
            std::make_pair(oid("native"), std::string("_tag")));
  layout.members.erase(a);
  auto& encoded = pg_interface.objects[v].attrs["volume_meta"];
  encoded.clear(); encode(layout, encoded);
  controller.on_recovery_progress();
  EXPECT_FALSE(controller.is_logical_member(a));
  EXPECT_TRUE(controller.is_logical_member(b));
  EXPECT_EQ(controller.listing_attribute(a, "_tag"),
            std::make_pair(a, std::string("_tag")));
}

TEST(WeavePGController, SnapshotAccessAndSnapshotDeleteMaterializeBeforeNativeLookup) {
  for (const bool snapshot_read : {true, false}) {
    auto owner = std::make_unique<FakeWeavePG>();
    auto& pg_interface = *owner;
    const auto a = oid("a"), v = pg_interface.new_volume(a);
    pg_interface.put(v, "AAAA");
    pg_interface.pool_snap_sequence = 7;
    WeaveVolumeMeta layout{v, 2, 4, {{a, WeaveMemberMeta{0, 4, {}, 1, 3}}}};
    encode(layout, pg_interface.objects[v].attrs["volume_meta"]);
    WeavePGController controller(g_ceph_context, std::move(owner), true);
    controller.initialize();
    OpTracker tracker(g_ceph_context, false, 1);
    auto target = a;
    if (snapshot_read) target.snap = 7;
    auto* message = new MOSDOp(0, 1, target, spg_t(), 1,
      snapshot_read ? CEPH_OSD_FLAG_READ : CEPH_OSD_FLAG_WRITE,
      CEPH_FEATURES_SUPPORTED_DEFAULT);
    message->ops.resize(1);
    message->ops[0].op.op = snapshot_read ? CEPH_OSD_OP_STAT : CEPH_OSD_OP_DELETE;
    auto op = tracker.create_request<OpRequest, Message*>(message);
    ASSERT_EQ(op->maybe_init_op_info(OSDMap()), 0);
    EXPECT_EQ(controller.preprocess_client_op(op), RequestDisposition::kDeferred);
    EXPECT_EQ(message->get_hobj(), target);
    ASSERT_EQ(pg_interface.io.size(), 1u);
    EXPECT_EQ(pg_interface.io.front().oid, v);
    controller.on_pg_change(false);
    pg_interface.complete(); op.reset(); tracker.on_shutdown();
  }
}

TEST_F(WeaveConversion, MaterializationRetainsOriginalSnapshotSequence) {
  volume.members.at(a).snap_sequence = 3;
  volume.members.at(b).snap_sequence = 5;
  prepare_unpack();
  EXPECT_EQ(job->copy_snap_sequence(a), std::optional<snapid_t>(3));
  EXPECT_EQ(job->copy_snap_sequence(b), std::optional<snapid_t>(5));
  EXPECT_FALSE(job->copy_snap_sequence(v));
  job->cancel();
  EXPECT_FALSE(job->copy_snap_sequence(a));
}

TEST(WeavePGController, ConcurrentDeletesReadProjectedMetadataAndPublishOnCommit) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  const auto a = oid("a"), b = oid("b"), v = pg_interface.new_volume(a);
  pg_interface.put(v, "AAAABBBB");
  WeaveVolumeMeta layout{v, 2, 4,
    {{a, WeaveMemberMeta{0, 4, {}, 1}}, {b, WeaveMemberMeta{1, 4, {}, 1}}}};
  encode(layout, pg_interface.objects[v].attrs["volume_meta"]);
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  controller.initialize();
  OpTracker tracker(g_ceph_context, false, 1);
  auto request = [&](const hobject_t& id, int opcode) {
    auto* message = new MOSDOp(0, 1, id, spg_t(), 1, CEPH_OSD_FLAG_ONDISK,
                              CEPH_FEATURES_SUPPORTED_DEFAULT);
    message->ops.resize(1); message->ops[0].op.op = opcode;
    auto op = tracker.create_request<OpRequest, Message*>(message);
    EXPECT_EQ(op->maybe_init_op_info(OSDMap()), 0);
    return op;
  };
  auto first = request(a, CEPH_OSD_OP_DELETE);
  auto second = request(b, CEPH_OSD_OP_DELETE);
  ASSERT_EQ(controller.preprocess_client_op(first), RequestDisposition::kTranslated);
  ASSERT_EQ(controller.preprocess_client_op(second), RequestDisposition::kTranslated);
  auto projected = pg_interface.objects[v].attrs["volume_meta"];
  WeaveTransaction txn{
    [&](const char* key, bufferlist& out) { EXPECT_STREQ(key, "_volume_meta"); out = projected; return 0; },
    [&](const char* key, const bufferlist& value) { EXPECT_STREQ(key, "_volume_meta"); projected = value; }};
  ASSERT_EQ(controller.prepare_member_delete(first, txn), 0);
  ASSERT_EQ(controller.prepare_member_delete(second, txn), 0);
  WeaveVolumeMeta after;
  auto p = projected.cbegin(); decode(after, p);
  EXPECT_TRUE(after.members.empty());
  auto before_commit = request(a, CEPH_OSD_OP_STAT);
  EXPECT_EQ(controller.preprocess_client_op(before_commit), RequestDisposition::kTranslated);
  controller.finish_request(before_commit);
  controller.on_commit(pg_interface.inspect(v).info, true, first);
  auto after_first = request(a, CEPH_OSD_OP_STAT);
  EXPECT_EQ(controller.preprocess_client_op(after_first), RequestDisposition::kNative);
  auto still_second = request(b, CEPH_OSD_OP_STAT);
  EXPECT_EQ(controller.preprocess_client_op(still_second), RequestDisposition::kTranslated);
  controller.finish_request(still_second);
  controller.on_commit(pg_interface.inspect(v).info, true, second);
  auto after_second = request(b, CEPH_OSD_OP_STAT);
  EXPECT_EQ(controller.preprocess_client_op(after_second), RequestDisposition::kNative);
  controller.finish_request(first); controller.finish_request(second);
  controller.on_pg_change(false);
  first.reset(); second.reset(); before_commit.reset(); after_first.reset();
  still_second.reset(); after_second.reset();
  tracker.on_shutdown();
}

class WeaveMemberXattrs : public WeaveDurableRecovery {
protected:
  bufferlist projected;

  void SetUp() override {
    WeaveDurableRecovery::SetUp();
    pack();
    drain();
    projected = pg_interface->objects[v].attrs.at(kVolumeMetaAttr);
  }

  OpRequestRef xattr_request(const hobject_t& id, int opcode = CEPH_OSD_OP_SETXATTR,
                            int flags = 0) {
    auto op = request(id, opcode, false, flags);
    auto* message = static_cast<MOSDOp*>(op->get_nonconst_req());
    auto& entry = message->ops.front();
    entry.op.xattr.name_len = 3;
    entry.indata.append("tag");
    if (opcode == CEPH_OSD_OP_SETXATTR) {
      entry.op.xattr.value_len = 3;
      entry.indata.append("new");
    }
    EXPECT_EQ(op->maybe_init_op_info(OSDMap()), 0);
    return op;
  }

  WeaveTransaction transaction() {
    return {
      [this](const char* key, bufferlist& out) {
        EXPECT_STREQ(key, kVolumeMetaXattr);
        out = projected;
        return 0;
      },
      [this](const char* key, const bufferlist& value) {
        EXPECT_STREQ(key, kVolumeMetaXattr);
        projected = value;
      }};
  }

  void stage(const OpRequestRef& op, version_t version) {
    auto txn = transaction();
    ASSERT_EQ(controller->prepare_member_write(op, txn), 0);
    controller->finish_member_write(op, version, utime_t(500, 0), txn);
  }

  uint64_t published_version(const hobject_t& id) {
    auto stat = request(id, CEPH_OSD_OP_STAT);
    EXPECT_EQ(controller->preprocess_client_op(stat), RequestDisposition::kTranslated);
    const auto version = controller->logical_user_version(stat, 0);
    controller->finish_request(stat);
    return version;
  }

  WeaveVolumeMeta projected_metadata() const {
    WeaveVolumeMeta metadata;
    auto p = projected.cbegin();
    decode(metadata, p);
    return metadata;
  }
};

TEST_F(WeaveMemberXattrs, SetAndRemoveStayPackedWithoutConversionIO) {
  const auto acquired = pg_interface->acquired;
  for (auto opcode : {CEPH_OSD_OP_SETXATTR, CEPH_OSD_OP_RMXATTR}) {
    auto op = xattr_request(a, opcode);
    ASSERT_EQ(controller->preprocess_client_op(op), RequestDisposition::kTranslated);
    EXPECT_EQ(op->get_req<MOSDOp>()->get_hobj(), v);
    EXPECT_TRUE(controller->is_logical_member(a));
    EXPECT_TRUE(pg_interface->io.empty());
    EXPECT_TRUE(pg_interface->cpu.empty());
    controller->finish_request(op);
  }
  EXPECT_EQ(pg_interface->acquired, acquired);
}

TEST_F(WeaveMemberXattrs, MemberMetadataPublishesOnCommitAndSurvivesRestart) {
  auto op = xattr_request(a);
  ASSERT_EQ(controller->preprocess_client_op(op), RequestDisposition::kTranslated);
  stage(op, 30);
  EXPECT_EQ(published_version(a), 7u);
  const auto metadata = projected_metadata();
  EXPECT_EQ(metadata.members.at(a).user_version, 30u);
  EXPECT_EQ(metadata.members.at(a).mtime, utime_t(500, 0));
  EXPECT_EQ(metadata.members.at(b).user_version, 7u);
  EXPECT_EQ(metadata.members.at(b).mtime, utime_t(123, 456));

  // Model durable transaction completion, separately from callback delivery.
  pg_interface->objects[v].attrs[kVolumeMetaAttr] = projected;
  controller->on_commit(pg_interface->inspect(v).info, true, op);
  EXPECT_EQ(published_version(a), 30u);
  restart(false);
  EXPECT_EQ(published_version(a), 30u);
  EXPECT_EQ(published_version(b), 7u);
  EXPECT_EQ(pg_interface->objects[v].data.to_str(), "AAAABBBB");
}

TEST_F(WeaveMemberXattrs, QueuedWritesRefreshMetadataAndCommitOnlyTheirOwnMember) {
  auto first = xattr_request(a);
  auto second = xattr_request(b, CEPH_OSD_OP_RMXATTR);
  ASSERT_EQ(controller->preprocess_client_op(first), RequestDisposition::kTranslated);
  ASSERT_EQ(controller->preprocess_client_op(second), RequestDisposition::kTranslated);
  stage(first, 30);
  stage(second, 40);
  const auto metadata = projected_metadata();
  EXPECT_EQ(metadata.members.at(a).user_version, 30u);
  EXPECT_EQ(metadata.members.at(b).user_version, 40u);

  // Even a late callback must merge its delta rather than an older snapshot.
  controller->on_commit(pg_interface->inspect(v).info, true, second);
  EXPECT_EQ(published_version(a), 7u);
  EXPECT_EQ(published_version(b), 40u);
  controller->on_commit(pg_interface->inspect(v).info, true, first);
  EXPECT_EQ(published_version(a), 30u);
  EXPECT_EQ(published_version(b), 40u);
}

TEST_F(WeaveMemberXattrs, RestartDiscardsUncommittedMetadata) {
  auto op = xattr_request(a);
  ASSERT_EQ(controller->preprocess_client_op(op), RequestDisposition::kTranslated);
  stage(op, 30);
  restart(false);
  EXPECT_EQ(published_version(a), 7u);
  EXPECT_EQ(published_version(b), 7u);
}

TEST_F(WeaveMemberXattrs, DurableMetadataRecoversWithoutCompletionCallback) {
  auto op = xattr_request(a);
  ASSERT_EQ(controller->preprocess_client_op(op), RequestDisposition::kTranslated);
  stage(op, 30);
  pg_interface->objects[v].attrs[kVolumeMetaAttr] = projected;
  restart(false);
  EXPECT_EQ(published_version(a), 30u);

  auto write = request(a, CEPH_OSD_OP_WRITEFULL);
  ASSERT_EQ(controller->preprocess_client_op(write), RequestDisposition::kDeferred);
  drain();
  EXPECT_FALSE(controller->is_logical_member(a));
  EXPECT_EQ(pg_interface->objects[a].state.info.user_version, 30u);
  EXPECT_EQ(pg_interface->objects[a].state.info.mtime, utime_t(500, 0));
  EXPECT_EQ(pg_interface->objects[a].state.snap_sequence, 3u);
}

TEST_F(WeaveMemberXattrs, PoolSnapshotsKeepNativeCopyOnWrite) {
  pg_interface->pool_snap_sequence = 9;
  auto op = xattr_request(a);
  EXPECT_EQ(controller->preprocess_client_op(op), RequestDisposition::kDeferred);
  EXPECT_FALSE(op->is_weave_member_op());
  EXPECT_FALSE(pg_interface->io.empty());
  drain();
  EXPECT_EQ(controller->preprocess_client_op(op), RequestDisposition::kNative);
}

TEST_F(WeaveMemberXattrs, ClientSnapshotsKeepNativeCopyOnWrite) {
  auto op = xattr_request(a);
  static_cast<MOSDOp*>(op->get_nonconst_req())->set_snap_seq(9);
  EXPECT_EQ(controller->preprocess_client_op(op), RequestDisposition::kDeferred);
  EXPECT_FALSE(op->is_weave_member_op());
  drain();
  EXPECT_EQ(controller->preprocess_client_op(op), RequestDisposition::kNative);
}

TEST_F(WeaveMemberXattrs, MutationsCannotSkipTheVolumeLock) {
  auto op = xattr_request(a, CEPH_OSD_OP_SETXATTR, CEPH_OSD_FLAG_SKIPRWLOCKS);
  EXPECT_EQ(controller->preprocess_client_op(op), RequestDisposition::kDeferred);
  EXPECT_FALSE(op->is_weave_member_op());
  drain();
}

TEST_F(WeaveDurableRecovery, MaterializationRetryIsIndependentOfScansAndCleanup) {
  pack(); drain();
  pg_interface->allow_acquire = false;
  auto write = request(a, CEPH_OSD_OP_WRITEFULL);
  ASSERT_EQ(controller->preprocess_client_op(write), RequestDisposition::kDeferred);
  EXPECT_EQ(pg_interface->retries.count(WeaveRetryKind::kMaterialization), 1u);

  auto pass = std::make_shared<WeaveReclaimPass>();
  std::weak_ptr<WeaveReclaimPass> pending_cleanup = pass;
  controller->request_cleanup(100, std::move(pass));
  EXPECT_EQ(pg_interface->retries.count(WeaveRetryKind::kCleanup), 1u);
  pg_interface->settings.background = true;
  controller->scan_candidates();
  EXPECT_TRUE(pg_interface->io.empty());
  EXPECT_EQ(pg_interface->retries.count(WeaveRetryKind::kMaterialization), 1u);

  const auto requeued = pg_interface->requeued;
  pg_interface->allow_acquire = true;
  pg_interface->tick(WeaveRetryKind::kMaterialization);
  EXPECT_EQ(pg_interface->requeued, requeued + 1);
  ASSERT_EQ(controller->preprocess_client_op(write), RequestDisposition::kDeferred);
  pg_interface->tick(WeaveRetryKind::kCleanup); // The running job owns the next continuation.
  drain();
  EXPECT_FALSE(controller->is_logical_member(a));
  EXPECT_EQ(pg_interface->requeued, requeued + 2);
  EXPECT_FALSE(pending_cleanup.expired());
  pg_interface->tick(WeaveRetryKind::kCleanup);
  EXPECT_TRUE(pending_cleanup.expired());
}

TEST_F(WeaveDurableRecovery, CleanupRetriesItsOwnPassWithoutCandidateScan) {
  pack(); drain();
  pg_interface->allow_acquire = false;
  auto pass = std::make_shared<WeaveReclaimPass>();
  std::weak_ptr<WeaveReclaimPass> pending_cleanup = pass;
  controller->request_cleanup(100, std::move(pass));
  EXPECT_FALSE(pending_cleanup.expired());
  EXPECT_EQ(pg_interface->retries.count(WeaveRetryKind::kCleanup), 1u);
  pg_interface->allow_acquire = true;
  controller->scan_candidates();
  EXPECT_TRUE(pg_interface->io.empty());
  pg_interface->tick(WeaveRetryKind::kCleanup);
  ASSERT_FALSE(pg_interface->io.empty());
  drain();
  EXPECT_TRUE(pending_cleanup.expired());
  EXPECT_FALSE(controller->is_logical_member(a));
  EXPECT_TRUE(pg_interface->retries.empty());
}

TEST_F(WeaveDurableRecovery, RejectedAndEmptyCleanupReleasePassImmediately) {
  auto rejected = std::make_shared<WeaveReclaimPass>();
  std::weak_ptr<WeaveReclaimPass> pending_rejected = rejected;
  pg_interface->primary_role = false;
  controller->request_cleanup(100, std::move(rejected));
  EXPECT_TRUE(pending_rejected.expired());

  auto empty = std::make_shared<WeaveReclaimPass>();
  std::weak_ptr<WeaveReclaimPass> pending_empty = empty;
  pg_interface->primary_role = true;
  controller->request_cleanup(100, std::move(empty));
  EXPECT_TRUE(pending_empty.expired());
  EXPECT_TRUE(pg_interface->io.empty());
  EXPECT_TRUE(pg_interface->retries.empty());
}

TEST_F(WeaveDurableRecovery, PGChangeReleasesCleanupAndRejectsStaleRetry) {
  pack(); drain();
  pg_interface->allow_acquire = false;
  auto pass = std::make_shared<WeaveReclaimPass>();
  std::weak_ptr<WeaveReclaimPass> pending_cleanup = pass;
  controller->request_cleanup(100, std::move(pass));
  ASSERT_FALSE(pending_cleanup.expired());
  ASSERT_EQ(pg_interface->retries.count(WeaveRetryKind::kCleanup), 1u);
  auto stale_retry = pg_interface->retries.at(WeaveRetryKind::kCleanup);

  controller->on_pg_change(false);
  ++pg_interface->generation;
  EXPECT_TRUE(pending_cleanup.expired());

  // An old queued retry must not advance the new PG generation's pass.
  auto next = std::make_shared<WeaveReclaimPass>();
  std::weak_ptr<WeaveReclaimPass> pending_next = next;
  controller->request_cleanup(100, std::move(next));
  ASSERT_FALSE(pending_next.expired());
  pg_interface->allow_acquire = true;
  stale_retry();
  EXPECT_TRUE(pg_interface->io.empty());
  EXPECT_FALSE(pending_next.expired());
  pg_interface->tick(WeaveRetryKind::kCleanup);
  ASSERT_FALSE(pg_interface->io.empty());
  drain();
  EXPECT_TRUE(pending_next.expired());
}

TEST(WeaveService, ReclaimPassStaysActiveUntilEveryPGReleasesReference) {
  WeaveService service(g_ceph_context);
  using Result = WeaveService::ReclaimResult;
  auto submit = [&](WeaveService::Dispatch dispatch) {
    return service.request_reclaim(37, [dispatch = std::move(dispatch)] { return dispatch; });
  };
  std::promise<WeaveReclaimPass::Ref> dispatched;
  EXPECT_EQ(submit([&](unsigned percent, WeaveReclaimPass::Ref pass) {
    EXPECT_EQ(percent, 37u);
    dispatched.set_value(std::move(pass));
  }), Result::kAccepted);
  auto future = dispatched.get_future();
  ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  auto first = future.get();
  auto second = first;
  std::promise<void> drained;
  service.post([&] { drained.set_value(); });
  ASSERT_EQ(drained.get_future().wait_for(std::chrono::seconds(5)), std::future_status::ready);
  first.reset();
  EXPECT_EQ(submit({}), Result::kAlreadyRunning);
  second.reset();
  EXPECT_EQ(submit([](unsigned, WeaveReclaimPass::Ref) {}), Result::kAccepted);
  service.shutdown();
  EXPECT_EQ(submit({}), Result::kStopping);
}
TEST(WeaveService, ConversionLeaseCanOnlyReleaseItsSlotOnce) {
  WeaveService service(g_ceph_context);
  const spg_t pgid;
  auto old = service.acquire(pgid);
  ASSERT_NE(old, nullptr);
  EXPECT_EQ(service.acquire(pgid), nullptr);
  old->reset();
  auto current = service.acquire(pgid);
  ASSERT_NE(current, nullptr);
  old->reset(); old.reset();
  EXPECT_EQ(service.acquire(pgid), nullptr);
  current.reset();
  EXPECT_NE(service.acquire(pgid), nullptr);
}
} // namespace

namespace {
class WeaveReadRouterTest : public ::testing::Test {
protected:
  FakeWeavePG pg_interface;
  WeaveCatalog catalog;
  WeaveMemberTranslator translator{g_ceph_context, catalog};
  WeaveReadRouter router{pg_interface, translator};
  OpTracker tracker{g_ceph_context, false, 1};
  hobject_t a = oid("a"), b = oid("b"), v = oid("volume");
  WeaveVolumeMeta metadata;
  OpRequestRef op;
  void SetUp() override {
    v.nspace = ".ceph-internal-weave";
    metadata = {v, 2, 4, {{a, {0, 3, {}, 11}}, {b, {1, 4, {}, 12}}}};
    catalog.upsert(metadata);
    translator.activate(2, 4);
    pg_interface.read_route = WeaveReadRoute{v, pg_shard_t(1, shard_id_t(0)), 7, eversion_t(7, 9)};
    encode(metadata, pg_interface.read_metadata);
    auto* m = new MOSDOp(0, 17, a, spg_t(), 7, CEPH_OSD_FLAG_READ, CEPH_FEATURES_SUPPORTED_DEFAULT);
    m->allow_weave_redirect(true);
    m->read(1, 8);
    op = tracker.create_request<OpRequest, Message*>(m);
    ASSERT_EQ(op->maybe_init_op_info(OSDMap()), 0);
  }
  MOSDOp* message() { return static_cast<MOSDOp*>(op->get_nonconst_req()); }
  void TearDown() override {
    translator.finish_request(op);
    op.reset(); tracker.on_shutdown();
  }
};

TEST_F(WeaveReadRouterTest, RedirectRestoresOriginalRequestAndReplicaRetranslates) {
  ASSERT_EQ(translator.preprocess(op), 0);
  ASSERT_TRUE(router.redirect(op));
  EXPECT_EQ(message()->get_hobj(), a);
  EXPECT_EQ(message()->ops.front().op.extent.length, 8u);
  EXPECT_EQ(pg_interface.redirects, 1u);
  message()->set_weave_read_route(*pg_interface.read_route);
  catalog.clear(); // replicas do not need the primary's catalog
  ASSERT_EQ(router.accept(op), 0);
  EXPECT_EQ(message()->get_hobj(), v);
  EXPECT_EQ(message()->ops.front().op.extent.offset, 1u);
  EXPECT_EQ(message()->ops.front().op.extent.length, 2u);
  EXPECT_EQ(translator.logical_user_version(op, 99), 11u);
}

TEST_F(WeaveReadRouterTest, RejectsStaleRouteRemovedMemberAndWrongTarget) {
  message()->set_weave_read_route(*pg_interface.read_route);
  pg_interface.read_route_result = -EAGAIN;
  EXPECT_EQ(router.accept(op), -EAGAIN);
  pg_interface.read_route_result = 0;
  pg_interface.read_route->target = pg_shard_t(2, shard_id_t(1));
  EXPECT_EQ(router.accept(op), -EAGAIN);
  pg_interface.read_route->target = pg_shard_t(1, shard_id_t(0));
  metadata.members.erase(a);
  pg_interface.read_metadata.clear(); encode(metadata, pg_interface.read_metadata);
  EXPECT_EQ(router.accept(op), -EAGAIN);
  EXPECT_EQ(message()->get_hobj(), a);
  EXPECT_FALSE(op->is_weave_member_op());
}

TEST_F(WeaveReadRouterTest, RejectsMalformedMetadataAndUnrelatedPhysicalObject) {
  message()->set_weave_read_route(*pg_interface.read_route);
  pg_interface.read_metadata.clear(); pg_interface.read_metadata.append("broken");
  EXPECT_EQ(router.accept(op), -EAGAIN);
  pg_interface.read_metadata.clear(); encode(metadata, pg_interface.read_metadata);
  pg_interface.read_route->volume.nspace.clear();
  message()->set_weave_read_route(*pg_interface.read_route);
  EXPECT_EQ(router.accept(op), -EAGAIN);
}

TEST_F(WeaveReadRouterTest, FallbackDoesNotRedirectAgain) {
  message()->allow_weave_redirect(false);
  ASSERT_EQ(translator.preprocess(op), 0);
  EXPECT_FALSE(router.redirect(op));
  EXPECT_EQ(pg_interface.redirects, 0u);
}
} // namespace

TEST(WeavePGController, ReplicaPromotionReloadsCatalogAfterServingDirectReads) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  pg_interface.primary_role = false;
  auto a = oid("a"), v = oid("volume");
  v.nspace = ".ceph-internal-weave";
  WeaveVolumeMeta metadata{v, 2, 4, {{a, {0, 3, {}, 11}}}};
  pg_interface.put(v, "AAAABBBB");
  encode(metadata, pg_interface.objects[v].attrs["volume_meta"]);
  encode(metadata, pg_interface.read_metadata);
  pg_interface.read_route = WeaveReadRoute{v, pg_shard_t(1, shard_id_t(0)), 7, eversion_t(7, 9)};
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  OpTracker tracker(g_ceph_context, false, 1);
  auto request = [&] {
    auto* m = new MOSDOp(0, 17, a, spg_t(), 7, CEPH_OSD_FLAG_READ, CEPH_FEATURES_SUPPORTED_DEFAULT);
    m->read(0, 3);
    auto op = tracker.create_request<OpRequest, Message*>(m);
    EXPECT_EQ(op->maybe_init_op_info(OSDMap()), 0);
    return op;
  };
  auto direct = request();
  static_cast<MOSDOp*>(direct->get_nonconst_req())->set_weave_read_route(*pg_interface.read_route);
  EXPECT_EQ(controller.preprocess_client_op(direct), RequestDisposition::kTranslated);
  controller.finish_request(direct);
  controller.on_pg_change(false);
  pg_interface.primary_role = true;
  controller.initialize();
  auto primary = request();
  EXPECT_EQ(controller.preprocess_client_op(primary), RequestDisposition::kTranslated);
  EXPECT_EQ(primary->get_req<MOSDOp>()->get_hobj(), v);
  controller.finish_request(primary);
  controller.on_pg_change(false);
  direct.reset(); primary.reset(); tracker.on_shutdown();
}

TEST(WeavePGController, RequeuedMembersRetainLogicalCapabilities) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  auto a = oid("allowed-member"), v = oid("volume");
  a.nspace = "user";
  v.nspace = ".ceph-internal-weave";
  WeaveVolumeMeta metadata{v, 2, 4, {{a, {0, 3, {}, 11}}}};
  pg_interface.put(v, "AAAABBBB");
  encode(metadata, pg_interface.objects[v].attrs["volume_meta"]);
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  controller.initialize();
  OSDCap caps;
  ASSERT_TRUE(caps.parse("allow r pool=pool namespace=user object_prefix allowed-"));
  OpTracker tracker(g_ceph_context, false, 1);
  auto* message = new MOSDOp(0, 17, a, spg_t(), 7, CEPH_OSD_FLAG_READ,
                            CEPH_FEATURES_SUPPORTED_DEFAULT);
  message->read(0, 3);
  auto op = tracker.create_request<OpRequest, Message*>(message);
  ASSERT_EQ(op->maybe_init_op_info(OSDMap()), 0);
  auto permitted = [&] {
    const auto& id = message->get_hobj();
    const auto& key = id.get_key().empty() ? id.oid.name : id.get_key();
    return caps.is_capable("pool", id.nspace, {}, key,
                           true, false, {}, entity_addr_t());
  };
  for (unsigned attempt = 0; attempt < 2; ++attempt) {
    EXPECT_EQ(controller.prepare_request(op), RequestDisposition::kNative);
    EXPECT_TRUE(permitted()) << "logical capability check on attempt " << attempt;
    EXPECT_EQ(controller.preprocess_client_op(op), RequestDisposition::kTranslated);
    EXPECT_FALSE(permitted()); // The client has no access to the physical Volume.
  }
  controller.finish_request(op);
  controller.on_pg_change(false);
  op.reset();
  tracker.on_shutdown();
}

TEST(WeavePGController, FailedCatalogLoadRejectsRequestsUntilReloadSucceeds) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  auto a = oid("a"), v = oid("volume");
  v.nspace = ".ceph-internal-weave";
  WeaveVolumeMeta metadata{v, 2, 4, {{a, {0, 3, {}, 11}}}};
  pg_interface.put(v, "AAAABBBB");
  encode(metadata, pg_interface.objects[v].attrs["volume_meta"]);
  pg_interface.metadata_result = -EIO;
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  controller.initialize();
  OpTracker tracker(g_ceph_context, false, 1);
  auto* message = new MOSDOp(0, 17, a, spg_t(), 7, CEPH_OSD_FLAG_READ,
                            CEPH_FEATURES_SUPPORTED_DEFAULT);
  message->read(0, 3);
  auto op = tracker.create_request<OpRequest, Message*>(message);
  ASSERT_EQ(op->maybe_init_op_info(OSDMap()), 0);
  EXPECT_EQ(controller.prepare_request(op), RequestDisposition::kRejected);
  EXPECT_EQ(pg_interface.last_error, -EIO);
  pg_interface.metadata_result = 0;
  controller.initialize();
  EXPECT_EQ(controller.prepare_request(op), RequestDisposition::kNative);
  EXPECT_EQ(controller.preprocess_client_op(op), RequestDisposition::kTranslated);
  EXPECT_EQ(controller.logical_user_version(op, 99), 11u);
  controller.finish_request(op);
  controller.on_pg_change(false);
  op.reset();
  tracker.on_shutdown();
}

TEST(WeavePGController, MissingCatalogDefersReadsWritesAndListingUntilRecovery) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  pg_interface.missing = true;
  auto a = oid("a"), v = oid("volume");
  v.nspace = ".ceph-internal-weave";
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  controller.initialize();
  OpTracker tracker(g_ceph_context, false, 1);
  std::vector<OpRequestRef> requests;
  for (auto opcode : {CEPH_OSD_OP_READ, CEPH_OSD_OP_WRITEFULL, CEPH_OSD_OP_PGLS}) {
    auto* message = new MOSDOp(0, 17, a, spg_t(), 7,
      opcode == CEPH_OSD_OP_WRITEFULL ? CEPH_OSD_FLAG_WRITE : CEPH_OSD_FLAG_READ,
      CEPH_FEATURES_SUPPORTED_DEFAULT);
    message->ops.resize(1);
    message->ops.front().op.op = opcode;
    auto op = tracker.create_request<OpRequest, Message*>(message);
    EXPECT_EQ(op->maybe_init_op_info(OSDMap()), 0);
    EXPECT_EQ(controller.prepare_request(op), RequestDisposition::kDeferred);
    requests.push_back(std::move(op));
  }
  pg_interface.put(v, "AAAABBBB");
  WeaveVolumeMeta metadata{v, 2, 4, {{a, {0, 3, {}, 11}}}};
  encode(metadata, pg_interface.objects[v].attrs["volume_meta"]);
  pg_interface.missing = false;
  pg_interface.metadata_result = -EIO;
  controller.on_recovery_progress();
  EXPECT_EQ(pg_interface.last_error, -EIO);
  pg_interface.metadata_result = 0;
  controller.initialize();
  EXPECT_EQ(controller.prepare_request(requests.front()), RequestDisposition::kNative);
  EXPECT_EQ(controller.preprocess_client_op(requests.front()), RequestDisposition::kTranslated);
  EXPECT_EQ(controller.logical_user_version(requests.front(), 99), 11u);
  std::vector<hobject_t> listed;
  hobject_t next = hobject_t::get_max();
  controller.merge_listing(hobject_t(), 10, listed, next);
  EXPECT_EQ(listed, std::vector<hobject_t>{a});
  controller.finish_request(requests.front());
  controller.on_pg_change(false);
  requests.clear();
  tracker.on_shutdown();
}

TEST(WeavePGController, TruncateHistoryBeforePublicationKeepsNativeSources) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  auto a = oid("a"), b = oid("b");
  pg_interface.put(a, "AAAA");
  pg_interface.put(b, "BBBB");
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  controller.initialize();
  controller.on_commit(pg_interface.inspect(a).info, true, {});
  controller.on_commit(pg_interface.inspect(b).info, true, {});
  controller.scan_candidates();
  ASSERT_FALSE(pg_interface.io.empty());
  pg_interface.complete();
  pg_interface.complete();
  pg_interface.objects[a].state.info.truncate_seq = 1;
  pg_interface.objects[a].state.info.truncate_size = 4;
  pg_interface.run_cpu();
  while (!pg_interface.io.empty()) pg_interface.complete();
  EXPECT_TRUE(pg_interface.inspect(a).exists);
  EXPECT_TRUE(pg_interface.inspect(b).exists);
  EXPECT_EQ(pg_interface.objects[a].data.to_str(), "AAAA");
  EXPECT_EQ(pg_interface.objects[b].data.to_str(), "BBBB");
  OpTracker tracker(g_ceph_context, false, 1);
  auto* message = new MOSDOp(0, 17, a, spg_t(), 7, CEPH_OSD_FLAG_READ,
                            CEPH_FEATURES_SUPPORTED_DEFAULT);
  message->read(0, 4);
  auto op = tracker.create_request<OpRequest, Message*>(message);
  ASSERT_EQ(op->maybe_init_op_info(OSDMap()), 0);
  EXPECT_EQ(controller.preprocess_client_op(op), RequestDisposition::kNative);
  controller.finish_request(op);
  controller.on_pg_change(false);
  op.reset();
  tracker.on_shutdown();
}

TEST(WeavePGController, UserVolumeAttributeCannotPublishMemberMapping) {
  auto owner = std::make_unique<FakeWeavePG>();
  auto& pg_interface = *owner;
  auto forged = oid("ordinary"), logical = oid("claimed-member"), volume = oid("victim");
  volume.nspace = ".ceph-internal-weave";
  pg_interface.put(forged, "user-data");
  pg_interface.put(volume, "PRIVATE!");
  WeaveVolumeMeta metadata{volume, 2, 4, {{logical, {0, 4, {}, 11}}}};
  encode(metadata, pg_interface.objects[forged].attrs["volume_meta"]);
  WeavePGController controller(g_ceph_context, std::move(owner), true);
  controller.initialize();
  OpTracker tracker(g_ceph_context, false, 1);
  auto* message = new MOSDOp(0, 17, logical, spg_t(), 7, CEPH_OSD_FLAG_READ,
                            CEPH_FEATURES_SUPPORTED_DEFAULT);
  message->read(0, 4);
  auto op = tracker.create_request<OpRequest, Message*>(message);
  ASSERT_EQ(op->maybe_init_op_info(OSDMap()), 0);
  EXPECT_EQ(controller.prepare_request(op), RequestDisposition::kNative);
  EXPECT_EQ(controller.preprocess_client_op(op), RequestDisposition::kNative);
  std::vector<hobject_t> listed{forged};
  hobject_t next = hobject_t::get_max();
  controller.merge_listing(hobject_t(), 10, listed, next);
  EXPECT_EQ(listed, std::vector<hobject_t>{forged});
  controller.finish_request(op);
  controller.on_pg_change(false);
  op.reset();
  tracker.on_shutdown();
}

TEST(WeavePGController, CorruptPrivateMetadataFailsClosedAndCanBeReloaded) {
  for (bool wrong_source : {false, true}) {
    SCOPED_TRACE(wrong_source);
    auto owner = std::make_unique<FakeWeavePG>();
    auto& pg_interface = *owner;
    auto logical = oid("a"), volume = pg_interface.new_volume(logical);
    pg_interface.put(volume, "AAAABBBB");
    WeaveVolumeMeta metadata{volume, 2, 4, {{logical, {0, 4, {}, 11}}}};
    if (wrong_source) {
      metadata.volume_oid.oid.name = "different-volume";
      encode(metadata, pg_interface.objects[volume].attrs["volume_meta"]);
      metadata.volume_oid = volume;
    } else {
      pg_interface.objects[volume].attrs["volume_meta"].append("broken");
    }
    WeavePGController controller(g_ceph_context, std::move(owner), true);
    controller.initialize();
    OpTracker tracker(g_ceph_context, false, 1);
    auto* message = new MOSDOp(0, 17, logical, spg_t(), 7, CEPH_OSD_FLAG_READ,
                              CEPH_FEATURES_SUPPORTED_DEFAULT);
    message->read(0, 4);
    auto op = tracker.create_request<OpRequest, Message*>(message);
    ASSERT_EQ(op->maybe_init_op_info(OSDMap()), 0);
    EXPECT_EQ(controller.prepare_request(op), RequestDisposition::kRejected);
    EXPECT_EQ(pg_interface.last_error, -EIO);
    auto& encoded = pg_interface.objects[volume].attrs["volume_meta"];
    encoded.clear();
    encode(metadata, encoded);
    controller.initialize();
    EXPECT_EQ(controller.prepare_request(op), RequestDisposition::kNative);
    EXPECT_EQ(controller.preprocess_client_op(op), RequestDisposition::kTranslated);
    EXPECT_EQ(controller.logical_user_version(op, 99), 11u);
    controller.finish_request(op);
    controller.on_pg_change(false);
    op.reset();
    tracker.on_shutdown();
  }
}
