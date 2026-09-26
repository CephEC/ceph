// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "gtest/gtest.h"

#include "common/TrackedOp.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "osd/ECUtil.h"
#include "osd/ECBackend.h"
#include "osd/OpRequest.h"
#include "osd/ClassHandler.h"
#include "osd/weave/detail/WeaveMemberTranslator.h"
#include "osd/weave/detail/WeaveScheduler.h"
#include "osd/weave/detail/WeaveCandidateIndex.h"
#include "osd/weave/WeaveECAdapter.h"
#include "osd/weave/detail/WeaveRequestContext.h"
#include "osd/weave/detail/WeaveLayout.h"
#include "osd/weave/detail/WeaveCatalog.h"
#include "osdc/WeaveReadSession.h"
#include "test/unit.cc"

#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <set>
#include <sstream>

using namespace ceph::weave;

namespace {
constexpr uint64_t UNIT = 4096;

hobject_t object(const std::string& name) {
  return hobject_t(sobject_t(object_t(name), CEPH_NOSNAP));
}

object_info_t candidate_info(const std::string& name, uint64_t size,
                             uint64_t version = 1) {
  object_info_t info;
  info.soid = object(name);
  info.size = size;
  info.version = eversion_t(1, version);
  info.user_version = version;
  info.mtime = utime_t(version, 0);
  return info;
}

WeaveVolumeMeta metadata(const hobject_t& volume_oid, const hobject_t& first,
                    const hobject_t& second) {
  return {volume_oid, 2, UNIT,
    {{first, WeaveMemberMeta{0, 100, utime_t(1, 0), 11}},
     {second, WeaveMemberMeta{1, 200, utime_t(2, 0), 22}}}};
}

struct TestTracker {
  TestTracker() : tracker(g_ceph_context, false, 1) {}
  ~TestTracker() { tracker.on_shutdown(); }
  OpTracker tracker;
};

MOSDOp* write_request(const hobject_t& oid, const std::string& value,
                     uint64_t offset = 0, bool full = true) {
  auto* message = new MOSDOp(
    0, 1, oid, spg_t(), 1, CEPH_OSD_FLAG_ONDISK,
    CEPH_FEATURES_SUPPORTED_DEFAULT);
  message->ops.resize(1);
  auto& op = message->ops.front();
  op.op.op = full ? CEPH_OSD_OP_WRITEFULL : CEPH_OSD_OP_WRITE;
  op.op.extent.offset = offset;
  op.op.extent.length = value.size();
  op.indata.append(value);
  return message;
}

class EncodedLayout : public ::testing::Test {
protected:
  void SetUp() override {
    ErasureCodeProfile profile{{"k", "2"}, {"m", "1"},
                              {"technique", "reed_sol_van"}};
    std::ostringstream errors;
    ASSERT_EQ(0, ErasureCodePluginRegistry::instance().factory(
      "jerasure", g_conf().get_val<std::string>("erasure_code_dir"),
      profile, &ec, &errors)) << errors.str();
  }

  std::map<int, bufferlist> encode_image(const std::string& image) {
    bufferlist input;
    input.append(image);
    std::map<int, bufferlist> result;
    EXPECT_EQ(0, ECUtil::encode(stripe, ec, input, {0, 1, 2}, &result));
    return result;
  }

  ECUtil::stripe_info_t stripe{2, 2 * UNIT};
  ceph::ErasureCodeInterfaceRef ec;
};
} // anonymous namespace

TEST(WeaveCandidateIndex, SkipsLargeOutliersAndGroupsSmallerObjects) {
  WeaveCandidateIndex index;
  const ceph::mono_time now{};
  const std::vector<uint64_t> sizes{48, 48, 48, 20, 19, 18, 17};
  for (size_t i = 0; i < sizes.size(); ++i)
    index.upsert(candidate_info(std::to_string(i), sizes[i]), now);

  auto selected = index.select(4, 1, 1, 0, 1024, 10, now);
  std::vector<hobject_t> members;
  for (const auto& entry : selected) members.push_back(entry.oid);
  EXPECT_EQ(members, (std::vector<hobject_t>{
    object("3"), object("4"), object("5"), object("6")}));
  for (const auto& entry : selected) index.erase(entry.oid);
  EXPECT_TRUE(index.select(4, 1, 1, 0, 1024, 10, now).empty());
}

TEST(WeaveCandidateIndex, AccountsForStripeUnitRounding) {
  WeaveCandidateIndex index;
  const ceph::mono_time now{};
  index.upsert(candidate_info("first", 16), now);
  index.upsert(candidate_info("second", 14), now);
  ASSERT_EQ(index.select(2, 8, 1, 0, 1024, 10, now).size(), 2u);

  index.upsert(candidate_info("second", 17, 2), now);
  EXPECT_TRUE(index.select(2, 8, 1, 0, 1024, 10, now).empty());
}

TEST(WeaveCandidateIndex, NewVersionRestartsQuietPeriodAndStaleUpdateCannotReplaceIt) {
  WeaveCandidateIndex index;
  const ceph::mono_time start{};
  index.upsert(candidate_info("a", 20), start);
  index.upsert(candidate_info("b", 20), start);
  index.upsert(candidate_info("a", 20, 2), start + std::chrono::seconds(20));
  index.upsert(candidate_info("a", 100, 1), start + std::chrono::seconds(25));

  EXPECT_TRUE(index.select(2, 1, 1, 30, 1024, 10,
    start + std::chrono::seconds(49)).empty());
  ASSERT_EQ(index.select(2, 1, 1, 30, 1024, 10,
    start + std::chrono::seconds(50)).size(), 2u);
  index.erase(object("b"));
  EXPECT_TRUE(index.select(2, 1, 1, 30, 1024, 10,
    start + std::chrono::seconds(60)).empty());
}

TEST(WeaveCandidateIndex, ExcludesEitherNativeTruncateFieldFromSelection) {
  WeaveCandidateIndex index;
  const ceph::mono_time now{};
  auto sequence = candidate_info("a-sequence", 20);
  sequence.truncate_seq = 1;
  index.upsert(sequence, now);
  auto size = candidate_info("b-size", 20);
  size.truncate_size = 10;
  index.upsert(size, now);
  index.upsert(candidate_info("c-normal", 20), now);
  index.upsert(candidate_info("d-normal", 20), now);

  std::set<hobject_t> members;
  for (const auto& entry : index.select(2, 1, 1, 0, 1024, 10, now))
    members.insert(entry.oid);
  EXPECT_EQ(members, (std::set<hobject_t>{
    object("c-normal"), object("d-normal")}));
}

TEST(WeaveCandidateIndex, TruncateHistoryRemovesCandidateUntilCleared) {
  WeaveCandidateIndex index;
  const ceph::mono_time now{};
  index.upsert(candidate_info("a", 20), now);
  index.upsert(candidate_info("b", 20, 2), now);
  auto selected = [&] {
    std::set<hobject_t> members;
    for (const auto& entry : index.select(2, 1, 1, 0, 1024, 10, now))
      members.insert(entry.oid);
    return members;
  };
  ASSERT_EQ(selected(), (std::set<hobject_t>{object("a"), object("b")}));

  auto truncated = candidate_info("a", 20, 2);
  truncated.truncate_seq = 1;
  truncated.truncate_size = 10;
  index.upsert(truncated, now);
  EXPECT_TRUE(selected().empty());

  // An older commit must not disqualify a newer eligible version.
  auto stale = candidate_info("b", 20, 1);
  stale.truncate_seq = 1;
  index.upsert(stale, now);
  EXPECT_TRUE(selected().empty());

  index.upsert(candidate_info("a", 20, 3), now);
  EXPECT_EQ(selected(), (std::set<hobject_t>{object("a"), object("b")}));
}

TEST(WeaveCandidateIndex, RejectsOverflowingVolumeGeometry) {
  WeaveCandidateIndex index;
  const ceph::mono_time now{};
  for (unsigned i = 0; i < 4; ++i)
    index.upsert(candidate_info(std::to_string(i),
      std::numeric_limits<uint64_t>::max() - i), now);
  EXPECT_TRUE(index.select(4, UNIT, 1, 0,
    std::numeric_limits<uint64_t>::max(), 10, now).empty());
}

TEST(WeaveCandidateIndex, AdmissionAndRetentionStayBounded) {
  WeaveCandidateIndex index;
  const ceph::mono_time now{};
  index.configure(4, 4096, 1 << 20, 64 << 20);
  for (unsigned i = 0; i < 100000; ++i) {
    index.upsert(candidate_info("small-" + std::to_string(i), 4096), now);
    index.upsert(candidate_info("large-" + std::to_string(i), 17 << 20), now);
  }
  EXPECT_TRUE(index.empty());
  for (unsigned i = 0; i < 10000; ++i)
    index.upsert(candidate_info("eligible-" + std::to_string(i), 1 << 20), now);
  EXPECT_EQ(index.size(), WeaveCandidateIndex::kMaxCandidates);
  auto selected = index.select(4, 4096, 1 << 20, 0, 64 << 20, 10, now);
  ASSERT_EQ(selected.size(), 4u);
  for (const auto& item : selected) index.erase(item.oid);
  index.upsert(candidate_info("new", 1 << 20), now);
  EXPECT_EQ(index.size(), WeaveCandidateIndex::kMaxCandidates - 3);
}

TEST(WeaveCandidateIndex, PolicyChangesPruneAndLaterCommitsRediscover) {
  WeaveCandidateIndex index;
  const ceph::mono_time now{};
  index.configure(2, 4, 1, 32);
  index.upsert(candidate_info("a", 8), now);
  index.upsert(candidate_info("b", 16), now);
  index.configure(2, 4, 9, 32);
  ASSERT_EQ(index.size(), 1u);
  index.configure(2, 4, 1, 16);
  EXPECT_TRUE(index.empty());
  index.upsert(candidate_info("a", 8, 2), now);
  EXPECT_EQ(index.size(), 1u);
  index.configure(2, 4, 1, 15); // rounding leaves only a four-byte slot
  EXPECT_TRUE(index.empty());
}

TEST(WeaveCatalog, SnapshotSequenceSurvivesReload) {
  const auto first = object("first"), second = object("second");
  auto layout = metadata(object("volume"), first, second);
  layout.members.at(first).snap_sequence = 17;
  layout.members.at(second).snap_sequence = 29;
  bufferlist encoded;
  encode(layout, encoded);
  WeaveCatalog catalog;
  ASSERT_EQ(catalog.load_from_disk(layout.volume_oid, encoded), 0);
  EXPECT_EQ(catalog.lookup(first)->members.at(first).snap_sequence, 17u);
  EXPECT_EQ(catalog.lookup(second)->members.at(second).snap_sequence, 29u);
}

TEST(WeaveECAdapter, RejectedClassReturnsExecutionNodeError) {
  const auto previous = g_conf().get_val<std::string>("osd_class_load_list");
  g_conf().set_val("osd_class_load_list", "hello");
  bufferlist encoded, input, output;
  const std::string name = "weave_denied_regression", method = "missing";
  encoded.append(name); encoded.append(method);
  ClsParmContext context(name.size(), method.size(), 0, 0, encoded);
  const int result = WeaveECAdapter::execute_data_class(context, input, output);
  g_conf().set_val("osd_class_load_list", previous.c_str());
  EXPECT_EQ(result, -EPERM);
  EXPECT_EQ(output.length(), 0u);
}

TEST_F(EncodedLayout, CompleteMembersStayOnOneShardAcrossStripesAndRecover) {
  const uint64_t slot_size = 3 * UNIT;
  const std::string first = std::string(UNIT, 'a') +
    std::string(UNIT, 'b') + std::string(UNIT - 17, 'c');
  const std::string second = std::string(UNIT, '1') +
    std::string(UNIT, '2') + std::string(UNIT - 101, '3');
  std::vector<bufferlist> members(2);
  members[0].append(first);
  members[1].append(second);
  bufferlist image;
  ASSERT_TRUE(interleave_members(members, UNIT, slot_size, image));
  auto encoded = encode_image(image.to_str());
  ASSERT_EQ(encoded.at(0).to_str(), first + std::string(17, '\0'));
  ASSERT_EQ(encoded.at(1).to_str(), second + std::string(101, '\0'));

  encoded.erase(0);
  bufferlist restored;
  WeaveECAdapter backend(stripe, ec);
  ASSERT_EQ(0, backend.decode_member(member_read_flags(0, 0), encoded, restored));
  EXPECT_EQ(restored.to_str(), first + std::string(17, '\0'));
}

TEST_F(EncodedLayout, ReadCompletionCanStartAnotherReadExactlyOnce) {
  ObjectStore::CollectionHandle collection;
  ECBackend backend(nullptr, coll_t(), collection, nullptr, g_ceph_context,
                    ec, 2 * UNIT);
  std::map<hobject_t, std::list<boost::tuple<uint64_t, uint64_t, uint32_t>>> reads;
  using Results = std::map<hobject_t, std::pair<int, extent_map>>;
  unsigned first = 0, second = 0;
  // Empty reads exercise immediate completion without a PG or ObjectStore.
  backend.objects_read_and_reconstruct(reads, false,
    make_gen_lambda_context<Results&&>([&](Results&&) {
      ++first;
      backend.objects_read_and_reconstruct(reads, false,
        make_gen_lambda_context<Results&&>([&](Results&&) { ++second; }));
    }));
  EXPECT_EQ(first, 1u);
  EXPECT_EQ(second, 1u);
}

TEST(WeaveMemberTranslator, OrdinaryWritesLargerThanOneUnitDoNotWaitForAggregation) {
  TestTracker tracking;
  WeaveCatalog catalog;
  WeaveMemberTranslator translator(g_ceph_context, catalog);
  translator.activate(4, UNIT);
  auto* message = write_request(object("ordinary"), std::string(8 * UNIT, 'a'));
  auto request = tracking.tracker.create_request<OpRequest, Message*>(message);
  EXPECT_EQ(translator.preprocess(request), WeaveMemberTranslator::kPreprocessContinue);
  EXPECT_FALSE(request->is_weave_member_op());
}

TEST(WeaveCatalog, ReplacingCompleteGroupRemovesPreviousMember) {
  const auto volume_oid = object("volume");
  const auto first = object("first");
  const auto second = object("second");
  const auto third = object("third");
  auto layout = metadata(volume_oid, first, second);
  WeaveCatalog catalog;
  catalog.upsert(layout);
  catalog.upsert(metadata(volume_oid, first, third));
  EXPECT_NE(catalog.lookup(first), nullptr);
  EXPECT_EQ(catalog.lookup(second), nullptr);
  EXPECT_NE(catalog.lookup(third), nullptr);
}

TEST(WeaveCatalog, RemappingObjectDetachesItFromOldVolume) {
  const auto first_volume = object("volume-1");
  const auto second_volume = object("volume-2");
  const auto member = object("member");
  const auto old_peer = object("old-peer");
  const auto new_peer = object("new-peer");
  WeaveCatalog catalog;
  catalog.upsert(metadata(first_volume, member, old_peer));
  const auto pinned = catalog.lookup_volume(first_volume);
  catalog.upsert(metadata(second_volume, member, new_peer));
  const auto detached = catalog.lookup_volume(first_volume);
  ASSERT_NE(detached, nullptr);
  EXPECT_EQ(detached->members.count(member), 0u);
  EXPECT_EQ(detached->members.at(old_peer).shard, 1u);
  EXPECT_EQ(pinned->members.count(member), 1u);

  // Delayed deletion of the former member cannot erase its new mapping.
  catalog.remove_member(first_volume, member);
  auto result = catalog.lookup(member);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->volume_oid, second_volume);
  // Persisting the old Volume cannot resurrect members moved to another one.
  bufferlist encoded;
  detached->encode(encoded);
  WeaveCatalog restored;
  ASSERT_EQ(restored.load_from_disk(first_volume, encoded), 0);
  EXPECT_EQ(restored.lookup(member), nullptr);
  EXPECT_NE(restored.lookup(old_peer), nullptr);

  catalog.remove_member(first_volume, old_peer);
  ASSERT_NE(catalog.lookup_volume(first_volume), nullptr);
  EXPECT_TRUE(catalog.lookup_volume(first_volume)->members.empty());
  catalog.remove_volume(first_volume);
  EXPECT_EQ(catalog.lookup_volume(first_volume), nullptr);
  ASSERT_NE(catalog.lookup(member), nullptr);
  EXPECT_EQ(catalog.lookup(member)->volume_oid, second_volume);
  EXPECT_EQ(catalog.lookup(old_peer), nullptr);
  EXPECT_NE(catalog.lookup(new_peer), nullptr);
}

TEST(WeaveCatalog, SparseAndEmptyVolumesReloadWithoutChangingPinnedReaders) {
  const auto volume_oid = object("volume");
  const auto first = object("first");
  const auto second = object("second");
  WeaveCatalog catalog;
  catalog.upsert(metadata(volume_oid, first, second));
  const auto pinned = catalog.lookup(first);
  catalog.remove_member(volume_oid, first);
  EXPECT_EQ(catalog.lookup(first), nullptr);
  const auto sparse = catalog.lookup(second);
  ASSERT_NE(sparse, nullptr);
  EXPECT_EQ(sparse->data_shards, 2u);
  EXPECT_EQ(sparse->slot_size, UNIT);
  EXPECT_EQ(sparse->members.at(second).shard, 1u);
  EXPECT_EQ(pinned->members.at(first).user_version, 11u);
  EXPECT_EQ(pinned->members.at(second).user_version, 22u);

  bufferlist encoded;
  sparse->encode(encoded);
  // The format version is part of the durable contract: bumping it silently
  // would strand every Volume already on disk, so pin what this codec writes.
  ASSERT_GE(encoded.length(), 2u);
  auto header = encoded.cbegin();
  char struct_v = 0, struct_compat = 0;
  header.copy(1, &struct_v);
  header.copy(1, &struct_compat);
  EXPECT_EQ(struct_v, 1);
  EXPECT_EQ(struct_compat, 1);
  WeaveCatalog restored;
  ASSERT_EQ(restored.load_from_disk(volume_oid, encoded), 0);
  EXPECT_EQ(restored.lookup(first), nullptr);
  const auto reloaded = restored.lookup(second);
  ASSERT_NE(reloaded, nullptr);
  EXPECT_EQ(reloaded->data_shards, 2u);
  EXPECT_EQ(reloaded->slot_size, UNIT);
  EXPECT_EQ(reloaded->members.at(second).shard, 1u);
  EXPECT_EQ(reloaded->members.at(second).user_version, 22u);
  std::optional<hobject_t> next;
  EXPECT_EQ(restored.list_objects(hobject_t(), 10, next),
            (std::vector<hobject_t>{second}));

  restored.remove_member(volume_oid, second);
  EXPECT_EQ(restored.lookup(second), nullptr);
  const auto empty = restored.lookup_volume(volume_oid);
  ASSERT_NE(empty, nullptr);
  EXPECT_TRUE(empty->members.empty());
  EXPECT_EQ(empty->data_shards, 2u);
  EXPECT_EQ(empty->slot_size, UNIT);
  // Pinned sparse and full metadata remain valid across later deletion.
  EXPECT_EQ(reloaded->members.at(second).user_version, 22u);
  EXPECT_EQ(sparse->members.count(second), 1u);
  EXPECT_EQ(pinned->members.count(first), 1u);

  bufferlist empty_encoded;
  empty->encode(empty_encoded);
  std::vector<std::pair<hobject_t, bufferlist>> snapshot{{volume_oid, empty_encoded}};
  ASSERT_EQ(restored.replace_from_disk(snapshot), 0);
  EXPECT_TRUE(restored.list_objects(hobject_t(), 10, next).empty());
  const auto volumes = restored.list_volumes();
  ASSERT_EQ(volumes.size(), 1u);
  EXPECT_EQ(volumes.front()->volume_oid, volume_oid);
  EXPECT_TRUE(volumes.front()->members.empty());
  EXPECT_EQ(volumes.front()->data_shards, 2u);
  EXPECT_EQ(volumes.front()->slot_size, UNIT);
}

TEST(WeaveCatalog, PaginationDoesNotOmitOrRepeatMembers) {
  const auto volume_oid = object("volume");
  WeaveVolumeMeta layout{volume_oid, 3, UNIT, {}};
  std::set<hobject_t> expected;
  for (uint8_t i = 0; i < 3; ++i) {
    const auto oid = object("member-" + std::to_string(i));
    layout.members.emplace(oid, WeaveMemberMeta{i, 100, utime_t(1, 0)});
    expected.insert(oid);
  }
  WeaveCatalog catalog;
  catalog.upsert(layout);
  std::optional<hobject_t> next;
  auto first = catalog.list_objects(hobject_t(), 2, next);
  ASSERT_TRUE(next);
  std::optional<hobject_t> final_next;
  auto second = catalog.list_objects(*next, 2, final_next);
  std::set<hobject_t> actual(first.begin(), first.end());
  for (const auto& oid : second) EXPECT_TRUE(actual.insert(oid).second);
  EXPECT_EQ(actual, expected);
  EXPECT_FALSE(final_next);
}

TEST(WeaveMemberTranslator, ReloadedSparseMetadataClampsReadAndPreservesLogicalStat) {
  const auto volume_oid = object("volume");
  const auto first = object("first");
  const auto second = object("second");
  bufferlist encoded;
  auto layout = metadata(volume_oid, first, second);
  layout.members.erase(first);
  layout.encode(encoded);
  std::vector<std::pair<hobject_t, bufferlist>> snapshot{{volume_oid, encoded}};
  WeaveCatalog catalog;
  WeaveMemberTranslator translator(g_ceph_context, catalog);
  translator.activate(2, UNIT);
  catalog.replace_from_disk(snapshot);

  TestTracker tracking;
  auto* message = new MOSDOp(
    0, 1, second, spg_t(), 1, CEPH_OSD_FLAG_ONDISK,
    CEPH_FEATURES_SUPPORTED_DEFAULT);
  message->ops.resize(2);
  message->ops[0].op.op = CEPH_OSD_OP_READ;
  message->ops[0].op.extent.offset = 150;
  message->ops[0].op.extent.length = 100;
  message->ops[1].op.op = CEPH_OSD_OP_STAT;
  auto request = tracking.tracker.create_request<OpRequest, Message*>(message);
  // Native class requests also own a context, but have no published mapping.
  request->ensure_weave_context();
  EXPECT_FALSE(request->is_weave_member_op());
  ASSERT_EQ(translator.preprocess(request), WeaveMemberTranslator::kPreprocessContinue);
  EXPECT_TRUE(request->is_weave_member_op());
  EXPECT_EQ(message->get_hobj(), volume_oid);
  EXPECT_EQ(uint64_t(message->ops[0].op.extent.offset), 150u);
  EXPECT_EQ(uint64_t(message->ops[0].op.extent.length), 50u);
  EXPECT_TRUE(is_member_read(message->ops[0].op.flags));
  EXPECT_EQ(member_id(message->ops[0].op.flags), 1u);
  uint64_t size;
  utime_t mtime;
  EXPECT_EQ(message->ops[1].outdata.length(), 0u);
  ASSERT_TRUE(translator.encode_logical_stat(request, message->ops[1].outdata));
  auto p = message->ops[1].outdata.cbegin();
  decode(size, p);
  decode(mtime, p);
  EXPECT_EQ(size, 200u);
  EXPECT_EQ(mtime, utime_t(2, 0));
  translator.finish_request(request);
  EXPECT_FALSE(request->is_weave_member_op());
}

TEST(WeaveMemberTranslator, FinalDeleteTranslatesWithoutPublishingDeletion) {
  const auto volume_oid = object("volume");
  const auto first = object("first");
  const auto second = object("second");
  WeaveCatalog catalog;
  WeaveMemberTranslator translator(g_ceph_context, catalog);
  translator.activate(2, UNIT);
  catalog.upsert(metadata(volume_oid, first, second));
  TestTracker tracking;
  auto* message = new MOSDOp(
    0, 1, second, spg_t(), 1, CEPH_OSD_FLAG_ONDISK,
    CEPH_FEATURES_SUPPORTED_DEFAULT);
  message->ops.resize(4);
  message->ops[0].op.op = CEPH_OSD_OP_ASSERT_VER;
  message->ops[0].op.assert_ver.ver = 22;
  message->ops[1].op.op = CEPH_OSD_OP_READ;
  message->ops[1].op.extent.offset = 150;
  message->ops[1].op.extent.length = 100;
  message->ops[2].op.op = CEPH_OSD_OP_STAT;
  message->ops[3].op.op = CEPH_OSD_OP_DELETE;
  auto request = tracking.tracker.create_request<OpRequest, Message*>(message);
  ASSERT_TRUE(translator.supports_member_ops(message->ops));
  ASSERT_EQ(translator.preprocess(request), WeaveMemberTranslator::kPreprocessContinue);
  EXPECT_TRUE(request->is_weave_member_op());
  EXPECT_EQ(message->get_hobj(), volume_oid);
  EXPECT_EQ(uint16_t(message->ops[0].op.op), CEPH_OSD_OP_ASSERT_VER);
  EXPECT_EQ(uint64_t(message->ops[0].op.assert_ver.ver), 22u);
  EXPECT_EQ(uint64_t(message->ops[1].op.extent.length), 50u);
  EXPECT_EQ(member_id(message->ops[1].op.flags), 1u);
  EXPECT_EQ(uint16_t(message->ops[3].op.op), CEPH_OSD_OP_DELETE);
  ASSERT_NE(catalog.lookup(second), nullptr);
  EXPECT_EQ(catalog.lookup(second)->members.at(second).user_version, 22u);
  translator.finish_request(request);
  EXPECT_FALSE(request->is_weave_member_op());
  EXPECT_NE(catalog.lookup(second), nullptr);
}

TEST(WeaveMemberTranslator, DeferredDeleteRestoresPayloadAndResolvesCurrentLocation) {
  const auto member = object("member");
  const auto peer = object("peer");
  const auto old_volume = object("old-volume");
  const auto new_volume = object("new-volume");
  WeaveCatalog catalog;
  WeaveMemberTranslator translator(g_ceph_context, catalog);
  translator.activate(2, UNIT);
  catalog.upsert(metadata(old_volume, member, peer));
  TestTracker tracking;
  auto* message = new MOSDOp(
    0, 1, member, spg_t(), 1, CEPH_OSD_FLAG_ONDISK,
    CEPH_FEATURES_SUPPORTED_DEFAULT);
  message->ops.resize(2);
  message->ops[0].op.op = CEPH_OSD_OP_CMPXATTR;
  message->ops[0].op.xattr.name_len = 3;
  message->ops[0].op.xattr.value_len = 5;
  message->ops[0].indata.append("keyvalue");
  message->ops[1].op.op = CEPH_OSD_OP_DELETE;
  auto request = tracking.tracker.create_request<OpRequest, Message*>(message);
  ASSERT_EQ(translator.preprocess(request), WeaveMemberTranslator::kPreprocessContinue);
  request->ensure_weave_context().mark_member_deleted();
  translator.finish_request(request);
  EXPECT_EQ(message->get_hobj(), member);
  EXPECT_EQ(message->ops[0].indata.to_str(), "keyvalue");
  EXPECT_EQ(uint32_t(message->ops[0].op.xattr.name_len), 3u);
  EXPECT_FALSE(request->is_weave_member_op());
  catalog.remove_volume(old_volume);
  catalog.upsert(metadata(new_volume, member, peer));
  ASSERT_EQ(translator.preprocess(request), WeaveMemberTranslator::kPreprocessContinue);
  EXPECT_EQ(message->get_hobj(), new_volume);
  EXPECT_FALSE(request->get_weave_context()->member_deleted());
  translator.finish_request(request);
  catalog.remove_volume(new_volume);
  ASSERT_EQ(translator.preprocess(request), WeaveMemberTranslator::kPreprocessContinue);
  EXPECT_EQ(message->get_hobj(), member);
  EXPECT_EQ(message->ops[0].indata.to_str(), "keyvalue");
  EXPECT_FALSE(request->is_weave_member_op());
}

TEST(WeaveMemberTranslator, StandaloneDeleteKeepsVolumeUntilCommittedRemoval) {
  const auto volume_oid = object("volume");
  const auto member = object("member");
  auto layout = metadata(volume_oid, object("deleted"), member);
  layout.members.erase(object("deleted"));
  WeaveCatalog catalog;
  WeaveMemberTranslator translator(g_ceph_context, catalog);
  translator.activate(2, UNIT);
  catalog.upsert(layout);
  const auto pinned = catalog.lookup(member);
  TestTracker tracking;
  auto* message = new MOSDOp(
    0, 1, member, spg_t(), 1, CEPH_OSD_FLAG_ONDISK,
    CEPH_FEATURES_SUPPORTED_DEFAULT);
  message->ops.resize(1);
  message->ops[0].op.op = CEPH_OSD_OP_DELETE;
  auto request = tracking.tracker.create_request<OpRequest, Message*>(message);
  ASSERT_EQ(translator.preprocess(request), WeaveMemberTranslator::kPreprocessContinue);
  EXPECT_TRUE(request->is_weave_member_op());
  EXPECT_EQ(message->get_hobj(), volume_oid);
  EXPECT_EQ(uint16_t(message->ops[0].op.op), CEPH_OSD_OP_DELETE);
  EXPECT_NE(catalog.lookup(member), nullptr);
  catalog.remove_member(volume_oid, member);
  EXPECT_EQ(catalog.lookup(member), nullptr);
  ASSERT_NE(catalog.lookup_volume(volume_oid), nullptr);
  EXPECT_TRUE(catalog.lookup_volume(volume_oid)->members.empty());
  const auto volumes = catalog.list_volumes();
  ASSERT_EQ(volumes.size(), 1u);
  EXPECT_EQ(volumes.front()->volume_oid, volume_oid);
  EXPECT_TRUE(volumes.front()->members.empty());
  EXPECT_EQ(pinned->members.at(member).shard, 1u);
}

TEST(WeaveMemberTranslator, NonfinalDeleteAndWriteCompoundsUseNativeFallback) {
  const auto member = object("member");
  WeaveCatalog catalog;
  WeaveMemberTranslator translator(g_ceph_context, catalog);
  translator.activate(2, UNIT);
  catalog.upsert(metadata(object("volume"), member, object("peer")));
  TestTracker tracking;
  auto* message = new MOSDOp(
    0, 1, member, spg_t(), 1, CEPH_OSD_FLAG_ONDISK,
    CEPH_FEATURES_SUPPORTED_DEFAULT);
  message->ops.resize(2);
  message->ops[0].op.op = CEPH_OSD_OP_DELETE;
  message->ops[1].op.op = CEPH_OSD_OP_STAT;
  auto request = tracking.tracker.create_request<OpRequest, Message*>(message);
  EXPECT_FALSE(translator.supports_member_ops(message->ops));
  EXPECT_EQ(translator.preprocess(request), -EOPNOTSUPP);
  EXPECT_FALSE(request->is_weave_member_op());
  EXPECT_EQ(message->get_hobj(), member);
  message->ops[0].op.op = CEPH_OSD_OP_WRITEFULL;
  message->ops[1].op.op = CEPH_OSD_OP_DELETE;
  EXPECT_FALSE(translator.supports_member_ops(message->ops));
  EXPECT_EQ(translator.preprocess(request), -EOPNOTSUPP);
  EXPECT_FALSE(request->is_weave_member_op());
  EXPECT_EQ(message->get_hobj(), member);
  EXPECT_NE(catalog.lookup(member), nullptr);
}

TEST(WeaveCatalog, RejectsOverlappingAndOutOfRangeShardAssignments) {
  const auto volume_oid = object("volume");
  const auto first = object("first");
  const auto second = object("second");
  auto overlapping = metadata(volume_oid, first, second);
  overlapping.members.at(second).shard = 0;
  auto out_of_range = metadata(volume_oid, first, second);
  out_of_range.members.at(second).shard = 2;
  WeaveCatalog catalog;
  for (const auto* invalid : {&overlapping, &out_of_range}) {
    bufferlist encoded;
    invalid->encode(encoded);
    EXPECT_EQ(catalog.load_from_disk(volume_oid, encoded), -EINVAL);
  }
  EXPECT_EQ(catalog.lookup(first), nullptr);
  EXPECT_EQ(catalog.lookup(second), nullptr);
}

TEST(WeaveCatalog, RejectsMemberAndVolumeSizeOverflow) {
  const auto volume_oid = object("volume");
  const auto first = object("first");
  const auto second = object("second");
  auto oversized_member = metadata(volume_oid, first, second);
  oversized_member.members.at(second).size = UNIT + 1;
  auto overflowing_volume = metadata(volume_oid, first, second);
  overflowing_volume.slot_size = std::numeric_limits<uint64_t>::max() / 2 + 1;
  WeaveCatalog catalog;
  for (const auto* invalid : {&oversized_member, &overflowing_volume}) {
    bufferlist encoded;
    invalid->encode(encoded);
    EXPECT_EQ(catalog.load_from_disk(volume_oid, encoded), -EINVAL);
  }
  EXPECT_EQ(catalog.lookup(second), nullptr);
}

TEST(WeaveCatalog, RejectsDuplicateLogicalObjectsBeforePublishing) {
  const auto volume_oid = object("volume");
  const auto member = object("member");
  bufferlist encoded;
  ENCODE_START(1, 1, encoded);
  encode(volume_oid, encoded);
  encode(uint32_t{2}, encoded);
  encode(UNIT, encoded);
  encode(uint32_t{2}, encoded);
  for (uint8_t shard = 0; shard < 2; ++shard) {
    encode(member, encoded);
    encode(WeaveMemberMeta{shard, 100, utime_t(1, 0)}, encoded);
  }
  ENCODE_FINISH(encoded);
  WeaveCatalog catalog;
  EXPECT_EQ(catalog.load_from_disk(volume_oid, encoded), -EINVAL);
  EXPECT_EQ(catalog.lookup(member), nullptr);
}

TEST(WeaveCatalog, RejectsForeignCodecVersionAndTrailingData) {
  const auto volume_oid = object("volume");
  const auto first = object("first");
  const auto second = object("second");
  auto layout = metadata(volume_oid, first, second);

  // The v1 field layout under any other struct version belongs to a different
  // codec: {2,2} is caught by the compat floor, {3,1} only by the explicit
  // version check. Neither may publish a mapping.
  const std::vector<std::pair<uint8_t, uint8_t>> foreign_versions{
    {2, 2}, {3, 1}, {4, 4}, {4, 1}};
  for (const auto& [version, compat] : foreign_versions) {
    bufferlist foreign;
    ENCODE_START(version, compat, foreign);
    encode(layout.volume_oid, foreign);
    encode(layout.data_shards, foreign);
    encode(layout.slot_size, foreign);
    encode(layout.members, foreign);
    ENCODE_FINISH(foreign);
    WeaveCatalog catalog;
    EXPECT_EQ(catalog.load_from_disk(volume_oid, foreign), -EINVAL);
    EXPECT_EQ(catalog.lookup(first), nullptr);
  }

  bufferlist trailing;
  layout.encode(trailing);
  trailing.append("extra");
  WeaveCatalog catalog;
  EXPECT_EQ(catalog.load_from_disk(volume_oid, trailing), -EINVAL);
  EXPECT_EQ(catalog.lookup(first), nullptr);
}

TEST(WeaveCatalog, RejectsTruncatedMemberWithoutReplacingPublishedVolume) {
  const auto volume_oid = object("volume");
  const auto first = object("first");
  const auto second = object("second");
  WeaveCatalog catalog;
  catalog.upsert(metadata(volume_oid, first, second));

  // The header claims two members but the payload stops after the first: the
  // attribute is incomplete, so the published mapping must stay untouched.
  const auto layout = metadata(volume_oid, first, second);
  bufferlist encoded;
  ENCODE_START(1, 1, encoded);
  encode(layout.volume_oid, encoded);
  encode(layout.data_shards, encoded);
  encode(layout.slot_size, encoded);
  encode(uint32_t(layout.members.size()), encoded);
  const auto& member = layout.members.at(first);
  encode(first, encoded);
  encode(member, encoded);
  ENCODE_FINISH(encoded);
  EXPECT_EQ(catalog.load_from_disk(volume_oid, encoded), -EINVAL);

  ASSERT_NE(catalog.lookup(first), nullptr);
  EXPECT_EQ(catalog.lookup(first)->members.at(first).user_version, 11u);
}

TEST(WeaveCatalog, RejectsInvalidEmptyGeometryAndExcessMembers) {
  const auto volume_oid = object("volume");
  auto no_shards = WeaveVolumeMeta{volume_oid, 0, UNIT, {}};
  auto no_slot = WeaveVolumeMeta{volume_oid, 2, 0, {}};
  auto too_many_shards = WeaveVolumeMeta{volume_oid, 257, UNIT, {}};
  auto too_many_members = metadata(volume_oid, object("first"), object("second"));
  too_many_members.members.emplace(object("third"), WeaveMemberMeta{0, 100, {}});
  WeaveCatalog catalog;
  for (const auto* invalid :
       {&no_shards, &no_slot, &too_many_shards, &too_many_members}) {
    bufferlist encoded;
    invalid->encode(encoded);
    EXPECT_EQ(catalog.load_from_disk(volume_oid, encoded), -EINVAL);
  }
  EXPECT_EQ(catalog.lookup_volume(volume_oid), nullptr);
}

TEST(WeaveCatalog, KeepsAuthoritativeMappingsBeyond4096Objects) {
  WeaveCatalog catalog;
  for (uint32_t i = 0; i < 4100; ++i) {
    const auto volume_oid = object("volume-" + std::to_string(i));
    const auto member = object("member-" + std::to_string(i));
    const auto peer = object("peer-" + std::to_string(i));
    catalog.upsert(metadata(volume_oid, member, peer));
  }
  EXPECT_NE(catalog.lookup(object("member-0")), nullptr);
  EXPECT_NE(catalog.lookup(object("member-4099")), nullptr);
}

TEST(WeaveCatalog, ReplacingSnapshotDropsMissingVolumes) {
  WeaveCatalog catalog;
  const auto old_volume = object("old-volume");
  const auto old_member = object("old-member");
  const auto new_volume = object("new-volume");
  const auto new_member = object("new-member");
  catalog.upsert(metadata(old_volume, old_member, object("old-peer")));
  bufferlist encoded;
  metadata(new_volume, new_member, object("new-peer")).encode(encoded);
  std::vector<std::pair<hobject_t, bufferlist>> snapshot{
    {new_volume, std::move(encoded)}};
  ASSERT_EQ(catalog.replace_from_disk(snapshot), 0);
  EXPECT_EQ(catalog.lookup(old_member), nullptr);
  EXPECT_NE(catalog.lookup(new_member), nullptr);
}

TEST(WeaveCatalog, ConflictingDiskOwnershipRejectsBothScanOrdersAtomically) {
  const auto member = object("member");
  const auto old_volume = object("old-volume");
  bufferlist first, second;
  metadata(object("volume-a"), member, object("peer-a")).encode(first);
  metadata(object("volume-b"), member, object("peer-b")).encode(second);
  for (const bool reverse : {false, true}) {
    WeaveCatalog catalog;
    catalog.upsert(metadata(old_volume, object("old-member"), object("old-peer")));
    std::vector<std::pair<hobject_t, bufferlist>> snapshot{
      {object("volume-a"), first}, {object("volume-b"), second}};
    if (reverse) std::reverse(snapshot.begin(), snapshot.end());
    EXPECT_EQ(catalog.replace_from_disk(snapshot), -EEXIST);
    EXPECT_NE(catalog.lookup_volume(old_volume), nullptr);
    EXPECT_FALSE(catalog.contains(member));
  }
}

TEST(WeaveCatalog, MismatchedDiskIdentityRejectsBothScanOrdersAtomically) {
  const auto old_volume = object("old-volume");
  const auto new_volume = object("new-volume");
  const auto new_member = object("new-member");
  bufferlist valid, mismatched;
  metadata(new_volume, new_member, object("new-peer")).encode(valid);
  metadata(object("claimed-volume"), object("member"), object("peer")).encode(mismatched);
  for (const bool reverse : {false, true}) {
    WeaveCatalog catalog;
    catalog.upsert(metadata(old_volume, object("old-member"), object("old-peer")));
    const auto original = catalog.lookup_volume(old_volume);
    std::vector<std::pair<hobject_t, bufferlist>> snapshot{
      {new_volume, valid}, {object("actual-volume"), mismatched}};
    if (reverse) std::reverse(snapshot.begin(), snapshot.end());
    EXPECT_EQ(catalog.replace_from_disk(snapshot), -EINVAL);
    EXPECT_EQ(catalog.lookup_volume(old_volume), original);
    EXPECT_FALSE(catalog.contains(new_member));
    EXPECT_FALSE(catalog.contains(object("member")));
  }
}

TEST(WeaveCatalog, DamagedDiskSnapshotDoesNotPublishPartialMappings) {
  WeaveCatalog catalog;
  const auto original = metadata(object("old-volume"), object("old-member"), object("old-peer"));
  catalog.upsert(original);
  bufferlist valid, broken;
  metadata(object("new-volume"), object("new-member"), object("new-peer")).encode(valid);
  broken.append("broken");
  std::vector<std::pair<hobject_t, bufferlist>> snapshot{
    {object("new-volume"), valid}, {object("broken-volume"), broken}};
  EXPECT_EQ(catalog.replace_from_disk(snapshot), -EINVAL);
  EXPECT_NE(catalog.lookup_volume(original.volume_oid), nullptr);
  EXPECT_FALSE(catalog.contains(object("new-member")));
}

TEST(WeaveScheduler, NewCandidatesDoNotPostponeAnEarlierPositiveDeadline) {
  WeaveScheduler scheduler(g_ceph_context);
  std::promise<void> entered;
  std::promise<void> release;
  auto release_future = release.get_future().share();
  scheduler.post([&] {
    entered.set_value();
    release_future.wait();
  });
  const auto ready = entered.get_future().wait_for(std::chrono::seconds(5));
  if (ready != std::future_status::ready) {
    release.set_value();
    FAIL() << "scheduler worker did not start";
  }
  std::atomic<bool> superseded = false;
  std::promise<void> scanned;
  auto scanned_future = scanned.get_future();
  const spg_t pgid(pg_t(1, 7), shard_id_t(0));
  scheduler.schedule(pgid, 0.01, [&] { superseded = true; });
  scheduler.schedule(pgid, 60, [&] { scanned.set_value(); });
  release.set_value();
  EXPECT_EQ(scanned_future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  scheduler.shutdown();
  EXPECT_FALSE(superseded);
}

TEST(WeaveReadSession, AllowsOneDetourAndFallsBackOnMapChange) {
  WeaveReadSession request;
  WeaveReadRoute route{object("volume"), pg_shard_t(8, shard_id_t(1)), 7, eversion_t(7, 9)};
  EXPECT_TRUE(request.may_redirect());
  ASSERT_TRUE(request.redirect(route));
  EXPECT_FALSE(request.may_redirect());
  EXPECT_FALSE(request.refresh(7, {3, 8, 9}, 3));
  ASSERT_TRUE(request.route());
  EXPECT_TRUE(request.refresh(8, {3, 8, 9}, 3));
  EXPECT_FALSE(request.route());
  EXPECT_FALSE(request.redirect(route));
}

TEST(WeaveReadSession, RejectsWrongShardAndNeverRedirectsAfterFailure) {
  for (auto target : {pg_shard_t(3, shard_id_t(0)), pg_shard_t(8, shard_id_t(4)),
                       pg_shard_t(9, shard_id_t(1)), pg_shard_t(8, shard_id_t::NO_SHARD)}) {
    WeaveReadSession request;
    ASSERT_TRUE(request.redirect({object("volume"), target, 7, eversion_t(7, 9)}));
    EXPECT_TRUE(request.refresh(7, {3, 8, 9}, 3));
    EXPECT_FALSE(request.route());
  }
  WeaveReadSession request;
  request.fallback();
  EXPECT_FALSE(request.may_redirect());
}

TEST(WeaveReadWire, PreservesLogicalRequestAndClassArguments) {
  const auto logical = object("logical");
  auto source = ceph::make_message<MOSDOp>(0, 17, logical, spg_t(pg_t(1, 2), shard_id_t(1)),
    7, CEPH_OSD_FLAG_READ, CEPH_FEATURES_SUPPORTED_DEFAULT);
  source->allow_weave_redirect(false);
  source->set_weave_read_route({object("volume"), pg_shard_t(8, shard_id_t(1)), 7, eversion_t(7, 9)});
  source->ops.resize(1);
  auto& op = source->ops.front();
  op.op.op = CEPH_OSD_OP_CALL;
  op.op.cls.class_len = 3; op.op.cls.method_len = 4; op.op.cls.indata_len = 3;
  op.indata.append("clsmetharg");
  source->set_retry_attempt(1);
  bufferlist wire;
  encode_message(source.get(), CEPH_FEATURES_SUPPORTED_DEFAULT, wire);
  auto p = wire.cbegin();
  auto decoded = boost::intrusive_ptr<MOSDOp>(static_cast<MOSDOp*>(decode_message(g_ceph_context, 0, p)), false);
  ASSERT_TRUE(decoded);
  decoded->finish_decode();
  ASSERT_TRUE(decoded->get_weave_read_route());
  EXPECT_EQ(decoded->get_hobj().oid, logical.oid);
  EXPECT_EQ(decoded->get_weave_read_route()->version, eversion_t(7, 9));
  EXPECT_EQ(decoded->ops.front().op.op, CEPH_OSD_OP_CALL);
  EXPECT_EQ(decoded->ops.front().indata.to_str(), "clsmetharg");
  EXPECT_EQ(decoded->get_retry_attempt(), 1);
}

TEST(WeaveReadWire, NegotiatesLegacyRequestsAndPreservesRedirectReplies) {
  for (bool capable : {false, true}) {
    uint64_t features = CEPH_FEATURES_SUPPORTED_DEFAULT;
    if (!capable) features &= ~CEPH_FEATURE_WEAVE_READ_REDIRECT;
    auto source = ceph::make_message<MOSDOp>(0, 17, object("logical"), spg_t(), 7,
      CEPH_OSD_FLAG_READ, features);
    source->allow_weave_redirect(true);
    source->read(2, 3);
    bufferlist wire;
    encode_message(source.get(), features, wire);
    auto p = wire.cbegin();
    auto decoded = boost::intrusive_ptr<MOSDOp>(static_cast<MOSDOp*>(decode_message(g_ceph_context, 0, p)), false);
    ASSERT_TRUE(decoded);
    decoded->finish_decode();
    EXPECT_EQ(decoded->get_header().version, capable ? 10 : 8);
    EXPECT_EQ(decoded->allows_weave_redirect(), capable);
    EXPECT_EQ(decoded->ops.front().op.extent.offset, 2u);
    auto reply = ceph::make_message<MOSDOpReply>(decoded.get(), -EAGAIN, 7, CEPH_OSD_FLAG_ONDISK, true);
    if (capable) reply->set_weave_read_route(
      {object("volume"), pg_shard_t(8, shard_id_t(1)), 7, eversion_t(7, 9)});
    wire.clear();
    encode_message(reply.get(), features, wire);
    p = wire.cbegin();
    auto response = boost::intrusive_ptr<MOSDOpReply>(static_cast<MOSDOpReply*>(decode_message(g_ceph_context, 0, p)), false);
    ASSERT_TRUE(response);
    EXPECT_EQ(response->get_weave_read_route().has_value(), capable);
    EXPECT_EQ(response->get_oid(), object_t("logical"));
    std::vector<OSDOp> returned;
    response->claim_ops(returned);
    ASSERT_EQ(returned.size(), 1u);
    EXPECT_EQ(returned.front().op.op, CEPH_OSD_OP_READ);
  }
}

TEST(WeaveReadWire, RetiredFeatureBitDoesNotOptOlderPeersIntoProtocol) {
  const uint64_t features = CEPH_FEATURES_SUPPORTED_DEFAULT & ~CEPH_FEATURE_SERVER_QUINCY;
  auto source = ceph::make_message<MOSDOp>(0, 17, object("logical"), spg_t(), 7,
    CEPH_OSD_FLAG_READ, features);
  source->allow_weave_redirect(true);
  source->read(0, 3);
  bufferlist wire;
  encode_message(source.get(), features, wire);
  auto p = wire.cbegin();
  auto decoded = boost::intrusive_ptr<MOSDOp>(static_cast<MOSDOp*>(decode_message(g_ceph_context, 0, p)), false);
  ASSERT_TRUE(decoded);
  decoded->finish_decode();
  EXPECT_EQ(decoded->get_header().version, 8);
  EXPECT_FALSE(decoded->allows_weave_redirect());
  auto reply = ceph::make_message<MOSDOpReply>(decoded.get(), 0, 7, CEPH_OSD_FLAG_ONDISK, true);
  wire.clear(); encode_message(reply.get(), features, wire);
  EXPECT_EQ(reply->get_header().version, 8);
}
