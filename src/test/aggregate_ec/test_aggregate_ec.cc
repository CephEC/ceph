// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "gtest/gtest.h"

#include "messages/MOSDOp.h"
#include "osd/OpRequest.h"
#include "osd/aggregate_ec/Aggregator.h"
#include "osd/aggregate_ec/ECBackendIntegration.h"
#include "osd/aggregate_ec/XAttr.h"
#include "common/TrackedOp.h"
#include "test/unit.cc"
#include "osd/osd_types.h"

using namespace ceph::aggregate_ec;

namespace {
constexpr uint64_t CHUNK_SIZE = 4096;
struct TestTracker {
  TestTracker() : tracker(g_ceph_context, false, 1) {}
  ~TestTracker() { tracker.on_shutdown(); }
  OpTracker tracker;
};


MOSDOp *make_write(const hobject_t &oid, char value, uint64_t length) {
  auto *m = new MOSDOp(
    0, 1, oid, spg_t(), 1, CEPH_OSD_FLAG_ONDISK,
    CEPH_FEATURES_SUPPORTED_DEFAULT);
  m->ops.resize(1);
  auto &write = m->ops.front();
  write.op.op = CEPH_OSD_OP_WRITEFULL;
  write.op.extent.offset = 0;
  write.op.extent.length = length;
  write.indata.append(std::string(length, value));
  return m;
}

MOSDOp *make_read(const hobject_t &oid, uint64_t offset, uint64_t length) {
  auto *m = new MOSDOp(
    0, 1, oid, spg_t(), 1, CEPH_OSD_FLAG_ONDISK,
    CEPH_FEATURES_SUPPORTED_DEFAULT);
  m->ops.resize(1);
  auto &read = m->ops.front();
  read.op.op = CEPH_OSD_OP_READ;
  read.op.extent.offset = offset;
  read.op.extent.length = length;
  return m;
}

MOSDOp *make_partial_write(const hobject_t &oid, uint64_t offset,
                           char value, uint64_t length) {
  auto *m = make_write(oid, value, length);
  m->ops.front().op.op = CEPH_OSD_OP_WRITE;
  m->ops.front().op.extent.offset = offset;
  return m;
}

MOSDOp *make_setxattr(const hobject_t &oid,
                      const std::string &name,
                      const std::string &value) {
  auto *m = new MOSDOp(
    0, 1, oid, spg_t(), 1, CEPH_OSD_FLAG_ONDISK,
    CEPH_FEATURES_SUPPORTED_DEFAULT);
  m->ops.resize(1);
  auto &setxattr = m->ops.front();
  setxattr.op.op = CEPH_OSD_OP_SETXATTR;
  setxattr.op.xattr.name_len = name.size();
  setxattr.op.xattr.value_len = value.size();
  setxattr.indata.append(name);
  setxattr.indata.append(value);
  return m;
}

MOSDOp *make_delete(const hobject_t &oid) {
  auto *m = new MOSDOp(
    0, 1, oid, spg_t(), 1, CEPH_OSD_FLAG_ONDISK,
    CEPH_FEATURES_SUPPORTED_DEFAULT);
  m->ops.resize(1);
  m->ops.front().op.op = CEPH_OSD_OP_DELETE;
  return m;
}

OpRequestRef track(OpTracker &tracker, MOSDOp *m) {
  return tracker.create_request<OpRequest, Message*>(m);
}

volume_t decode_metadata(const OSDOp &op) {
  EXPECT_EQ(op.op.op, CEPH_OSD_OP_SETXATTR);
  auto p = op.indata.cbegin();
  std::string name;
  ceph::buffer::list encoded;
  p.copy(op.op.xattr.name_len, name);
  EXPECT_EQ(name, "volume_meta");
  p.copy(op.op.xattr.value_len, encoded);
  volume_t metadata;
  auto encoded_it = encoded.cbegin();
  metadata.decode(encoded_it);
  return metadata;
}

volume_t make_metadata(const hobject_t &volume_oid,
                       const hobject_t &object_oid,
                       uint8_t chunk_id,
                       uint64_t object_size,
                       uint32_t capacity = 2) {
  volume_t metadata(volume_oid, capacity, spg_t(), CHUNK_SIZE);
  chunk_t chunk(chunk_id, spg_t());
  chunk.set_from_op(chunk_id, object_size, object_oid);
  metadata.add_chunk(object_oid, chunk);
  return metadata;
}

void load_metadata(Aggregator &aggregator, const volume_t &metadata) {
  ceph::buffer::list encoded;
  metadata.encode(encoded);
  std::vector<ceph::buffer::list> all_metadata;
  all_metadata.push_back(std::move(encoded));
  aggregator.load_metadata(all_metadata);
}
} // anonymous namespace

TEST(ECBackendIntegration, KeepsAggregateReadExtentsVisible) {
  ECUtil::stripe_info_t stripe_info(2, CHUNK_SIZE * 2);
  ECBackendIntegration native(
    false, false, stripe_info, ceph::ErasureCodeInterfaceRef());
  ECBackendIntegration aggregate(
    true, false, stripe_info, ceph::ErasureCodeInterfaceRef());

  EXPECT_EQ(
    native.backend_read_extent(100, 200),
    std::make_pair(uint64_t{0}, uint64_t{CHUNK_SIZE * 2}));
  EXPECT_EQ(
    aggregate.backend_read_extent(100, 200),
    std::make_pair(uint64_t{100}, uint64_t{200}));
  EXPECT_EQ(
    aggregate.shard_read_extent(CHUNK_SIZE + 100, 200),
    std::make_pair(uint64_t{100}, uint64_t{200}));
  EXPECT_EQ(
    aggregate.shard_read_extent(100, CHUNK_SIZE + 1),
    std::make_pair(uint64_t{0}, uint64_t{CHUNK_SIZE}));
}

TEST(ECBackendIntegration, ExposesDecodeAndRecoveryDecisions) {
  ECUtil::stripe_info_t stripe_info(2, CHUNK_SIZE * 2);
  ECBackendIntegration aggregate(
    true, false, stripe_info, ceph::ErasureCodeInterfaceRef());

  EXPECT_TRUE(aggregate.can_return_without_decode(1));
  EXPECT_FALSE(aggregate.can_return_without_decode(2));
  EXPECT_TRUE(aggregate.needs_full_reconstruction(2, 2));

  hobject_t oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  ECBackendIntegration::ObjectReads reads;
  reads[oid].push_back(boost::make_tuple(100, 200, 7));
  aggregate.align_for_full_reconstruction(reads);
  ASSERT_EQ(reads[oid].size(), 1u);
  const auto &extent = reads[oid].front();
  EXPECT_EQ(extent.get<0>(), 0u);
  EXPECT_EQ(extent.get<1>(), CHUNK_SIZE * 2);
  EXPECT_EQ(extent.get<2>(), 7u);
}

TEST(AggregateChunk, RejectsOversizedObject) {
  TestTracker test_tracker;
  hobject_t oid(sobject_t(object_t("oversized"), CEPH_NOSNAP));
  Chunk chunk;
  EXPECT_EQ(
    chunk.bind(
      0, track(test_tracker.tracker, make_write(oid, 'x', CHUNK_SIZE + 1)),
      CHUNK_SIZE),
    -E2BIG);
  EXPECT_TRUE(chunk.empty());
}

TEST(AggregateChunk, PreservesOffsetOfNewPartialWrite) {
  TestTracker test_tracker;
  hobject_t oid(sobject_t(object_t("partial"), CEPH_NOSNAP));
  Chunk chunk;
  ASSERT_EQ(
    chunk.bind(
      1,
      track(
        test_tracker.tracker,
        make_partial_write(oid, 100, 'p', 20)),
      CHUNK_SIZE),
    0);
  ASSERT_EQ(chunk.ops().size(), 1u);
  const auto &write = chunk.ops().front();
  EXPECT_EQ(write.op.op, CEPH_OSD_OP_WRITE);
  EXPECT_EQ(
    static_cast<uint64_t>(write.op.extent.offset), CHUNK_SIZE);
  EXPECT_EQ(
    static_cast<uint64_t>(write.op.extent.length), CHUNK_SIZE);
  EXPECT_EQ(write.indata.length(), CHUNK_SIZE);
  EXPECT_EQ(write.indata[99], '\0');
  EXPECT_EQ(write.indata[100], 'p');
  EXPECT_EQ(write.indata[119], 'p');
  EXPECT_EQ(write.indata[120], '\0');
  EXPECT_EQ(chunk.data_length(), 120u);
}

TEST(AggregateVolume, BuildsOneStripeFromTwoObjects) {
  TestTracker test_tracker;
  hobject_t first(sobject_t(object_t("first"), CEPH_NOSNAP));
  hobject_t second(sobject_t(object_t("second"), CEPH_NOSNAP));

  Volume volume(2, CHUNK_SIZE, spg_t());
  volume.bind_oid(first);
  ASSERT_EQ(
    volume.add(
      track(test_tracker.tracker, make_write(first, 'a', 100))), 0);
  ASSERT_EQ(
    volume.add(
      track(test_tracker.tracker, make_write(second, 'b', 200))), 1);
  ASSERT_TRUE(volume.full());

  auto *message = volume.generate_write_op();
  ASSERT_NE(message, nullptr);
  EXPECT_EQ(message->get_hobj(), first);
  ASSERT_EQ(message->ops.size(), 3u);

  const auto &first_write = message->ops[0];
  EXPECT_EQ(first_write.op.op, CEPH_OSD_OP_WRITE);
  EXPECT_EQ(static_cast<uint64_t>(first_write.op.extent.offset), 0u);
  EXPECT_EQ(static_cast<uint64_t>(first_write.op.extent.length), CHUNK_SIZE);
  EXPECT_EQ(first_write.indata.length(), CHUNK_SIZE);
  EXPECT_EQ(first_write.indata[0], 'a');

  const auto &second_write = message->ops[1];
  EXPECT_EQ(second_write.op.op, CEPH_OSD_OP_WRITE);
  EXPECT_EQ(static_cast<uint64_t>(second_write.op.extent.offset), CHUNK_SIZE);
  EXPECT_EQ(static_cast<uint64_t>(second_write.op.extent.length), CHUNK_SIZE);
  EXPECT_EQ(second_write.indata.length(), CHUNK_SIZE);
  EXPECT_EQ(second_write.indata[0], 'b');

  auto metadata = decode_metadata(message->ops.back());
  EXPECT_EQ(metadata.get_oid(), first);
  EXPECT_EQ(metadata.get_size(), 2u);
  EXPECT_EQ(metadata.get_chunk_size(), CHUNK_SIZE);
  EXPECT_TRUE(
    metadata.get_chunk_map().find(first) != metadata.get_chunk_map().end());
  EXPECT_TRUE(
    metadata.get_chunk_map().find(second) != metadata.get_chunk_map().end());
  EXPECT_EQ(metadata.get_chunk(first).get_offset(), 100u);
  EXPECT_EQ(metadata.get_chunk(second).get_offset(), 200u);
  message->put();
}

TEST(AggregateVolume, RejectsDuplicatePendingObject) {
  TestTracker test_tracker;
  hobject_t oid(sobject_t(object_t("duplicate"), CEPH_NOSNAP));
  Volume volume(2, CHUNK_SIZE, spg_t());
  volume.bind_oid(oid);

  ASSERT_EQ(
    volume.add(track(test_tracker.tracker, make_write(oid, 'a', 100))), 0);
  EXPECT_EQ(
    volume.add(track(test_tracker.tracker, make_setxattr(oid, "key", "v"))),
    -EEXIST);
  EXPECT_EQ(volume.occupied(), 1u);
  EXPECT_EQ(volume.buffered(), 1u);
}

TEST(AggregateVolume, ReusesFreeSlotFromPersistedMetadata) {
  TestTracker test_tracker;
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t existing(sobject_t(object_t("existing"), CEPH_NOSNAP));
  hobject_t added(sobject_t(object_t("added"), CEPH_NOSNAP));
  auto metadata = make_metadata(volume_oid, existing, 1, 200);

  Volume volume(metadata);
  ASSERT_TRUE(volume.empty());
  ASSERT_EQ(
    volume.add(track(test_tracker.tracker, make_write(added, 'n', 100))), 0);
  ASSERT_TRUE(volume.full());
  EXPECT_EQ(volume.oid(), volume_oid);

  auto *message = volume.generate_write_op();
  ASSERT_NE(message, nullptr);
  EXPECT_EQ(message->get_hobj(), volume_oid);
  ASSERT_EQ(message->ops.size(), 2u);
  EXPECT_EQ(
    static_cast<uint64_t>(message->ops.front().op.extent.offset), 0u);
  auto updated = decode_metadata(message->ops.back());
  EXPECT_EQ(updated.get_size(), 2u);
  EXPECT_EQ(updated.get_chunk(existing).get_offset(), 200u);
  EXPECT_EQ(updated.get_chunk(added).get_offset(), 100u);
  message->put();
}

TEST(VolumeCatalog, ReloadsRealVolumeOid) {
  const spg_t pgid(pg_t(7, 42), shard_id_t(1));
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t first(sobject_t(object_t("first"), CEPH_NOSNAP));
  hobject_t second(sobject_t(object_t("second"), CEPH_NOSNAP));
  volume_t metadata(volume_oid, 2, spg_t(), CHUNK_SIZE);
  chunk_t first_chunk(0, spg_t());
  first_chunk.set_from_op(0, 100, first);
  chunk_t second_chunk(1, spg_t());
  second_chunk.set_from_op(1, 200, second);
  metadata.add_chunk(first, first_chunk);
  metadata.add_chunk(second, second_chunk);

  ceph::buffer::list encoded;
  metadata.encode(encoded);
  VolumeCatalog catalog(pgid);
  ASSERT_EQ(catalog.load_from_disk(encoded), 0);
  ASSERT_EQ(catalog.size(), 2u);
  ASSERT_NE(catalog.lookup(first), nullptr);
  EXPECT_EQ(catalog.lookup(first)->volume_oid, volume_oid);
  EXPECT_EQ(catalog.lookup(second)->volume_oid, volume_oid);
  EXPECT_EQ(catalog.lookup(first)->info.get_spg(), pgid);
}

TEST(VolumeCatalog, UpdatingMetadataRemovesDeletedObjectKey) {
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t first(sobject_t(object_t("first"), CEPH_NOSNAP));
  hobject_t second(sobject_t(object_t("second"), CEPH_NOSNAP));
  volume_t metadata(volume_oid, 2, spg_t(), CHUNK_SIZE);
  chunk_t first_chunk(0, spg_t());
  first_chunk.set_from_op(0, 100, first);
  chunk_t second_chunk(1, spg_t());
  second_chunk.set_from_op(1, 200, second);
  metadata.add_chunk(first, first_chunk);
  metadata.add_chunk(second, second_chunk);

  VolumeCatalog catalog;
  catalog.upsert(volume_oid, metadata);
  metadata.remove_chunk(second);
  catalog.upsert(volume_oid, metadata);

  EXPECT_NE(catalog.lookup(first), nullptr);
  EXPECT_EQ(catalog.lookup(second), nullptr);
  EXPECT_EQ(catalog.size(), 1u);
}

TEST(VolumeMetadata, ReplacingChunkDoesNotInflateObjectCount) {
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  volume_t metadata(volume_oid, 2, spg_t(), CHUNK_SIZE);
  chunk_t first(0, spg_t());
  first.set_from_op(0, 100, object_oid);
  metadata.add_chunk(object_oid, first);
  chunk_t replacement(1, spg_t());
  replacement.set_from_op(1, 200, object_oid);
  metadata.add_chunk(object_oid, replacement);

  EXPECT_EQ(metadata.get_size(), 1u);
  EXPECT_FALSE(metadata.get_chunk_bitmap()[0]);
  EXPECT_TRUE(metadata.get_chunk_bitmap()[1]);
  EXPECT_EQ(metadata.get_chunk(object_oid).get_offset(), 200u);
}

TEST(VolumeMetadata, AssignmentResizesChunkBitmap) {
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  volume_t source = make_metadata(volume_oid, object_oid, 1, 100, 2);
  volume_t assigned;

  assigned = source;

  EXPECT_EQ(assigned.get_cap(), 2u);
  ASSERT_EQ(assigned.get_chunk_bitmap().size(), 2u);
  EXPECT_FALSE(assigned.get_chunk_bitmap()[0]);
  EXPECT_TRUE(assigned.get_chunk_bitmap()[1]);
}

TEST(VolumeCatalog, RemappingObjectDetachesItFromOldVolume) {
  hobject_t first_volume(sobject_t(object_t("volume-1"), CEPH_NOSNAP));
  hobject_t second_volume(sobject_t(object_t("volume-2"), CEPH_NOSNAP));
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  VolumeCatalog catalog;
  catalog.upsert(
    first_volume, make_metadata(first_volume, object_oid, 0, 100, 1));
  catalog.upsert(
    second_volume, make_metadata(second_volume, object_oid, 0, 200, 1));

  catalog.remove_volume(first_volume);
  auto metadata = catalog.lookup(object_oid);
  ASSERT_NE(metadata, nullptr);
  EXPECT_EQ(metadata->volume_oid, second_volume);
  EXPECT_EQ(metadata->info.get_chunk(object_oid).get_offset(), 200u);
}

TEST(VolumeCatalog, ProvidesStablePagination) {
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  volume_t metadata(volume_oid, 3, spg_t(), CHUNK_SIZE);
  for (uint8_t i = 0; i < 3; ++i) {
    hobject_t oid(sobject_t(object_t("object-" + std::to_string(i)), CEPH_NOSNAP));
    chunk_t chunk(i, spg_t());
    chunk.set_from_op(i, 100 + i, oid);
    metadata.add_chunk(oid, chunk);
  }
  VolumeCatalog catalog;
  catalog.upsert(volume_oid, metadata);

  std::optional<hobject_t> next;
  auto first_page = catalog.list_objects(hobject_t(), 2, next);
  ASSERT_EQ(first_page.size(), 2u);
  ASSERT_TRUE(next.has_value());
  std::optional<hobject_t> final_next;
  auto second_page = catalog.list_objects(*next, 2, final_next);
  EXPECT_EQ(second_page.size(), 1u);
  EXPECT_FALSE(final_next.has_value());
}

TEST(VolumeCatalog, KeepsAuthoritativeMappingsBeyond4096Objects) {
  VolumeCatalog catalog;
  constexpr uint32_t object_count = 4100;
  for (uint32_t i = 0; i < object_count; ++i) {
    hobject_t volume_oid(
      sobject_t(object_t("volume-" + std::to_string(i)), CEPH_NOSNAP));
    hobject_t object_oid(
      sobject_t(object_t("object-" + std::to_string(i)), CEPH_NOSNAP));
    catalog.upsert(
      volume_oid, make_metadata(volume_oid, object_oid, 0, 100, 1));
  }

  hobject_t first(sobject_t(object_t("object-0"), CEPH_NOSNAP));
  hobject_t last(sobject_t(object_t("object-4099"), CEPH_NOSNAP));
  EXPECT_EQ(catalog.size(), object_count);
  EXPECT_NE(catalog.lookup(first), nullptr);
  EXPECT_NE(catalog.lookup(last), nullptr);
}

TEST(VolumeCatalog, ReplacingSnapshotDropsMissingVolumes) {
  hobject_t old_volume(sobject_t(object_t("old-volume"), CEPH_NOSNAP));
  hobject_t old_object(sobject_t(object_t("old-object"), CEPH_NOSNAP));
  hobject_t new_volume(sobject_t(object_t("new-volume"), CEPH_NOSNAP));
  hobject_t new_object(sobject_t(object_t("new-object"), CEPH_NOSNAP));
  VolumeCatalog catalog;
  catalog.upsert(
    old_volume, make_metadata(old_volume, old_object, 0, 100, 1));

  auto replacement = make_metadata(new_volume, new_object, 0, 200, 1);
  ceph::buffer::list encoded;
  replacement.encode(encoded);
  std::vector<ceph::buffer::list> snapshot;
  snapshot.push_back(std::move(encoded));
  ASSERT_EQ(catalog.replace_from_disk(snapshot), 0);

  EXPECT_EQ(catalog.lookup(old_object), nullptr);
  EXPECT_NE(catalog.lookup(new_object), nullptr);
  EXPECT_EQ(catalog.size(), 1u);
}

TEST(AggregateXAttr, UsesCompleteLogicalObjectIdentity) {
  hobject_t first(
    object_t("same"), "key", CEPH_NOSNAP, 1, 7, "namespace-a");
  hobject_t second(
    object_t("same"), "key", CEPH_NOSNAP, 1, 7, "namespace-b");
  hobject_t prefix_name(
    object_t("same-more"), "key", CEPH_NOSNAP, 1, 7, "namespace-a");

  EXPECT_NE(xattr_name(first, "attr"), xattr_name(second, "attr"));
  EXPECT_EQ(
    xattr_name(first, "attr").find(xattr_prefix(first)), 0u);
  EXPECT_NE(
    xattr_name(prefix_name, "attr").find(xattr_prefix(first)), 0u);
}

TEST(Aggregator, ExistingWriteFullStaysInOriginalVolume) {
  TestTracker test_tracker;
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  Aggregator aggregator(
    g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(2, CHUNK_SIZE, false, 1.0);
  load_metadata(
    aggregator, make_metadata(volume_oid, object_oid, 1, 200));

  auto *message = make_write(object_oid, 'n', 150);
  auto request = track(test_tracker.tracker, message);
  ASSERT_EQ(
    aggregator.preprocess(request), Aggregator::PREPROCESS_CONTINUE);
  EXPECT_TRUE(request->is_aggregateEC_translated_op());
  EXPECT_EQ(message->get_hobj(), volume_oid);
  ASSERT_EQ(message->ops.size(), 2u);
  EXPECT_EQ(message->ops[0].op.op, CEPH_OSD_OP_WRITE);
  EXPECT_EQ(
    static_cast<uint64_t>(message->ops[0].op.extent.offset), CHUNK_SIZE);
  EXPECT_EQ(
    static_cast<uint64_t>(message->ops[0].op.extent.length), CHUNK_SIZE);
  EXPECT_EQ(message->ops[0].indata.length(), CHUNK_SIZE);
  auto updated = decode_metadata(message->ops[1]);
  EXPECT_EQ(updated.get_oid(), volume_oid);
  EXPECT_EQ(updated.get_chunk(object_oid).get_offset(), 150u);
}

TEST(Aggregator, SerializesMetadataChangesWithReusedVolume) {
  TestTracker test_tracker;
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t existing(sobject_t(object_t("existing"), CEPH_NOSNAP));
  hobject_t added(sobject_t(object_t("added"), CEPH_NOSNAP));
  Aggregator aggregator(g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(3, CHUNK_SIZE, true, 60.0);
  load_metadata(
    aggregator, make_metadata(volume_oid, existing, 1, 100, 3));

  auto added_request = track(
    test_tracker.tracker, make_write(added, 'a', 50));
  ASSERT_EQ(
    aggregator.preprocess(added_request), Aggregator::PREPROCESS_CONSUMED);

  auto *existing_write = make_write(existing, 'b', 80);
  auto existing_request = track(test_tracker.tracker, existing_write);
  EXPECT_EQ(
    aggregator.preprocess(existing_request),
    Aggregator::PREPROCESS_CONSUMED);
  EXPECT_EQ(existing_write->get_hobj(), existing);
  EXPECT_FALSE(existing_request->is_aggregateEC_translated_op());
  aggregator.shutdown();
}

TEST(Aggregator, ClipsReadsToLogicalObjectBoundary) {
  TestTracker test_tracker;
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  Aggregator aggregator(
    g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(2, CHUNK_SIZE, false, 1.0);
  load_metadata(
    aggregator, make_metadata(volume_oid, object_oid, 1, 100));

  auto *message = make_read(object_oid, 50, 100);
  auto request = track(test_tracker.tracker, message);
  ASSERT_EQ(
    aggregator.preprocess(request), Aggregator::PREPROCESS_REDIRECT);
  EXPECT_EQ(message->get_hobj(), volume_oid);
  EXPECT_EQ(
    static_cast<uint64_t>(message->ops[0].op.extent.offset),
    CHUNK_SIZE + 50);
  EXPECT_EQ(
    static_cast<uint64_t>(message->ops[0].op.extent.length), 50u);
}

TEST(Aggregator, ReadPastLogicalEofTargetsPhysicalEof) {
  TestTracker test_tracker;
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  Aggregator aggregator(
    g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(2, CHUNK_SIZE, false, 1.0);
  load_metadata(
    aggregator, make_metadata(volume_oid, object_oid, 1, 100));

  auto *message = make_read(object_oid, 100, 100);
  auto request = track(test_tracker.tracker, message);
  ASSERT_EQ(
    aggregator.preprocess(request), Aggregator::PREPROCESS_REDIRECT);
  EXPECT_EQ(
    static_cast<uint64_t>(message->ops[0].op.extent.offset),
    2 * CHUNK_SIZE);
  EXPECT_EQ(
    static_cast<uint64_t>(message->ops[0].op.extent.length), 1u);
}

TEST(Aggregator, RejectsOversizedNewObjectBeforeNormalWritePath) {
  TestTracker test_tracker;
  hobject_t object_oid(sobject_t(object_t("oversized"), CEPH_NOSNAP));
  Aggregator aggregator(
    g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(2, CHUNK_SIZE, false, 1.0);

  auto request = track(
    test_tracker.tracker,
    make_write(object_oid, 'x', CHUNK_SIZE + 1));
  EXPECT_EQ(aggregator.preprocess(request), -E2BIG);
}

TEST(Aggregator, ExistingPartialWriteUsesChunkRelativeOffset) {
  TestTracker test_tracker;
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  Aggregator aggregator(
    g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(2, CHUNK_SIZE, false, 1.0);
  load_metadata(
    aggregator, make_metadata(volume_oid, object_oid, 1, 100));

  auto *message = make_partial_write(object_oid, 90, 'p', 20);
  auto request = track(test_tracker.tracker, message);
  ASSERT_EQ(
    aggregator.preprocess(request), Aggregator::PREPROCESS_CONTINUE);
  ASSERT_EQ(message->ops.size(), 2u);
  EXPECT_EQ(
    static_cast<uint64_t>(message->ops[0].op.extent.offset),
    CHUNK_SIZE + 90);
  EXPECT_EQ(
    static_cast<uint64_t>(message->ops[0].op.extent.length), 20u);
  auto updated = decode_metadata(message->ops[1]);
  EXPECT_EQ(updated.get_chunk(object_oid).get_offset(), 110u);
}

TEST(Aggregator, CompoundWritesPersistFinalLogicalSize) {
  TestTracker test_tracker;
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  Aggregator aggregator(g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(2, CHUNK_SIZE, false, 1.0);
  load_metadata(
    aggregator, make_metadata(volume_oid, object_oid, 1, 100));

  auto *message = make_partial_write(object_oid, 0, 'a', 10);
  message->ops.emplace_back();
  auto &second = message->ops.back();
  second.op.op = CEPH_OSD_OP_WRITE;
  second.op.extent.offset = 200;
  second.op.extent.length = 10;
  second.indata.append(std::string(10, 'b'));
  auto request = track(test_tracker.tracker, message);

  ASSERT_EQ(
    aggregator.preprocess(request), Aggregator::PREPROCESS_CONTINUE);
  ASSERT_EQ(message->ops.size(), 3u);
  EXPECT_EQ(
    static_cast<uint64_t>(message->ops[0].op.extent.offset), CHUNK_SIZE);
  EXPECT_EQ(
    static_cast<uint64_t>(message->ops[1].op.extent.offset),
    CHUNK_SIZE + 200);
  auto updated = decode_metadata(message->ops.back());
  EXPECT_EQ(updated.get_chunk(object_oid).get_offset(), 210u);
}

TEST(Aggregator, SameSizeWritePersistsLogicalMtime) {
  TestTracker test_tracker;
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  auto metadata = make_metadata(volume_oid, object_oid, 1, 100);
  metadata.get_chunk(object_oid).set_mtime(utime_t(10, 0));
  Aggregator aggregator(g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(2, CHUNK_SIZE, false, 1.0);
  load_metadata(aggregator, metadata);

  auto *message = make_partial_write(object_oid, 10, 'p', 20);
  message->set_mtime(utime_t(20, 0));
  auto request = track(test_tracker.tracker, message);
  ASSERT_EQ(
    aggregator.preprocess(request), Aggregator::PREPROCESS_CONTINUE);
  ASSERT_EQ(message->ops.size(), 2u);
  auto updated = decode_metadata(message->ops.back());
  EXPECT_EQ(updated.get_chunk(object_oid).get_offset(), 100u);
  EXPECT_EQ(updated.get_chunk(object_oid).get_mtime(), utime_t(20, 0));
}

TEST(Aggregator, ConvertsPhysicalReadOffsetBackToLogicalOffset) {
  TestTracker test_tracker;
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  Aggregator aggregator(g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(2, CHUNK_SIZE, false, 1.0);
  load_metadata(
    aggregator, make_metadata(volume_oid, object_oid, 1, 100));

  auto *message = make_read(object_oid, 50, 25);
  message->ops.front().op.op = CEPH_OSD_OP_SPARSE_READ;
  auto request = track(test_tracker.tracker, message);
  ASSERT_EQ(
    aggregator.preprocess(request), Aggregator::PREPROCESS_REDIRECT);
  auto logical = aggregator.logical_offset(
    request, message->ops.front().op.extent.offset);
  ASSERT_TRUE(logical.has_value());
  EXPECT_EQ(*logical, 50u);
}

TEST(Aggregator, ExistingSetXattrIsNamespacedInOriginalVolume) {
  TestTracker test_tracker;
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  Aggregator aggregator(
    g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(2, CHUNK_SIZE, false, 1.0);
  load_metadata(
    aggregator, make_metadata(volume_oid, object_oid, 1, 100));

  auto *message = make_setxattr(object_oid, "key", "value");
  auto request = track(test_tracker.tracker, message);
  ASSERT_EQ(
    aggregator.preprocess(request), Aggregator::PREPROCESS_CONTINUE);
  EXPECT_EQ(message->get_hobj(), volume_oid);
  auto p = message->ops[0].indata.cbegin();
  std::string key;
  std::string value;
  p.copy(message->ops[0].op.xattr.name_len, key);
  p.copy(message->ops[0].op.xattr.value_len, value);
  EXPECT_EQ(key, xattr_name(object_oid, "key"));
  EXPECT_EQ(value, "value");
}

TEST(Aggregator, PartialDeleteUpdatesAuthoritativeCatalog) {
  TestTracker test_tracker;
  hobject_t volume_oid(sobject_t(object_t("volume"), CEPH_NOSNAP));
  hobject_t first(sobject_t(object_t("first"), CEPH_NOSNAP));
  hobject_t second(sobject_t(object_t("second"), CEPH_NOSNAP));
  volume_t metadata(volume_oid, 2, spg_t(), CHUNK_SIZE);
  chunk_t first_chunk(0, spg_t());
  first_chunk.set_from_op(0, 100, first);
  chunk_t second_chunk(1, spg_t());
  second_chunk.set_from_op(1, 200, second);
  metadata.add_chunk(first, first_chunk);
  metadata.add_chunk(second, second_chunk);

  Aggregator aggregator(
    g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(2, CHUNK_SIZE, false, 1.0);
  load_metadata(aggregator, metadata);

  auto *message = make_delete(first);
  auto request = track(test_tracker.tracker, message);
  ASSERT_EQ(
    aggregator.preprocess(request), Aggregator::PREPROCESS_CONTINUE);
  ASSERT_EQ(message->ops.size(), 2u);
  EXPECT_EQ(message->ops[0].op.op, CEPH_OSD_OP_SETXATTR);
  EXPECT_EQ(message->ops[1].op.op, CEPH_OSD_OP_ZERO);
  auto updated = decode_metadata(message->ops[0]);
  EXPECT_EQ(updated.get_chunk_map().count(first), 0u);
  EXPECT_EQ(updated.get_chunk_map().count(second), 1u);

  aggregator.update_cache(volume_oid, message->ops);
  auto missing = track(test_tracker.tracker, make_read(first, 0, 10));
  EXPECT_EQ(aggregator.preprocess(missing), -ENOENT);
  auto present = track(test_tracker.tracker, make_read(second, 0, 10));
  EXPECT_EQ(
    aggregator.preprocess(present), Aggregator::PREPROCESS_REDIRECT);
}

TEST(Aggregator, EnabledFlushTimerRetainsPartialVolumeUntilShutdown) {
  TestTracker test_tracker;
  hobject_t object_oid(sobject_t(object_t("object"), CEPH_NOSNAP));
  Aggregator aggregator(
    g_ceph_context, spg_t(), nullptr, nullptr);
  aggregator.activate(2, CHUNK_SIZE, true, 60.0);

  auto request = track(
    test_tracker.tracker, make_write(object_oid, 'x', 100));
  EXPECT_EQ(
    aggregator.preprocess(request), Aggregator::PREPROCESS_CONSUMED);
  aggregator.shutdown();
  EXPECT_FALSE(aggregator.initialized());
}
