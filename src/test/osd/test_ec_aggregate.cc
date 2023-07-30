// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include <memory>
#include "osd/AggregateBuffer.h"

#include "test/unit.cc"

#define CHUNK_SIZE 134217728
#define AGGREGATE_BUFFER_CAP 4
#define SHARDS 6
#define FLUSH_TIMEOUT 1

MOSDOp* gen_writefull_op(hobject_t &soid, char content, uint64_t size) {
  MOSDOp* m1 = new MOSDOp(0, 0, soid, spg_t(), epoch_t(), 0, 1);
  std::string filledStringX(size, content);
  ceph::buffer::list bl;
  bl.append(filledStringX);
  OSDOp writefull_op{CEPH_OSD_OP_WRITEFULL};
  writefull_op.op.extent.offset = 0;
  writefull_op.op.extent.length = size;
  writefull_op.indata = std::move(bl);
  OSDOp setxattr_op{CEPH_OSD_OP_SETXATTR};
  std::string key("kw");
  std::string value("vw");
  setxattr_op.op.xattr.name_len = key.length();
  setxattr_op.op.xattr.value_len = value.length();
  setxattr_op.indata.append(key);
  setxattr_op.indata.append(value);
  m1->ops.push_back(writefull_op);
  m1->ops.push_back(setxattr_op);
  return m1;
}

int aggregate_write_op(std::shared_ptr<AggregateBuffer> m_aggregate_buffer, 
  hobject_t &soid, char content, uint64_t size, OpTracker &op_tracker) {
  auto m = gen_writefull_op(soid, content, size);
  auto op = op_tracker.create_request<OpRequest, Message*>(m);
  return m_aggregate_buffer->write(op, m);
}

TEST(ec_aggregate, write_aggregate)
{
  // Normal write flow
  CephContext ctx = CephContext(0, CODE_ENVIRONMENT_UTILITY_NODOUT, 0);
  auto m_aggregate_buffer = std::make_shared<AggregateBuffer>(&ctx, spg_t(), nullptr);
  ASSERT_FALSE(m_aggregate_buffer->is_initialized());
  m_aggregate_buffer->init(AGGREGATE_BUFFER_CAP, CHUNK_SIZE, true, FLUSH_TIMEOUT);

  auto op_tracker = OpTracker(&ctx, false, SHARDS);

  hobject_t soid1(sobject_t(object_t("test_write1"), 0));
  int r = aggregate_write_op(m_aggregate_buffer, soid1, 'x', CHUNK_SIZE /2,  op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);

  ASSERT_FALSE(m_aggregate_buffer->get_active_volume().empty());
  auto aggregated_m = m_aggregate_buffer->get_active_volume().generate_op();
  ASSERT_TRUE(aggregated_m->get_hobj() == soid1 &&
              aggregated_m->ops.size() == 3);
  m_aggregate_buffer->update_cache(aggregated_m->get_hobj(), &(aggregated_m->ops));
  m_aggregate_buffer->clear();

  hobject_t soid2(sobject_t(object_t("test_write2"), 0));
  // oversize
  r = aggregate_write_op(m_aggregate_buffer, soid2, 'y', CHUNK_SIZE + 1,  op_tracker);
  ASSERT_EQ(r, AGGREGATE_FAILED);

  r = aggregate_write_op(m_aggregate_buffer, soid2, 'y', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);

  // aggregate soid2 into soid1
  aggregated_m = m_aggregate_buffer->get_active_volume().generate_op();
  ASSERT_EQ(aggregated_m->get_hobj(), soid1);
  ASSERT_EQ(m_aggregate_buffer->get_active_volume().get_volume_info().get_size(), 2);
  ASSERT_TRUE(m_aggregate_buffer->should_reply_buffered_op());
  m_aggregate_buffer->clear();
  m_aggregate_buffer.reset();
}

TEST(ec_aggregate, wait_for_aggregate)
{
  CephContext ctx = CephContext(0, CODE_ENVIRONMENT_UTILITY_NODOUT, 0);
  auto m_aggregate_buffer = std::make_shared<AggregateBuffer>(&ctx, spg_t(), nullptr);
  m_aggregate_buffer->init(AGGREGATE_BUFFER_CAP, CHUNK_SIZE, true, FLUSH_TIMEOUT);
  auto op_tracker = OpTracker(&ctx, false, SHARDS);

  ASSERT_TRUE(m_aggregate_buffer->get_active_volume().empty());
  hobject_t soid1(sobject_t(object_t("test_write1"), 0));
  int r = aggregate_write_op(m_aggregate_buffer, soid1, 'x', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);

  hobject_t soid2(sobject_t(object_t("test_write2"), 0));
  r = aggregate_write_op(m_aggregate_buffer, soid2, 'y', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);

  hobject_t soid3(sobject_t(object_t("test_write3"), 0));
  r = aggregate_write_op(m_aggregate_buffer, soid3, 'z', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);

  hobject_t soid4(sobject_t(object_t("test_write4"), 0));
  r = aggregate_write_op(m_aggregate_buffer, soid4, 'a', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);
  ASSERT_TRUE(m_aggregate_buffer->get_active_volume().full());
  
  // After waiting for the first four write requests to be aggregated and 
  // placed on the disk, the fifth request can enter the buffer
  hobject_t soid5(sobject_t(object_t("test_write5"), 0));
  r = aggregate_write_op(m_aggregate_buffer, soid5, 'b', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_OP);
  m_aggregate_buffer->clear();
  m_aggregate_buffer.reset();
}

TEST(ec_aggregate, update_cache_and_read)
{
  CephContext ctx = CephContext(0, CODE_ENVIRONMENT_UTILITY_NODOUT, 0);
  auto m_aggregate_buffer = std::make_shared<AggregateBuffer>(&ctx, spg_t(), nullptr);
  m_aggregate_buffer->init(AGGREGATE_BUFFER_CAP, CHUNK_SIZE, true, FLUSH_TIMEOUT);

  auto op_tracker = OpTracker(&ctx, false, SHARDS);

  // write object
  hobject_t soid1(sobject_t(object_t("test_write1"), 0));
  int r = aggregate_write_op(m_aggregate_buffer, soid1, 'x', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);

  // aggregate object into a volume
  auto aggregated_m = m_aggregate_buffer->get_active_volume().generate_op();
  // cached object->volume map and ec data chunk
  m_aggregate_buffer->update_cache(aggregated_m->get_hobj(), &(aggregated_m->ops));
  m_aggregate_buffer->clear();
  ASSERT_TRUE(m_aggregate_buffer->is_object_exist(soid1));

  // read ec data chunk from cache
  ASSERT_TRUE(m_aggregate_buffer->is_volume_cached(soid1));
  extent_map ext_map;
  m_aggregate_buffer->ec_cache_read(ext_map);
  m_aggregate_buffer->clear_ec_cache();
  ASSERT_FALSE(m_aggregate_buffer->is_volume_cached(soid1));

  // translate object_read_op to volume_read_op
  MOSDOp* m1 = new MOSDOp(0, 0, soid1, spg_t(), epoch_t(), 0, 0);
  m1->read(0, CHUNK_SIZE / 2);
  auto op = op_tracker.create_request<OpRequest, Message*>(m1);
  ASSERT_TRUE(m_aggregate_buffer->need_translate_op(m1));
  r = m_aggregate_buffer->op_translate(op, m1->ops);
  ASSERT_EQ(r, AGGREGATE_REDIRECT);
  ASSERT_EQ(soid1, m_aggregate_buffer->get_inflight_volume().get_oid());
  m_aggregate_buffer->clear();
  m_aggregate_buffer.reset();
}

TEST(ec_aggregate, translate_stat_op)
{
  CephContext ctx = CephContext(0, CODE_ENVIRONMENT_UTILITY_NODOUT, 0);
  auto m_aggregate_buffer = std::make_shared<AggregateBuffer>(&ctx, spg_t(), nullptr);
  m_aggregate_buffer->init(AGGREGATE_BUFFER_CAP, CHUNK_SIZE, true, FLUSH_TIMEOUT);

  auto op_tracker = OpTracker(&ctx, false, SHARDS);

  // write object
  hobject_t soid1(sobject_t(object_t("test_write1"), 0));
  int r = aggregate_write_op(m_aggregate_buffer, soid1, 'x', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);

  // aggregate object into a volume
  auto aggregated_m = m_aggregate_buffer->get_active_volume().generate_op();
  // cached object->volume map and ec data chunk
  m_aggregate_buffer->update_cache(aggregated_m->get_hobj(), &(aggregated_m->ops));
  m_aggregate_buffer->clear();

  // translate stat op
  MOSDOp* m1 = new MOSDOp(0, 0, soid1, spg_t(), epoch_t(), 0, 0);
  m1->stat();
  auto op1 = op_tracker.create_request<OpRequest, Message*>(m1);
  ASSERT_TRUE(m_aggregate_buffer->need_translate_op(m1));
  r = m_aggregate_buffer->op_translate(op1, m1->ops);
  ASSERT_EQ(r, AGGREGATE_CONTINUE);
  // check stat_op result
  uint64_t obj_length = 0;
  auto bp = m1->ops[0].outdata.cbegin();
  decode(obj_length, bp);
  ASSERT_EQ(obj_length, CHUNK_SIZE / 2);
  m_aggregate_buffer->clear();
  m_aggregate_buffer.reset();
}

TEST(ec_aggregate, translate_delete_op)
{
  CephContext ctx = CephContext(0, CODE_ENVIRONMENT_UTILITY_NODOUT, 0);
  auto m_aggregate_buffer = std::make_shared<AggregateBuffer>(&ctx, spg_t(), nullptr);
  m_aggregate_buffer->init(AGGREGATE_BUFFER_CAP, CHUNK_SIZE, true, FLUSH_TIMEOUT);

  auto op_tracker = OpTracker(&ctx, false, SHARDS);

  // write object
  hobject_t soid1(sobject_t(object_t("test_write1"), 0));
  int r = aggregate_write_op(m_aggregate_buffer, soid1, 'x', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);

  hobject_t soid2(sobject_t(object_t("test_write2"), 0));
  r = aggregate_write_op(m_aggregate_buffer, soid2, 'y', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);

  // aggregate object into a volume
  auto aggregated_m = m_aggregate_buffer->get_active_volume().generate_op();
  // cached object->volume map and ec data chunk
  m_aggregate_buffer->update_cache(aggregated_m->get_hobj(), &(aggregated_m->ops));
  m_aggregate_buffer->clear();

  // translate delete op (delete soid1)
  // soid1 and soid2 are aggregated to single volume(named soid1)
  // after deleting soid1, there is still soid2 in volume
  // so the volume is not deleted
  // but the metadata is updated and the disk space where soid1 is located is zeroed
  MOSDOp* m1 = new MOSDOp(0, 0, soid1, spg_t(), epoch_t(), 0, 0);
  m1->remove();
  auto op1 = op_tracker.create_request<OpRequest, Message*>(m1);
  ASSERT_TRUE(m_aggregate_buffer->need_translate_op(m1));
  r = m_aggregate_buffer->op_translate(op1, m1->ops);
  ASSERT_EQ(r, AGGREGATE_CONTINUE);
  ASSERT_TRUE(m1->ops.size() == 2 &&
              m1->ops[0].op.op == CEPH_OSD_OP_SETXATTR &&
              m1->ops[1].op.op == CEPH_OSD_OP_ZERO);
  // delete success, update meta cache
  m_aggregate_buffer->update_cache(m1->get_hobj(), &(m1->ops));
  ASSERT_FALSE(m_aggregate_buffer->is_object_exist(soid1));

  // translate delete op (delete soid2)
  MOSDOp* m2 = new MOSDOp(0, 0, soid2, spg_t(), epoch_t(), 0, 0);
  m2->remove();
  auto op2 = op_tracker.create_request<OpRequest, Message*>(m2);
  ASSERT_TRUE(m_aggregate_buffer->need_translate_op(m2));
  r = m_aggregate_buffer->op_translate(op2, m2->ops);
  ASSERT_EQ(r, AGGREGATE_CONTINUE);
  // delete the volume(its oid is equal to soid1)
  ASSERT_TRUE(m2->get_hobj() == soid1 &&
              m2->ops[0].op.op == CEPH_OSD_OP_DELETE);
  // delete success, update meta cache
  m_aggregate_buffer->update_cache(m2->get_hobj(), &(m2->ops));
  ASSERT_FALSE(m_aggregate_buffer->is_object_exist(soid2));
  m_aggregate_buffer->clear();
  m_aggregate_buffer.reset();
}

TEST(ec_aggregate, translate_xattr_op)
{
  CephContext ctx = CephContext(0, CODE_ENVIRONMENT_UTILITY_NODOUT, 0);
  auto m_aggregate_buffer = std::make_shared<AggregateBuffer>(&ctx, spg_t(), nullptr);
  m_aggregate_buffer->init(AGGREGATE_BUFFER_CAP, CHUNK_SIZE, true, FLUSH_TIMEOUT);

  auto op_tracker = OpTracker(&ctx, false, SHARDS);

  // write object
  hobject_t soid1(sobject_t(object_t("test_write1"), 0));
  int r = aggregate_write_op(m_aggregate_buffer, soid1, 'x', CHUNK_SIZE /2, op_tracker);
  ASSERT_TRUE(r == AGGREGATE_PENDING_REPLY);

  // aggregate object into a volume
  auto aggregated_m = m_aggregate_buffer->get_active_volume().generate_op();
  // cached object->volume map and ec data chunk
  m_aggregate_buffer->update_cache(aggregated_m->get_hobj(), &(aggregated_m->ops));
  m_aggregate_buffer->clear();

  // setxattr op
  MOSDOp* m = new MOSDOp(0, 0, soid1, spg_t(), epoch_t(), 0, 0);
  OSDOp setxattr_op{CEPH_OSD_OP_SETXATTR};
  std::string key("k1");
  std::string value("v1");
  setxattr_op.op.xattr.name_len = key.length();
  setxattr_op.op.xattr.value_len = value.length();
  setxattr_op.indata.append(key);
  setxattr_op.indata.append(value);
  m->ops.push_back(setxattr_op);

  // getxattr op
  OSDOp getxattr_op{CEPH_OSD_OP_GETXATTR};
  getxattr_op.op.xattr.name_len = key.length();
  getxattr_op.indata.append(key);
  m->ops.push_back(getxattr_op);

  // getxattrs op
  OSDOp getxattrs_op{CEPH_OSD_OP_GETXATTRS};
  m->ops.push_back(getxattrs_op);

  auto op = op_tracker.create_request<OpRequest, Message*>(m);
  // translate all xattr op
  r = m_aggregate_buffer->op_translate(op, m->ops);
  ASSERT_EQ(r, AGGREGATE_CONTINUE);

  // check translated setxattr op
  for (auto &sub_op : m->ops) {
  // all objects are aggregated into volume,
  // their xattr is also aggregated onto volume
  // in order to distinguish the xattr of different objects in the volume
  // it is necessary to put the object name as prefix of the xattr key.
    std::string key;
    auto bp = sub_op.indata.cbegin();
    bp.copy(sub_op.op.xattr.name_len, key);
    ASSERT_TRUE(key.compare(0, soid1.oid.name.length(), soid1.oid.name) == 0);
  }
  m_aggregate_buffer->clear();
  m_aggregate_buffer.reset();
}

TEST(ec_aggregate, translate_overwrite_op)
{
  CephContext ctx = CephContext(0, CODE_ENVIRONMENT_UTILITY_NODOUT, 0);
  auto m_aggregate_buffer = std::make_shared<AggregateBuffer>(&ctx, spg_t(), nullptr);
  m_aggregate_buffer->init(AGGREGATE_BUFFER_CAP, CHUNK_SIZE, true, FLUSH_TIMEOUT);

  auto op_tracker = OpTracker(&ctx, false, SHARDS);

  // write object
  hobject_t soid1(sobject_t(object_t("test_write1"), 0));
  int r = aggregate_write_op(m_aggregate_buffer, soid1, 'x', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);

  hobject_t soid2(sobject_t(object_t("test_write2"), 0));
  r = aggregate_write_op(m_aggregate_buffer, soid2, 'y', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);

  // aggregate object into a volume
  auto aggregated_m = m_aggregate_buffer->get_active_volume().generate_op();
  // cached object->volume map and ec data chunk
  m_aggregate_buffer->update_cache(aggregated_m->get_hobj(), &(aggregated_m->ops));
  m_aggregate_buffer->clear();

  // overwrite soid2
  MOSDOp* m = new MOSDOp(0, 0, soid2, spg_t(), epoch_t(), 0, 0);
  std::string filledStringX(CHUNK_SIZE / 4, 'z');
  ceph::buffer::list bl;
  bl.append(filledStringX);
  m->writefull(bl);
  auto op = op_tracker.create_request<OpRequest, Message*>(m);
  ASSERT_TRUE(m_aggregate_buffer->need_aggregate_op(m));
  r = m_aggregate_buffer->write(op, m);
  ASSERT_EQ(r, AGGREGATE_CONTINUE);
  ASSERT_EQ(m->ops[0].op.extent.offset, CHUNK_SIZE);
  ASSERT_EQ(m->ops[0].op.extent.length, CHUNK_SIZE);
  m_aggregate_buffer->clear();
  m_aggregate_buffer.reset();
}

TEST(ec_aggregate, translate_cls_op)
{
  CephContext ctx = CephContext(0, CODE_ENVIRONMENT_UTILITY_NODOUT, 0);
  auto m_aggregate_buffer = std::make_shared<AggregateBuffer>(&ctx, spg_t(), nullptr);
  m_aggregate_buffer->init(AGGREGATE_BUFFER_CAP, CHUNK_SIZE, true, FLUSH_TIMEOUT);

  auto op_tracker = OpTracker(&ctx, false, SHARDS);

  // write object
  hobject_t soid1(sobject_t(object_t("test_write1"), 0));
  int r = aggregate_write_op(m_aggregate_buffer, soid1, 'x', CHUNK_SIZE /2, op_tracker);
  ASSERT_EQ(r, AGGREGATE_PENDING_REPLY);
  // aggregate object into a volume
  auto aggregated_m = m_aggregate_buffer->get_active_volume().generate_op();
  // cached object->volume map and ec data chunk
  m_aggregate_buffer->update_cache(aggregated_m->get_hobj(), &(aggregated_m->ops));
  m_aggregate_buffer->clear();

  // translate cls op
  MOSDOp* m = new MOSDOp(0, 0, soid1, spg_t(), epoch_t(), 0, 0);
  OSDOp cls_op{CEPH_OSD_OP_CALL};
  std::string cls_class("openssl_md5");
  std::string cls_method("compute");
  cls_op.indata.append("openssl_md5");
  cls_op.indata.append("compute");
  cls_op.op.cls.class_len = cls_class.length();
  cls_op.op.cls.method_len = cls_method.length();
  cls_op.op.cls.argc = 0;
  cls_op.op.cls.indata_len = 0;
  m->ops.push_back(cls_op);

  auto op = op_tracker.create_request<OpRequest, Message*>(m);
  r = m_aggregate_buffer->op_translate(op, m->ops);
  ASSERT_EQ(r, AGGREGATE_CONTINUE);
  ASSERT_EQ(m->ops[0].op.op, CEPH_OSD_OP_EC_CALL);
  ASSERT_EQ(m->ops[0].op.extent.offset, 0);
  ASSERT_EQ(m->ops[0].op.extent.length, CHUNK_SIZE / 2);
  auto cls_ctx = m_aggregate_buffer->get_cls_ctx(soid1);
  ASSERT_NE(cls_ctx, nullptr);
  m_aggregate_buffer->finish_cls(soid1);
  cls_ctx = m_aggregate_buffer->get_cls_ctx(soid1);
  ASSERT_EQ(cls_ctx, nullptr);
  m_aggregate_buffer->clear();
  m_aggregate_buffer.reset();
}