/*
 * test_cls_parquet_scan
 *
 * gtest for cls_parquet_scan
 *
 */

#include <iostream>
#include <errno.h>
#include <set>
#include <sstream>
#include <string>

#include "cls/parquet_scan/cls_parquet_scan_client.h"
#include "gtest/gtest.h"
#include "include/rados/librados.hpp"
#include "test/librados/test_cxx.h"


using namespace librados;

// 在OSD端执行count算子统计行数
TEST(ClsParquetScan, Count) {
  Rados cluster;
  // 获取临时pool（并不是获取了真的pool，仅测试用
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  // 创建rados客户端上下文
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist out;
  std::string column_name = "c0";

 // 调用parquet_scan类的count方法
  ASSERT_EQ(0, rados::cls::parquet_scan::sum(&ioctx, "myobject", column_name, out));
  std::cout << "result = " << out.c_str() << std::endl;

  // 销毁临时pool
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));

}

// 在OSD端执行sql语句
TEST(ClsParquetScan, SqlExec) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  std::string sql = "SELECT *;";

 // 调用parquet_scan类的count方法
  ASSERT_EQ(0, rados::cls::parquet_scan::sql_exec(&ioctx,"myobject", sql, out));

  std::cout << "result = " << out.c_str() << std::endl;

  // 销毁临时pool
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));

}
