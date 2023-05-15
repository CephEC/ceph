/*
 * test_cls_parquet_op
 *
 * gtest for cls_parquet_op
 *
 */

#include <iostream>
#include <errno.h>
#include <set>
#include <sstream>
#include <string>


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
  bufferlist in,out;
  std::string file_name("/home/parquet/tripdata_128M.parquet");
  encode(file_name,in);
  ASSERT_EQ(0, ioctx.exec("myobject", "parquet_op", "write", in, out));  
  in.clear();
  std::string column_name = "trip_time";
//   int file_size=0;
  encode(column_name,in);
//   encode(file_size,in);  
 // 调用parquet_op类的sum方法
  ASSERT_EQ(0, ioctx.exec("myobject", "parquet_op", "sum", in, out));
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
//   std::string sql = "SELECT *;";
  std::string file_name("/home/parquet/tripdata_128M.parquet");
  encode(file_name,in);
  ASSERT_EQ(0, ioctx.exec("myobject", "parquet_op", "write", in, out));  
  in.clear();  
  std::string offset_str("2095000");  
  encode(offset_str,in);
 // 调用parquet_op类的select方法
  ASSERT_EQ(0, ioctx.exec("myobject", "parquet_op", "select", in, out));

  std::cout << "result = " << out.c_str() << std::endl;

  // 销毁临时pool
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));

}
