/*
 * cls_parquet_scan.cc
 *
 * parquet_scan方法在OSD端执行的主要流程
 *
 */

/** \file
 *
 * This is an OSD class that implements methods for ndp 
 * on parquet file
 *
 */

#include "objclass/objclass.h"
#include <errno.h>
#include <string>
#include <sstream>
#include <cstdio>
#include <include/compat.h>

using ceph::bufferlist;
using std::string;
using ceph::decode;
using ceph::encode;

CLS_VER(1,0)
CLS_NAME(parquet_scan)

static int count(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  if (in->length() == 0)
    return -EINVAL;
  
  /*
   * TODO: 测试阶段可以直接调C++标准库文件接口比如fstream读取本地parquet文件
   * 
   * parquet->Arrow
   * 
   * Arrow->datafusion处理->Arrow/parquet->char*->append到out后面
   *
   * 最后看在客户端怎么呈现处理结果
   *
   * 临时文件路径: /home/ceph/parquet/
   *
   */





  /*
   *
   */
  CLS_LOG(5,"count: ");
  
  out->append("temp test result");
  return 0;
}

static int sql_exec(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  if (in->length() == 0)
    return -EINVAL;

  string sql;
  // 获取输入参数（这里是一条sql语句，见test_cls_parquet.cc
  // 请自己把这个方法拆成不同类型的sql语句分开进行单元测试
  auto iter = in->cbegin();
  try {
    decode(sql, iter);
  } catch (const ceph::buffer::error &err) {
    CLS_LOG(5, "count: invalid decode of input");
    return -EINVAL;
  }

   /*
   * TODO: 测试阶段可以直接调C++标准库文件接口比如fstream读取本地parquet文件
   * 
   * 流程同上，执行自己的sql语句
   *
   * 临时文件路径: /home/ceph/parquet/
   *
   */





  /*
   *
   */
  CLS_LOG(5, "sql_exec: ");
  out->append("temp test result");
  return 0;
}

// 注册parquet_scan类
CLS_INIT(parquet_scan)
{
  CLS_LOG(5, "loading cls_parquet_scan");

  cls_handle_t h_class;
  cls_method_handle_t h_count;
  cls_method_handle_t h_sql_exec;

  cls_register("parquet_scan", &h_class);
  cls_register_cxx_method(h_class, "count",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          count, &h_count);
  cls_register_cxx_method(h_class, "sql_exec",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          sql_exec, &h_sql_exec);
}
