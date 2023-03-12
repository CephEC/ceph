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
#include <fstream>
#include <sys/types.h>
#include <fcntl.h>

#include <include/compat.h>

#include "osd/osd_types.h"

#include "arrow/api.h"
#include "arrow/buffer.h"
#include "arrow/compute/api.h"
#include "arrow/device.h"
#include "arrow/io/api.h"
#include "arrow/io/interfaces.h"


#include "parquet/file_reader.h"
#include "parquet/arrow/reader.h"
#include "parquet/column_reader.h"
#include "parquet/api/reader.h"

using ceph::bufferlist;
using std::string;
using ceph::decode;
using ceph::encode;

CLS_VER(1,0)
CLS_NAME(parquet_scan)

void get_table_from_binary_stream(bufferlist *in, 
		std::shared_ptr<arrow::Table>* table)
{
 
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  // 文件流转化为 arrow::Buffer
  auto arrow_buffer = 
	  std::make_shared<arrow::Buffer>((uint8_t*)in->c_str(), in->length());  

  // 根据 ArrowBuffer 构造 BufferReader  
  auto arrow_buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer); 

  // 从文件流中解析parquet文件
  std::unique_ptr<parquet::ParquetFileReader> parquet_reader = 
	  		parquet::ParquetFileReader::Open(arrow_buffer_reader);
  
  // 获取parquet文件元数据
  std::shared_ptr<parquet::FileMetaData> metadata = parquet_reader->metadata();  
  
  CLS_LOG(1, "row groups: %d", metadata->num_row_groups());
  CLS_LOG(1, "rows: %d", metadata->num_rows());
  CLS_LOG(1, "columns: %d", metadata->num_columns());

  // 从parquet文件中获取table
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  
  if (arrow::Status::OK() != parquet::arrow::FileReader::Make(pool,
			 std::move(parquet_reader),
			 &arrow_reader)) {
    CLS_LOG(1, "failed to build arrow_reader.");
    return;
  }    
  
   arrow_reader->ReadTable(table);

 
}

static int sum(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  if (in->length() == 0)
    return -EINVAL;

  // 参数解析
  ClsParmContext *pctx = (ClsParmContext *)hctx;
  std::string column_name;
  try {
    auto in_iter = (pctx->parm_data).cbegin();
    decode(column_name, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: sum: failed to decode entry: %s", err.what());
    return -EINVAL;
  }

  CLS_LOG(1, "get the sum of column %s", column_name.c_str());
  
  std::shared_ptr<arrow::Table> table;
  get_table_from_binary_stream(in, &table);
  
  arrow::Datum sum;
  sum = arrow::compute::Sum({table->GetColumnByName(column_name)}).ValueOrDie();

  int64_t result = sum.scalar_as<arrow::Int64Scalar>().value;

  CLS_LOG(1, "Datum sum: %ld", result);
  
  std::string res = std::to_string(result);
  
  out->append(res.c_str());
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
  cls_method_handle_t h_sum;
  cls_method_handle_t h_sql_exec;

  cls_register("parquet_scan", &h_class);
  cls_register_cxx_method(h_class, "sum",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          sum, &h_sum);
  cls_register_cxx_method(h_class, "sql_exec",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          sql_exec, &h_sql_exec);
}
