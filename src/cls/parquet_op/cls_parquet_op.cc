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
#include <fstream>
#include <cstdio>
#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>
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
#include "parquet/file_writer.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/column_reader.h"
#include "parquet/api/reader.h"

using ceph::bufferlist;
using std::string;
using ceph::decode;
using ceph::encode;

CLS_VER(1,0)
CLS_NAME(parquet_op)

// 读parquet文件 输出到arrow_buffer
int read_origin_parquet_file(bufferlist* indata, const char* file_name)
{
   // 打开parquet文件
  int fd = std::open(file_name, O_RDONLY);
  if (fd < 0) {
    CLS_LOG(1,"failed to open file %s",file_name);
    return ERR;
  }

  struct stat info;
  stat(file_name, &info);
  int file_size = info.st_size;

  CLS_LOG(1,"read from:%s,  size:%ld",file_name,file_size/1024/1024);

  if (file_size != indata->read_fd(fd, file_size)) {
    CLS_LOG(1,"failed to read");
    return ERR;
  }
 
  std::close(fd);
  return file_size;
}
static int write(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  if (in->length() == 0)
    return -EINVAL;
  // 参数解析,从in中解析出文件名
  std::string file_name;
  try {
    auto in_iter = in->cbegin();
    decode(file_name, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: sum: failed to decode entry: %s", err.what());
    return -EINVAL;
  }
  CLS_LOG(1, "write parquet file %s to object ", file_name.c_str());
  
    //写对象
  bufferlist content;
  read_origin_parquet_file(&content, const char* file_name);  
  int r = cls_cxx_write_full(hctx, &content);
  if (r < 0)
    return r;

  return 0;
}


void get_table_from_binary_stream(bufferlist *in,
	        int file_size,	
		std::shared_ptr<arrow::Table>* table)
{
 
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  // 文件流转化为 arrow::Buffer
  auto arrow_buffer = 
	  std::make_shared<arrow::Buffer>((uint8_t*)in->c_str(), file_size);  

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

  // 参数解析,从in中解析出要计算的列名和文件大小

  std::string column_name;
  uint64_t file_size;
  try {
    auto in_iter = in->cbegin();
    decode(column_name, in_iter);
    // decode(file_size, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: sum: failed to decode entry: %s", err.what());
    return -EINVAL;
  }
  CLS_LOG(1, "get the sum of column %s", column_name.c_str());
  
    
  in.clear(); 
  //读取对象大小到file_size
  if (cls_cxx_stat(hctx, file_size, NULL) == 0);
    return -EEXIST;
  //读对象到in  
  int r = cls_cxx_read(hctx, 0, file_size, in);
  if (r < 0)
    return r;
  std::shared_ptr<arrow::Table> table;
  get_table_from_binary_stream(in, file_size, &table);
  
  arrow::Datum sum;
  sum = arrow::compute::Sum({table->GetColumnByName(column_name)}).ValueOrDie();

  int64_t result = sum.scalar_as<arrow::Int64Scalar>().value;

  CLS_LOG(1, "Datum sum: %ld", result);
  
  std::string res = std::to_string(result);
  
  out->append(res.c_str());
  return 0;
}

std::shared_ptr<arrow::Buffer> write_table_to_arrow_buffer(
				std::shared_ptr<arrow::Table> new_table, 
				bufferlist* outdata, 
				int file_size)
{
  arrow::MemoryPool* pool = arrow::default_memory_pool();
	
  // parquet to arrow
  std::shared_ptr<arrow::io::BufferOutputStream> out_arrow_stream;
  out_arrow_stream = arrow::io::BufferOutputStream::Create(file_size).ValueOrDie();
  const int64_t chunk_size = std::max(static_cast<int64_t>(1), new_table->num_rows()); 
  
  CLS_LOG(1, "write table to output stream");
  parquet::arrow::WriteTable(*new_table.get(), 
                              pool, out_arrow_stream, 
                              chunk_size); 
  auto out_arrow_buffer = out_arrow_stream->Finish();
  std::shared_ptr<::arrow::Buffer> out_buffer = out_arrow_buffer.ValueOrDie();

  CLS_LOG(1, "append result to bufferlist");	  
  char *out_ptr = reinterpret_cast<char*>(const_cast<uint8_t*>(out_buffer->data()));
  outdata->append(out_ptr, out_buffer->size());
  return out_arrow_buffer.ValueOrDie();
}


static int select(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{


  if (in->length() == 0)
    return -EINVAL;

  // 参数解析
  std::string offset_str;
  unsigned long int file_size;
  try {
    auto in_iter = in->cbegin();
    decode(offset_str, in_iter);
    // decode(file_size, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: select: failed to decode entry: %s", err.what());
    return -EINVAL;
  }
   
  in.clear(); 
  //读取对象大小到file_size
  if (cls_cxx_stat(hctx, file_size, NULL) == 0)
    return -EEXIST;  
  //读对象到in
  int r = cls_cxx_read(hctx, 0, file_size, in);
  if (r < 0)
    return r;
  int offset = std::stoi(offset_str);

  CLS_LOG(1, "get the offset %d", offset);
  
  std::shared_ptr<arrow::Table> table;
  get_table_from_binary_stream(in, file_size, &table);
  
  std::shared_ptr<arrow::Table> new_table = table->Slice(0, offset);

  std::shared_ptr<arrow::Buffer> out_buffer = write_table_to_arrow_buffer(new_table, out, file_size);  
  
  return 0;

}


// 注册parquet_scan类
CLS_INIT(parquet_op)
{
  CLS_LOG(5, "loading cls_parquet_op");

  cls_handle_t h_class;
  cls_method_handle_t h_write;
  cls_method_handle_t h_sum;
  cls_method_handle_t h_select;

  cls_register("parquet_op", &h_class);
  cls_register_cxx_method(h_class, "write",
                           CLS_METHOD_WR,
                          sum, &h_write);
  cls_register_cxx_method(h_class, "sum",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          sum, &h_sum);
  cls_register_cxx_method(h_class, "select",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          select, &h_select);
}
