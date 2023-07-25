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
CLS_NAME(parquet_scan)

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
  CLS_LOG(1, "rows: %ld", metadata->num_rows());
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


static int filter(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{


  if (in->length() == 0)
    return -EINVAL;

  // 参数解析
  ClsParmContext *pctx = (ClsParmContext *)hctx;
  uint64_t offset;
  try {
    auto in_iter = (pctx->parm_data).cbegin();
    decode(offset, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: filter: failed to decode entry: %s", err.what());
    return -EINVAL;
  }

  CLS_LOG(1, "get the offset %ld", offset);
  
  std::shared_ptr<arrow::Table> table;
  get_table_from_binary_stream(in, in->length(), &table);
  
  std::shared_ptr<arrow::Table> new_table = table->Slice(0, offset);

  std::shared_ptr<arrow::Buffer> out_buffer = write_table_to_arrow_buffer(new_table, out, in->length());  
  
  return 0;
}


// 注册parquet_scan类
CLS_INIT(parquet_scan)
{
  CLS_LOG(5, "loading cls_parquet_scan");

  cls_handle_t h_class;
  cls_method_handle_t h_filter;

  cls_register("parquet_scan", &h_class);
  cls_register_cxx_method(h_class, "filter",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          filter, &h_filter);
}
