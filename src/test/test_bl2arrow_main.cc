/*
 * test_bl2arrow
 *
 * gtest for bl2arrow func
 */

#include <iostream>
#include <error.h>
#include <string>
#include <fstream>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "stdlib.h"
#include <memory>

#include "include/buffer.h"

#include "arrow/device.h"
#include "arrow/io/memory.h"
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/type_fwd.h"
#include "arrow/table.h"
#include "arrow/util/utf8.h"
#include "arrow/util/io_util.h"
#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/compute/api.h"
#include "arrow/result.h"

#include "parquet/schema.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/api/reader.h"
#include "parquet/api/writer.h"
#include "parquet/file_writer.h"
#include "parquet/column_reader.h"
#include "parquet/test_util.h"

#include "arrow/compute/api.h"


using namespace std;

#define ERR -1
#define SUCCESS 0


// const char* parquet_file_path = "/home/fhvhv_tripdata_2022-11.parquet";
const char* parquet_file_path = "/home/ceph/src/s3select/parquet_mix_types.parquet";
const char* output_file_name = "/home/result.parquet";

// 读parquet文件 输出到arrow_buffer
int read_origin_parquet_file(bufferlist& indata, const char* file_name)
{
   // 打开parquet文件
  int fd = ::open(file_name, O_RDONLY);
  if (fd < 0) {
    cout << "failed to open file " << file_name << endl;
    return ERR;
  }

  struct stat info;
  stat(file_name, &info);
  int file_size = info.st_size;

  cout << "read from: " << file_name << ", size: " << file_size << endl;

  if (file_size != indata.read_fd(fd, file_size)) {
    cout << "failed to read" << endl;
    return ERR;
  }
 
  ::close(fd);

  return file_size;
}

// parquet 文件写入 arrow buffer
std::shared_ptr<arrow::Buffer> get_arrow_buffer_from_parquet_binary(
				arrow::MemoryPool* pool, 
				bufferlist& indata)
{
 
  // 文件流转化为 arrow::Buffer
  auto arrow_buffer = std::make_shared<arrow::Buffer>((uint8_t*)indata.c_str(), indata.length());  
  return arrow_buffer;
}



std::shared_ptr<arrow::Table> get_arrow_table_from_arrow_buffer(
				arrow::MemoryPool* pool, 
				std::shared_ptr<arrow::Buffer> arrow_buffer)
{
 
  // 根据 ArrowBuffer 构造 BufferReader  
  auto arrow_buffer_reader = std::make_shared<::arrow::io::BufferReader>(arrow_buffer);

  // 从文件流中解析parquet文件
  std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::Open(arrow_buffer_reader);
  
  // 获取parquet文件元数据
  std::shared_ptr<parquet::FileMetaData> metadata = parquet_reader->metadata();
  cout << "Parquet schema: " << metadata->schema()->ToString() << endl;

  int num_row_groups = metadata->num_row_groups();
  cout << "row groups: " << num_row_groups << endl;

  int num_rows = metadata->num_rows();
  cout << "rows: " << num_rows << endl;

  int num_columns = metadata->num_columns();
  cout << "colunms: " << num_columns << endl;
  
  // 从parquet文件中获取table
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  if (arrow::Status::OK() != parquet::arrow::FileReader::Make(pool,
 			  			std::move(parquet_reader),
 					        &arrow_reader)) {
    cout << "failed to build arrow_reader." << endl;
    return nullptr;
  }
  std::shared_ptr<arrow::Table> table;
  arrow_reader->ReadTable(&table);  

  return table;
 
}


void check_table_in_arrow_buffer(std::shared_ptr<arrow::Buffer> out_arrow_buffer)
{
  std::unique_ptr<parquet::arrow::FileReader> reader;
  OpenFile(std::make_shared<::arrow::io::BufferReader>(out_arrow_buffer),
	               ::arrow::default_memory_pool(), &reader);
  auto metadata = reader->parquet_reader()->metadata();
  cout << "--- check out arrow buffer meta" << endl;
  cout << "buffer columns: " << metadata->num_rows() << endl;
}

// 将ArrowBuffer中的数据写入Parquet文件
std::shared_ptr<arrow::Buffer> write_table_to_arrow_buffer(
		arrow::MemoryPool* pool, 
		std::shared_ptr<arrow::Table> new_table, 
		bufferlist& outdata, 
		int file_size)
{
  // parquet to arrow
  std::shared_ptr<arrow::io::BufferOutputStream> out_arrow_stream;
  out_arrow_stream = arrow::io::BufferOutputStream::Create(file_size).ValueOrDie();

  /* std::unique_ptr<parquet::ParquetFileWriter> parquet_file_writer = */
	  /* parquet::ParquetFileWriter::Open( */
			  /* out_arrow_stream, */
			  /* parquet_schema); */
  /* std::unique_ptr<parquet::arrow::FileWriter> file_writer; */
 /* std::shared_ptr<parquet::ArrowWriterProperties> default_writer_properties = */
		/* parquet::ArrowWriterProperties::Builder().build(); */
  /* parquet::arrow::FileWriter::Make(pool,  */
		  /* std::move(parquet_file_writer),  */
		  /* new_table->schema(), */
		  /* default_writer_properties,			        */
		  /* &file_writer); */

  /* file_writer->WriteTable(*new_table, std::numeric_limits<int64_t>::max()); */


  cout << "--- start to write table in stream buffer" << endl;
  const int64_t chunk_size = std::max(static_cast<int64_t>(1), new_table->num_rows()); 
  parquet::arrow::WriteTable(*new_table.get(), 
                               pool, out_arrow_stream, 
                              chunk_size); 
  cout << "table columns: " << new_table->num_rows() << endl; 

  auto out_arrow_buffer = out_arrow_stream->Finish();
  shared_ptr<::arrow::Buffer> out_buffer = out_arrow_buffer.ValueOrDie();

  cout << "buffer size: " << out_buffer->size() << endl;

  char *out_ptr = reinterpret_cast<char*>(const_cast<uint8_t*>(out_buffer->data()));
  outdata.append(out_ptr, out_buffer->size());
  return out_arrow_buffer.ValueOrDie();
}

// client
int write_parquet_file(bufferlist& outdata, const int& file_size)
{
  int fd;

  fd = ::open(output_file_name, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  if (fd < 0) {
    cout << "failed to open output file" << endl;
    return ERR;
  }

  outdata.write_fd(fd);

  cout << "output file size: " << outdata.length() << endl;

  return SUCCESS;
}

// 检查parquet文件是否合法
int check_parquet_file()
{
  
  bufferlist indata;
 
  cout << "--- check output file" << endl;
  int file_size = read_origin_parquet_file(indata, output_file_name);

  cout << "output file size: " << file_size << endl;
 
  std::shared_ptr<arrow::Buffer> buffer = 
	  get_arrow_buffer_from_parquet_binary(arrow::default_memory_pool(), indata);

  std::shared_ptr<arrow::Table> table = 
	   get_arrow_table_from_arrow_buffer(arrow::default_memory_pool(), buffer); 

  return SUCCESS;
}

void check_in_and_out_data(bufferlist& in, bufferlist& out)
{
  if (in.c_str() == out.c_str()) {
    cout << "indata is consistant to outdata" << endl;
  } else {
    cout << "data is break" << endl;
  }
}

void get_field_name(shared_ptr<arrow::Table> table)
{
  
}

int cls_main(bufferlist& indata, bufferlist& outdata, const int& file_size)
{
  // default memory pool
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  cout << "cls_main: memory pool" << pool << endl;
 
  std::shared_ptr<arrow::Buffer> buffer = get_arrow_buffer_from_parquet_binary(pool, indata);
  std::shared_ptr<arrow::Table> table =  get_arrow_table_from_arrow_buffer(pool, buffer);

  std::shared_ptr<arrow::Buffer> out_buffer = write_table_to_arrow_buffer(pool, table, outdata, file_size);

  
  return 0; 

}


int main()
{
  bufferlist indata, outdata;
 
  int file_size = read_origin_parquet_file(indata, parquet_file_path);

  cls_main(indata, outdata, file_size);

  write_parquet_file(outdata, file_size);

  // bug: 从buffer中读table会出错，而且返回的size大于原始size，这是因为传过来的table没有压缩 
  // check_parquet_file();

  return 0;
}



