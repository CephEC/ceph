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

#include "include/buffer.h"

#include "arrow/device.h"
#include "arrow/io/memory.h"
#include "arrow/api.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/type_fwd.h"
#include "arrow/table.h"
#include "arrow/util/utf8.h"
#include "arrow/array.h"
#include "arrow/type.h"

#include "parquet/schema.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/api/reader.h"
#include "parquet/api/writer.h"
#include "parquet/file_writer.h"
#include "parquet/column_reader.h"
#include "parquet/test_util.h"


#include <gtest/gtest.h>


using namespace std;
using namespace arrow;


const char* parquet_file_path = "/home/root/fhvhv_tripdata_2022-11.parquet";
const int file_size = 464298215;

TEST(Bl2Arrow, BufferlistToArrowTable) {
  // 打开parquet文件
  int fd = ::open(parquet_file_path, O_RDONLY);
  EXPECT_LE(0, fd);

  // read parquet
  bufferlist bl;
  EXPECT_EQ(file_size, (unsigned)bl.read_fd(fd, file_size));
 
  EXPECT_EQ(file_size, bl.length());

  auto arrow_buffer = std::make_shared<arrow::Buffer>((uint8_t*)bl.c_str(), file_size);  

  EXPECT_EQ(arrow_buffer->ToString(), bl.to_str());
  // bufferlist to arrow buffer

  auto arrow_buffer_reader = std::make_shared<::arrow::io::BufferReader>(arrow_buffer);
  
  EXPECT_EQ(arrow_buffer_reader->buffer()->ToString(), bl.to_str());
  // 从文件流中解析parquet文件
  auto parquet_reader = parquet::ParquetFileReader::Open(std::move(arrow_buffer_reader));
 // std::shared_ptr<parquet::FileMetaData> metadata = parquet_reader->metadata();
 //  std::shared_ptr<parquet::Statistics> stats = metadata->RowGroup(0)->ColumnChunk(0)->statistics();

//  cout << metadata->SerializeToString() << endl;

  ::close(fd);

}
