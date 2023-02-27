/*
 * cls_parquet_scan_client.cc
 *
 * 声明parquet_scan类的方法
 *
 */

#include "cls/parquet_scan/cls_parquet_scan_client.h"
#include "include/encoding.h"
#include "include/rados/librados.hpp"

#include <errno.h>
#include <sstream>

using librados::bufferlist;

namespace rados {
  namespace cls {
    namespace parquet_scan {
      int sql_exec(librados::IoCtx *ioctx,
	           const std::string& oid,
		   const std::string& sql,
		   bufferlist& result)
      {
        bufferlist in;
	// sql语句编码进in
	encode(sql, in);

	return ioctx->exec(oid, "parquet_scan", "sql_exec", in, result);
      }   
    } // namespace parquet_scan
  } // namespace cls
} // namespace rados
