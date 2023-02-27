#ifndef CEPH_CLS_PARQUET_SCAN_CLIENT_H
#define CEPH_CLS_PARQUET_SCAN_CLIENT_H

#include "include/rados/librados_fwd.hpp"
#include <string>

#include "include/rados/librados.hpp"

namespace rados {
  namespace cls {
    namespace parquet_scan {
      extern int sql_exec(librados::IoCtx *ioctx,
		          const std::string& oid,
			  const std::string& sql,
			  librados::bufferlist& result);
    } // namespace parquet_scan
  } // namespace cls
} // namespace rados

#endif // CEPH_CLS_PARQUET_SCAN_CLIENT_H
