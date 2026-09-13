// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// Object-local Parquet scanning over the complete aggregate input buffer.

#include <cerrno>
#include <exception>
#include <memory>
#include <new>
#include <utility>

#include "objclass/objclass.h"
#include "osd/osd_types.h"
#include "cls_parquet_scan_types.h"
#include "scan.h"

#include "arrow/buffer.h"

using ceph::bufferlist;

CLS_VER(1, 0)
CLS_NAME(parquet_scan)

namespace {

int status_to_errno(const arrow::Status& status)
{
  if (status.IsOutOfMemory()) {
    return -ENOMEM;
  }
  if (status.IsCapacityError()) {
    return -EOVERFLOW;
  }
  if (status.IsNotImplemented()) {
    return -EOPNOTSUPP;
  }
  if (status.IsInvalid() || status.IsTypeError() || status.IsKeyError() ||
      status.IsIndexError() || status.IsSerializationError() ||
      status.IsExpressionValidationError()) {
    return -EINVAL;
  }
  if (status.IsCancelled()) {
    return -ECANCELED;
  }
  return -EIO;
}

int scan(cls_method_context_t hctx, bufferlist* in, bufferlist* out)
{
  if (!hctx || !in || !out || in->length() == 0) {
    CLS_LOG(1, "parquet_scan::scan: missing context or empty Parquet input");
    return -EINVAL;
  }

  // This repository's aggregate path passes the complete object in `in` and
  // the exec request separately in ClsParmContext, not a normal OSD context.
  auto* pctx = static_cast<ClsParmContext*>(hctx);
  if (pctx->parm_data.length() == 0 ||
      pctx->parm_data.length() > ceph::parquet_scan::MAX_REQUEST_BYTES) {
    CLS_LOG(1, "parquet_scan::scan: JSON request must contain 1..%u bytes",
            ceph::parquet_scan::MAX_REQUEST_BYTES);
    return pctx->parm_data.length() == 0 ? -EINVAL : -EOVERFLOW;
  }

  try {
    const auto input = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(in->c_str()), in->length());
    auto result = ceph::parquet_scan::scan(input, pctx->parm_data.to_str());
    if (!result.ok()) {
      CLS_LOG(1, "parquet_scan::scan: %s", result.status().ToString().c_str());
      return status_to_errno(result.status());
    }

    auto scanned = std::move(result).ValueOrDie();
    if (!scanned.ipc) {
      CLS_LOG(1, "parquet_scan::scan: scanner returned no IPC buffer");
      return -EIO;
    }
    ceph::parquet_scan::ScanResponse response;
    response.stats_json = std::move(scanned.stats_json);
    response.ipc.append(reinterpret_cast<const char*>(scanned.ipc->data()),
                        scanned.ipc->size());
    encode(response, *out);
    return 0;
  } catch (const ceph::buffer::error& error) {
    CLS_LOG(1, "parquet_scan::scan: malformed buffer: %s", error.what());
    return -EINVAL;
  } catch (const std::bad_alloc& error) {
    CLS_LOG(1, "parquet_scan::scan: allocation failed: %s", error.what());
    return -ENOMEM;
  } catch (const std::exception& error) {
    CLS_LOG(1, "parquet_scan::scan: unexpected failure: %s", error.what());
    return -EIO;
  } catch (...) {
    CLS_LOG(1, "parquet_scan::scan: unknown failure");
    return -EIO;
  }
}

} // anonymous namespace

CLS_INIT(parquet_scan)
{
  CLS_LOG(5, "loading cls_parquet_scan");
  cls_handle_t h_class;
  cls_method_handle_t h_scan;
  cls_register("parquet_scan", &h_class);
  cls_register_cxx_method(h_class, "scan", CLS_METHOD_RD, scan, &h_scan);
}
