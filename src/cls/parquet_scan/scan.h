// Object-local Parquet scan. The caller supplies the complete object buffer.
#pragma once

#include <memory>
#include <string>

#include <arrow/buffer.h>
#include <arrow/result.h>

namespace ceph::parquet_scan {

struct ScanResult {
  std::shared_ptr<arrow::Buffer> ipc;
  std::string stats_json;
};

// JSON v1: projection OR aggregates, optional predicate, and a row-mode LIMIT.
// Numeric/bool/string top-level dependencies only; unsupported types fail.
// Comparisons drop NULL rows and AND/OR use Kleene semantics. SUM checks integer
// overflow; AVG returns double. Empty COUNT is zero; other empty aggregates NULL.
// Statistics describe evaluated batches before LIMIT: rows_matched can exceed
// rows_returned. Unvisited groups after LIMIT are neither scanned nor skipped.
// columns_read counts unique selected physical columns, zero when none decoded.
// The IPC byte cap includes schema and EOS; exceeding it returns an error, never
// a truncated success. It is not a cap on total Parquet decoder memory.
arrow::Result<ScanResult> scan(const std::shared_ptr<arrow::Buffer>& input,
                               const std::string& request_json);

}  // namespace ceph::parquet_scan
