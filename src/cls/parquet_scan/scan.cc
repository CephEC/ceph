#include "scan.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <map>
#include <set>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/utf8.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>

#include "json_spirit/json_spirit.h"

namespace ceph::parquet_scan {
namespace {
using arrow::Result;
using arrow::Status;
using json_spirit::Value;
constexpr size_t max_request_bytes = 1024 * 1024;
constexpr unsigned max_predicate_nodes = 256;
constexpr unsigned max_predicate_depth = 32;

// json_spirit accepts some non-JSON numeric spellings. Validate the grammar and
// bound recursion BEFORE invoking its recursive parser. Integer tokens must fit
// int64/uint64; otherwise a parser fallback to double would lose precision.
class JsonSyntax {
 public:
  explicit JsonSyntax(const std::string& text) : text(text) {}
  bool valid() {
    if (!value(0)) return false;
    space();
    return pos == text.size();
  }
 private:
  const std::string& text;
  size_t pos = 0;
  unsigned nodes = 0;
  void space() {
    while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\n' ||
           text[pos] == '\r' || text[pos] == '\t')) ++pos;
  }
  bool take(char c) {
    space();
    if (pos == text.size() || text[pos] != c) return false;
    ++pos;
    return true;
  }
  bool string() {
    if (!take('"')) return false;
    while (pos < text.size()) {
      const unsigned char c = text[pos++];
      if (c == '"') return true;
      if (c < 0x20) return false;
      if (c != '\\') continue;
      if (pos == text.size()) return false;
      const char escaped = text[pos++];
      if (escaped == 'u') {
        for (int i = 0; i != 4; ++i) {
          if (pos == text.size()) return false;
          const char h = text[pos++];
          if (!((h >= '0' && h <= '9') || (h >= 'a' && h <= 'f') ||
                (h >= 'A' && h <= 'F'))) return false;
        }
      } else if (std::string_view("\"\\/bfnrt").find(escaped) == std::string_view::npos) {
        return false;
      }
    }
    return false;
  }
  bool digit() const {
    return pos < text.size() && text[pos] >= '0' && text[pos] <= '9';
  }
  bool number() {
    const size_t start = pos;
    if (pos < text.size() && text[pos] == '-') ++pos;
    if (!digit()) return false;
    if (text[pos] == '0') ++pos;
    else while (digit()) ++pos;
    bool integer = true;
    if (pos < text.size() && text[pos] == '.') {
      integer = false;
      ++pos;
      if (!digit()) return false;
      while (digit()) ++pos;
    }
    if (pos < text.size() && (text[pos] == 'e' || text[pos] == 'E')) {
      integer = false;
      ++pos;
      if (pos < text.size() && (text[pos] == '+' || text[pos] == '-')) ++pos;
      if (!digit()) return false;
      while (digit()) ++pos;
    }
    if (integer) {
      const char* begin = text.data() + start;
      const char* end = text.data() + pos;
      if (*begin == '-') {
        int64_t v;
        if (std::from_chars(begin, end, v).ec != std::errc()) return false;
      } else {
        uint64_t v;
        if (std::from_chars(begin, end, v).ec != std::errc()) return false;
      }
    }
    return true;
  }
  bool value(unsigned depth) {
    if (depth > 64 || ++nodes > 4096) return false;
    space();
    if (pos == text.size()) return false;
    const char c = text[pos];
    if (c == '"') return string();
    if (c == '{' || c == '[') {
      ++pos;
      const char close = c == '{' ? '}' : ']';
      if (take(close)) return true;
      do {
        if (c == '{' && (!string() || !take(':'))) return false;
        if (!value(depth + 1)) return false;
        if (take(close)) return true;
      } while (take(','));
      return false;
    }
    for (const auto literal : {"true", "false", "null"}) {
      const size_t n = std::strlen(literal);
      if (text.compare(pos, n, literal) == 0) {
        pos += n;
        return true;
      }
    }
    return number();
  }
};

using Object = std::map<std::string, const Value*>;
Result<Object> object(const Value& v, std::initializer_list<const char*> allowed) {
  if (v.type() != json_spirit::obj_type) return Status::Invalid("expected JSON object");
  Object out;
  for (const auto& pair : v.get_obj()) {
    if (std::find(allowed.begin(), allowed.end(), pair.name_) == allowed.end())
      return Status::Invalid("unknown field: ", pair.name_);
    if (!out.emplace(pair.name_, &pair.value_).second)
      return Status::Invalid("duplicate field: ", pair.name_);
  }
  return out;
}
Result<std::string> required_string(const Object& obj, const char* key) {
  const auto it = obj.find(key);
  if (it == obj.end() || it->second->type() != json_spirit::str_type)
    return Status::Invalid("required string field: ", key);
  return it->second->get_str();
}
Result<uint64_t> nonnegative(const Value& v, const char* key) {
  if (v.type() != json_spirit::int_type || (!v.is_uint64() && v.get_int64() < 0))
    return Status::Invalid(key, " must be a nonnegative integer");
  return v.get_uint64();
}

struct Expr {
  std::string op;
  std::string column;
  Value value;
  std::vector<Expr> args;
  int field = -1;
  int leaf = -1;
  std::shared_ptr<arrow::Scalar> scalar;
};
Result<Expr> parse_expr(const Value& v, unsigned depth, unsigned* nodes) {
  if (depth > max_predicate_depth || ++*nodes > max_predicate_nodes)
    return Status::Invalid("predicate exceeds depth/node limit");
  ARROW_ASSIGN_OR_RAISE(auto obj, object(v, {"op", "column", "value", "args"}));
  Expr expr;
  ARROW_ASSIGN_OR_RAISE(expr.op, required_string(obj, "op"));
  if (expr.op == "and" || expr.op == "or") {
    if (obj.size() != 2 || !obj.count("args") ||
        obj.at("args")->type() != json_spirit::array_type ||
        obj.at("args")->get_array().empty())
      return Status::Invalid("and/or require only op and nonempty args");
    for (const auto& arg : obj.at("args")->get_array()) {
      ARROW_ASSIGN_OR_RAISE(auto child, parse_expr(arg, depth + 1, nodes));
      expr.args.push_back(std::move(child));
    }
  } else {
    ARROW_ASSIGN_OR_RAISE(expr.column, required_string(obj, "column"));
    if (expr.op == "is_null" || expr.op == "is_not_null") {
      if (obj.size() != 2) return Status::Invalid("null test accepts only op and column");
    } else {
      if (expr.op != "eq" && expr.op != "ne" && expr.op != "lt" &&
          expr.op != "le" && expr.op != "gt" && expr.op != "ge")
        return Status::Invalid("unsupported predicate op: ", expr.op);
      if (obj.size() != 3 || !obj.count("value"))
        return Status::Invalid("comparison requires only op, column and value");
      expr.value = *obj.at("value");
      const auto type = expr.value.type();
      if (type != json_spirit::bool_type && type != json_spirit::int_type &&
          type != json_spirit::real_type && type != json_spirit::str_type)
        return Status::Invalid("comparison value must be bool, number or string");
    }
  }
  return expr;
}
struct Aggregate {
  std::string op;
  std::string name;
  std::string column;
  bool star = false;
  int field = -1;
  uint64_t count = 0;
  int64_t signed_sum = 0;
  uint64_t unsigned_sum = 0;
  double floating_sum = 0;
  long double average_sum = 0;
  std::shared_ptr<arrow::DataType> type;
  std::shared_ptr<arrow::Scalar> extreme;
};
struct Request {
  bool has_projection = false;
  std::vector<std::string> projection;
  bool has_predicate = false;
  Expr predicate;
  std::vector<Aggregate> aggregates;
  uint64_t limit = std::numeric_limits<uint64_t>::max();
  int64_t batch_size = 8192;
  int64_t max_output_bytes = 16777216;
};
// The bundled json_spirit handles BMP escapes but not UTF-16 surrogate pairs.
// Normalize just those pairs to UTF-8 without changing other JSON tokens.
Result<std::string> normalize_surrogates(const std::string& text) {
  std::string out;
  size_t copied = 0;
  const auto hex4 = [&](size_t offset) {
    unsigned value = 0;
    for (size_t j = offset; j < offset + 4; ++j) {
      const char c = text[j];
      const unsigned digit = c <= '9' ? c - '0' : (c <= 'F' ? c - 'A' + 10 : c - 'a' + 10);
      value = (value << 4) | digit;
    }
    return value;
  };
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] != '\\') continue;
    if (text[++i] != 'u') continue;
    const size_t start = i - 1;
    unsigned codepoint = hex4(i + 1);
    i += 4;
    if (codepoint >= 0xdc00 && codepoint <= 0xdfff)
      return Status::Invalid("unpaired low Unicode surrogate");
    if (codepoint < 0xd800 || codepoint > 0xdbff) continue;
    if (i + 6 >= text.size() || text[i + 1] != '\\' || text[i + 2] != 'u')
      return Status::Invalid("unpaired high Unicode surrogate");
    const unsigned low = hex4(i + 3);
    if (low < 0xdc00 || low > 0xdfff) return Status::Invalid("invalid Unicode surrogate pair");
    codepoint = 0x10000 + ((codepoint - 0xd800) << 10) + low - 0xdc00;
    out.append(text, copied, start - copied);
    out.push_back(static_cast<char>(0xf0 | (codepoint >> 18)));
    out.push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3f)));
    out.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3f)));
    out.push_back(static_cast<char>(0x80 | (codepoint & 0x3f)));
    i += 6;
    copied = i + 1;
  }
  if (copied != 0) out.append(text, copied, text.size() - copied);
  return out;
}

Result<Request> parse_request(const std::string& text) {
  if (text.empty() || text.size() > max_request_bytes)
    return Status::Invalid("request must contain 1..1048576 bytes");
  if (!JsonSyntax(text).valid()) return Status::Invalid("invalid or excessively complex JSON");
  arrow::util::InitializeUTF8();
  if (!arrow::util::ValidateUTF8(reinterpret_cast<const uint8_t*>(text.data()), text.size()))
    return Status::Invalid("request is not UTF-8");
  ARROW_ASSIGN_OR_RAISE(auto normalized, normalize_surrogates(text));
  const auto& json = normalized.empty() ? text : normalized;
  Value root;
  auto begin = json.cbegin();
  if (!json_spirit::read(begin, json.cend(), root)) return Status::Invalid("invalid JSON");
  ARROW_ASSIGN_OR_RAISE(auto obj, object(root, {"version", "projection", "predicate",
      "aggregates", "limit", "batch_size", "max_output_bytes"}));
  if (!obj.count("version")) return Status::Invalid("version is required");
  ARROW_ASSIGN_OR_RAISE(auto version, nonnegative(*obj.at("version"), "version"));
  if (version != 1) return Status::Invalid("unsupported request version");
  Request req;
  if (obj.count("projection")) {
    req.has_projection = true;
    const auto& v = *obj.at("projection");
    if (v.type() != json_spirit::array_type) return Status::Invalid("projection must be an array");
    std::set<std::string> names;
    for (const auto& col : v.get_array()) {
      if (col.type() != json_spirit::str_type || !names.insert(col.get_str()).second)
        return Status::Invalid("projection requires unique column names");
      req.projection.push_back(col.get_str());
    }
  }
  if (obj.count("predicate")) {
    req.has_predicate = true;
    unsigned nodes = 0;
    ARROW_ASSIGN_OR_RAISE(req.predicate, parse_expr(*obj.at("predicate"), 1, &nodes));
  }
  if (obj.count("aggregates")) {
    if (req.has_projection || obj.count("limit"))
      return Status::Invalid("aggregates cannot be combined with projection or limit");
    const auto& v = *obj.at("aggregates");
    if (v.type() != json_spirit::array_type || v.get_array().empty())
      return Status::Invalid("aggregates must be a nonempty array");
    std::set<std::string> names;
    for (const auto& item : v.get_array()) {
      ARROW_ASSIGN_OR_RAISE(auto spec, object(item, {"op", "column", "as"}));
      Aggregate agg;
      ARROW_ASSIGN_OR_RAISE(agg.op, required_string(spec, "op"));
      ARROW_ASSIGN_OR_RAISE(agg.name, required_string(spec, "as"));
      if (!names.insert(agg.name).second) return Status::Invalid("duplicate aggregate output name");
      if (agg.op != "count" && agg.op != "sum" && agg.op != "min" &&
          agg.op != "max" && agg.op != "avg") return Status::Invalid("unsupported aggregate op");
      agg.star = agg.op == "count" && !spec.count("column");
      if (!agg.star) {
        ARROW_ASSIGN_OR_RAISE(agg.column, required_string(spec, "column"));
      }
      req.aggregates.push_back(std::move(agg));
    }
  }
  if (obj.count("limit")) {
    ARROW_ASSIGN_OR_RAISE(req.limit, nonnegative(*obj.at("limit"), "limit"));
  }
  if (obj.count("batch_size")) {
    ARROW_ASSIGN_OR_RAISE(auto n, nonnegative(*obj.at("batch_size"), "batch_size"));
    if (n < 1 || n > 65536) return Status::Invalid("batch_size must be in [1,65536]");
    req.batch_size = n;
  }
  if (obj.count("max_output_bytes")) {
    ARROW_ASSIGN_OR_RAISE(auto n, nonnegative(*obj.at("max_output_bytes"), "max_output_bytes"));
    if (n < 1 || n > 67108864) return Status::Invalid("max_output_bytes must be in [1,67108864]");
    req.max_output_bytes = n;
  }
  return req;
}

bool signed_integer(arrow::Type::type id) {
  return id == arrow::Type::INT8 || id == arrow::Type::INT16 ||
         id == arrow::Type::INT32 || id == arrow::Type::INT64;
}
bool unsigned_integer(arrow::Type::type id) {
  return id == arrow::Type::UINT8 || id == arrow::Type::UINT16 ||
         id == arrow::Type::UINT32 || id == arrow::Type::UINT64;
}
bool floating(arrow::Type::type id) {
  return id == arrow::Type::FLOAT || id == arrow::Type::DOUBLE;
}
bool strings(arrow::Type::type id) {
  return id == arrow::Type::STRING || id == arrow::Type::LARGE_STRING;
}
Result<int> resolve_column(const std::string& name, const arrow::Schema& schema,
                           const parquet::arrow::SchemaManifest& manifest,
                           std::set<int>* dependencies) {
  const auto indices = schema.GetAllFieldIndices(name);
  if (indices.size() != 1) return Status::Invalid("unknown or ambiguous column: ", name);
  const int field = indices.front();
  const auto id = schema.field(field)->type()->id();
  if (!signed_integer(id) && !unsigned_integer(id) && !floating(id) &&
      !strings(id) && id != arrow::Type::BOOL)
    return Status::NotImplemented("unsupported column type: ", name, ": ",
                                  schema.field(field)->type()->ToString());
  if (static_cast<size_t>(field) >= manifest.schema_fields.size() ||
      !manifest.schema_fields[field].is_leaf() ||
      !manifest.schema_fields[field].children.empty())
    return Status::NotImplemented("nested column dependency: ", name);
  dependencies->insert(field);
  return field;
}

Result<std::shared_ptr<arrow::Scalar>> literal(
    const Value& v, const std::shared_ptr<arrow::DataType>& type) {
  const auto id = type->id();
  if (signed_integer(id) || unsigned_integer(id)) {
    if (v.type() != json_spirit::int_type)
      return Status::Invalid("integer column requires an integer literal");
    std::shared_ptr<arrow::Scalar> scalar;
    if (v.is_uint64()) scalar = std::make_shared<arrow::UInt64Scalar>(v.get_uint64());
    else scalar = std::make_shared<arrow::Int64Scalar>(v.get_int64());
    // Safe integer-to-integer casts reject negative unsigned and range overflow.
    ARROW_ASSIGN_OR_RAISE(auto converted, arrow::compute::Cast(arrow::Datum(scalar), type));
    return converted.scalar();
  }
  if (floating(id)) {
    if (v.type() != json_spirit::real_type && v.type() != json_spirit::int_type)
      return Status::Invalid("floating column requires a numeric literal");
    const long double exact = v.type() == json_spirit::real_type ?
        static_cast<long double>(v.get_real()) :
        (v.is_uint64() ? static_cast<long double>(v.get_uint64()) :
                         static_cast<long double>(v.get_int64()));
    const double converted = id == arrow::Type::FLOAT ?
        static_cast<double>(static_cast<float>(exact)) : static_cast<double>(exact);
    if (!std::isfinite(converted))
      return Status::Invalid("nonfinite or out-of-range numeric literal");
    if (v.type() == json_spirit::int_type && static_cast<long double>(converted) != exact)
      return Status::Invalid("integer literal is not exactly representable by floating column");
    if (id == arrow::Type::FLOAT)
      return std::static_pointer_cast<arrow::Scalar>(
          std::make_shared<arrow::FloatScalar>(static_cast<float>(converted)));
    return std::static_pointer_cast<arrow::Scalar>(std::make_shared<arrow::DoubleScalar>(converted));
  }
  if (id == arrow::Type::BOOL && v.type() == json_spirit::bool_type)
    return std::static_pointer_cast<arrow::Scalar>(std::make_shared<arrow::BooleanScalar>(v.get_bool()));
  if (strings(id) && v.type() == json_spirit::str_type) {
    if (id == arrow::Type::STRING)
      return std::static_pointer_cast<arrow::Scalar>(std::make_shared<arrow::StringScalar>(v.get_str()));
    return std::static_pointer_cast<arrow::Scalar>(std::make_shared<arrow::LargeStringScalar>(v.get_str()));
  }
  return Status::Invalid("literal type does not match column type");
}
Status bind_expr(Expr* expr, const arrow::Schema& schema,
                 const parquet::arrow::SchemaManifest& manifest,
                 std::set<int>* dependencies) {
  if (!expr->args.empty()) {
    for (auto& arg : expr->args) ARROW_RETURN_NOT_OK(bind_expr(&arg, schema, manifest, dependencies));
    return Status::OK();
  }
  ARROW_ASSIGN_OR_RAISE(expr->field, resolve_column(expr->column, schema, manifest, dependencies));
  expr->leaf = manifest.schema_fields[expr->field].column_index;
  if (expr->op != "is_null" && expr->op != "is_not_null") {
    ARROW_ASSIGN_OR_RAISE(expr->scalar, literal(expr->value, schema.field(expr->field)->type()));
  }
  return Status::OK();
}
const char* comparison(const std::string& op) {
  if (op == "eq") return "equal";
  if (op == "ne") return "not_equal";
  if (op == "lt") return "less";
  if (op == "le") return "less_equal";
  if (op == "gt") return "greater";
  return "greater_equal";
}
Result<arrow::Datum> evaluate(const Expr& expr, const arrow::RecordBatch& batch,
                              const std::vector<int>& positions) {
  if (!expr.args.empty()) {
    ARROW_ASSIGN_OR_RAISE(auto result, evaluate(expr.args.front(), batch, positions));
    for (size_t i = 1; i < expr.args.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto right, evaluate(expr.args[i], batch, positions));
      ARROW_ASSIGN_OR_RAISE(result, arrow::compute::CallFunction(
          expr.op == "and" ? "and_kleene" : "or_kleene", {result, right}));
    }
    return result;
  }
  const arrow::Datum values(batch.column(positions[expr.field]));
  if (expr.op == "is_null") return arrow::compute::CallFunction("is_null", {values});
  if (expr.op == "is_not_null") return arrow::compute::CallFunction("is_valid", {values});
  return arrow::compute::CallFunction(comparison(expr.op), {values, arrow::Datum(expr.scalar)});
}

template <typename T>
bool range_may_match(const std::string& op, T min, T max, T value) {
  if (min > max) return true;  // Malformed/uncertain statistics never eliminate rows.
  if (op == "eq") return value >= min && value <= max;
  if (op == "ne") return min != value || max != value;
  if (op == "lt") return min < value;
  if (op == "le") return min <= value;
  if (op == "gt") return max > value;
  return max >= value;
}
int64_t signed_value(const arrow::Scalar& scalar) {
  switch (scalar.type->id()) {
    case arrow::Type::INT8: return static_cast<const arrow::Int8Scalar&>(scalar).value;
    case arrow::Type::INT16: return static_cast<const arrow::Int16Scalar&>(scalar).value;
    case arrow::Type::INT32: return static_cast<const arrow::Int32Scalar&>(scalar).value;
    default: return static_cast<const arrow::Int64Scalar&>(scalar).value;
  }
}
bool may_match(const Expr& expr, const parquet::RowGroupMetaData& group) {
  if (!expr.args.empty()) {
    if (expr.op == "and") {
      for (const auto& arg : expr.args) if (!may_match(arg, group)) return false;
      return true;
    }
    for (const auto& arg : expr.args) if (may_match(arg, group)) return true;
    return false;
  }
  const auto chunk = group.ColumnChunk(expr.leaf);
  if (!chunk->is_stats_set()) return true;
  const auto stats = chunk->statistics();
  if (!stats) return true;
  const bool known_nulls = stats->HasNullCount() && stats->null_count() >= 0 &&
                           stats->null_count() <= group.num_rows();
  if (expr.op == "is_null") return !known_nulls || stats->null_count() != 0;
  if (expr.op == "is_not_null") return !known_nulls || stats->null_count() != group.num_rows();
  if (known_nulls && stats->null_count() == group.num_rows()) return false;
  if (!stats->HasMinMax()) return true;
  // Only signed physical integer statistics have unambiguous ordering here.
  // Do not convert int64 statistics or constants to double (not even transiently).
  if (!signed_integer(expr.scalar->type->id())) return true;
  const int64_t value = signed_value(*expr.scalar);
  if (chunk->type() == parquet::Type::INT64) {
    const auto typed = std::dynamic_pointer_cast<parquet::Int64Statistics>(stats);
    return !typed || range_may_match(expr.op, typed->min(), typed->max(), value);
  }
  if (chunk->type() == parquet::Type::INT32) {
    const auto typed = std::dynamic_pointer_cast<parquet::Int32Statistics>(stats);
    return !typed || range_may_match<int64_t>(expr.op, typed->min(), typed->max(), value);
  }
  return true;
}

// Enforce the cap at every IPC write, including schema, padding and end marker.
// No full result table is retained; the only accumulated bytes are this stream.
class BoundedOutput final : public arrow::io::OutputStream {
 public:
  BoundedOutput(std::shared_ptr<arrow::io::BufferOutputStream> stream, int64_t maximum)
      : stream(std::move(stream)), maximum(maximum) {
    set_mode(arrow::io::FileMode::WRITE);
  }
  Status Close() override { return stream->Close(); }
  bool closed() const override { return stream->closed(); }
  Result<int64_t> Tell() const override { return position; }
  Status Write(const void* data, int64_t size) override {
    if (size < 0) return Status::Invalid("negative IPC write size");
    if (size > maximum - position) return Status::CapacityError("max_output_bytes exceeded");
    ARROW_RETURN_NOT_OK(stream->Write(data, size));
    position += size;
    return Status::OK();
  }
  Result<std::shared_ptr<arrow::Buffer>> Finish() { return stream->Finish(); }
 private:
  std::shared_ptr<arrow::io::BufferOutputStream> stream;
  int64_t maximum;
  int64_t position = 0;
};

Status add_count(uint64_t amount, uint64_t* total) {
  if (amount > std::numeric_limits<uint64_t>::max() - *total)
    return Status::CapacityError("row count overflow");
  *total += amount;
  return Status::OK();
}
template <typename Array>
Status signed_sum(const arrow::Array& input, int64_t* total) {
  const auto& array = static_cast<const Array&>(input);
  for (int64_t i = 0; i < array.length(); ++i) {
    if (!array.IsNull(i) && __builtin_add_overflow(*total, static_cast<int64_t>(array.Value(i)), total))
      return Status::CapacityError("signed integer SUM overflow");
  }
  return Status::OK();
}
template <typename Array>
Status unsigned_sum(const arrow::Array& input, uint64_t* total) {
  const auto& array = static_cast<const Array&>(input);
  for (int64_t i = 0; i < array.length(); ++i) {
    if (!array.IsNull(i) && __builtin_add_overflow(*total, static_cast<uint64_t>(array.Value(i)), total))
      return Status::CapacityError("unsigned integer SUM overflow");
  }
  return Status::OK();
}
Status sum_integer(Aggregate* agg, const arrow::Array& array) {
  switch (array.type_id()) {
    case arrow::Type::INT8: return signed_sum<arrow::Int8Array>(array, &agg->signed_sum);
    case arrow::Type::INT16: return signed_sum<arrow::Int16Array>(array, &agg->signed_sum);
    case arrow::Type::INT32: return signed_sum<arrow::Int32Array>(array, &agg->signed_sum);
    case arrow::Type::INT64: return signed_sum<arrow::Int64Array>(array, &agg->signed_sum);
    case arrow::Type::UINT8: return unsigned_sum<arrow::UInt8Array>(array, &agg->unsigned_sum);
    case arrow::Type::UINT16: return unsigned_sum<arrow::UInt16Array>(array, &agg->unsigned_sum);
    case arrow::Type::UINT32: return unsigned_sum<arrow::UInt32Array>(array, &agg->unsigned_sum);
    case arrow::Type::UINT64: return unsigned_sum<arrow::UInt64Array>(array, &agg->unsigned_sum);
    default: return Status::TypeError("integer SUM requires integer column");
  }
}

template <typename Array>
Result<std::shared_ptr<arrow::Scalar>> string_extreme(const arrow::Array& input, bool minimum) {
  const auto& array = static_cast<const Array&>(input);
  int64_t best = -1;
  for (int64_t i = 0; i < array.length(); ++i) {
    if (array.IsNull(i)) continue;
    if (best == -1 || (minimum ? array.GetView(i) < array.GetView(best) :
                               array.GetView(i) > array.GetView(best))) best = i;
  }
  if (best == -1) return arrow::MakeNullScalar(input.type());
  return array.GetScalar(best);
}
Status accumulate(Aggregate* agg, const arrow::RecordBatch& batch,
                   const std::vector<int>& positions) {
  if (agg->star) return add_count(batch.num_rows(), &agg->count);
  const auto& array = batch.column(positions[agg->field]);
  ARROW_ASSIGN_OR_RAISE(auto counted, arrow::compute::Count(array));
  const int64_t count = static_cast<const arrow::Int64Scalar&>(*counted.scalar()).value;
  ARROW_RETURN_NOT_OK(add_count(count, &agg->count));
  if (agg->op == "count" || count == 0) return Status::OK();
  const auto id = array->type_id();
  if (agg->op == "sum") {
    // Arrow 6 integer sum kernels wrap. Perform checked accumulation instead.
    if (signed_integer(id) || unsigned_integer(id)) return sum_integer(agg, *array);
    ARROW_ASSIGN_OR_RAISE(auto sum, arrow::compute::Sum(array));
    agg->floating_sum += static_cast<const arrow::DoubleScalar&>(*sum.scalar()).value;
  } else if (agg->op == "avg") {
    // AVG intentionally has floating output; integer predicates and SUM never
    // take this conversion path. Mean avoids overflowing integer batch sums.
    auto options = arrow::compute::CastOptions::Safe();
    options.allow_float_truncate = true;  // AVG's documented double result.
    ARROW_ASSIGN_OR_RAISE(auto values, arrow::compute::Cast(*array, arrow::float64(), options));
    ARROW_ASSIGN_OR_RAISE(auto mean, arrow::compute::Mean(values));
    agg->average_sum += static_cast<long double>(
        static_cast<const arrow::DoubleScalar&>(*mean.scalar()).value) * count;
  } else {
    std::shared_ptr<arrow::Scalar> candidate;
    if (id == arrow::Type::STRING) {
      ARROW_ASSIGN_OR_RAISE(candidate, string_extreme<arrow::StringArray>(*array, agg->op == "min"));
    } else if (id == arrow::Type::LARGE_STRING) {
      ARROW_ASSIGN_OR_RAISE(candidate, string_extreme<arrow::LargeStringArray>(*array, agg->op == "min"));
    } else {
      ARROW_ASSIGN_OR_RAISE(auto pair, arrow::compute::MinMax(array));
      const auto& values = static_cast<const arrow::StructScalar&>(*pair.scalar()).value;
      candidate = values[agg->op == "min" ? 0 : 1];
    }
    if (!agg->extreme) agg->extreme = std::move(candidate);
    else {
      ARROW_ASSIGN_OR_RAISE(auto better, arrow::compute::CallFunction(
          agg->op == "min" ? "less" : "greater", {arrow::Datum(candidate), arrow::Datum(agg->extreme)}));
      const auto& boolean = static_cast<const arrow::BooleanScalar&>(*better.scalar());
      if (boolean.is_valid && boolean.value) agg->extreme = std::move(candidate);
    }
  }
  return Status::OK();
}
Result<std::shared_ptr<arrow::Scalar>> aggregate_value(const Aggregate& agg) {
  if (agg.op == "count")
    return std::static_pointer_cast<arrow::Scalar>(std::make_shared<arrow::UInt64Scalar>(agg.count));
  if (agg.count == 0) return arrow::MakeNullScalar(agg.type);
  if (agg.op == "min" || agg.op == "max") return agg.extreme;
  if (agg.op == "avg")
    return std::static_pointer_cast<arrow::Scalar>(
        std::make_shared<arrow::DoubleScalar>(static_cast<double>(agg.average_sum / agg.count)));
  if (agg.type->id() == arrow::Type::INT64)
    return std::static_pointer_cast<arrow::Scalar>(std::make_shared<arrow::Int64Scalar>(agg.signed_sum));
  if (agg.type->id() == arrow::Type::UINT64)
    return std::static_pointer_cast<arrow::Scalar>(std::make_shared<arrow::UInt64Scalar>(agg.unsigned_sum));
  return std::static_pointer_cast<arrow::Scalar>(std::make_shared<arrow::DoubleScalar>(agg.floating_sum));
}

struct Stats {
  uint64_t row_groups_total = 0;
  uint64_t row_groups_skipped = 0;
  uint64_t row_groups_scanned = 0;
  uint64_t columns_read = 0;
  uint64_t rows_scanned = 0;
  uint64_t rows_matched = 0;
  uint64_t rows_returned = 0;
  uint64_t output_bytes = 0;
  std::string json() const {
    return "{\"row_groups_total\":" + std::to_string(row_groups_total) +
        ",\"row_groups_skipped\":" + std::to_string(row_groups_skipped) +
        ",\"row_groups_scanned\":" + std::to_string(row_groups_scanned) +
        ",\"columns_read\":" + std::to_string(columns_read) +
        ",\"rows_scanned\":" + std::to_string(rows_scanned) +
        ",\"rows_matched\":" + std::to_string(rows_matched) +
        ",\"rows_returned\":" + std::to_string(rows_returned) +
        ",\"output_bytes\":" + std::to_string(output_bytes) + "}";
  }
};

Result<ScanResult> scan_impl(const std::shared_ptr<arrow::Buffer>& input,
                             const std::string& request_json) {
  ARROW_ASSIGN_OR_RAISE(auto req, parse_request(request_json));
  if (!input || input->size() == 0) return Status::Invalid("empty Parquet object");
  auto source = std::make_shared<arrow::io::BufferReader>(input);
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(source, arrow::default_memory_pool(), &reader));
  reader->set_batch_size(req.batch_size);
  reader->set_use_threads(false);
  std::shared_ptr<arrow::Schema> schema;
  ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));
  const auto& manifest = reader->manifest();
  const auto metadata = reader->parquet_reader()->metadata();
  if (metadata->num_rows() < 0 || metadata->num_row_groups() < 0)
    return Status::Invalid("invalid Parquet row counts");
  std::set<int> dependencies;
  if (req.has_predicate)
    ARROW_RETURN_NOT_OK(bind_expr(&req.predicate, *schema, manifest, &dependencies));
  std::vector<int> projection;
  std::vector<std::shared_ptr<arrow::Field>> output_fields;
  if (req.aggregates.empty()) {
    if (!req.has_projection) {
      for (const auto& field : schema->fields()) req.projection.push_back(field->name());
    }
    for (const auto& name : req.projection) {
      ARROW_ASSIGN_OR_RAISE(auto index, resolve_column(name, *schema, manifest, &dependencies));
      projection.push_back(index);
      output_fields.push_back(schema->field(index));
    }
  } else {
    for (auto& agg : req.aggregates) {
      if (!agg.star) {
        ARROW_ASSIGN_OR_RAISE(agg.field, resolve_column(agg.column, *schema, manifest, &dependencies));
        const auto id = schema->field(agg.field)->type()->id();
        if ((agg.op == "sum" || agg.op == "avg") &&
            !signed_integer(id) && !unsigned_integer(id) && !floating(id))
          return Status::NotImplemented(agg.op, " requires a numeric column");
        if (agg.op == "min" || agg.op == "max") agg.type = schema->field(agg.field)->type();
        else if (agg.op == "sum")
          agg.type = signed_integer(id) ? arrow::int64() :
                     (unsigned_integer(id) ? arrow::uint64() : arrow::float64());
      }
      if (agg.op == "count") agg.type = arrow::uint64();
      if (agg.op == "avg") agg.type = arrow::float64();
      output_fields.push_back(arrow::field(agg.name, agg.type, agg.op != "count"));
    }
  }
  const auto output_schema = arrow::schema(std::move(output_fields));
  std::vector<int> leaves;
  std::vector<int> positions(schema->num_fields(), -1);
  std::vector<std::shared_ptr<arrow::Field>> read_fields;
  for (const int field : dependencies) {
    positions[field] = leaves.size();
    leaves.push_back(manifest.schema_fields[field].column_index);
    read_fields.push_back(schema->field(field));
  }
  const auto read_schema = arrow::schema(std::move(read_fields));
  ARROW_ASSIGN_OR_RAISE(auto buffer_stream,
                        arrow::io::BufferOutputStream::Create(std::min<int64_t>(4096, req.max_output_bytes)));
  auto sink = std::make_shared<BoundedOutput>(std::move(buffer_stream), req.max_output_bytes);
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(sink, output_schema));
  Stats stats;
  stats.row_groups_total = metadata->num_row_groups();

  // Counters describe evaluated batches, not only the LIMIT-trimmed output.
  const auto consume = [&](std::shared_ptr<arrow::RecordBatch> batch) -> Status {
    ARROW_RETURN_NOT_OK(add_count(batch->num_rows(), &stats.rows_scanned));
    if (req.has_predicate) {
      ARROW_ASSIGN_OR_RAISE(auto mask, evaluate(req.predicate, *batch, positions));
      ARROW_ASSIGN_OR_RAISE(auto filtered, arrow::compute::Filter(arrow::Datum(batch), mask));
      batch = filtered.record_batch();
    }
    ARROW_RETURN_NOT_OK(add_count(batch->num_rows(), &stats.rows_matched));
    if (!req.aggregates.empty()) {
      for (auto& agg : req.aggregates) ARROW_RETURN_NOT_OK(accumulate(&agg, *batch, positions));
      return Status::OK();
    }
    const int64_t rows = std::min<uint64_t>(batch->num_rows(), req.limit - stats.rows_returned);
    if (rows == 0) return Status::OK();
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(projection.size());
    for (const int index : projection) {
      const auto& column = batch->column(positions[index]);
      columns.push_back(rows == batch->num_rows() ? column : column->Slice(0, rows));
    }
    auto output = arrow::RecordBatch::Make(output_schema, rows, std::move(columns));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*output));
    return add_count(rows, &stats.rows_returned);
  };
  for (int group_index = 0; group_index < metadata->num_row_groups(); ++group_index) {
    if (req.aggregates.empty() && stats.rows_returned == req.limit) break;
    const auto group = metadata->RowGroup(group_index);
    if (group->num_rows() < 0) return Status::Invalid("negative Parquet row group size");
    if (req.has_predicate && !may_match(req.predicate, *group)) {
      ++stats.row_groups_skipped;
      continue;
    }
    ++stats.row_groups_scanned;
    // With no dependencies, metadata provides row cardinality (count(*) and
    // zero-column projection). No arbitrary physical column needs decoding.
    if (leaves.empty()) {
      int64_t remaining = group->num_rows();
      while (remaining > 0) {
        const int64_t rows = std::min(remaining, req.batch_size);
        ARROW_RETURN_NOT_OK(consume(arrow::RecordBatch::Make(
            read_schema, rows, std::vector<std::shared_ptr<arrow::Array>>{})));
        remaining -= rows;
        if (req.aggregates.empty() && stats.rows_returned == req.limit) break;
      }
      continue;
    }
    std::unique_ptr<arrow::RecordBatchReader> batches;
    ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader({group_index}, leaves, &batches));
    stats.columns_read = leaves.size();  // Unique selected physical columns, not column chunks.
    for (;;) {
      std::shared_ptr<arrow::RecordBatch> batch;
      ARROW_RETURN_NOT_OK(batches->ReadNext(&batch));
      if (!batch) break;
      ARROW_RETURN_NOT_OK(consume(std::move(batch)));
      if (req.aggregates.empty() && stats.rows_returned == req.limit) break;
    }
  }
  if (!req.aggregates.empty()) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(req.aggregates.size());
    for (const auto& agg : req.aggregates) {
      ARROW_ASSIGN_OR_RAISE(auto value, aggregate_value(agg));
      ARROW_ASSIGN_OR_RAISE(auto array, arrow::MakeArrayFromScalar(*value, 1));
      columns.push_back(std::move(array));
    }
    const auto batch = arrow::RecordBatch::Make(output_schema, 1, std::move(columns));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    stats.rows_returned = 1;
  }
  // Close writes the schema even when no batches were produced, and the EOS.
  ARROW_RETURN_NOT_OK(writer->Close());
  ARROW_ASSIGN_OR_RAISE(auto ipc, sink->Finish());
  stats.output_bytes = ipc->size();
  return ScanResult{std::move(ipc), stats.json()};
}
}  // namespace

Result<ScanResult> scan(const std::shared_ptr<arrow::Buffer>& input,
                        const std::string& request_json) {
  try {
    return scan_impl(input, request_json);
  } catch (const std::bad_alloc&) {
    return Status::OutOfMemory("Parquet scan allocation failed");
  } catch (const std::length_error& e) {
    return Status::CapacityError("Parquet scan capacity error: ", e.what());
  } catch (const std::exception& e) {
    return Status::Invalid("Parquet scan failed: ", e.what());
  } catch (...) {
    // json_spirit's Error_position is not a std::exception.
    return Status::Invalid("Parquet scan parser/file exception");
  }
}
}  // namespace ceph::parquet_scan
