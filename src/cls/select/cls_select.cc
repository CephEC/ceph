// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * This is a simple example RADOS class, designed to be usable as a
 * template for implementing new methods.
 *
 * Our goal here is to illustrate the interface between the OSD and
 * the class and demonstrate what kinds of things a class can do.
 *
 * Note that any *real* class will probably have a much more
 * sophisticated protocol dealing with the in and out data buffers.
 * For an example of the model that we've settled on for handling that
 * in a clean way, please refer to cls_lock or cls_version for
 * relatively simple examples of how the parameter encoding can be
 * encoded in a way that allows for forward and backward compatibility
 * between client vs class revisions.
 */

/*
 * A quick note about bufferlists:
 *
 * The bufferlist class allows memory buffers to be concatenated,
 * truncated, spliced, "copied," encoded/embedded, and decoded.  For
 * most operations no actual data is ever copied, making bufferlists
 * very convenient for efficiently passing data around.
 *
 * bufferlist is actually a typedef of buffer::list, and is defined in
 * include/buffer.h (and implemented in common/buffer.cc).
 */
#include <algorithm>
#include <string>
#include <sstream>
#include <cerrno>

#include "objclass/objclass.h"
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
//


#include <errno.h>
#include <array>
#include <string.h>
#include <string_view>

#include "common/ceph_crypto.h"
#include "common/split.h"
#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"
#include "common/safe_io.h"
#include "common/errno.h"
#include "auth/Crypto.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/tokenizer.hpp>
#define BOOST_BIND_GLOBAL_PLACEHOLDERS
#ifdef HAVE_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wimplicit-const-int-float-conversion"
#endif
#ifdef HAVE_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION
#pragma clang diagnostic pop
#endif
#undef BOOST_BIND_GLOBAL_PLACEHOLDERS

#include <liboath/oath.h>


#include <s3select/include/s3select.h>


using std::string;
using std::ostringstream;
using namespace s3selectEngine;
using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

CLS_VER(1,0)
CLS_NAME(select)


static int run_s3select(const char* query, const char* input, size_t input_length, bufferlist* output)
{
  int status = 0;
  csv_object::csv_defintions csv;
  s3selectEngine::s3select s3select_syntax;
  s3selectEngine::csv_object m_s3_csv_object;

  s3select_syntax.parse_query(query);
  csv.row_delimiter = '\n';
  csv.column_delimiter = '|';
  csv.output_row_delimiter = '\n';
  csv.output_column_delimiter = '|';
  csv.use_header_info=true;

  m_s3_csv_object.set_csv_query(&s3select_syntax, csv);
  if (s3select_syntax.get_error_description().empty() == false) {
    CLS_LOG(10, "%s: s3select_syntax error %s", __func__, query);
    return -1;
  } else {
    //query is correct(syntax), processing is starting.
    std::string ans;
    status = m_s3_csv_object.run_s3select_on_stream(ans, input, input_length, input_length);
    if (status < 0) {
      CLS_LOG(10, "%s: s3select process error %s", __func__, query);
      return -2;
    }
    output->append(std::move(ans));
  }
  return status;
}

static int s3_select(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{ 
  std::string sql;
  bool cache = true;
  uint32_t flags = 0;
  try {
    auto iter = in->cbegin();
    DECODE_START(1, iter);
    decode(cache, iter);
    decode(sql, iter);
    DECODE_FINISH(iter);
  } catch (ceph::buffer::error &e) {
    return -EINVAL;
  }
  uint64_t size = 0;
  bufferlist read_bl;
	int r = cls_cxx_stat(hctx, &size, NULL);
  if (r < 0)
    return r;
  if (!cache) {
    flags = CEPH_OSD_OP_FLAG_FADVISE_NOCACHE;
  }
  r = cls_cxx_read2(hctx, 0, size, &read_bl, flags);
  if (r < 0)
    return r;
  int status = run_s3select(sql.c_str(), read_bl.c_str(), read_bl.length(), out);
  // this return value will be returned back to the librados caller
  return status;
}


/**
 * initialize class
 *
 * We do two things here: we register the new class, and then register
 * all of the class's methods.
 */
CLS_INIT(select)
{
  // this log message, at level 0, will always appear in the ceph-osd
  // log file.
  CLS_LOG(0, "loading cls_select");

  cls_handle_t h_class;
  cls_method_handle_t h_s3_select;

  cls_register("select", &h_class);

  // There are two flags we specify for methods:
  //
  //    RD : whether this method (may) read prior object state
  //    WR : whether this method (may) write or update the object
  //
  // A method can be RD, WR, neither, or both.  If a method does
  // neither, the data it returns to the caller is a function of the
  // request and not the object contents.

  cls_register_cxx_method(h_class, "s3_select",
			  CLS_METHOD_RD,
			  s3_select, &h_s3_select);
}
