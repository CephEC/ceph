#include "osd/osd_types.h"
#include "include/rados/objclass.h"

#include <vector>

#include <openssl/md5.h>

CLS_VER(1, 0)
CLS_NAME(openssl_md5)

cls_handle_t h_class;

cls_method_handle_t h_compute_md5;

static int compute(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  // in modified EC cls, in is target object data instead of cls data
  std::string obj_data(in->to_str());
  MD5_CTX context;
  MD5_Init(&context);
  MD5_Update(&context, obj_data.c_str(), obj_data.length());
  unsigned char digest[MD5_DIGEST_LENGTH];
  MD5_Final(digest, &context);

  char md5String[2 * MD5_DIGEST_LENGTH + 1];
  for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
      sprintf(&md5String[i * 2], "%02x", (unsigned int)digest[i]);
  }
  out->append(md5String);
  return 0;
}

CLS_INIT(openssl_md5) {
  cls_register("openssl_md5", &h_class);
  cls_register_cxx_method(h_class, "compute", CLS_METHOD_RD, compute, &h_compute_md5);
}
