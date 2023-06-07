#include "include/rados/objclass.h"
#include "objclass/objclass.h"

CLS_VER(1, 0)
CLS_NAME(opencv_thumbnail)

cls_handle_t h_class;

cls_method_handle_t h_downscale_2x;

static int downscale_2x(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  *out = *in;
  return 0;
}

void __cls_init() {
  cls_register("opencv_thumbnail", &h_class);

  cls_register_cxx_method(h_class, "downscale_2x", CLS_METHOD_RD, downscale_2x, &h_downscale_2x);
}
