#include "include/rados/objclass.h"
#include "objclass/objclass.h"

#include <vector>

#include <opencv2/opencv.hpp>

#include "cls_opencv_thumbnail_types.hh"

CLS_VER(1, 0)
CLS_NAME(opencv_thumbnail)

cls_handle_t h_class;

cls_method_handle_t h_downscale;

using std::vector;

using ceph::encode;
using ceph::decode;

using cv::Mat;
using cv::resize;
using cv::imdecode;
using cv::imencode;

// cls链路：
// 1. 客户端发起请求，为cls请求准备自定义的子op，通过exec方法作为ObjectOperation的data发到服务器端
// 2. 服务端接到Message之后，解码发现是cls op，交给class handler处理
// 3. class handler调用对应的cls算子，之前客户端传过来的data作为**参数**而不是被处理的数据，交给对应的cls方法
// 4. cls算子decode data，获取客户端指定的参数，在这里再自行选择要读写的数据（没有指定oid的话，如何保证连接的osd上一定有想处理的数据？）

// the implementation accepts an in-memory file, reshape it according to 
static void downscale_impl(bufferlist *in, bufferlist *out, opencv_thumbnail_op_t &op) {
  // read and decode image
  Mat file_buf(1, in->length(), CV_8U, in->c_str());

  Mat img_raw;
  imdecode(file_buf, cv::ImreadModes::IMREAD_UNCHANGED, &img_raw);

  Mat img_out;
  if(op.is_ratio_shape) {
    // ensure that we are not upscaling the image
    ceph_assert(op.shape.ratio.x * op.shape.ratio.y <= 1);
    resize(img_raw, img_out, cv::Size(), op.shape.ratio.x, op.shape.ratio.y);
  } else {
    ceph_assert(op.shape.fixed.x <= img_raw.cols && op.shape.fixed.y <= img_raw.rows);
    resize(img_raw, img_out, cv::Size(op.shape.fixed.x, op.shape.fixed.y), 0, 0);
  }

  vector<unsigned char> file_out; 
  imencode("jpeg", img_out, file_out);

  out->append(reinterpret_cast<char*>(file_out.data()), file_out.size());
}

static int downscale(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  opencv_thumbnail_op_t op;
  decode(op, *in);

  // TODO 从osd读取对象，交给cls处理

  downscale_impl(nullptr, out, op);

  return 0;
}

CLS_INIT(opencv_thumbnail) {
  cls_register("opencv_thumbnail", &h_class);

  cls_register_cxx_method(h_class, "downscale", CLS_METHOD_RD, downscale, &h_downscale);
}
