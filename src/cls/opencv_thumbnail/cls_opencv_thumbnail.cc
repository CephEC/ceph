#include "osd/osd_types.h"
#include "include/rados/objclass.h"

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

// cls链路：
// 1. 客户端发起请求，为cls请求准备自定义的子op，通过exec方法作为ObjectOperation的data发到服务器端
// 2. 服务端接到Message之后，解码发现是cls op，交给class handler处理
// 3. class handler调用对应的cls算子，之前客户端传过来的data作为被处理的数据，交给对应的cls方法（原生方案是在cls里读取数据，这里修改成了读好再调用cls）
// 4. cls算子decode param，获取客户端指定的参数，进行对应处理

// the implementation accepts an in-memory file, reshape it according to 
static void downscale_impl(bufferlist *in, bufferlist *out, opencv_thumbnail_op_t &op) {
  using cv::Mat;
  using cv::resize;
  using cv::imdecode;
  using cv::imencode;

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
    ceph_assert(op.shape.fixed.x <= static_cast<uint32_t>(img_raw.cols)
      && op.shape.fixed.y <= static_cast<uint32_t>(img_raw.rows));
    resize(img_raw, img_out, cv::Size(op.shape.fixed.x, op.shape.fixed.y), 0, 0);
  }

  vector<unsigned char> file_out; 
  imencode(".jpg", img_out, file_out);

  out->append(reinterpret_cast<char*>(file_out.data()), file_out.size());

  cls_log(4, "in %s: mode %s, in data size %u, out data size %u", __func__, op.is_ratio_shape ? "ratio" : "fixed", in->length(), out->length());
}

static int downscale(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  opencv_thumbnail_op_t op;
  decode(op, reinterpret_cast<ClsParmContext*>(hctx)->parm_data);

  // in modified EC cls, in is target object data instead of cls data
  try {
    downscale_impl(in, out, op);
  } catch (const cv::Exception& e) {
    // OpenCV failed to perform the resize operation
    CLS_ERR("in %s: opencv image resize failed: %s", __func__, e.what());
    return -EINVAL;
  }

  return 0;
}

CLS_INIT(opencv_thumbnail) {
  cls_register("opencv_thumbnail", &h_class);

  cls_register_cxx_method(h_class, "downscale", CLS_METHOD_RD, downscale, &h_downscale);
}
