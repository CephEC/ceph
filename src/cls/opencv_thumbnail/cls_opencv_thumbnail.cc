#include "osd/osd_types.h"
#include "include/rados/objclass.h"

#include <vector>
#include <cmath>
#include <limits>

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
static int downscale_impl(bufferlist *in, bufferlist *out, const opencv_thumbnail_op_t &op) {
  using cv::Mat;
  using cv::resize;
  using cv::imdecode;
  using cv::imencode;

  // read and decode image
  if (!in->length() || in->length() > std::numeric_limits<int>::max())
    return -EINVAL;
  Mat file_buf(1, in->length(), CV_8U, in->c_str());

  Mat img_raw;
  imdecode(file_buf, cv::ImreadModes::IMREAD_UNCHANGED, &img_raw);
  if (img_raw.empty()) return -EINVAL;

  Mat img_out;
  if(op.is_ratio_shape) {
    // ensure that we are not upscaling the image
    if (!std::isfinite(op.shape.ratio.x) || !std::isfinite(op.shape.ratio.y) ||
        op.shape.ratio.x <= 0 || op.shape.ratio.x > 1 ||
        op.shape.ratio.y <= 0 || op.shape.ratio.y > 1)
      return -EINVAL;
    resize(img_raw, img_out, cv::Size(), op.shape.ratio.x, op.shape.ratio.y);
  } else {
    if (!op.shape.fixed.x || !op.shape.fixed.y ||
        op.shape.fixed.x > static_cast<uint32_t>(img_raw.cols) ||
        op.shape.fixed.y > static_cast<uint32_t>(img_raw.rows))
      return -EINVAL;
    resize(img_raw, img_out, cv::Size(op.shape.fixed.x, op.shape.fixed.y), 0, 0);
  }

  vector<unsigned char> file_out; 
  if (!imencode(".jpg", img_out, file_out)) return -EINVAL;

  out->append(reinterpret_cast<char*>(file_out.data()), file_out.size());

  cls_log(20, "in %s: mode %s, in data size %u, out data size %u", __func__, op.is_ratio_shape ? "ratio" : "fixed", in->length(), out->length());
  return 0;
}

static int downscale(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  opencv_thumbnail_op_t op;

  // in modified EC cls, in is target object data instead of cls data
  try {
    auto p = reinterpret_cast<ClsParmContext*>(hctx)->parm_data.cbegin();
    decode(op, p);
    if (!p.end()) return -EINVAL;
    return downscale_impl(in, out, op);
  } catch (const ceph::buffer::error&) {
    return -EINVAL;
  } catch (const cv::Exception& e) {
    // OpenCV failed to perform the resize operation
    CLS_ERR("in %s: opencv image resize failed: %s", __func__, e.what());
    return -EINVAL;
  }

}

CLS_INIT(opencv_thumbnail) {
  cls_register("opencv_thumbnail", &h_class);

  cls_register_cxx_method(h_class, "downscale", CLS_METHOD_RD, downscale, &h_downscale);
}
