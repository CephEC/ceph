#include "cls_opencv_thumbnail_client.hh"

namespace rados {
  namespace cls {
    namespace opencv_thumbnail {
      
      int downscale_ratio(librados::IoCtx *ioctx,
	        const std::string& oid,
                float x, float y,
                bufferlist& out) {
        bufferlist in;

        opencv_thumbnail_op_t cls_op;

        cls_op.is_ratio_shape = true;
        cls_op.shape.ratio.x = x;
        cls_op.shape.ratio.y = y;

        encode(cls_op, in);

	return ioctx->exec(oid, "opencv_thumbnail", "downscale", in, out);
      }

      int downscale_fixed(librados::IoCtx *ioctx,
	        const std::string& oid,
                uint32_t x, uint32_t y,
                bufferlist& out) {
        bufferlist in;

        opencv_thumbnail_op_t cls_op;

        cls_op.is_ratio_shape = false;
        cls_op.shape.fixed.x = x;
        cls_op.shape.fixed.y = y;

        encode(cls_op, in);

	return ioctx->exec(oid, "opencv_thumbnail", "downscale", in, out);
      }
    }
  }
}
