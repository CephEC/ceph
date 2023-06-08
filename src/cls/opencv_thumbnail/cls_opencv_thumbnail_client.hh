#include "include/types.h"
#include "include/rados/librados.hpp"

#include "cls_opencv_thumbnail_types.hh"

namespace rados {
  namespace cls {
    namespace opencv_thumbnail {
      
      int downscale_ratio(librados::IoCtx *ioctx,
	        const std::string& oid,
                float x, float y,
                bufferlist& out);


      int downscale_fixed(librados::IoCtx *ioctx,
	        const std::string& oid,
                uint32_t x, uint32_t y,
                bufferlist& out);
    }
  }
}
