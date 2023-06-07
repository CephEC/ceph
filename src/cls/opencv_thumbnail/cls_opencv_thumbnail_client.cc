#include "include/types.h"
#include "include/rados/librados.hpp"

#include <iostream>

#include "cls_opencv_thumbnail_types.hh"

using std::cout;
using std::endl;

int main(int argc, char **argv) {
  if(argc <= 4) {
    cout << "usage: " << argv[0] << " <oid> <downscale-type> <x-param> <y-param>" << endl;
    cout << "where downscale type in [ratio, fixed]" << endl;
    exit(0);
  }

  librados::ObjectReadOperation op;

  op.assert_exists();

  opencv_thumbnail_op_t cls_op;
  
  // TODO 获取对象hoid

  if(!strcmp(argv[2], "ratio")) {
    cls_op.is_ratio_shape = true;
    cls_op.shape.ratio.x = std::stof(argv[3]);
    cls_op.shape.ratio.y = std::stof(argv[4]);
  } else if(!strcmp(argv[2], "fixed")) {
    cls_op.is_ratio_shape = false;
    cls_op.shape.fixed.x = std::stoi(argv[3]);
    cls_op.shape.fixed.y = std::stoi(argv[4]);
  } else {
    cout << "unsupported downscale type " << argv[2] << endl;
    abort();
  }

  bufferlist bl;

  encode(cls_op, bl);

  op.exec("opencv_thumbnail", "downscale_2x", bl);
  
  return 0;
}