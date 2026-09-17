// Real librados checks for an isolated EC pool; the harness supplies fixtures.
#include "include/ceph_assert.h"
#include "include/rados/librados.hpp"
#include "cls/opencv_thumbnail/cls_opencv_thumbnail_types.hh"
#include <cerrno>
#include <fstream>
#include <iostream>
#include <limits>
#include <set>
#include <stdexcept>

using ceph::bufferlist;
static void require(bool ok, const std::string& text) {
  if (!ok) throw std::runtime_error(text);
}
static void thumbnail(librados::IoCtx& io, const std::string& key) {
  auto call = [&](bufferlist args, int expected) {
    bufferlist out;
    const int r = io.exec(key, "opencv_thumbnail", "downscale", args, out);
    require(r == expected, "thumbnail result " + std::to_string(r));
    if (!r) require(out.length() > 2 && (unsigned char)out[0] == 0xff &&
                    (unsigned char)out[1] == 0xd8, "JPEG output");
    else require(!out.length(), "invalid thumbnail must have no output");
  };
  call({}, -EINVAL);
  bufferlist truncated; truncated.append("\1", 1); call(truncated, -EINVAL);
  for (auto ratio : {std::pair<float,float>{2,2}, {2,.25}, {0,1}, {-1,1},
                    {std::numeric_limits<float>::infinity(),1},
                    {std::numeric_limits<float>::quiet_NaN(),1}, {1e-30f,1}}) {
    opencv_thumbnail_op_t op; op.shape.ratio.x = ratio.first; op.shape.ratio.y = ratio.second;
    bufferlist args; encode(op, args); call(args, -EINVAL);
  }
  for (auto dims : {std::pair<uint32_t,uint32_t>{0,1}, {1,0}, {0xffffffffu,1}}) {
    opencv_thumbnail_op_t op; op.is_ratio_shape = false;
    op.shape.fixed.x = dims.first; op.shape.fixed.y = dims.second;
    bufferlist args; encode(op, args); call(args, -EINVAL);
  }
  opencv_thumbnail_op_t op; op.shape.ratio.x = .5f; op.shape.ratio.y = .5f;
  bufferlist args; encode(op,args); call(args,0);
  args.append("extra"); call(args,-EINVAL);
  op.is_ratio_shape = false; op.shape.fixed.x = 16; op.shape.fixed.y = 16;
  args.clear(); encode(op,args); call(args,0);
}
static void listing(librados::IoCtx& io, bool create, bool filtered = true) {
  io.set_namespace("review-list");
  std::set<std::string> expected;
  bufferlist data; data.append(std::string(17003, 'x'));
  for (unsigned i = 0; i < 56; ++i) {
    const std::string key = "item-" + std::to_string(i);
    if (!filtered || i % 3 == 0) expected.insert(key);
    if (create) {
      auto payload = data;
      require(io.write_full(key,payload)==0,"create listing object");
      if (i % 3 != 2) {
        bufferlist value; value.append(i % 3 == 0 ? "match" : "different");
        require(io.setxattr(key,"tag",value)==0,"set listing xattr");
      }
    }
  }
  bufferlist filter;
  encode(std::string("plain"),filter); encode(std::string("_tag"),filter);
  encode(std::string("match"),filter);
  std::set<std::string> actual;
  for (auto it=filtered ? io.nobjects_begin(filter) : io.nobjects_begin();
       it!=io.nobjects_end();++it)
    require(actual.insert(it->get_oid()).second,"duplicate filtered listing entry");
  require(actual==expected,"filtered listing exact set, got " + std::to_string(actual.size()));
}
static void read_error(librados::IoCtx& io, const std::string& key) {
  bufferlist read, args, called;
  int rr=-999, cr=-999, sr=-999;
  uint64_t size=0xabcdef;
  time_t mtime=0;
  librados::ObjectReadOperation op;
  op.read(0,4096,&read,&rr);
  op.exec("openssl_md5","compute",args,&called,&cr);
  op.stat(&size,&mtime,&sr);
  require(io.operate(key,&op,nullptr)==-EIO,"injected read error");
  require(rr==-EIO && !called.length() && size==0xabcdef,
          "failed READ must stop later CALL and STAT");
}
int main(int argc,char** argv) {
  try {
    require(argc>=4,"CONF POOL MODE [OBJECT]");
    librados::Rados cluster;
    require(cluster.init("admin")==0,"init");
    require(cluster.conf_read_file(argv[1])==0,"config");
    cluster.conf_set("rados_osd_op_timeout","45");
    cluster.conf_set("admin_socket","");
    require(cluster.connect()==0,"connect");
    librados::IoCtx io; require(cluster.ioctx_create(argv[2],io)==0,"ioctx");
    const std::string mode=argv[3], key=argc>4?argv[4]:"image0";
    if(mode=="thumbnail") thumbnail(io,key);
    else if(mode=="list-create" || mode=="list-check") listing(io,mode=="list-create");
    else if(mode=="list-all") listing(io,false,false);
    else if(mode=="read-error") read_error(io,key);
    else throw std::runtime_error("unknown mode");
    std::cout<<"PASS: "<<mode<<" "<<argv[2]<<" "<<key<<"\n";
    io.close(); cluster.shutdown();
  } catch (const std::exception& e) { std::cerr<<e.what()<<"\n"; return 1; }
}
