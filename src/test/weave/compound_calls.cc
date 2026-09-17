#include "include/rados/librados.hpp"
#include <fstream>
// Real librados regression: run with CONF POOL PARQUET_OBJECT OUTPUT_DIRECTORY.
#include <iostream>
#include <stdexcept>
using ceph::bufferlist;
void require(bool good, const char* what) { if (!good) throw std::runtime_error(what); }
int main(int argc, char** argv) {
  require(argc == 5, "args: conf pool object root");
  librados::Rados cluster;
  require(cluster.init("admin") == 0, "init");
  require(cluster.conf_read_file(argv[1]) == 0, "config");
  cluster.conf_set("rados_osd_op_timeout", "45");
  cluster.conf_set("admin_socket", "");
  require(cluster.connect() == 0, "connect");
  librados::IoCtx io;
  require(cluster.ioctx_create(argv[2], io) == 0, "ioctx");
  std::string key = argv[3], root = argv[4];
  bufferlist args[2], scalar[2];
  args[0].append(R"({"version":1,"projection":["tag","id"],"predicate":{"op":"and","args":[{"op":"ge","column":"id","value":7},{"op":"ne","column":"tag","value":"skip"}]},"limit":3,"batch_size":2})");
  args[1].append(R"({"version":1,"projection":["id"],"predicate":{"op":"lt","column":"id","value":5},"limit":2,"batch_size":1})");
  for (int i = 0; i < 2; ++i)
    require(io.exec(key, "parquet_scan", "scan", args[i], scalar[i]) == 0, "scalar call");
  bufferlist all;
  require(io.read(key, all, 0, 0) >= 0, "scalar read");
  bufferlist out[2], attr;
  int result[2] = {-999,-999}, ar = -999, sr = -999;
  uint64_t size = 0;
  time_t mtime = 0;
  librados::ObjectReadOperation op;
  op.getxattr("weave-test", &attr, &ar);
  op.exec("parquet_scan", "scan", args[0], &out[0], &result[0]);
  op.exec("parquet_scan", "scan", args[1], &out[1], &result[1]);
  op.stat(&size, &mtime, &sr);
  require(io.operate(key, &op, nullptr) == 0, "compound operation");
  require(ar >= 0 && sr == 0 && attr.to_str() == "value-" + key, "compound attrs");
  require(size == all.length(), "logical size");
  for (int i = 0; i < 2; ++i) {
    require(result[i] == 0 && out[i].contents_equal(scalar[i]), "compound distinct results");
    std::ofstream file(root + "/compound-" + std::to_string(i) + ".bin", std::ios::binary);
    auto bytes = out[i].to_str(); file.write(bytes.data(), bytes.size());
  }
  bufferlist empty, bad_out, read_out, good_out;
  int bad = -999, rr = -999, good = -999;
  librados::ObjectReadOperation stop;
  stop.exec("parquet_scan", "scan", empty, &bad_out, &bad);
  stop.exec("parquet_scan", "scan", args[1], &good_out, &good);
  stop.read(3, 701, &read_out, &rr);
  uint64_t skipped_size = 0xabcdef;
  stop.stat(&skipped_size, &mtime, &sr);
  require(io.operate(key, &stop, nullptr) == -EINVAL, "failure terminates compound");
  require(good_out.length() == 0, "non-FAILOK error must not execute later CALL");
  require(read_out.length() == 0, "non-FAILOK CALL must not execute later READ");
  require(skipped_size == 0xabcdef, "non-FAILOK CALL must not produce later STAT");
  librados::ObjectReadOperation read_first;
  read_first.read(3, 701, &read_out, &rr);
  read_first.exec("parquet_scan", "scan", args[1], &good_out, &good);
  require(io.operate(key, &read_first, nullptr) == 0, "READ before CALL");
  require(read_out.to_str() == all.to_str().substr(3, 701) &&
          good_out.contents_equal(scalar[1]), "earlier async READ precedes CALL");
  librados::ObjectReadOperation keep;
  keep.exec("parquet_scan", "scan", empty, &bad_out, &bad);
  keep.set_op_flags2(librados::OP_FAILOK);
  keep.exec("parquet_scan", "scan", args[1], &good_out, &good);
  keep.read(3, 701, &read_out, &rr);
  keep.stat(&size, &mtime, &sr);
  require(io.operate(key, &keep, nullptr) == 0, "FAILOK continues compound");
  require(bad == -EINVAL && good == 0 && good_out.contents_equal(scalar[1]), "FAILOK class outputs");
  require(rr >= 0 && read_out.to_str() == all.to_str().substr(3, 701) && size == all.length(), "FAILOK following logical read/stat");
  std::cout << "PASS: compound CALL arguments, distinct results, failure, FAILOK and following read/stat\n";
  io.close(); cluster.shutdown();
}
