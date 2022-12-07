/**
 * @file test_local_volume.cc
 * @author shuaixiaoyu (931402924@qq.com)
 * @brief chunk、volume测试，简单请求聚合与元数据管理
 *  1. 生成若干obj->生成请求->封装进oprequest
      OpRequest* req = generate_request()
      for(1-4)
          print chunk
          volume.add(req)
      print volume
      OpRequest* req = volume_generate_op()
  2. 管理volume元数据，命名/写入/读取/解析，列出volume中管理的对象名
      store->init
      store->list_collection()
      store->open_collection(coll_t::volume())
      store->write()
      store->read()
 * @version 0.1
 * @date 2022-11-17
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "include/types.h"

#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.hpp"


#include "acconfig.h"


#include "common/Timer.h"
#include "common/ceph_mutex.h"

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "mds/inode_backtrace.h"
#include "include/random.h"
#include <iostream>
#include <fstream>

#include "messages/MOSDOp.h"
#include "osdc/Objecter.h"
#include "osd/OpRequest.h"

#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <errno.h>
#include <dirent.h>
#include <stdexcept>
#include <climits>
#include <locale>
#include <memory>
#include <optional>

#include "include/util.h"
#include "common/hobject.h"
#include "osd/OSDMap.h"

#include "simple_aggregate_buffer.h"


using namespace std::chrono_literals;
using namespace librados;
using ceph::util::generate_random_number;
using std::cerr;
using std::cout;
using std::dec;
using std::hex;
using std::less;
using std::list;
using std::map;
using std::multiset;
using std::ofstream;
using std::ostream;
using std::pair;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

// 默认pg参数
const char *pool_name = "default_pool";
const unsigned default_object_size = 1 << 27;
const unsigned default_object_num = 4;
const int ERR = -1;

//std::shared_ptr<OSDMap> test_map(new OSDMap());

SimpleAggregateBuffer aggregate_buffer;



void usage(ostream& out)
{
  out <<					\
"usage: test_volume [options] [commands]\n"
"\n"
"PG COMMANDS\n"
"   ls <pgid>                        list volumes in given pg\n"  // 列出当前pg中所有objects
"\n"
"VOLUME COMMANDS\n"
"   lsvolumes <pgid>                 list volumes in given pg\n"  // 列出当前pg中所有volume
"   lschunks <volumeid>              list chunks in given volume\n"  // 列出volume中所有chunk（oid）
"\n"
"OBJECT COMMANDS\n"
"   get <obj-name> <outfile>         fetch object\n"  // 单个查询
"   put <obj-name> <infile> [--offset offset]\n"      // 单个写入
"                                    write object with start offset (default:0)\n"
"   batch-put <obj_nums>\n"                           // 批量写入，随机生成对象名
"                                    write [obj_nums] objects\n"
"   rm <obj-name> ... [--force-full] remove object(s), --force-full forces remove when cluster is full\n" // 单个删除

/** xattr操作，管理方式待定 */
"   listxattr <obj-name>             list attrs of this object\n"
"   getxattr <obj-name> <attr>       get the <attr> attribute of this object\n"
"   setxattr <obj-name> attr val\n"
"   rmxattr <obj-name> attr\n"

/** 查询stat */
"   stat <obj-name>                  stat the named object\n" // 指定pg，获取obj状态（修改时间，大小etc）
"   stat2 <obj-name>                 stat2 the named object (with high precision time)\n"
"\n"

"IMPORT AND EXPORT\n"
"   export [filename]\n"
"       Serialize pool contents to a file or standard out.\n"
"   import [--dry-run] [--no-overwrite] < filename | - >\n"
"       Load pool contents from a file or standard in\n"
"\n"

"GLOBAL OPTIONS:\n"
"   --obj-name-file file\n"
"        use the content of the specified file in place of <obj-name>\n"
"   -O object_size\n"
"        set the object size for put/get ops and for write benchmarking\n"
"\n"
"GENERIC OPTIONS:\n";
  generic_client_usage();
}




[[noreturn]] static void usage_exit()
{
  usage(cerr);
  exit(1);
}



template <typename I, typename T>
static int sistrtoll(I &i, T *val) {
  std::string err;
  *val = strict_iecstrtoll(i->second.c_str(), &err);
  if (err != "") {
   cerr << "Invalid value for " << i->first << ": " << err << std::endl;
   return -EINVAL;
  } else {
    return 0;
  }
}


bufferlist generate_random(uint64_t len, int frag)
{
  static const char alphanum[] = "0123456789"
                                 "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                 "abcdefghijklmnopqrstuvwxyz";
  uint64_t per_frag = len / frag;
  bufferlist bl;
  for (int i = 0; i < frag; i++ ) {
    bufferptr bp(per_frag);
    for (unsigned int j = 0; j < len; j++) {
      bp[j] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    bl.append(bp);
  }
  return bl;
}

std::string generate_prefix()
{
  bufferlist bl = generate_random(10, 1);
  return std::string("obj_") + std::string(bl.c_str()) + std::string("_");
}

MOSDOp* prepare_osd_op(std::string& oid, bufferlist& bl, size_t len, uint64_t off)
{
  std::vector<OSDOp> ops;
  ops.emplace_back();
  ops.back().op.op = CEPH_OSD_OP_WRITE;

  OSDOp& osd_op = ops.back();
  osd_op.op.extent.offset = off;
  osd_op.op.extent.length = len;
  osd_op.indata.claim_append(bl);

  auto m = new MOSDOp();
  m->ops = ops;
  return m;
}


/**
 * @brief 顺序生成
 *
 **/
static void generate_objects_and_ops(std::list<MOSDOp*> ops, const unsigned object_num, const unsigned object_size)
{
  char* object_contents = new char[object_size];
  memset(object_contents, 'z', object_size);

  std::vector<string> name(object_num);
  unique_ptr<bufferlist> contents[object_num];

  std::string prefix = generate_prefix();
  for(unsigned i = 0; i < object_num; i++) {
    name[i] = prefix + std::to_string(i);
    //std::string object_name = prefix + std::to_string(i);
    //sprintf(name[i], "%s", prefix.c_str());
    cout << "generate object " << name[i] << std::endl;
    contents[i] = std::make_unique<bufferlist>();
    snprintf(object_contents, object_size, "batch put obj_%16d", i);
    contents[i]->append(object_contents, object_size);

    MOSDOp* m = prepare_osd_op(name[i], *contents[i], object_size, 0);
    m->set_type(CEPH_MSG_OSD_OP);
    m->set_reqid(osd_reqid_t());
    // encode message payload, m->finish_decode() 解析payload
    // m->encode_payload(0);
    // OpRequest是对message在OSD端做的封装，方便OSD的OpTracker做跟踪，主要的信息都在message/MOSDOp中，这里不好拆就不拆了
    // OpRequestRef 是以个指向OpRequest的intrusive_ptr，主要是为了跟踪oprequest
    // OpRequestRef op = op_tracker.create_request<OpRequest, Message*>(m);
    ops.push_back(m);
  }
}


/**
 * @brief 批量写入卷测试
 *
 */
int batch_put_objects(const unsigned obj_num, const unsigned obj_size)
{
  cout << "enter batch put, " << std::endl
          << "num: " << obj_num << std::endl
          << "size: " << obj_size << std::endl;
  if (obj_num < 1 || !obj_size)
              return ERR;
  //generate object & request
  std::list<MOSDOp*> ops(obj_num);
  generate_objects_and_ops(ops, obj_num, obj_size);

  cout << "start to batch put " << std::endl;
  // for(req in list) volume.add, volume开启线程flush
  std::list<MOSDOp*>::iterator i = ops.begin();
  while(i != ops.end()) {
    int ret = aggregate_buffer.write(*i/*, *test_map*/);
    if (ret < 0) {
      goto failed;
    }
    i++;
  }
failed:
    // clear buffer

  return 0;

}


int batch_delete_objects(const unsigned obj_num, const unsigned object_size) 
{

}


/**********************************************

**********************************************/
static int run_test(const std::map < std::string, std::string > &opts,
                                             std::vector<const char*> &nargs)
{
  int ret = 0;
  unsigned object_size = default_object_size;
  uint64_t object_offset = 0;
  bool object_size_specified = false;
  bool object_offset_specified = false;
  std::map<std::string, std::string>::const_iterator i;

  std::string prefix;

  // const char *output = NULL;
  std::optional<std::string> object_name;
  std::string input_file;

  auto it = nargs.begin();

  while(it != nargs.end()) {
    cout << *it << " " << std::endl;
    it++;
  }

  i = opts.find("object-size");
  if (i != opts.end()) {
    if (sistrtoll(i, &object_size)) {
      return -EINVAL;
    }
    object_size_specified = true;
                                        }
  i = opts.find("offset");
  if (i != opts.end()) {
    if (sistrtoll(i, &object_offset)) {
      return -EINVAL;
    }
    object_offset_specified = true;
  }

  ceph_assert(!nargs.empty());

  unsigned object_num = 0;
  if (strcmp(nargs[0], "batch-put") == 0) {
    if (!object_num) {
      object_num = default_object_num;
      cout << "use default object num" << default_object_num << std::endl;
    } else {
      object_num = std::stoi(nargs[1]);
    }
    int ret = batch_put_objects(object_num, object_size);
    if(ret < 0) {
      cerr << __func__ << ": failed to batch put objs." << std::endl;
    }
  } else {

  }

  return ret;
}


int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);

  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage(cout);
    exit(0);
  }

  


  std::map < std::string, std::string > opts;
  std::string val;

  for (auto j = args.begin(); j != args.end(); ++j) {
    if (strcmp(*j, "--") == 0) {
      break;
    } else if ((j+1) == args.end()) {
      // This can't be a formatting call (no format arg)
      break; 
    } 
  }

  /// 根据配置文件初始化ceph全局上下文
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                                           CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  
  std::vector<const char*>::iterator i;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--offset", (char*)NULL)) {
      opts["offset"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--object-size", (char*)NULL)) {
      opts["object-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-O", (char*)NULL)) {
      opts["object-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-objects", (char*)NULL)) {
      opts["max-objects"] = val;
    } else {
      if (val[0] == '-')
        usage_exit();
      ++i;
    }
  }

  if (args.empty()) {
    cerr << "test_local_volume: you must give an action. Try --help" << std::endl;
    return 1;
  }

  return run_test(opts, args);
}



