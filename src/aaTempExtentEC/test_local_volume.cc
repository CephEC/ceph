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
#include "Objecter.h"
#include "OpRequest.h"

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

#include "cls/lock/cls_lock_client.h"
#include "include/compat.h"
#include "include/util.h"
#include "common/hobject.h"



#include "osd/ECUtil.h"

#include "volume.h"
#include "chunk.h"

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
const unsigned default_block_size = 1 << 27;
const unsigned default_obj_num = 4;
const int ERR = -1;

/**
 * @brief 试作型聚合缓存
 * 
 */
class SimpleAggregationCache
{
public:
  int write(MOSDOp* op) {
    // is_empty
    // volume.add
  }
  int read();
  int flush();




  static SimpleAggregationCache& get_instance() {
    static SimpleAggregationCache instance;
    return instance;
  }

private:
  std::vector<Volume*> volumes;
}

SimpleAggregationCache& cache = SimpleAggregationCache::get_instance();

/**
 * @brief 试作型简易卷
 * 
 * 和Volume区别在于chunk中保存的是MOSDOp还是OpRequest
 * 
 */
class SimpleVolume {
    // read from config
    constexpr uint64_t default_volume_capacity = 4;
    constexpr uint64_t FULL_SIGNAL = -1;

    volume_t volume_info;

public:
    SimpleVolume(uint64_t _cap) : volume_lock(ceph::make_mutex("SimpleVolume::lock")),
                                  cap(_cap), size(0), is_flushed(false), vol_op(nullptr) { 
      // 这里初始化的感觉怪怪的【
      bitmap.resize(cap);
      chunks.resize(cap);
      bitmap.assign(cap, false);
      chunks.assign(cap, nullptr);

      flush_timer = new SafeTimer(g_ceph_context, volume_lock);
      flush_timer->init();
    }

    SimpleVolume() : 

    bool full() { return size == cap; }
    bool empty() { return size == 0; }

    object_info_t find_object(hobject_t soid);
    
    /**
     * @brief 在volume里找空位填入
     * 
     * @param chunk 
     * @return int 
     */
    int add_chunk(Chunk* chunk) {
      // 无所谓，PG会上锁
      // std::lock_guard locker{volume_lock};
      int ret = FULL_SIGNAL;
      
      if (full()) {
        cout << "full volume failed to add chunk. " << std::endl;
        return ret;
      } 

      for (int i = 0; i < cap; i++)
      {
        if (bitmap[i]) continue;
        else {
          bitmap[i] = true;
          chunks[i] = chunk;
          size++;
          ret = 0;
          // 计时器清零

          break;
        }
      }

      return ret;
    }

    void remove_chunk(hobject_t soid);
    void clear();


private:
    ceph::mutex flush_lock;
    ceph::condition_variable flush_cond;
    SafeTimer flush_timer;

    bool is_flushed;

    std::vector<bool> bitmap;
    // chunk的顺序要与volume_info中chunk_set中chunk的顺序一致
    std::vector<Chunk*> chunks;
    uint64_t cap;
    uint64_t size;
    
    // 计时器 safe timer
    // https://blog.csdn.net/tiantao2012/article/details/78426276
    

    // create_request()
    //OpRequestRef vol_op
    MOSDOp* vol_op;
}


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



namespace detail {

  [[noreturn]] static void usage_exit()
  {
    usage(cerr);
    exit(1);
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

string generate_object_name(int i) 
{
  bufferlist bl = generate_random(10, 1);
  return string(bl.c_str()) + string("_obj_") + string(i) ;
}

MOSDOp* prepare_osd_op(const std::string& oid, const bufferlist& bl, size_t len, uint64_t off)
{
  int flags = 0;
  osdc_opvec ops;
  ops.emplace_back();
  ops.back().op.op = CEPH_OSD_OP_WRITE;
  out_bl.push_back(nullptr);
  ceph_assert(ops.size() == out_bl.size());
  out_handler.emplace_back();
  ceph_assert(ops.size() == out_handler.size());
  out_rval.push_back(nullptr);
  ceph_assert(ops.size() == out_rval.size());
  out_ec.push_back(nullptr);
  ceph_assert(ops.size() == out_ec.size());

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
 */
static void generate_objects_and_ops(std::list<MOSDOp*> ops, const unsigned obj_num, const unsigned object_size) 
{
  char* object_contents = new char[object_size];
  memset(object_contents, 'z', object_size);

  std::vector<string> name(obj_num);
  unique_ptr<bufferlist> contents[obj_num];
  
  for(int i = 0; i < obj_num; i++) {
    name[i] = generate_object_name(i);
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
int batch_put_objects(const unsigned obj_num, const unsigned object_size) 
{
  if (obj_num < 1 || !object_size)
    return ERR;
  // generate object & request
  std::list<MOSDOp*> ops(obj_num);
  generate_objects_and_ops(ops, obj_num);

  // for(req in list) volume.add, volume开启线程flush
  std::list<MOSDOp*>::iterator i = ops.begin();
  while(i != ops.end()) {
    // volume怎么存
  }
}

int batch_delete_objects(const unsigned obj_num, const unsigned object_size) 
{

}


/**********************************************

**********************************************/
static int run_test(const std::map < std::string, std::string > &opts,
                             std::vector<const char*> &nargs)
{
  int ret;
  unsigned object_size = default_block_size;
  uint64_t obj_offset = 0;
  bool obj_offset_specified = false;
  std::map<std::string, std::string>::const_iterator i;

  std::string prefix;

  const char *output = NULL;
  std::optional<std::string> obj_name;
  std::string input_file;

  //Rados rados;
  //IoCtx io_ctx;

  i = opts.find("object-size");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &object_size)) {
      return -EINVAL;
    }
    block_size_specified = true;
  }
  i = opts.find("offset");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &obj_offset)) {
      return -EINVAL;
    }
    obj_offset_specified = true;
  }

  ceph_assert(!nargs.empty());

  unsigned obj_num = 0;
  // list pools?
  if (strcmp(nargs[0], "batch-put") == 0) {
      if (!obj_num) {
        obj_num = default_obj_num; 
        cout << "use default object num" << default_obj_num << std::endl;
      } else {
        obj_num = std::stoi(nargs[1]);
      }
      int ret = batch_put_objects(obj_num);
      if(ret < 0) {
        cerr << __func__ << ": failed to batch put objs." << std::endl;
      }
  } else {

  }

  




  auto i = request_pool.request_list.begin();
  std::vector<Volume*> ...;
  while() {
    // 依次封装并写入volume


  }

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

  // 根据配置文件初始化ceph全局上下文
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


