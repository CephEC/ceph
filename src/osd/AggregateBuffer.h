/**
 * @brief 聚合缓存
 *
 */
#ifndef CEPH_AGGREGATEBUFFER_H
#define CEPH_AGGREGATEBUFFER_H


#include "include/types.h"
#include "messages/MOSDOp.h"

#include "common/ceph_context.h"
#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/Thread.h"

//using std::cout;

//class volume_t;
//class Volume;
//class SimpleAggregateBuffer;

class AggregateBuffer
{
  //friend class SimpleVolume;
  //friend class FlushContext;
public:
  AggregateBuffer(CephContext* _cct, PrimaryLogPG *_pg) :cct(_cct), pg(_pg) {
    std::cout << "init aggregate buffer " << index++ << std::endl;
 };

  ~AggregateBuffer() { /*flush_timer->shutdown();*/ }

  
  /**
   * @brief 对象写入buffer，在add_chunk中执行对象元数据的创建操作
   *
   * @param op
   * @param osdmap
   * @return int
   */
   int write(OpRequestRef op);
   // int write_list(MOSDOp*, const OSDMap &osdmap);
  
private:
  CephContext* cct;
  PrimaryLogPG *pg;

  static int index;
};


#endif // !CEPH_AGGREGATEBUFFER_H
