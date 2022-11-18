
/**
 * @brief 生成max_objects数量的请求和对象
 * for each req
 */


#include "osd/OpRequest.h"

/** 生成的request池，list管理 */
struct RequestPool {
  
  int create(std::vector<librados::ObjectWriteOperation>& ops);
  
  
  unsigned num_objs;
  boost::intrusive::list<OpRequest> request_list;

}