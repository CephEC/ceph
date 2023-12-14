#include "include/types.h"
#include "include/rados/librados.hpp"

#include "cls_graph_types.hh"

namespace rados {
  namespace cls {
    namespace graph {
      int graph_sampling(librados::IoCtx *ioctx,
	        const std::string& oid,
            const std::vector<int>& src_nodes, 
            const std::vector<int>& sample_nums,
            bufferlist& out);
    }
  }
}
