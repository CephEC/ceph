#include "cls_graph_client.hh"

namespace rados {
  namespace cls {
    namespace graph {
      
      int graph_sampling(librados::IoCtx *ioctx,
	        const std::string& oid,
            const std::vector<int>& src_nodes, 
            const std::vector<int>& sample_nums,
            bufferlist& out) {
        bufferlist in;

        graph_op_t cls_op;

        cls_op.src_nodes = src_nodes;
        cls_op.sample_nums = sample_nums;

        encode(cls_op, in);

	return ioctx->exec(oid, "graph", "graph_sampling", in, out);
      }
    }
  }
}
