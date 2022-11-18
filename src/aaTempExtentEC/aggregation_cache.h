// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_AGGREGATIONCACHE_H
#define CEPH_AGGREGATIONCACHE_H

#include <map>
#include <list>
#ifdef WITH_SEASTAR
#include <boost/smart_ptr/local_shared_ptr.hpp>
#else
#include <memory>
#endif

#include "common/ceph_mutex.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "include/unordered_map.h"

#include "osd/osd_op_util.h"

//#include "osd/ClassHandler.h"
#include "messages/MOSDOp.h"

using std::ostream;
using std::string;
using std::vector;

using ceph::bufferlist;

class AggregationCache {
    PerfCounters *perfcounter;
public:
    CephContext *cct;

    
}


#endif // ! CEPH_AGGREGATIONCACHE_H

