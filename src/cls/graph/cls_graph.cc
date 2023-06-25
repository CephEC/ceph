// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * This is a simple example RADOS class, designed to be usable as a
 * template for implementing new methods.
 *
 * Our goal here is to illustrate the interface between the OSD and
 * the class and demonstrate what kinds of things a class can do.
 *
 * Note that any *real* class will probably have a much more
 * sophisticated protocol dealing with the in and out data buffers.
 * For an example of the model that we've settled on for handling that
 * in a clean way, please refer to cls_lock or cls_version for
 * relatively simple examples of how the parameter encoding can be
 * encoded in a way that allows for forward and backward compatibility
 * between client vs class revisions.
 */

/*
 * A quick note about bufferlists:
 *
 * The bufferlist class allows memory buffers to be concatenated,
 * truncated, spliced, "copied," encoded/embedded, and decoded.  For
 * most operations no actual data is ever copied, making bufferlists
 * very convenient for efficiently passing data around.
 *
 * bufferlist is actually a typedef of buffer::list, and is defined in
 * include/buffer.h (and implemented in common/buffer.cc).
 */

#include <algorithm>
#include <string>
#include <sstream>
#include <cerrno>
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <random>
#include <nlohmann/json.hpp>

#include "objclass/objclass.h"
#include "osd/osd_types.h"
#include "cls_graph_types.hh"

using std::string;
using std::ostringstream;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using json = nlohmann::json;

CLS_VER(1,0)
CLS_NAME(graph)

template <typename T>
std::unordered_map<int, T> parse_dict(const std::string& raw_data) {
	std::istringstream iss(raw_data);
	std::unordered_map<int, T> dict;
	json j;
	iss >> j;
	for (const auto& item : j.items()) {
		int key = std::stoi(item.key());
		T value = item.value();
		dict[key] = value;
	}
	return dict;
}

template <typename T>
std::vector<T> sampling(const std::vector<T>& src_nodes, int sample_num, const std::unordered_map<int, std::vector<T>>& neighbor_table) {
	std::vector<T> results;
	std::random_device rd;
	std::mt19937 gen(rd());
	for (const auto& sid : src_nodes) {
		std::vector<T> res(sample_num);
		std::uniform_int_distribution<> dis(0, neighbor_table.at(sid).size() - 1);
		for (int i = 0; i < sample_num; i++) {
			res[i] = neighbor_table.at(sid)[dis(gen)];
		}
		results.insert(results.end(), res.begin(), res.end());
	}
	return results;
}

std::vector<std::vector<int>> multihop_sampling2(const std::vector<int>& src_nodes, const std::vector<int>& sample_nums, bufferlist &data) {
	auto neighbor_table = parse_dict<std::vector<int>>(data.to_str());
	std::vector<std::vector<int>> sampling_result;
	sampling_result.push_back(src_nodes);
	assert(neighbor_table.size() != 0);

	for (size_t k = 0; k < sample_nums.size(); k++) {
		auto hopk_result = sampling(sampling_result[k], sample_nums[k], neighbor_table);
		sampling_result.push_back(hopk_result);
	}

	return sampling_result;
}

std::string encode_two_dimentional_vector(const std::vector<std::vector<int>>& vec) {
    json j(vec);
    return j.dump();
}

std::pair<std::vector<int>, std::vector<int>> decode_and_unpack(const std::string& encoded_data) {
    std::string json_data(encoded_data);
    json data = json::parse(json_data);

    std::vector<int> array1 = data["array1"];
    std::vector<int> array2 = data["array2"];

    return {array1, array2};
}

static int graph_sampling(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
	uint64_t size;
	bufferlist read_bl;
	int r = cls_cxx_stat(hctx, &size, NULL);
  if (r < 0)
    return r;
	r = cls_cxx_read(hctx, 0, size, &read_bl);
  if (r < 0)
    return r;
  auto parm = decode_and_unpack(in->c_str());
	// parm.first -> src_nodes   parm.first-> sample_nums
	auto sampling_result = multihop_sampling2(parm.first, parm.second, read_bl);
	auto encoded_res = encode_two_dimentional_vector(sampling_result);
	out->append(encoded_res);
  return 0;
}

/**
 * initialize class
 *
 * We do two things here: we register the new class, and then register
 * all of the class's methods.
 */
CLS_INIT(graph)
{
  // this log message, at level 0, will always appear in the ceph-osd
  // log file.
  CLS_LOG(0, "loading cls_graph");

  cls_handle_t h_graph;
  cls_method_handle_t h_graph_sampling;

  cls_register("graph", &h_graph);

  cls_register_cxx_method(h_graph, "graph_sampling",
			  CLS_METHOD_RD,
			  graph_sampling, &h_graph_sampling);
}
