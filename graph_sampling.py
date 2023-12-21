import rados
import json
import sys
import numpy as np
import time
import pickle
import json
from data import CoraData
import pickle

def pack_and_encode(array1, array2):
    data = {
        'array1': array1,
        'array2': array2
    }
    json_data = json.dumps(data)
    encoded_data = json_data.encode('utf-8')
    return encoded_data

def decode_two_dimensional_vector(json_string):
    return json.loads(json_string)

def write_json_to_file(json_string, filename):
    # 将 JSON 字符串解析为 Python 对象
    data = json.loads(json_string)

    # 将 Python 对象写入文件
    with open(filename, 'w') as file:
        json.dump(data, file, indent=0)  # indent 参数用于指定缩进空格数，可选

def send_cls_request(conf_file, obj_name, pool_name, src_nodes, sample_nums):
    print(obj_name)
    # 连接到集群
    cluster = rados.Rados(conffile=conf_file)
    cluster.connect()

    # 打开存储池
    ioctx = cluster.open_ioctx(pool_name)
    
    # 先将参数编码为json格式，再以json的形式序列化，注意参数顺序
    parm = pack_and_encode(src_nodes, sample_nums)
    # 发送Cls请求
    r, res = ioctx.execute(obj_name, "graph", "graph_sampling", parm, 24288000)

    # 关闭上下文和断开连接
    ioctx.close()
    cluster.shutdown()

    # 对处理结果解码，并打印输出
    decoded_res = decode_two_dimensional_vector(res)
    
    return res
    #for row in decoded_res:
    #  print(row)

if __name__ == "__main__":
    conf_file = '/home/wzs/ceph/build/ceph.conf'  # Ceph配置文件的路径
    pool_name = sys.argv[1]  # 存储池的名称
    prefix_array = ['val', 'test', 'train']
    NUM_NEIGHBORS_LIST = [10,10]
    BTACH_SIZE = 10000     # 批处理大小
    data = CoraData().data
    with open('dataset/train_idx.pkl','rb') as f:
        train_index = pickle.load(f)
    with open('dataset/test_idx.pkl','rb') as f:
        test_index = pickle.load(f)
    with open('dataset/val_idx.pkl','rb') as f:
        val_index = pickle.load(f)
    index_set = [val_index, test_index, train_index]

    def keystoint(x):
        return {int(k): v for k, v in x}
    for i in range(len(prefix_array)):
        test_name = "/home/wzs/ceph/build/dataset/mydict_" + prefix_array[i] + ".json"
        with open(test_name, 'r') as f:
            mydict = json.load(f,object_pairs_hook=keystoint)
        
        batch_src_index = np.random.choice(index_set[i], size=(BTACH_SIZE,))
        src_node_A = list()
        src_node_B = list()
        src_node_C = list()
        for j in batch_src_index:
            if(mydict[j]=="A"): src_node_A.append(j)
            elif(mydict[j]=="B"): src_node_B.append(j)
            else: src_node_C.append(j)
        src_node_A = (np.array(src_node_A).astype(int)).tolist()
        src_node_B = (np.array(src_node_B).astype(int)).tolist()
        src_node_C = (np.array(src_node_C).astype(int)).tolist()
        src_nodes_set = [src_node_A, src_node_B, src_node_C]
        num_set = ['A', 'B', 'C']
        for j in range(len(src_nodes_set)):
            obj_name = 'dict' +num_set[j]  + '_' + prefix_array[i]
            json_res = send_cls_request(conf_file, obj_name, pool_name, src_nodes_set[j], NUM_NEIGHBORS_LIST)
            write_json_to_file(json_res, obj_name+".res")