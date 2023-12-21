import rados
import json
import sys

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

def send_cls_request(conf_file, obj_name, pool_name, src_nodes, sample_nums):
    # 连接到集群
    cluster = rados.Rados(conffile=conf_file)
    cluster.connect()

    # 打开存储池
    ioctx = cluster.open_ioctx(pool_name)
    
    # 先将参数编码为json格式，再以json的形式序列化，注意参数顺序
    parm = pack_and_encode(src_nodes, sample_nums)
    # 发送Cls请求
    r, res = ioctx.execute(obj_name, "graph", "graph_sampling", parm, 8192)

    # 关闭上下文和断开连接
    ioctx.close()
    cluster.shutdown()

    # 对处理结果解码，并打印输出
    decoded_res = decode_two_dimensional_vector(res)
    for row in decoded_res:
      print(row)

if __name__ == "__main__":
    conf_file = '/home/zfy/ceph/build/ceph.conf'  # Ceph配置文件的路径
    pool_name = sys.argv[1]  # 存储池的名称
    obj_name = sys.argv[2]   # 操作的对象名称
    src_nodes = [6]
    sample_nums = [10, 10]
    send_cls_request(conf_file, obj_name, pool_name, src_nodes, sample_nums)