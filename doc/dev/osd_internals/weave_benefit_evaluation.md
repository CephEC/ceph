# Weave 单机收益评估

## 1. 背景与问题

背景是一份已封闭的日志分区：微批导入产生许多大小接近的 Parquet 对象，随后反复执行读取、过滤和统计，偶尔修正历史记录，最后按保留期淘汰。每个测试 Parquet 对应一个 RADOS 对象；本实验不包含 RGW、S3、Spark 或 Trino 接入开销。

六个 BlueStore OSD 使用 4+2 EC，普通 EC 和 Weave 采用相同编码与数据内容。4+2 的理想冗余率在两组中均为 1.5，聚合本身不会把这个比例再降低。需要验证的是：分片访问次数、OSD 间数据搬运、primary 计算负载、客户端查询完成时间，以及这些收益是否能抵消聚合和后续物化成本。

当前 D2/D3 尚未修复，因此实验固定 PG 数，保留已有数据的解释能力。这里评价正常稳定状态的机制收益，不把性能实验当作部署正确性验收。D4 候选再发现不作为本轮重点。

## 2. Baseline 与消融对照

| 组 | 数据布局 | 查询位置 | 用途 |
| --- | --- | --- | --- |
| native-client | 普通 EC | 完整 Parquet 拉回客户端，用 Arrow 过滤和投影 | 应用侧全对象读取的基线 |
| native-pushdown | 普通 EC | 同一个 `parquet_scan.scan` data-class | 隔离一般查询下推的收益 |
| weave-primary | 已聚合 | 使用同一个 data-class，关闭客户端直读重定向 | 测聚合布局及 shard 执行的贡献 |
| weave-direct | 已聚合 | 使用同一个 data-class，允许客户端直读目标 shard | 测完整 Weave 路径 |

普通 READ 只需 native-client、weave-primary、weave-direct 三组。native-pushdown 是“当前分支的原生 EC 路径”，不是另行编译的上游 Ceph。

先写 Weave 测试池、完成打包并停止后台聚合，再向普通 EC 池写入完全相同的对象，避免 baseline 在实验途中被聚合。两池的对象内容、大小、名称、EC 参数、PG 数和客户端并发相同；两个 pool 的 CRUSH 排列可能不同，保存 PG 映射以便复核。

## 3. 可复现的业务数据与查询

固定随机种子生成列：`event_id`、`status`、`bytes`、`tenant`、随机 payload。Parquet 使用 Snappy、固定 row group，保存每个对象的实际长度和 SHA256。默认 32 个对象、每对象 16384 行；文件实际大小以 manifest 为准，不强行称为整 1 MiB。

查询语义为：

```sql
SELECT event_id, bytes FROM partition WHERE status < threshold;
```

`status` 在 0–99 均匀分布。threshold=1、50、100 分别代表约 1%、50%、100% 的返回行比例。状态随机分布于 row group，避免让某一组单独享有整组跳过的优势。客户端基线只解码实际需要的三列；OSD data-class 也只解码对应依赖列。没有 SQL 引擎规划器参与。

正式计时前，逐对象验证完整原始字节和完整查询结果；计时期间继续校验长度或返回行数。所有组都使用同一客户端实现。客户端和 data-class 的 Parquet 解码均关闭内部多线程，应用并发由 QD 控制。

## 4. 单机网络模拟

使用 Linux network namespace 和 `tc netem`，无需 Docker 镜像。脚本先 `unshare --net`，再启动 MON、OSD 和客户端；网络配置只存在于该临时命名空间。脚本检查 namespace 身份，拒绝直接修改启动它的主机网络。

- 外层临时 namespace 中建立隔离 Linux bridge；六个 OSD 和客户端分别有自己的子 network namespace，通过七对 veth 连接。
- OSD 0–5 分别使用 `10.77.0.11`–`10.77.0.16`，客户端及 MON 使用 `10.77.0.100`。独立 namespace 保证出站连接也使用各自的源 IP；仅配置 IP 别名不能保证这一点。
- 在网桥通向每个节点的 veth 出口分别设置 netem，形成每个目标节点的独立入站带宽上限。
- 每次经过目标队列注入 RTT/2 延迟，请求和回复合计约一个设定 RTT。
- veth 使用 1500 MTU，数据包仅在目标节点的入口链路排队一次。没有真实网卡或主机公共网桥参与。
- 每个 profile 保存 ping 校准和各队列字节、包、丢包计数，任何计时窗口发生队列丢包都使该轮失败。
- 切换 profile 时删除并重建队列，再核对内核实际 rate/delay，防止 netem 保留上个 profile 未显式覆盖的限速。

| profile | 人为增加的 RTT | 每个目的节点的入站速率上限 |
| --- | --- | --- |
| local | 0 | 不额外限速 |
| rtt0 | 0 | 1 Gbit/s |
| lan | 0.2 ms | 1 Gbit/s |
| rtt2 | 2 ms | 1 Gbit/s |
| slow | 2 ms | 100 Mbit/s |

这些参数是可控敏感性实验，不对应某个已实测的机房。Docker 本身不会自动提供这些延迟；如使用容器，同样需要每节点独立网络和 netem。不要在主机的 `lo`、网卡或 Docker 公共网桥上直接套用本脚本中的 qdisc 命令。

## 5. 实验矩阵与执行

小规模试运行：

```bash
bash src/test/weave/benefit_benchmark.sh /root/ceph/build /tmp/weave-benefit-new \
  --objects 32 --rows 16384 --pgs 1 \
  --profiles rtt0 rtt2 --workloads read scan-1 \
  --qd 1 8 --repetitions 3 --duration 2 --min-ops 64
```

脚本需要创建 network namespace 的权限，以及构建对应的 librados Python 扩展、NumPy、PyArrow、iproute2 和 ping。工作目录必须不存在，启动前检查至少有 8 GiB 空闲空间。本轮 64 对象试跑每套约占 2.7 GiB 实际磁盘；六个稀疏块文件的逻辑大小合计 24 GiB。空闲空间检查不是配额，大数据矩阵应另行预算。

脚本正常退出或可处理的中断后，先停止自己创建的守护进程和 namespace keeper，再删除本轮 MON/OSD 数据目录及块文件、压缩日志，只保留结果和复核材料。`cleanup.json` 保存清理记录。没有完整的守护进程停止记录时保留数据并报提示，防止删除仍被使用的数据；SIGKILL 或主机掉电后的残留需人工确认进程退出再清理。不同实验应顺序执行，避免同时占用磁盘和 CPU。

生成摘要、CSV 和图表：

```bash
/usr/bin/python3.10 src/test/weave/report_benefit.py /tmp/weave-benefit-new
```

其中 `dataset.json` 固定数据，`topology.json` 描述网络节点，`network.log` 保存实际 qdisc 设置和校准，`results.json` 保存逐轮原始数据，`stopped.json` 记录守护进程退出状态。报表生成器只汇总完成的测量，不补造失败或尚未执行的格子。

完整后续矩阵：

| 维度 | 建议取值 | 排除的混淆因素 |
| --- | --- | --- |
| PG 数 | 1、8、32，分别建新池 | 1 PG 的 primary 热点不能代表成熟部署 |
| 对象大小 | 调整 rows，使文件约 64 KiB、1 MiB、4 MiB、16 MiB | 避免只选择最有利对象尺寸 |
| 返回比例 | 1%、50%、100% | 低选择率结果不能推广到全量返回 |
| 并发 | 1、8、32 | 分辨单请求延迟和并行吞吐 |
| 网络 | local、lan、rtt2、slow | 分辨 CPU、延迟和带宽限制 |
| 重复 | 至少 5 轮，每格至少 30 秒 | 正式统计需要足够独立重复和尾部样本 |
| 缓存 | 本脚本先测热路径；冷路径另做超缓存数据集 | 不把刚写入和已预热的缓存差异当作收益 |

多 PG 时，脚本按每个 PG 的候选数计算可打包组数，保留凑不齐组的原生对象参与查询，报告实际聚合覆盖率；不只挑已经聚合的对象来计时。

每轮随机化组别和并发的执行次序；计时前预热相同的对象集合并校验结果，配置变更、连接建立、预热和验证不计入性能窗口。采用固定并发的闭环负载，报告的是该负载下的响应时间，不声称这是开放式固定到达率的生产 P99。

## 6. 指标、成本和反例

每轮保存原始请求延迟、操作数、完成时间、输入逻辑字节、应用回复字节、各目标 IP 的 TCP/IP 队列字节、进程 CPU 秒及 `/proc/PID/io` 字节。网络计数包含协议、ACK 和少量同命名空间控制流量，不能与应用 payload 字节混称。进程 I/O 是单机记账量，不是六块独立磁盘的设备统计。

主要比较：

- native-client → native-pushdown：一般查询下推收益。
- native-pushdown → weave-primary：Weave 布局及执行路径的增量收益。
- weave-primary → weave-direct：直读的增量收益，包含重定向协调成本。
- 全量 READ、100% 返回、低延迟网络：可能没有收益甚至变慢，应原样报告。
- 多 PG：普通 EC 的 primary 已分散后，收益是否仍然存在。

打包阶段在 rtt2 条件下单独记录耗时、CPU、网络和物理对象计数变化；不要只报告打包后的快路径。查询完毕后，从不同 Volume 各选择一个成员进行单字节覆盖，比较普通 EC 与 Weave 延迟，并验证所有其他对象内容不变。该过程揭示整卷物化代价；少量覆盖样本不用于声称写 P99。

对于固定大小、固定对象数的一次整分区扫描，用重复轮次的吞吐中位数估算扫描耗时。若每次扫描节约时间为正，可报告 `ceil(打包秒数 / 每次扫描节约秒数)` 的串行墙钟回本次数。该数不是 TCO、能耗或网络回本次数，也不包含本实验未测的后续回收成本。

转换期间的前台干扰应另外测：使用相同预生成请求轨迹，对比普通 EC、Weave 暂停聚合、Weave 开启聚合三组，记录转换前/中/后的完成数、超时数、每秒延迟分布和 CPU/网络占用。再延长转换侧 I/O，检查前台读取仍能完成且内容正确。已有并发转换回归用于功能正确性；本脚本在聚合结束后计时，不能据此宣称转换期间没有资源竞争或延迟上升。若要量化此前“全程阻塞”实现的改进，应在独立旧版本构建上重放同一轨迹，不能用人为构造的慢 baseline 代替真实旧实现。

删除和空间保留还需单独测：同时过期、随机过期、不同保留期混放，记录清理前后实际空间和搬运量。理想模型中，k=4、成员独立存活概率 f、只清理空卷时，残留卷比例是 `1-(1-f)^4`，普通 EC 为 f；f=0.5 时，Weave 暂留空间是普通 EC 的 1.875 倍。这个分析模型说明收益边界，不是本轮磁盘实测。正式回收实验应使用真实分组和删除轨迹。

## 7. 单机结果的解释边界

六个 OSD 共用 CPU、内存和底层磁盘；network namespace 只模拟网络，不会创造六台独立服务器。当前小规模数据主要命中缓存，适合验证网络搬运与请求路径的机制差异，不足以外推生产磁盘吞吐、故障恢复速度或容量节省。

本脚本的客户端过滤基线是完整对象拉取后进行列裁剪。支持 Parquet footer/column range read 的成熟查询引擎可能发送更少数据，应在接入该引擎后增加 range-read 基线。native-pushdown 对照则在当前实现内使用与 Weave 完全相同的查询插件，避免将一般下推收益全部归给 Weave。
