# Weave 联合验收与审查修复状态

配置和命令名称已统一为当前 weave 接口；历史验收结果仍对应各节注明的版本与日期，不代表本次重跑。

上一轮修复完成日期：2026-09-14。仓库：`/root/ceph`。

后续 D1 专项修复：转换改为“有效元数据落盘即接管、删卷落盘才交还原生对象”，并补充取消、超时和冲突裁决。该轮实现及独立故障验收见 [weave_d1_recovery.md](weave_d1_recovery.md)；下文保留的是此前 R1–R3／F1–F7 的历史验收，不将其测试数量混入 D1 结果。

原交接文档列出的 **R1–R3、F1–F7 已全部修复并完成对应验证**。源码已重新编译、链接，真实集群回归使用更新后的 OSD。需求说明和发布说明已同步；隔离测试集群及临时脚本、数据已清理。未提交代码，未回滚或清理用户原有工作区改动。

本记录中的“完成”指上述十项审查问题及交接要求的验收、文档和清理，不扩大既有实验性功能的保证范围。当时尚缺的转换提交协议已在后续 D1 专项实现；Weave 整体 PG split／resharding 仍是独立边界，见第 5 节。

**此前设计复核（D1 实施前）：Weave 整体仍不完整。**当时将 D1–D5 汇总到 [问题与设计缺口台账](weave_issue_register.md)，模型确认了未发布 Volume 重载遮蔽新原生版本、PG split 哈希分离、关闭解释层跳过已有成员、PG reset 丢失候选四种缺口；必要物化的资源和永久错误处理另有静态发现。那次复核没有修改产品；D1 后续修复以专项记录为准。

2026-09-15 补充静态审查新增 D6–D8：失败打包记录缺少回收、计算下推资源预算不足、清理结果缺少可查询状态。当时均未修复。后续 D1 删除了导致 D6 增长的内存集合；D7/D8 仍未解决，也未做对应压力测试。

## 1. 联合改动 R1–R3

### R1 — 非 BlueStore 后端保持原生 EC 可用：已完成

`PrimaryLogPG` 仅对 BlueStore EC PG 启用 Weave。其他 ObjectStore 的默认 `load_attr_mirror` 仍返回 `-EOPNOTSUPP`，不以空 Catalog 掩盖不支持的后端。

最新二进制上，三个 MemStore OSD 的 2+1 EC 池通过原生 put／get／精确 list 和 `hello.say_hello`；后台聚合设置为 true 后，私有 Volume namespace 仍为空。BlueStore 4+2 聚合和直读回归同时通过。

测试中旧 MemStore 池的 PG 在 OSD 重启后没有保留，因此最终验证使用新建的 `weave-native-final` 池；这项结果不表示 MemStore 数据跨重启持久化。本门控也不是既有非 BlueStore 聚合数据的迁移方案。

### R2 — 复合 CALL 的错误短路及异步顺序：已完成

`PrimaryLogPG::do_osd_ops` 把未完成的 EC_CALL 作为解释屏障：

1. 前面存在 pending async READ 时，先完成 READ 并通过原生错误处理，不提前建立 CALL finisher。
2. 每次只排队当前一个 CALL，立即返回 `-EINPROGRESS`。
3. `execute_ctx` 仅在当前解释结果为 `-EINPROGRESS` 且存在待发 CALL 时发送；回调后的重放及 FAILOK 规则决定是否继续后继操作。

故障注入还发现一个额外问题：成员预处理提前编码逻辑 STAT，即使前置 READ 失败，客户端仍收到 STAT 输出。现已改为执行到 STAT 子操作时才调用 `encode_logical_stat`，预处理只清空结果。

已验证：

- 原生 EC、聚合副本直读、关闭直读后的 primary 下推：不同参数的多个 CALL 保持各自结果；非 FAILOK 错误不执行后继 CALL／READ／STAT；FAILOK 正常继续；READ→CALL、CALL→READ 正常。
- BlueStore READ 故障注入：原生对象和聚合成员的 READ→CALL→STAT 均返回前置 `-EIO`，后继 CALL 没有输出，STAT 哨兵值不变。
- 原生 CALL 已发出时暂停执行 OSD、触发 PG interval 重置，再恢复 OSD：请求重新派发并完成，随后写入成功取得对象锁。

永久真实客户端：`src/test/weave/compound_calls.cc`、`src/test/weave/regression_client.cc`。

### R3 — 执行副本插件不一致导致断言：已完成

`WeaveECAdapter::execute_data_class` 返回 class 加载的真实错误码；缺失 method 返回 `-EOPNOTSUPP`，不再断言执行节点必然具备 primary 的插件。

对实际接收下推 CALL 的 OSD 2，每次修改配置后都重启以清空 class 缓存。三种场景均完成请求且 OSD 存活：

| 执行节点故障 | 实测结果 |
| --- | --- |
| class load list 拒绝 `parquet_scan` | 返回 `-EPERM` |
| 插件目录缺少 `parquet_scan` | 加载失败触发现有 primary 回退，结果正确 |
| class 存在但缺少 method | 返回 `-EOPNOTSUPP` |

故障配置已恢复，最终回归通过。

## 2. 模块问题 F1–F7

### F1 — 聚合前后快照语义：已完成，旧格式边界已明确

原二进制已复现“聚合前快照可读、聚合后新建快照返回 ENOENT”。修复包括：

- Volume 元数据使用当前布局（v1／`ENCODE_START(1,1)`，最初编号 v4），保存每个源对象原生 `snapset.seq`。
- 快照请求按 head 查 Catalog；遇到转换中的对象先等待，已有聚合成员先物化，再由原生 snapset／clone 路径解释快照。
- 内部物化恢复源对象原来的快照序号，不套用物化时的最新池快照上下文。
- 有池快照历史或请求快照上下文时，删除先物化，再走原生 copy-on-write；无快照历史的逻辑删除仍可更新稀疏成员元数据。
- 私有 Volume 的创建／删除使用内部空快照上下文，避免为物理布局对象生成新的池快照克隆。

池快照和 self-managed snapshot 两套真实 4+2 测试均通过：对象创建前快照返回 ENOENT；聚合前、聚合后快照保持原字节、size／mtime／user_version 和 xattr；全量覆盖、局部覆盖、删除、删除后同名重建只改变当前 head；全部十个 OSD 重启后再次校验通过。测试确认源对象已物理打包，不能仅凭逻辑读成功判定覆盖了聚合路径。

**兼容范围：**只解码当前布局（v1，`struct_v` 必须为 1）；v2／v3 的 legacy 解码路径（`WeaveCatalog.cc` 的 `decode_legacy_member`）已移除，解析失败即按损坏处理（本版本不考虑与旧格式共存）。当时复现所用的 v3 场景已不再受支持。现有数据也不能回退到不认这一布局的 OSD。

永久测试：Catalog 编解码及物化单元测试；`src/test/weave/review_regressions.py` 的 `seed`／`mutate`／`verify` 阶段。

### F2 — OpenCV thumbnail 异常输入导致 OSD 退出：已完成

参数 decode 纳入异常处理，捕获 `ceph::buffer::error` 和 `cv::Exception`；客户端输入校验全部返回 `-EINVAL`。逐维拒绝非有限数、零／负比例、放大比例，拒绝零／超大固定尺寸及尾随参数字节；校验图像解码和 JPEG 编码结果。

原生 EC 和聚合调用均验证：空／截断参数，`{2,2}`、`{2,0.25}`、零、负数、NaN、Inf、极小比例，非法固定尺寸和尾随字节返回错误；合法比例和固定尺寸仍输出 JPEG。OSD 保持存活。

### F3 — xattr 过滤列举遗漏成员：已完成

`listing_attribute` 将逻辑成员的存储层属性名解析为 Volume 上保留的成员属性；过滤器仍接收逻辑对象身份。

聚合前后对 56 个对象做精确分页列举，覆盖匹配、不匹配、缺失属性；全量结果为 56 个，过滤结果恰为 19 个，无重复。测试将 `osd_max_pgls` 设为 7，实际跨页验证。

### F4 — 陈旧副本物理删除恢复期间遗漏成员：已完成

PGNLS 的 `MissingLoc::is_deleted` 过滤不再剔除当前有效 Catalog 中仍存活的逻辑成员，真正逻辑删除仍从 Catalog 移除。

使用新增的第七个 BlueStore OSD 替换陈旧副本：替换 acting set 到达 clean 后，将 56 个源对象打包成 14 个 Volume；冻结恢复，让保留原生来源的旧副本重新加入。该窗口内 primary 本地 missing 为 0，陈旧副本缺少 56 个来源删除及 14 个 Volume。

原二进制在此窗口列举为 **0 个**；修复后在 **active+recovering、恢复尚未结束** 时，全量 56 个和过滤 19 个精确结果集均通过。保存了修复前后的 PG query，不以恢复完成后的结果替代窗口验证。

### F5 — 候选索引无界保留：已完成

按最小对象大小及 `floor(max_volume_size / k / unit) * unit` 槽位上限准入，单 PG 候选上限为 **4096**；后台聚合关闭期间同样有界。配置收紧会淘汰不合格候选。

再发现策略已明确：被尺寸或容量排除的对象在下一次已提交修改时重新判断；放宽配置不会全 PG 扫描旧对象。现有合格候选在重新启用后台聚合后获得扫描唤醒。

单元测试连续登记各 100000 个 4 KiB 小对象和超上限对象，保留数为 0；登记 10000 个合格对象后保留数为 4096，选出并移除一组后可接纳新候选；配置收紧、对齐限制和再次提交均通过。这验证了索引节点数量的硬上界，未开展生产规模 RSS／性能测量。

### F6 — PG 进入 clean 后未唤醒：已完成

`PrimaryLogPG::on_clean` 在原生 clean 处理后明确调度 Weave，沿用既有角色、generation 和转换限制。

在真实恢复窗口内提交 8 个合格候选，随后不再写入；恢复解除并进入 clean 后，原候选自动打包。单元测试同时覆盖 clean 通知后的调度。

### F7 — busy 首组导致冷对象饥饿：已完成

候选选择时检查可用性，跳过 busy 对象继续寻找完整组；过期候选在遍历结束后淘汰。热点对象仍保留供后续扫描，不放松对象锁约束。

单元测试保证热点对象先被遍历，验证后续冷组仍被选中。真实集群中 16 个并发读者持续读取热点对象，期间累计完成 2683 次热点读取，冷对象组仍完成打包。

## 3. 最终构建与回归证据

以下目标完整构建通过：

```text
ninja -C build -j3 ceph-osd ceph-mon ceph-mgr rados ceph-kvstore-tool \
  ceph_test_objectstore unittest_weave cls_parquet_scan cls_openssl_md5 \
  cls_opencv_thumbnail ceph_test_weave_compound ceph_test_weave_regressions
```

随后针对 R2 的逻辑 STAT 补修，使用当前 `compile_commands.json` 重新编译受影响翻译单元，并执行 Ninja 生成的实际 archive／link 命令。更新后的 OSD 已全部重启，以下最终回归包含这次补修。Ninja 曾提示 `premature end of file` 并扩大重编译范围；没有清理用户构建目录。

| 检查 | 最终结果 |
| --- | --- |
| Weave 单元测试 | **81/81 通过** |
| BlueStore Volume 索引测试，`*BlueStoreVolumeAttrs*/2` | **3/3 通过** |
| `src/test/weave/check_boundaries.py /root/ceph` | 通过 |
| 新增 Python 回归客户端语法检查 | 通过 |
| `git diff --check` | 通过 |
| 真实 BlueStore 4+2 EC、MemStore 2+1 EC | R1–R3、F1–F4、F6–F7 对应场景通过 |

综合验收从 16 个 Parquet、8 个二进制对象和 40 个空对象重新开始，验证原生基线、物理打包、副本直读、primary 下推和重启。逐对象检查精确分页列举，完整／范围／跨边界／EOF 读取，逻辑元数据、xattr、MD5 和不同参数的 Parquet 扫描；包含含 NUL 数据、错误参数及复合请求。

最终还重跑了成员删除、同名重建、覆盖物化、删除全部非空成员；七个 BlueStore OSD 重启后，空 Volume 不复活成员。手工回收将物理对象数从 **45 降到 40**，最终精确保留 40 个原生空对象。

原交接阶段已取得的索引专项和联合证据继续保留：10000 个普通对象／1032 个 Volume、跨 1024 条索引重建批次、ObjectStore collection split／merge 范围、接收端拒绝路由后的 primary 回退、陈旧 primary 元数据恢复屏障、删除索引标记后的重建。它们属于历史验证，不冒充本轮全部重新执行过的场景。当前实现已删除旧库索引重建和完成标记，对应历史重建测试不再适用。

测试过程中已排除并修正的 fixture 问题：MemStore 重启后旧 PG 不存在；C++ `write_full` 消耗复用 bufferlist 导致后续对象为空；快照物理计数必须同时计入 clone 和 snapdir；Python `io.execute` 成功可能返回正的输出长度；候选集合不应依赖无关的 hobject 遍历顺序。对应有效场景均已重新通过。

## 4. 永久回归入口与本地归档

永久回归代码：

- `src/test/weave/test_weave.cc`、`test_weave_conversion.cc`：编解码、候选准入／公平性、转换、快照、列举及接口回归。
- `src/test/objectstore/store_test.cc`：BlueStore 索引维护和重新挂载。
- `src/test/weave/compound_calls.cc`：目标 `ceph_test_weave_compound`，参数为 `CONF POOL PARQUET_OBJECT OUTPUT_DIR`。
- `src/test/weave/regression_client.cc`：目标 `ceph_test_weave_regressions`，参数为 `CONF POOL MODE [OBJECT]`；模式包括 `thumbnail`、`list-create`、`list-check`、`list-all`、`read-error`。
- `src/test/weave/review_regressions.py`：参数为 `--conf CONF --pool POOL --state STATE [--self-managed] seed|mutate|verify`。

集成客户端要求独立测试池及相应 fixture。快照客户端先在关闭聚合时 `seed`，由测试环境启用聚合并确认物理打包后 `mutate`，重启后 `verify`；两种快照模式使用不同池。thumbnail 需要有效图像，复合 CALL 需要适配测试列的 Parquet；`read-error` 需要实际注入 READ 错误。这些客户端不负责管理 OSD 进程或恢复故障配置。

本机精简证据归档于：

```text
/root/ceph/build/weave-review-results/2026-09-14/
```

该目录位于已有构建目录内，不纳入源码提交。包含构建／单元测试／真实集群结果、F4 恢复窗口 query、CALL 重置追踪、快照校验状态、清理前后物理统计及源码／二进制 SHA256。`previous-review-status.md` 保存原交接检查点。

其中 `r2-read-error.log` 是发现逻辑 STAT 提前输出时的失败证据，最终修复后的两条 PASS 位于 `final-checks.log`；`f4-before.txt` 是原实现的失败证据。不要把这些保留的复现结果当作最终测试失败。

需求说明见 [weave_data_lake_requirements.md](weave_data_lake_requirements.md)，发布说明见仓库根目录 `PendingReleaseNotes`。

## 5. 保留边界与清理状态

- 此前验收只包含稳定态全重启。后续 D1 已补充提交协议及关键崩溃边界测试，见专项记录；其结论仍限定在所验证的 profile、版本和故障模型内。
- ObjectStore 索引 split／merge 范围通过，不等于 Weave 整体 PG split／resharding 已解决。
- 未穷举所有 EC profile、跨版本全集群组合或生产规模性能；快照元数据的旧格式和降级限制见 F1。
- 隔离集群共 1 个 Monitor、10 个 OSD（7 个 BlueStore、3 个 MemStore），已全部停止；额外恢复 OSD 9 单独停止，随后 supervisor 完成其余进程清理并以 0 退出。
- 原临时目录 `/tmp/ceph-weave-verify.tnrn3890` 已删除，包含临时脚本、输入数据、故障插件及 OSD 数据目录；仅保留上述精简证据。未删除其他会话资源。
- R1–R3、F1–F7 及本次交接列出的构建、回归、文档、资源清理工作均已完成。工作区改动保留供审阅，未执行 Git 提交。
