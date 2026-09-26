# Weave D1：持久化提交与故障恢复

配置和命令名称已统一为当前 weave 接口；历史验收结果仍对应各节注明的版本与日期，不代表本次重跑。

## 提交规则

Volume 的数据与 `volume_meta` 在同一条原生对象事务中提交。该事务提交后，
Volume 就是成员的权威来源；Catalog 是磁盘状态的缓存，Objecter 完成回调
只推动后续步骤，不决定已提交的映射是否有效。元数据使用当前布局（v1，
`ENCODE_START(1,1)`），保留成员的逻辑版本、mtime、xattr 与原生 `snapset.seq`。

反向物化先写出所有存活成员，等待各成员的持久化完成，再删除 Volume。
**Volume 删除事务是交还原生对象的提交点。**删除确认之前不撤销 Catalog
映射，也不释放成员预留。无需另加阶段日志：Volume 的存在及其有效元数据
足以决定当前归属。

| 磁盘状态 | 权威来源 | 恢复后的行为 |
| --- | --- | --- |
| Volume 尚未提交 | 原生成员 | 保留原生数据，旧转换请求失效 |
| Volume 已提交，部分或全部原生来源还在 | Volume | 加载映射；原生来源只是旧副本 |
| 已写出部分或全部物化副本，Volume 仍存在 | Volume | 保留映射；需要原生访问时重新完成物化 |
| 所有成员已提交且 Volume 已删除 | 原生成员 | 不再加载旧映射，允许新写、删除和同名重建 |

## 职责与约束

- `WeaveConversionJob` 负责转换顺序。`validate` 在写 Volume 之前执行；
  `publish` 和 `detach` 只反映已经提交的事务，不包含可回滚的发布决策。
- Controller 在选组时持有 PG 锁、排除修改冲突与原生等待者，并预留全部成员。提交前
  再检查完整源版本、资格、watcher、克隆、快照序号、现有归属与预留身份。
  打包预留阻止修改，但允许可直接服务的普通 head 只读请求：发布前读原生对象，
  发布后读 Volume；共享读锁不再阻止选组或提交。原生对象锁保护已开始的旧读，
  清理等待旧读结束，重新排队的请求重新解析路由。物化仍保留整个交接的读屏障。
  并发读取实现与验证见 [weave_pack_reads.md](weave_pack_reads.md)。
  删卷期间 PG 列举也等待，防止 xattr 过滤读取已删除的 Volume。
- Ceph host 保持原生单对象事务及持久化确认语义。PG reset 后未获准执行的
  旧请求必须经过任务身份校验；发往另一个 primary 的内部请求直接返回
  `-ECANCELED`，不能降级成普通逻辑写入或删除。已经提交的请求由 Ceph 的
  peering/recovery 确定结果，`op_cancel` 不被视为回滚。
- Volume 写入的错误回调也不代表事务未提交。任务先作废旧 tid，等待已经
  获准执行的原生事务释放对象锁，再强制重载磁盘映射，最后才允许新的访问。
  这同时覆盖 Objecter 超时而 OSD 仍 active 的情况。
- Controller 仅在 PG active 且本地元数据恢复完成后初始化。恢复扫描不再有
  `unpublished_or_retired_` 内存排除集合。
- Catalog 整体加载失败时不发布部分结果。两个磁盘 Volume 同时声称拥有
  同一成员时返回冲突，Controller 以 `-EIO` 关闭请求入口；不以扫描顺序
  或版本大小猜测归属。磁盘修复后可以重新初始化。

以上协议约束新执行的转换，不能恢复旧实现已经丢失的历史数据，也不提供
冲突元数据的自动修复。元数据只接受当前布局（v1）：没有兼容分支，其他
`struct_v` 的属性按损坏拒绝。
D2 的 PG split、D3 的功能关闭与混合版本准入、D4/D5/D7/D8 的独立问题
仍需分别处理。D6 所述排除集合增长随集合移除一并消失。

## 自动化测试

`src/test/weave/test_weave_conversion.cc` 将模拟 I/O 的落盘与回调分开，
在打包和物化的每一个读、CPU、写入、删除边界重建 Controller。测试分别
覆盖尚未提交与已提交但丢失回调的结果，然后执行新写、删除、同名重建、
清理和再次重建，检查数据、逻辑版本、mtime、xattr 与快照序号。
其他回归包括源验证失败、删卷失败重试、重复归属、迟到内部请求和列举屏障。

```sh
build/bin/unittest_weave --admin-socket= \
  --erasure-code-dir=/root/ceph/build/lib
python3 src/test/weave/check_boundaries.py /root/ceph
```

真实集群测试由 `src/test/weave/durable_recovery.sh` 启动独立的 Monitor 和
6 个 BlueStore OSD，使用 4+2 EC、单 PG、禁用自动扩 PG 的池。工作目录必须
未使用；脚本只停止自己启动的进程，并保留数据和日志供复查。

```sh
bash src/test/weave/durable_recovery.sh /root/ceph/build /tmp/ceph-weave-d1
# 只检查一个提交点：
bash src/test/weave/durable_recovery.sh /root/ceph/build /tmp/ceph-weave-d1-one \
  --point pack_committed:0
```

仅测试时设置 `osd_weave_debug_crash_point=checkpoint:index`。默认值为空，
无故障注入；匹配后 OSD 主动 abort。index 为零起始成员序号，卷级检查点为 0。

| 检查点 | 故障位置 |
| --- | --- |
| `pack_before_write:0` | 源验证完成，尚未提交 Volume |
| `pack_committed:0` | Volume 已持久化，尚未更新内存 Catalog |
| `pack_published:0` | Catalog 已更新，尚未删除来源 |
| `source_before_remove:0..3` / `source_removed:0..3` | 每个原生来源删除前后 |
| `member_before_write:0..3` / `member_written:0..3` | 每个物化成员写入前后 |
| `volume_before_remove:0` / `volume_removed:0` | 删除 Volume 的提交前后 |
| `volume_detached:0` | 内存映射撤销后、释放预留前 |

每个集群用例验证确实命中了指定故障点，重启故障 OSD，并通过
primary-affinity 切换到不同的 primary。然后执行已确认的新写、删除与
同名重建，检查已有快照，再重启全部 OSD 并重复校验。测试同时检查逻辑版本、
mtime、xattr，以及对象创建之前的快照不存在该对象。`results.json` 保存
各检查点、切换前后的 primary 和确认版本，`stopped.json` 记录清理结果。

## 本轮验收结果（2026-09-15）

证据目录：`build/weave-d1-results/2026-09-15/`。

- **119/119 Weave 单元测试通过**，含 28 个提交边界组合、错误回调结果裁决、
  超时先后顺序、旧 primary 请求拒绝及列举屏障。比原 81 项增加 38 项。
- **3/3 BlueStore Volume 索引测试通过**，验证事务更新、删除、克隆、重命名、
  集合隔离、旧库重建和重新挂载。
- **22/22 真实 4+2 EC 故障检查点通过**，每个检查点均命中指定主动崩溃，
  并经过不同 primary 接管及全 OSD 重启后的读写、删除、同名重建和池快照校验。
- **3/3 自管理快照故障场景通过**：`pack_committed:0`、`member_written:0`、
  `volume_removed:0`，使用相同的数据、版本、属性和重启断言。
- 更新后的 D1 诊断 probe 断言安全行为并通过；同一诊断程序的 D2/D3/D4
  仍断言旧缺口存在，**不计入修复通过数量**。原历史诊断目录未改写。
- 模块边界、Python／Shell 语法、配置校验和 `git diff --check` 通过。

本轮重新编译受影响的产品与测试翻译单元，并重新链接 `ceph-osd`、单测和
诊断程序；静态 `libcommon.a` 与共享 `libceph-common` 均更新。最终集群
测试前后 OSD 与单测二进制 SHA256 一致。未用旧诊断程序证明新修复。
最终三个隔离集群的 3 个 Monitor、18 个 OSD 均已停止；数据留在测试工作
目录供复查，压缩日志、配置、逐项结果及停止记录已经归档到证据目录。

超时后事务继续落盘的两种顺序由 Controller／Job 模型验证，未额外宣称做过
真实网络超时注入。真实集群覆盖表中的主动崩溃点和池／自管理快照；未穷举
其他 EC profile、生产规模、任意网络分区和混合版本。遗留的永久错误收敛与
后台租约限制仍按 D5 跟踪。
