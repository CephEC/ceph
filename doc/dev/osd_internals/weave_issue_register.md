# Weave 问题、修复方案与设计缺口台账

配置和命令名称已统一为当前 weave 接口；历史验收结果仍对应各节注明的版本与日期，不代表本次重跑。

初次记录：2026-09-14；补充审查：2026-09-16。用途：供后续代码和设计审查。

**当前 Weave 还不完整。**D1 已实现以磁盘元数据为权威的转换提交协议；PG 分裂、功能启停和版本准入仍缺少保证正确性的机制。不能把 D1 或 R1–R3、F1–F7 的验收完成理解为整个存储设计已经闭环。

本文汇总已遇到的问题及处理方案，并补充设计复核发现。前两次设计复核没有修改产品；后续 D1 实现和验收单独记录在 [weave_d1_recovery.md](weave_d1_recovery.md)，D6 的内存集合增长根因随 D1 一并移除。上一轮运行记录见 [weave_review_status.md](weave_review_status.md)，需求与非目标见 [weave_data_lake_requirements.md](weave_data_lake_requirements.md)。

## 1. 状态和证据如何阅读

| 标记 | 含义 |
| --- | --- |
| 已修复／已验证 | 产品代码已改，存在相应单元或真实集群验证；具体范围逐项注明 |
| 模型复现／未修复 | 使用当前产品实现及 FakeHost，或原生 PG 哈希函数，观察到缺口；没有将其称作真实 OSD 故障注入 |
| 静态确认／未修复 | 代码可以确认缺少机制；对运行后果的推断单独说明 |
| 待验证 | 尚无充分运行证据；不据此宣称已经发生故障 |

原问题编号 R1–R3、F1–F7 保留，方便对照。I1–I7 是为更早的联合实现／修复补充的审查编号，不代表它们是本轮新增问题。D1–D5 是首次设计复核的未完成工作；D6–D8 是 2026-09-15 补充的代码缺陷和能力缺口。

## 2. 较早的联合实现与修复 I1–I7

### I1 — Volume 元数据发现和索引维护

- **问题与根因：**Catalog 需要定位 Volume；逐 PG 扫描普通 onode 的成本随普通对象数增加。建立索引后，若属性变更、删除、克隆或重命名不同步维护，会造成漏加载或错误加载。
- **方案：**BlueStore 新增 RocksDB `V` 前缀，以 onode key 索引原始 `_volume_meta` 字节，在同一 KV 事务中维护。加载限定当前 collection／PG。索引从对象创建起维护，挂载不重建，不使用完成标记或提供旧库迁移。
- **代码：**[BlueStore.cc](../../../src/os/bluestore/BlueStore.cc) 的 `load_attr_mirror` 及 onode 事务维护（镜像机制本身在 [AttrMirror.cc](../../../src/os/AttrMirror.cc)）；[ObjectStore.h](../../../src/os/ObjectStore.h)、[PGBackend.cc](../../../src/osd/PGBackend.cc) 的加载接口。
- **证据：**BlueStore 专项 3/3 通过；历史实验覆盖 10000 个普通对象、1032 个 Volume、跨重建批次及 collection split／merge 索引范围。
- **边界：**索引是属性的事务性副本，不是转换事务日志。I1 没有解决 D1，也没有解决逻辑成员的 PG split 语义。

### I2 — 重排队请求丢失逻辑身份

- **触发：**请求已从成员名称翻译为 Volume，进入原生等待队列后重新执行。
- **根因：**若直接用物理身份继续执行，权限检查、对象查找和操作参数会基于错误对象；重复翻译还会破坏原始输入。
- **方案：**请求上下文保存原始对象及操作；在原生权限检查前恢复，再按当前映射重新解释，回复时恢复客户端操作形式。
- **代码：**[WeavePGControllerImpl.cc](../../../src/osd/weave/detail/WeavePGControllerImpl.cc) 的 `prepare_request`；[WeaveMemberTranslator.cc](../../../src/osd/weave/detail/WeaveMemberTranslator.cc) 的 `finish_request`；`WeaveRequestContext`。
- **证据：**逻辑权限、延迟删除及重排队单元测试；真实恢复期间请求等待后继续执行。

### I3 — Catalog 加载失败被当作空 Catalog

- **触发：**ObjectStore 加载错误，或私有 Volume 元数据损坏。
- **根因：**失败后仍激活空映射，会将仍存在的成员误报为不存在。
- **方案：**保留加载错误，拒绝激活；返回实际错误并允许后续重试，不能以“空列表成功”代替加载成功。
- **代码：**Controller 的 `initialize`、`reload_metadata`、`prepare_request`。
- **证据：**加载失败、损坏元数据、修复后重试的永久单元测试。
- **边界：**单条元数据解码和来源检查不等于冲突的多份 Volume 映射能够按事务先后裁决，后者属于 D1。

### I4 — 恢复期间 Catalog 不完整，及 missing 提前清零

- **触发：**primary 的本地 Volume 尚未恢复，或恢复已更新 missing 但事务尚未应用。
- **根因：**只看当前索引或 missing 集合，可能在物理元数据可读前开放逻辑读、写、列举。
- **方案：**本地 missing 与 `active_pushes` 共同形成屏障；恢复事务应用后重新加载并唤醒等待请求。
- **代码：**[WeaveCephHost.cc](../../../src/osd/weave/WeaveCephHost.cc) 的 `has_missing`；Controller 恢复入口；[PrimaryLogPG.cc](../../../src/osd/PrimaryLogPG.cc) 的 `_applied_recovered_object`／replica 回调。
- **证据：**单元测试；历史真实集群中陈旧 primary 恢复时读／写／列举等待，恢复后完成且删除不复活。

### I5 — 在途异步 CALL 在 PG 重置时留下上下文和锁

- **触发：**原生 EC CALL 正在远端执行时发生 interval 变化、角色变化或关闭。
- **根因：**后端回调与 PrimaryLogPG 上下文生命周期未协调，取消后可能仍持有请求和原生对象锁。
- **方案：**后端先销毁相应回调，再取消登记中的 CALL 上下文并释放锁；需要重试时恢复原生请求并重排队。
- **代码：**[ECBackend.cc](../../../src/osd/ECBackend.cc) 的 `on_change`；PrimaryLogPG 的 `cancel_async_calls` 及调用顺序。
- **证据：**真实 CALL 在途时暂停执行节点、触发 PG 重置、恢复节点；请求重试完成，后续写入成功。R2 修复后再次验证。
- **边界：**这验证了 CALL 取消，不等于后台转换的磁盘状态可在任意崩溃点恢复。

### I6 — 原生截断历史无法由成员元数据表达

- **触发：**对象带非零 `truncate_seq` 或 `truncate_size` 后参与打包。
- **根因：**成员元数据只保留部分逻辑属性，丢失截断历史可能改变后续读语义。
- **方案：**这类对象保持原生 EC；候选登记和发布前均检查，防止读取期间状态改变后仍发布。
- **代码：**[WeaveCandidateIndex.h](../../../src/osd/weave/detail/WeaveCandidateIndex.h) 的 `eligible`；Controller 的发布复核。
- **证据：**任一截断字段非零的候选排除、版本变化及发布前截断的永久单元测试。
- **取舍：**这是明确缩小可聚合对象范围，不是实现了截断历史的聚合编码。

### I7 — 普通属性或非当前对象伪造成员映射

- **触发：**普通对象存储名为 `volume_meta` 的属性，或加载器遇到恢复临时对象、clone、旧代际。
- **根因：**只解码属性内容，无法证明它来自实际发布的私有 Volume。
- **方案：**加载接口同时返回真实对象身份；只使用当前 head；检查私有 namespace、实际对象与声明的 Volume 身份一致，私有元数据损坏时报错。
- **代码：**BlueStore loader；Controller 的 `reload_metadata`。
- **证据：**普通属性不能发布映射、私有元数据损坏后关闭访问的单元测试；索引恢复联合回归。

## 3. 上一轮修复 R1–R3、F1–F7

### R1 — 不支持索引的后端连原生 EC 请求也失败

- **触发／根因：**MemStore EC PG 初始化 Weave，默认 loader 返回 `-EOPNOTSUPP`，阻断普通对象请求。
- **修复：**`PrimaryLogPG` 构造时增加 BlueStore 门控；其他后端走原生 EC，不伪造空 Catalog 成功。
- **证据：**实际 MemStore 2+1 池 put／get／list／`hello.say_hello` 通过，后台开启时无私有 Volume；BlueStore 聚合回归通过。
- **边界：**没有验证或提供既有非 BlueStore 聚合数据的迁移。关闭已有 BlueStore 数据的解释层是另一问题，见 D3。

### R2 — 复合 CALL 越过错误继续执行

- **触发／根因：**首次解释将多个异步 CALL 排队，并在第一个结果未知时处理后续操作；即使首个非 FAILOK CALL 失败，后续已排队 CALL 仍可能执行。
- **修复：**未完成 CALL 成为解释屏障；前置 READ 完成并经过错误处理后才建立当前 CALL finisher；每次仅派发一个 CALL，回放后再决定后继操作。
- **追加发现：**READ 错误注入暴露成员预处理提前填充 STAT 输出。已将 STAT 编码延迟到实际执行对应子操作时。
- **代码：**PrimaryLogPG 的 `do_osd_ops`／`execute_ctx`；WeaveMemberTranslator 的 `encode_logical_stat`。
- **证据：**原生、聚合直读、primary 下推的不同参数 CALL、FAILOK、READ→CALL、CALL→READ；原生及聚合 READ→CALL→STAT 注入 `-EIO`，后继无输出；CALL 在途 PG 重置后写锁释放。
- **审查重点：**不能把“整体错误码正确”当作后继操作没有执行的证据；永久客户端明确检查结果 buffer 和 STAT 哨兵值。

### R3 — 下推执行节点插件缺失触发断言

- **触发／根因：**primary 可加载 class 不代表远端执行 shard 的插件目录、允许列表和方法集一致。
- **修复：**[WeaveECAdapter.cc](../../../src/osd/weave/WeaveECAdapter.cc) 返回真实加载错误；空 class 返回 `-EIO`；缺失 method 返回 `-EOPNOTSUPP`。
- **证据：**真实执行节点每次重新启动以清空插件缓存。拒绝加载返回 EPERM；缺失插件走现有 primary 回退并成功；缺失方法返回 EOPNOTSUPP。三种情况 OSD 均存活。

### F1 — 聚合后新建快照读不到成员

- **触发／根因：**Catalog 只有 head 映射，原生来源被删除；快照身份直接走原生查找会得到 ENOENT。物化时使用最新 snapc 还会错误地把对象视为快照之后创建。
- **修复：**快照按 head 解析并物化；v1 元数据保存源 `snapset.seq`，恢复时沿用；有快照历史的删除走物化及原生 COW；私有 Volume 不生成新的池快照 clone。
- **代码：**[WeaveCatalog.h](../../../src/osd/weave/detail/WeaveCatalog.h)／[WeaveCatalog.cc](../../../src/osd/weave/detail/WeaveCatalog.cc)、Controller、ConversionJob、WeaveCephHost、PrimaryLogPG 的 snapc 接点。
- **证据：**池快照和 self-managed 两套真实测试，覆盖创建前不存在、聚合前后快照、全量／局部覆盖、删除、同名重建、字节及元数据、全 OSD 重启。
- **格式代价：**`ENCODE_START(1,1)`，只解码这一种布局：其他 `struct_v` 的属性按损坏拒绝，没有兼容分支，也不再有“以 Volume 快照序号兜底”的降级行为。不能回退到不认这一布局的 OSD。（该布局最初编号 v4：`c33178db7ed` 写 4、读 `[2,4]`，其中 v2/v3 走 `decode_legacy_member` 兼容解码；这些 legacy 路径已在工作区移除，2026-09-26 编码与解码统一为 1。）
- **未覆盖保证：**任意物化崩溃点、混合版本准入和资源耗尽下的快照读取可用性，分别见 D1、D3、D5。

### F2 — thumbnail 参数错误使 OSD 退出

- **触发／根因：**放大参数触发断言，空／截断参数的 decode 在 catch 外；只捕获 OpenCV 异常。
- **修复：**[cls_opencv_thumbnail.cc](../../../src/cls/opencv_thumbnail/cls_opencv_thumbnail.cc) 把参数解码和处理纳入异常捕获；逐维校验比例、固定尺寸、有限值和尾随字节；解码／编码失败返回 EINVAL。
- **证据：**原生和聚合路径的畸形参数、2×2／2×0.25 放大、零／负／NaN／Inf／极小比例、异常固定尺寸、尾随字节；合法参数仍输出 JPEG，OSD 存活。

### F3 — 按 xattr 过滤时漏掉聚合成员

- **触发／根因：**列举已合并逻辑名称，但过滤器仍向被删除的原生来源读属性。
- **修复：**Controller `listing_attribute` 解析为 Volume 上的成员属性；`pgls_filter` 仍把逻辑身份交给过滤器。
- **证据：**聚合前后全量 56、过滤 19 的精确集合，包含匹配、不匹配、缺失属性；页上限 7，实际跨页。

### F4 — 陈旧副本的来源删除恢复把活成员过滤掉

- **触发／根因：**替换 acting set 已完成聚合后，旧副本携带原生来源归队。primary 的有效 Catalog 与物理 DELETE 恢复状态并存，`MissingLoc::is_deleted` 错把逻辑成员视为已删除。
- **修复：**PGNLS 过滤时让当前有效 Catalog 成员优先于物理来源删除；实际逻辑删除仍由 Catalog 移除表达。
- **证据：**冻结恢复，primary 本地 missing 为 0，旧副本待处理 56 个来源删除和 14 个 Volume。旧代码列举为 0；新代码在 active+recovering 窗口全量 56、过滤 19 均正确。
- **审查重点：**证据必须来自恢复窗口内，不能只测 active+clean 后结果。

### F5 — 候选索引保留数无界

- **触发／根因：**提交时登记所有对象，选择时只跳过尺寸不合格项；大量不同小对象、超大对象长期留在 map／set。
- **修复：**按有效最小尺寸及条带对齐上限准入，最多 4096 项／PG；配置收紧会淘汰；容量满时不新增。
- **证据：**各 100000 个过小／过大对象保留 0，10000 个合格对象保留 4096；移除后可重新准入；配置变化测试通过。
- **已知取舍：**被排除对象仅在下次提交时重新判断，没有全 PG 再发现。内存有界已解决，静态历史数据的最终聚合覆盖率没有解决，见 D4。未测生产 RSS。

### F6 — PG 恢复到 clean 后候选不再调度

- **触发／根因：**恢复期间扫描因不 clean 放弃定时器；clean 转换没有再次通知，而此后没有新写入。
- **修复：**`PrimaryLogPG::on_clean` 完成原生处理后调度 Weave，沿用已有角色和 generation 检查。
- **证据：**真实恢复期间提交 8 个候选，之后没有额外写入；进入 clean 后自动打包。单元测试同时验证通知。
- **边界：**唤醒“还保留在索引中的候选”不等于恢复 PG reset 已清空的候选，后者是 D4。

### F7 — busy 首组使其他冷组饥饿

- **触发／根因：**确定性首组选中持续读锁占用的热点对象，整轮放弃；下一轮重复同一组。
- **修复：**候选选择时跳过 busy 对象继续凑组；过期项遍历后淘汰；热点保留供以后尝试，不放松锁约束。
- **证据：**单元测试保证热点先被遍历；真实 16 个并发读者累计完成 2683 次热点读期间，冷组成功打包。
- **后续调整：**打包并发读修复后，仅持有共享读锁的对象也可参与聚合；写锁、排队请求和其他阻塞仍被排除。详见 [weave_pack_reads.md](weave_pack_reads.md)。

## 4. 设计问题 D1–D8

### D1 — P1：转换缺少持久化提交状态与恢复裁决

**状态：已修复／已验证。**119 项 Weave 单测、22 个真实提交故障检查点及 3 个自管理快照故障场景通过。精确范围和证据见 [D1 专项记录](weave_d1_recovery.md)。

**原始根因：**打包在 Volume 落盘之后才复核源版本；物化在删卷之前撤销内存映射。`unpublished_or_retired_` 只在内存中排除尚未发布或已经撤下的 Volume，控制器重建后丢失排除信息。

代码：[WeaveConversionJob.cc](../../../src/osd/weave/detail/WeaveConversionJob.cc) 的 `build_volume`、`resolve_volume`、`submit_member_write`、`submit_volume_remove`；[WeavePGControllerImpl.cc](../../../src/osd/weave/detail/WeavePGControllerImpl.cc) 的 `reload_metadata`、`start_job`、`prepare_request`；[WeaveCatalog.cc](../../../src/osd/weave/detail/WeaveCatalog.cc) 的整体加载。

原诊断模型执行过如下链路：

1. 读取版本 1 的原生 A，提交新 Volume 写入。
2. PG reset 取消完成回调；模拟已提交的 Volume 写入仍落盘。原控制器记得它未发布，因此重载时跳过。
3. 原生 A 更新为版本 2。
4. 重建控制器，只保留磁盘对象和属性；内存“未发布”集合消失。
5. 新控制器将版本 1 的 Volume 加入 Catalog，逻辑 A 重新指向旧数据，即使版本 2 的原生 A 仍存在。

上述原始证据是 Controller／Job 模型复现，并非生产集群中已观察到的数据回退。新回归在相同时序下断言已提交映射生效，新写必须先完成持久化物化交接。

**修复方案：**数据与有效 `volume_meta` 原子提交即生效；源验证提前到提交前，成员预留跨越交接。所有物化成员先持久化，Volume 删除提交后才撤销映射并释放预留。取消后按磁盘恢复；错误回调先等待已获准的事务结束再重载，不能把超时当作回滚。移除内存排除集合，重复磁盘归属关闭请求入口。旧 primary 的内部读写／删除请求不能被新 primary 当作普通请求执行。

**验收方法：**永久单元回归覆盖各提交边界的未提交／已提交丢回调，以及两种超时顺序。独立的 BlueStore 4+2 集群脚本在 22 个关键点中止 OSD、切换 primary、执行已确认的新写／删除／同名重建，再全重启校验，并检查快照、版本、mtime 和 xattr。具体运行结果以专项记录为准；不扩大到 D2/D3 或旧实现已丢失历史的修复。

### D2 — P1：PG split 拆散成员与 Volume，合并后旧数据遮蔽已确认写入

**状态：独立 BlueStore 4+2 集群已复现不可访问及已确认写入的可见版本回退／未修复。属于数据正确性阻断项。**

Volume 使用一个成员的 hash，分组只保证“现在同 PG”，没有保证所有成员完整 hash 相同。`pg_num` 增长后，原来同 PG 的 A、B 可以进入不同子 PG，但 Volume 只能跟随一个 hash。

原生 `pg_t::contains`／`get_split_bits` 模型验证：pool 1、hash 0 和 1 在 `pg_num=1` 同 PG；增长到 2 后，B 属于子 PG，Volume 留在父 PG。当前子 PG 的本地 Catalog 没有该 Volume，客户端却会按 B 的逻辑 hash 路由过去。

2026-09-16 真实验证：16 个成员聚合后，实际 PG 数从 1 增至 2，两个 PG 均 active+clean 时，`member-1` 返回 ENOENT，其余 15 个可读。对 `member-1` 写入 `ACKNOWLEDGED NEW VALUE` 成功，新客户端读到 22 字节、版本 37。实际 PG 数合并回 1 后，同一名称却重新读出旧 Volume 内的 16384 字节、版本 2；全部 OSD 重启后仍如此。新原生对象被旧映射遮蔽，不能仅将 D2 视为扩容期间的暂时不可访问。

测试未运行 mgr；使用 `DaemonServer::_adjust_pgs` 相同的 `pg_num_actual` MON 命令和 `pgp_num_actual` 推进原生分裂／合并，每阶段以新客户端观测。该结果不代替 autoscaler、转换进行中、稀疏卷等其余矩阵。证据位于 `build/weave-design-audit/2026-09-16/`。

代码：[WeaveCephHost.cc](../../../src/osd/weave/WeaveCephHost.cc) 的 `new_volume`；Controller 的 `scan`；[OSD.cc](../../../src/osd/OSD.cc) 的 `split_pgs`。当前没有 Weave 专用的 split 前物化协调或 pool 层准入阻止。合并同样需要清理控制器旧状态，不能用底层索引范围正确代替整体证明。

**待设计方案：**初期可对包含 Weave 数据的池硬性禁止改变 `pg_num`；完整方案需要在 split 生效前持久化协调物化／重分组或设计跨 PG 映射。仅在文档中要求固定 PG 不构成实现上的保护。

**验收要求：**聚合成员、空／稀疏卷、转换进行中分别 split／merge，覆盖 autoscaler 发起变化；变化前后按客户端路由校验读写、列举及故障恢复。

### D3 — P1：启停和版本兼容没有数据格式准入保护

**状态：关闭解释层的后果已在独立 BlueStore 4+2 集群复现；版本准入缺失静态确认／未修复。**

当前 `osd_weave_enabled` 和后台开关默认都为 true；前者在构造时决定是否加载／解释 Catalog。把已有聚合数据的 OSD 以 enabled=false 启动，会跳过映射并走原生路径。模型确认：磁盘 Volume 存在、原生来源不存在，关闭的 Controller 没有加载成员，也没有拒绝这种配置。按原生路径查找会误报对象不存在。

2026-09-16 真实验证：4 个成员聚合后均可读取；关闭 `osd_weave_enabled` 并重启全部 OSD，4 个成员全部返回 ENOENT；重新开启并重启后恢复可读，长度、逻辑版本及记录的内容前缀与关闭前相同。每阶段使用新客户端以排除旧直读路由缓存。这验证了配置导致的不可访问，没有验证关闭期间写入的后果或混合版本接管。

v1 编解码限制只会在读取格式时暴露不兼容；目前没有对应的 pool 持久化功能标志、acting OSD 最低格式版本准入和禁止不安全降级的机制。`WEAVE_READ_REDIRECT` 协商保护的是可选直读消息，不等于所有可能成为 primary 的 OSD 都能解释 v1 数据。

代码：[osd.yaml.in](../../../src/common/options/osd.yaml.in) 的两个 enabled 选项；PrimaryLogPG 构造；Controller 的 `initialize`／`prepare_request`；[ceph_features.h](../../../src/include/ceph_features.h)。没有对混合版本接管作真实集群验证。

**待设计方案：**将“允许产生新聚合数据”和“必须解释已有格式”分开。池记录持久化特性和最低版本；写入当前格式前验证参与节点支持，禁止不兼容节点接管；关闭或降级必须先完成可验证的数据排空，或者明确拒绝操作。D1 的修复不能代替 D2/D3 的部署保护。

**验收要求：**不同 OSD 配置、重启、primary 切换、混合格式版本、关闭后重开、排空后关闭；任何拒绝都要明确报错，不能把实际存在的逻辑对象当成不存在。

### D4 — P2：候选只有提交事件来源，历史冷数据没有最终发现保证

**状态：模型复现／未修复。影响目标工作负载的功能覆盖，不直接等于数据损坏。**

`on_pg_change` 清空 `candidates_`；重新初始化只加载 Volume Catalog，不枚举普通对象。模型中两份合格对象已提交并有扫描定时器，reset 后对象仍存在，但初始化和 `schedule_work` 都不再生成扫描。

新启动时的存量对象、超过 4096 容量被拒绝的候选，以及配置放宽后重新合格的旧对象也只依赖下一次提交。目标场景恰好是写完后不再修改的冷数据，因此“有界索引”还需要“最终能够再次发现”的配套机制。

代码：Controller 的 `on_commit`、`initialize`、`on_pg_change`；[WeaveCandidateIndex.cc](../../../src/osd/weave/detail/WeaveCandidateIndex.cc)。F5 修复内存上界，F6 修复现有候选的 clean 唤醒，均不覆盖这里。

**待设计方案：**以有界批次和可恢复游标扫描原生对象，或提供持久化候选／待扫描范围；明确与实时提交索引的去重、版本复核、重启和容量公平性关系。

**验收要求：**停止写入后重启或切换 primary，超过索引容量的数据最终获得处理；持续写入时旧范围不饥饿，同时验证扫描 I/O 和内存仍有界。

### D5 — P2：必要物化与后台资源共用，永久失败没有收敛路径

**状态：模型复现及静态确认／未修复。打包来源删除持续 EIO 已有真实测试；尚未开展真实满盘、成员物化写入或删卷永久错误测试。**

覆盖写、不支持的操作和快照访问必须先物化；它们使用和可选后台打包相同的 `acquire`。`osd_weave_max_concurrent=0` 会拒绝所有租约，因此这些前台操作也会持续等待。`submit_member_write`／`submit_volume_remove` 对所有负返回值重试，没有区分临时错误、永久错误或可向等待客户端返回的失败。

2026-09-16 补充模型复现：即使没有 I/O 错误、租约可用，Volume 持续存在共享读者时，`start_deaggregation` 也会在建立成员预留前返回；新读仍被接纳，等待的覆盖写无法启动物化。连续 10 次调度与请求重试均无转换 I/O；读者排空后才启动并完成。必要物化缺少公平准入，不能依赖偶然出现的无读者窗口。另一个模型验证租约持续不可用时前台写不推进也不报错，恢复租约后才完成。

代码：Controller 的 `start_deaggregation`／`preprocess_client_op`；ConversionJob 的 `submit_member_write`／`submit_volume_remove`；[WeaveWorker.cc](../../../src/osd/weave/detail/WeaveWorker.cc) 的 `try_acquire`。

由代码推断：一个无法完成的转换会长期保留 PG 的转换槽、逻辑对象预留及 OSD 租约，默认并发为 1 时还会影响其他 PG。满盘时先写存活成员再删卷需要额外空间；已有文档说明了这个空间前提，但尚无完整的失败恢复和前台可用性策略。

**已单独处理的读取问题：**打包期间可直接服务的普通 head 只读请求已与修改预留分离，来源删除失败重试也不再阻塞这类读取。物化的读屏障、必要物化资源不足、永久错误时写请求与租约不能收敛的问题仍未解决，不能把这次读取修复算作 D5 全部完成。实现及专项验证见 [weave_pack_reads.md](weave_pack_reads.md)。

**待设计方案：**给必要物化明确的资源保证／优先级，区分“暂停可选后台工作”和“阻止前台正确性所需转换”；建立空间准入、错误分类及可恢复的失败状态。错误收敛必须遵守 D1 的交接规则，不能绕过持久化结果裁决而简单释放预留。

**验收要求：**并发限制为 0、资源长期占用、ENOSPC、持续 EIO、永久拒绝和客户端超时；验证等待请求如何结束、取消后谁拥有数据，以及其他 PG 的进展。

### D6 — P2：失败打包的内部记录没有回收路径

**状态：根因随 D1 修复移除；未声称做过长期 RSS 压测。**

原实现每次 `scan` 在读取来源前将新的 Volume ID 放入 `unpublished_or_retired_`，源读取、构建或写卷失败却未移除这个 ID，因而持续增长。D1 已删除整个集合及其登记路径，持久化结果由磁盘映射裁决，不再为每次失败积累“未发布卷”记录。

**例子：**同一组对象因持续读错误反复尝试打包，每次都没有成功生成卷，却持续留下新的“未发布卷”记录。F5 的 4096 候选上限约束不到这个集合。

代码：[WeavePGControllerImpl.cc](../../../src/osd/weave/detail/WeavePGControllerImpl.cc) 的 `scan`、`start_job`／`hooks.finish`；[WeaveConversionJob.cc](../../../src/osd/weave/detail/WeaveConversionJob.cc) 的 `submit_member_read`、`build_volume`；WeaveCephHost 的 `new_volume`。

**修复：**采用 D1 的元数据提交协议，移除该集合；提交后丢回调的卷作为有效卷加载，不能作为失败垃圾删除。

**验收要求：**连续注入源读取、卷写入、发布复核及清理失败，测量记录数和内存；确认失败重试有界，同时不破坏晚到写入后的映射隔离。

### D7 — P2：计算下推缺少独立的执行资源预算

**状态：静态确认的隔离能力缺口／未修复；影响程度待压测。**

data-class 在 OSD 进程内执行。Parquet 已限制请求大小、谓词复杂度、批次行数和结果字节数，并捕获分配失败；但解码使用 `arrow::default_memory_pool()`，输出上限不限制总解码内存，也没有每次扫描的独立执行时限。OpenCV 的参数校验同样不等于图像解码的内存预算。

**例子：**一个压缩后不大的 Parquet 含解压后很大的字符串列，即使最终只返回少量结果，也可能先消耗大量解码内存。长时间扫描也可能占用 OSD 执行资源、影响其他请求。这里没有宣称已经观察到 OSD OOM 或退出。

代码：[scan.h](../../../src/cls/parquet_scan/scan.h) 明确说明 IPC 上限不是解码内存上限；[scan.cc](../../../src/cls/parquet_scan/scan.cc) 的 `scan_impl`；[WeaveECAdapter.cc](../../../src/osd/weave/WeaveECAdapter.cc) 的 `execute_data_class`。

**待设计方案：**独立的计算并发准入、解码内存计量／上限、可协作取消的期限及公平调度；对不可中断的第三方解码阶段评估进程隔离。客户端超时和捕获 `bad_alloc` 不能代替这些预算。

**验收要求：**高压缩率、大变长字段、长扫描及并发调用，测量峰值内存、取消延迟和无关普通 I/O 的尾延迟。这与 D5 的必要物化等待是不同的资源问题。

### D8 — P2：清理任务缺少可查询的执行结果

**状态：模型复现及静态确认／未实现；不是异步返回本身的错误。**

清理命令只返回 `accepted`／`already_running`，后台完成通知不携带成功／失败统计；没有持久或可查询的清理轮次结果，无法直接查询处理了多少卷、失败原因、剩余任务及实际释放空间。现有日志和通用 PG 统计可辅助排查，但没有替代这些专用信息。

2026-09-16 模型向清理过程的 Volume 读取注入 EIO：清理游标在转换启动时已经前移，随后仍调用不带结果的完成回调，而 Volume 和成员映射均保留。失败卷没有本轮重试或失败记录；这不是返回了“清理成功”，而是接口无法表达本轮未完成回收的事实。

**例子：**管理员收到“已接受清理”，过一段时间空间没有下降，却无法通过该任务接口分辨是没有符合阈值的卷、等待资源，还是某个卷清理失败。

代码：[OSD.cc](../../../src/osd/OSD.cc) 的 `weave cleanup`、`snapshot_weave_reclaim`；[WeaveService.cc](../../../src/osd/weave/WeaveService.cc) 的 `ReclaimPass`；Controller 的 `Cleanup`／`finish_cleanup`。

**待设计方案：**增加任务 ID、状态查询、分原因计数、最后错误和完成时间；空间指标明确逻辑字节与包含 EC 冗余／填充的物理字节口径。诊断信息不能以“命令已接受”冒充回收成功。

**验收要求：**没有候选、成功清理、资源等待、读取失败、删除失败及任务取消均能得到可区分的状态。

## 5. 可复核证据与测试缺口

上一轮证据：`build/weave-review-results/2026-09-14/`，含 81/81 Weave 单元测试、3/3 BlueStore 测试、真实 4+2／2+1 场景、快照和故障注入记录。对应历史快照是其中的 `completed-review-status.md` 及源码 SHA256；本次文档继续更新不会改写历史归档。

首次设计复核的历史证据：

```text
build/weave-design-audit/2026-09-14/
  design_probes.cc
  design_probes
  build.log
  results.log
```

四个 probe 使用当时实现及已有 FakeHost，分别记录 D1、D4、D3 和 D2。运行方式：

```sh
build/weave-design-audit/2026-09-14/design_probes \
  --gtest_filter='WeaveDesignAudit.*' --admin-socket=
```

**注意：这些 probe 断言当前缺口确实存在，所以结果显示 4 个 PASS。它们不是正确性验收通过，也未纳入永久正常回归集。**后续修复时应将它们改为断言正确行为，并补真实故障测试。它们重新编译了诊断翻译单元，链接现有产品库，没有替换 `ceph-osd` 或正式 `unittest_weave`。

后续 D1 已完成专项故障矩阵，打包并发读修复已有 131 项单测及真实读取／故障回归，详见各专项文档。2026-09-16 证据新增于 `build/weave-design-audit/2026-09-16/`：7 项当前实现诊断（1 项 D1 安全性对照、6 项缺口断言），以及 D2 分裂／合并与 D3 启停真实集群结果。诊断 PASS 表示复现了预期行为，不能计作 D2–D5、D8 修复完成。

仍缺少 autoscaler／混合版本接管、真实满盘、物化永久错误和代表性规模的容量、CPU、网络及前台尾延迟验证。D7 仍只有静态隔离能力分析。

## 6. 功能取舍与建议审查顺序

单成员覆盖写脱离、跨 Volume 直接合并、应用生命周期 API 属于当前明确的非目标；它们不是“实现遗漏”。整卷物化的放大是既有取舍。RGW/S3 文件到 RADOS 对象的映射、查询引擎接入和性能收益仍未验证，因此当前也不能称为完整的数据湖解决方案。

D1 的提交协议和专项验证见独立记录。后续优先处理 D2／D3 的强制准入保护，再解决 D4 的最终发现和 D5 的资源／错误收敛。单纯继续增加正常读写测试，不会补齐这些设计机制。

就当前状态而言：**Weave 是主要功能可运行、已有针对性回归的实验实现；仍有明确的正确性和完整性缺口，不应按已完成的通用存储功能验收。**
