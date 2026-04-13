---
title: 场景题
created: 2026-04-13
updated: 2026-04-13
type: concept
tags: [Spark, Hive, 大数据, 面试, 场景题, 异常排查]
stage: 
---

# 场景题

> 异常任务排查与优化是数仓面试的高频场景题。本质是**故障定位 + 优化方案**的综合能力考察。

---

## 目录

- [1. 异常任务排查框架](#1-异常任务排查框架)
- [2. 数据倾斜](#2-数据倾斜)
- [3. GC / Memory 超限](#3-gc--memory-超限)
- [4. Shuffle 失败与重试](#4-shuffle-失败与重试)
- [5. 资源争抢（CPU/网络/磁盘）](#5-资源争抢cpu网络磁盘)
- [6. 数据质量问题](#6-数据质量问题)
- [7. 任务 Hang 与死锁](#7-任务-hang-与死锁)
- [8. 设计类场景题](#8-设计类场景题)
- [9. 排查工具链](#9-排查工具链)

---

## 1. 异常任务排查框架

### 1.1 通用排查流程（STAR 法）

```
Situation → 确认异常现象
     ↓
Task    → 明确排查目标（哪个阶段/任务/指标异常）
     ↓
Action  → 逐步排查（外部 → 内部，定性 → 定量）
     ↓
Result  → 定位根因，给出处理方案
```

### 1.2 异常分类速查表

| 现象 | 可能原因 | 优先级 |
|------|---------|--------|
| Task 长尾（少数极慢）| 数据倾斜 | 🔴 高 |
| 所有 Task 都慢 | 资源不足 / 数据膨胀 | 🔴 高 |
| 任务失败（OOM）| 内存不足 / 数据量超预估 | 🔴 高 |
| 任务卡住（Hang）| 外部依赖超时 / 数据倾斜严重 | 🔴 高 |
| Shuffle 重试 | Shuffle 服务异常 / 网络抖动 | 🟡 中 |
| 重复计算 | 依赖任务重复运行 | 🟡 中 |
| 结果不正确 | 数据质量问题 / UDF Bug | 🔴 高 |

### 1.3 排查第一步：确认问题边界

```
是任务慢？还是任务失败？
├── 失败 → 看 Executor Log / Application Log
│         ├── ExitCode: OOM → 内存问题
│         ├── ExitCode: 137 → 被 Kill（资源不足）
│         └── ExitCode: 1   → 业务逻辑异常
│
└── 慢 → 看 Spark UI / Hive Task Log
          ├── 个别 Task 慢 → 数据倾斜
          └── 所有 Task 慢 → 资源不足 / 計算復雑
```

---

## 2. 数据倾斜

> 详见 [[05-shuju-qingxie]]。此处重点补充**场景识别**而非重复原理。

### 2.1 面试中如何描述倾斜问题

**问题模板：**
> "任务原来跑 30 分钟，某天突然变成 3 小时，Spark UI 发现 Stage 1 有一个 Task 跑了 2 小时 50 分钟，其他 999 个 Task 各 3 秒。这是典型的数据倾斜，倾斜发生在 Shuffle Write 阶段，热点 key 是 `status='success'` 占总数据量 98%。"

**回答结构：**
```
1. 发现过程（Spark UI / 日志）
2. 定位根因（热点 key 是哪个）
3. 分析为什么（业务数据不均还是 SQL 写法问题）
4. 处理方案（根据场景选：广播/打散/AQE/两阶段聚合）
5. 预防措施（上线前检查 key 分布）
```

### 2.2 倾斜导致的衍生问题

```
数据倾斜
├── 拖慢整体任务（Stage 等待最慢 Task）
├── Executor OOM（热点分区数据量超过单节点容量）
├── Shuffle 失败（分区数据量超过 Shuffle 文件大小限制）
└── 任务 Hang（倾斜超过超时阈值被强制中断）
```

---

## 3. GC / Memory 超限

### 3.1 识别方法

```
Executor Log 关键字:
- "GC overhead limit exceeded"
- "OutOfMemoryError: Java heap space"
- "ExecutorLost: Exit reason: OOM"
```

**Spark UI 特征：**
- Task 进度不动，Executor 显示 0-100% 循环跳动（频繁 GC）
- 部分 Executor lost，任务重新计算拖长时间

### 3.2 根因分类

| 类型 | 触发原因 | 典型场景 |
|------|---------|---------|
| Old Gen GC（Full GC）| 数据在 Old 区堆积 | 大表 BROADCASTJOIN，小表在 Executor 堆叠 |
| Young GC 频繁 | Eden 区太小 | 并行任务太多，对象创建过快 |
| Off-Heap 溢出 | 外部shuffle/thrift server | Hive on Tez，Tez Container 内存配置不足 |

### 3.3 解决方案

```python
# Spark: 增加 Executor 内存
--conf spark.executor.memory=8g
--conf spark.executor.memoryOverhead=2g   # Off-heap 预留

# 开启自适应 GC（CMS / G1）
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:MaxGCPauseMills=500"
```

```sql
-- Hive: 调整 Tez 内存
SET hive.tez.container.size=4096;
SET hive.tez.java.opts=-Xmx3072m;
SET hive.auto.convert.join=true;  -- 减少数据量
```

### 3.4 面试回答模板

> "OOM 一般是内存数据量超过 Executor 容量，常见原因：(1) 大表 BROADCASTJOIN 导致Executor 接收数据超载；(2) 数据倾斜导致单分区数据量过大；(3) 并行任务太多，内存竞争。处理方案：增加 `executor.memory` + `memoryOverhead`、开启 AQE 动态调整分区、减少单节点并行度。"

---

## 4. Shuffle 失败与重试

### 4.1 Shuffle 失败的原因

```
Shuffle 失败
├── 🔴 Shuffle File 丢失（磁盘故障/网络丢包）
├── 🟡 Reduce 端请求超时（Map 端生成文件慢）
├── 🟡 Partition 数量过多（大量小文件）
└── 🟡 Spark Shuffle Service 异常
```

### 4.2 Spark Shuffle 流程回顾

```
Map Task
  └── 写 Shuffle File（本地磁盘）
        ↓
      标记到 Index File
        ↓
Reduce Task
  └── 通过 Shuffle Service 拉取 Shuffle File
        ↓
      失败点：任意一步都可能出问题
```

### 4.3 排查步骤

```
Step 1: 看 Task 失败原因
        └── "ShuffleFetchFailedException" = Shuffle 文件拉取失败

Step 2: 确认失败的是 Map 还是 Reduce
        └── Map 失败 → 磁盘/网络问题
        └── Reduce 失败 → 请求超时

Step 3: 看重试次数
        └── 默认 3 次，超过阈值任务彻底失败
```

### 4.4 解决方案

```python
# 增加 Shuffle 并行度（减少单分区数据量）
spark.sql("SET spark.sql.shuffle.partitions=400")

# 开启 Shuffle 压缩（减少网络传输）
spark.sql("SET spark.sql.shuffle.compress=true")

# 调整 fetch 等待超时
spark.sql("SET spark.network.timeout=300s")
spark.sql("SET spark.shuffle.io.maxRetries=4")

# 开启 External Shuffle Service（生产必备）
--conf spark.shuffle.service.enabled=true
```

```sql
-- Hive: 调整 Shuffle 参数
SET hive.exec.reducer.bytes.per.reducer=256000000;  -- 256MB per reducer
SET hive.merge.mapfiles=true;  -- 合并小文件
```

---

## 5. 资源争抢（CPU/网络/磁盘）

### 5.1 如何判断是资源争抢

```
任务慢，但数据量不大，也没有倾斜
    ↓
看 Executor CPU / 内存监控
    ├── CPU 使用率 100%，但 Task 数量不多 → 单任务计算密集
    ├── 内存使用率高，GC 不频繁 → 数据结构内存开销大
    └── 网络带宽占满 → Shuffle 数据量大，网络瓶颈
```

### 5.2 常见场景

#### 场景 A：单节点 CPU 争抢

```python
# 并行任务数 > 可用 Core 数，任务排队等待 CPU
# 解决：减少单节点并行度
spark.sql("SET spark.task.cpus=2")  # 每个 Task 用 2 个 CPU
spark.sql("SET spark.executor.cores=4")  # 每 Executor 4 核（原来可能配置了 8 核跑 8 并行）
```

#### 场景 B：网络带宽占满

```
大量 Shuffle 数据在网络间传输
    ↓
解决：开启 Shuffle 压缩、增加数据本地性
```

```python
spark.sql("SET spark.sql.shuffle.compress=true")
spark.sql("SET spark.shuffle.compress=true")

# 数据本地性调优
spark.sql("SET spark.locality.wait=3s")  # 等待数据本地再分发
spark.sql("SET spark.locality.wait.process=3s")
spark.sql("SET spark.locality.wait.node=1s")  # Node 级别优先
```

#### 场景 C：磁盘 IO 争抢

```bash
# Shuffle 数据写入本地磁盘，过多并发写同一磁盘导致 IO 队列堆积
# 解决：增加 Executor 数量，减少单 Executor 并行度
--conf spark.executor.instances=10
--conf spark.executor.cores=2  # 减少单 Executor 资源
```

### 5.3 资源配置公式

```
最佳 Executor 数 ≈ (总 Core 数 / 每任务所需 Core 数)

每 Executor 内存 = 总内存 / Executor 数

单 Executor 并行 Task 数 = Executor 核心数 / 每任务所需 CPU

# 示例：16 核 64GB 机器，跑 Spark
# 配置 4 个 Executor，每个 4 核 16GB
--conf spark.executor.cores=4
--conf spark.executor.memory=14g
--conf spark.executor.memoryOverhead=2g
# 每个 Executor 并行 4 个 Task，4 个 Executor 共 16 并行 = 满配 16 核
```

---

## 6. 数据质量问题

### 6.1 数据质量问题导致的异常

```
数据质量问题 → 任务失败 or 结果错误
├── NULL 值泛滥（导致 JOIN 结果爆炸 or 聚合失效）
├── 空字符串 vs NULL 混淆（Hive/ Spark 处理不一致）
├── 数据类型不一致（字符串 '123' vs 整数 123）
├── 重复数据（未去重的 Lookup 表导致 JOIN 膨胀）
├── 异常值/极端值（年龄=-1，金额=-9999）
└── 数据延迟到达（分区数据为空但任务仍运行）
```

### 6.2 典型问题：JOIN 笛卡尔积爆炸

```sql
-- 问题：两表都有大量重复 key，导致结果行数爆炸
SELECT a.id, b.name
FROM a JOIN b ON a.type = b.type
-- a 表 type 有 10000 个重复，b 表 type 有 5000 个重复
-- 结果：10000 * 5000 = 5000万行（原本可能只需要几千行）
```

**排查：**
```sql
SELECT type, COUNT(*) FROM a GROUP BY type ORDER BY COUNT(*) DESC LIMIT 10;
SELECT type, COUNT(*) FROM b GROUP BY type ORDER BY COUNT(*) DESC LIMIT 10;
```

**解决：**
```sql
-- 先去重再 JOIN
WITH dedup_a AS (SELECT DISTINCT id, type FROM a),
     dedup_b AS (SELECT DISTINCT type, name FROM b)
SELECT a.id, b.name
FROM dedup_a a JOIN dedup_b b ON a.type = b.type
```

### 6.3 NULL vs 空字符串问题

```sql
-- Hive 中：NULL 和 '' 是不同的
-- JOIN 时行为不同
SELECT * FROM a JOIN b ON a.id = b.id
-- 如果 id = '' 和 id = NULL，JOIN 结果不同（NULL 不会匹配任何值包括另一个 NULL）

-- 空字符串字段也参与 Shuffle，导致大量空 key 落入同一分区
```

**处理方案：**
```sql
-- 先清洗再 JOIN
SELECT *
FROM (
    SELECT COALESCE(id, CONCAT('n', CAST(RAND()*1000 AS INT))) AS safe_id, ...
    FROM a
) a
JOIN (
    SELECT COALESCE(id, CONCAT('n', CAST(RAND()*1000 AS INT))) AS safe_id, ...
    FROM b
) b ON a.safe_id = b.safe_id
```

---

## 7. 任务 Hang 与死锁

### 7.1 如何判断 Hang

```
Task 进度长时间不变（超过 10 分钟）
    ↓
区分：
├── 数据倾斜导致的长尾（Task 仍在跑，只是慢）
└── 真正的 Hang（Task 完全不动，等待资源/锁/外部依赖）
```

### 7.2 Hang 的常见原因

| 原因 | 特征 | 排查方法 |
|------|------|---------|
| 外部依赖超时 | Task 卡在某个 Stage | 看日志，确认依赖接口响应时间 |
| 数据倾斜严重超时 | 单 Task 数据量远超预期 | Spark UI Task 进度极慢 |
| 死锁（资源竞争）| 多个 Task 互相等待 | Executor Thread Dump |
| Shuffle Service 不可用 | 所有 Shuffle 操作卡住 | 检查 Shuffle Service 状态 |
| Disk 满 / IO 阻塞 | 写操作全部卡住 | `df -h` 检查磁盘使用率 |

### 7.3 死锁排查（Executor Thread Dump）

```bash
# Spark: 抓取 Thread Dump
# Spark UI → Executors → Thread Dump

# 或通过 jstack
jstack <pid>
```

**死锁典型模式：**
```
Thread A: 持有锁 X，等待锁 Y
Thread B: 持有锁 Y，等待锁 X
→ 互相等待，永久阻塞
```

**解决：** 增加锁粒度（减少锁竞争）或使用乐观并发框架。

---

## 8. 设计类场景题

### 8.1 经典场景：如何设计一个数据倾斜无忧的 ETL

**问题：** "你负责一个日均 10TB 数据量的 Hive ETL，发现每天都有数据倾斜导致任务延迟，如何从架构层面解决？"

**回答框架：**
```
1. 预防层（建表/入库）
   - 分桶表：高频 JOIN 字段分桶，减少 Shuffle
   - 合理的分区策略（按日期 + 业务类型双重分区）
   - 上游数据质量检查，热点 key 预警

2. 执行层（ETL 开发规范）
   - SQL 规范：禁止大表 JOIN 大表，优先广播
   - 聚合规范：两阶段聚合处理 GROUP BY 倾斜
   - 上线前：EXPLAIN 分析执行计划，估算数据分布

3. 监控层（任务治理）
   - 任务耗时监控：超过预估 2 倍自动告警
   - 关键节点 key 分布采样（每日自动跑分布检查）
   - 历史倾斜任务记录，形成知识库

4. 容错层（AQE + 告警）
   - 生产开启 AQE 自适应
   - 任务失败自动重试 + 倾斜自动处理
```

### 8.2 场景：任务失败率突然升高

**问题：** "某天 Hive 任务失败率突然从 1% 上升到 30%，但数据量没有明显变化，如何排查？"

**排查路径：**
```
Step 1: 确认是特定任务失败还是全局失败
        └── 全局 → 平台/集群问题
        └── 特定 → 任务自身逻辑

Step 2: 看失败日志
        ├── OOM → 内存问题，可能是某分区数据突增
        ├── Shuffle 失败 → 网络/磁盘/服务异常
        └── 业务逻辑错 → 数据质量问题（某字段突然格式变了）

Step 3: 对比昨天和今天的数据
        ├── 数据量变化 → 上游数据源问题
        └── 数据量不变 → 可能是 Schema 变化（新增字段/字段类型改变）

Step 4: 查监控
        └── 是否有定时任务在失败前修改了表结构？
        └── 是否有上游数据源在失败前切换了接口？
```

### 8.3 场景：如何优化一个跑了 2 小时的 SQL

**回答框架：**
```
1. 先 EXPLain 看执行计划（定位耗时在哪个 Stage）
   └── 大多数时间在 Shuffle Write → 优化 Shuffle
   └── 大多数时间在 Map → 优化数据源读取

2. 定位具体瓶颈
   ├── Task 数量过少 → 增加 shuffle partitions
   ├── 单 Task 数据量过大 → 增加并行度
   ├── 大量小文件 → 合并小文件
   └── 数据倾斜 → 找热点 key 处理

3. SQL 改写
   ├── 减少 Shuffle 次数（多个小聚合 vs 一次大聚合）
   ├── 过滤下沉（先 WHERE 再 JOIN）
   └── 物化中间结果复用

4. 资源调整
   ├── 增加 Executor 数量
   ├── 开启 AQE
   └── 开启广播 JOIN
```

---

## 9. 排查工具链

### 9.1 工具速查

| 工具 | 用途 | 入口 |
|------|------|------|
| Spark UI | Task/Stage/Executor 状态 | http://driver:4040 |
| Hive Tez UI | Tez DAG 可视化 | HiveServer2 Web UI |
| Ganglia | 集群 CPU/内存/网络 | 集群监控 |
| Grafana | 自定义指标监控 | 监控平台 |
| Yarn ResourceManager | 队列资源使用 | http://rm:8088 |
| Executor Log | 具体 Task 错误日志 | Spark UI → Executor |
| JVM Profiler | GC 分析/CPU 分析 | jstack/jstat/jmap |

### 9.2 常用诊断命令

```bash
# 查 Executor OOM 的 Heap Dump
jmap -dump:format=b,file=heap.bin <pid>

# 查 GC 情况
jstat -gc <pid> 1000  # 每秒打印 GC 统计

# 查磁盘使用
df -h
du -sh /data/spark/*

# 查网络带宽
sar -n DEV 1 10

# 查进程 CPU 使用
top -Hp <pid>
```

---

## 面试答题框架总结

```
场景题回答 = 现象 + 根因 + 方案 + 预防

1. 现象：先描述你观察到的具体异常（Task 耗时分布/错误日志/监控指标）
2. 根因：从现象倒推最可能的原因（分类：倾斜/资源/质量/外部）
3. 方案：给出 1-3 个解决方案（从简单到复杂：参数 → SQL改写 → 架构调整）
4. 预防：如何避免下次发生（监控/规范/自动化检查）

避免：只说"加内存/加分区"这种片面的回答，要展示系统思维。
```

---

**相关笔记：**
- [[05-shuju-qingxie]] — 数据倾斜专题
- [[02-spark]] — Spark 架构与 Shuffle
- [[01-hive]] — Hive 优化参数
