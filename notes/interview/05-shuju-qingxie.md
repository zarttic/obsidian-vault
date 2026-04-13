---
title: 数据倾斜
created: 2026-04-13
updated: 2026-04-13
type: concept
tags: [Spark, Hive, 大数据, 面试, 数据倾斜]
stage: 
---

# 数据倾斜

> 数据分布不均匀，大量数据集中在少数节点/分区，导致整体任务拖长。是离线数仓的高频故障和面试核心考点。

---

## 目录

- [1. 什么是数据倾斜](#1-什么是数据倾斜)
- [2. 倾斜的根因分类](#2-倾斜的根因分类)
- [3. 如何发现与定位](#3-如何发现与定位)
- [4. 处理方案库](#4-处理方案库)
- [5. 业务级规避](#5-业务级规避)
- [6. 面试高频问题](#6-面试高频问题)
- [7. 知识链](#7-知识链)

---

## 1. 什么是数据倾斜

### 1.1 本质

数据在集群各分区/节点间分布不均，少数分区数据量远超平均值。

### 1.2 典型表现

| 维度 | 正常任务 | 倾斜任务 |
|------|---------|---------|
| Task 数量 | 1000 个，各耗时 ~5s | 999 个 5s，1 个 1h |
| Stage 耗时 | 10s | 1h+ |
| Executor 日志 | 负载均衡 | 个别 Executor OOM |
| Spark UI | Task 耗时柱状图均衡 | 长尾柱状，右偏严重 |

### 1.3 倾斜 vs 其他慢任务

```
慢任务
├── 数据倾斜（分布不均，热点 key）
├── 资源不足（内存/OOM，CPU 争抢）
├── Shuffle 失败（重试拖长时间）
├── 数据膨胀（JOIN 笛卡尔积）
└── 任务本身计算复杂（Huge JSON 解析）
```

**倾斜的核心特征：** 绝大多数任务很快，少数极慢。资源并非不足，是分配不均。

---

## 2. 倾斜的根因分类

### 2.1 分类总览

```
倾斜根因
├── 🔴 Key 分布本身不均（业务数据）
├── 🔴 Shuffle 机制引入（聚合/关联操作）
├── 🟡 特定算子特性（COUNT DISTINCT / 窗口函数）
└── 🟡 数据质量问题（NULL / 空字符串 / 异常值）
```

### 2.2 业务数据天然不均

| 场景 | 热点 key | 示例 |
|------|---------|------|
| 用户行为 | 活跃用户 | `"user_id = '999999'` |
| 地域 | 超级城市 | `"city = '北上广深'` |
| 性别 | 性别字段 | `"sex = '女'`（数据中女性偏多）|
| 电商 | 爆款商品 | `"item_id = '2024爆款'` |
| 日志 | 默认值 | `"status = ''` 或 `'N/A'` |

### 2.3 Shuffle 引入的倾斜

Shuffle 是倾斜的**放大器**。任何需要按 key 重分区的操作都可能触发：

```
Shuffle 流程:
Map → (key, value) → Partition(key.hashCode % numPartitions) → Reduce

倾斜触发点:
1. key.hashCode 分布不均 → 部分 Partition 数据量远大于其他
2. 特定 key 相同值过多 → 落入同一 Partition
```

**高危操作（必然触发 Shuffle）：**

```sql
-- GROUP BY 任意字段
SELECT key, COUNT(*) FROM t GROUP BY key

-- JOIN（按 key 关联）
SELECT * FROM a JOIN b ON a.id = b.id

-- DISTINCT
SELECT DISTINCT key FROM t

-- 窗口函数（PARTITION BY 不均匀字段）
SELECT *, ROW_NUMBER() OVER (PARTITION BY status) FROM t
```

### 2.4 常见场景深度分析

#### 场景 A：JOIN 倾斜

```
大表 A JOIN 小表 B
问题：B 表的 key 集中（如 B 中 80% 行的 key = 'unknown'）
结果：所有匹配的 A 行都涌向同一个 Reduce 分区
```

```sql
-- 典型问题 SQL
SELECT a.id, b.name
FROM large_table a
JOIN dim_table b ON a.type = b.type  -- type 字段倾斜（如 80% 是 'OTHER'）
```

#### 场景 B：GROUP BY 倾斜

```
少数 key 数据量巨大
如：GROUP BY status，status='成功' 占 99%
```

#### 场景 C：COUNT DISTINCT 倾斜

```sql
-- 问题：单一 key 大量重复
SELECT COUNT(DISTINCT user_id) FROM log_table
-- 如果 user_id 大量重复（如爬虫/机器人），聚合在一个任务完成

-- 解决：用 GROUP BY + COUNT 替代
SELECT COUNT(*) FROM (SELECT user_id FROM log_table GROUP BY user_id) t
```

#### 场景 D：窗口函数倾斜

```sql
-- 问题：PARTITION BY 倾斜字段
SELECT *,
       SUM(amount) OVER (PARTITION BY dt)  -- dt 只有少量唯一值
FROM orders
```

#### 场景 E：NULL 值导致的倾斜

```sql
-- NULL 值默认 Hash 为 0，所有 NULL 落入同一分区
SELECT * FROM a JOIN b ON a.id <=> b.id  -- NULL-safe join，NULL 也参与 shuffle
```

**优化：** 先过滤或赋予随机 key：

```sql
-- 给 NULL 随机打散
SELECT *, IF(id IS NULL, CONCAT('n', RAND()), id) AS safe_id
FROM a JOIN b ON IF(a.id IS NULL, CONCAT('n', RAND()), a.id) = b.id
```

---

## 3. 如何发现与定位

### 3.1 发现工具

#### Spark UI（最直接）

```
Stage 页签 → Task 耗时柱状图
  └── 右偏严重（长尾）= 倾斜

具体操作：
1. 点击 slow task 的 Task ID
2. 看 Executor 日志，找具体倾斜的 key
```

#### 日志关键字

```
grep "SkewedPartition" spark.log
grep "KVStore" spark.ui.JVMCPUTime
```

#### Spark 配置开启倾斜监控

```python
spark.sql("SET spark.sql.skewJoin.enabled=true")
spark.sql("SET spark.sql.adaptive.enabled=true")  # AQE 自动检测倾斜
```

#### Hive 配置

```bash
set hive.map.aggr=true          # 开启 map 端聚合
set hive.groupby.skewindata=true # 开启数据倾斜优化
```

### 3.2 定位步骤

```
Step 1: 确认是倾斜还是其他慢任务
        └── Spark UI Task 柱状图：右偏 → 倾斜；均匀慢 → 资源不足/计算复杂

Step 2: 确认倾斜发生的 Stage（Map 还是 Reduce）
        └── 看 Stage DAG，Shuffle 边界处

Step 3: 确认导致倾斜的 Key（通过日志/采样分析）
        └── Spark: 查看 Slow Task 日志中的 key 值
        └── Hive: SELECT key, COUNT(*) GROUP BY key ORDER BY COUNT(*) DESC LIMIT 10

Step 4: 分析根因（业务不均还是 SQL 写法问题）
```

### 3.3 快速定位 SQL（Hive/Spark 通用）

```sql
-- 采样分析 key 分布
SELECT key, COUNT(*) as cnt
FROM table
GROUP BY key
ORDER BY cnt DESC
LIMIT 20;
```

---

## 4. 处理方案库

### 4.1 方案总览

```
处理策略
├── 🟢 参数调优（AQE / shuffle partitions）
├── 🟢 SQL 改写（广播、两阶段聚合）
├── 🟡 打散 key（加盐 / 随机前缀）
├── 🟡 预处理（预聚合、数据清洗）
└── 🔴 业务沟通（数据分布治理）
```

### 4.2 AQE 自适应查询（最简单有效）

> Spark 3.0+，自动处理倾斜。配置简单，覆盖大多数场景。

```python
spark.sql("SET spark.sql.adaptive.enabled=true")
spark.sql("SET spark.sql.adaptive.coalescePartitions.enabled=true")
spark.sql("SET spark.sql.adaptive.skewJoin.enabled=true")
spark.sql("SET spark.sql.adaptive.skewJoin.skewedPartitionFactor=5")  # 默认 5 倍
```

**AQE 做了什么：**
- 自动检测倾斜分区（> 中位数 5 倍）
- 将倾斜分区拆分为多个小任务并行处理
- 动态合并小分区（任务过少时）

### 4.3 广播 JOIN（解决小表 JOIN 倾斜）

```python
# Spark: 广播小表（< 10MB 自动触发，可手动指定）
from pyspark.sql import functions as F

result = a.join(F.broadcast(b), "key", "left")
```

```sql
-- Hive: 开启广播JOIN
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=10000000;  -- 10MB 阈值
```

**何时用：**
- 小表可放入内存（通常 < 20GB）
- 大表 JOIN 小表，小表 key 集中
- 绝对禁止：大表 JOIN 大表（会 OOM）

### 4.4 打散 key（Salting / 加盐）

> 给热点 key 加随机前缀，分散到不同分区，最后再合并。

```python
from pyspark.sql import functions as F

# Step 1: 对热点 key 加 N 个随机前缀，打散数据
def add_salt(df, key_col, num_salts=10):
    salted_df = df.withColumn(
        "salt_key",
        F.concat(df[key_col], F.lit("_"), (F.rand() * num_salts).cast("int"))
    )
    return salted_df

# Step 2: 两阶段聚合
# 阶段1：按 salt_key 局部聚合
# 阶段2：按原始 key 全局聚合
```

```sql
-- Hive SQL 两阶段聚合
SELECT key, SUM(cnt) FROM (
    SELECT CONCAT(key, '_', CAST(RAND()*10 AS INT)) AS key, SUM(cnt) AS cnt
    FROM table
    GROUP BY CONCAT(key, '_', CAST(RAND()*10 AS INT)), key  -- 保留原始 key 用于第二阶段
) t
GROUP BY key
```

### 4.5 两阶段聚合（处理 GROUP BY 倾斜）

**核心思想：** 局部聚合 + 全局聚合，避免在 Reduce 端单点聚合。

```sql
-- 示例：订单表按 status 聚合，但 status='已支付' 占 99%
-- 第一阶段：打散 + 局部聚合
WITH stage1 AS (
    SELECT
        status,
        CAST(RAND() * 9 AS INT) AS salt,
        COUNT(*) AS cnt,
        SUM(amount) AS amount
    FROM orders
    GROUP BY status, CAST(RAND() * 9 AS INT)
)
-- 第二阶段：按原始 key 聚合
SELECT status, SUM(cnt), SUM(amount)
FROM stage1
GROUP BY status
```

### 4.6 预聚合 / 预处理

```
时间换空间：在 ETL 前置阶段做轻度聚合，降低下游数据量
```

```sql
-- 将高频聚合结果预先写入中间表
CREATE TABLE agg_user_daily AS
SELECT user_id, dt, COUNT(*) as pv, SUM(amount) as amount
FROM events
GROUP BY user_id, dt;

-- 下游直接查询预聚合结果，避免全量 GROUP BY
SELECT * FROM agg_user_daily WHERE user_id = 'hot_user';
```

### 4.7 shuffle partitions 调参

```python
# 倾斜时增加分区数，稀释热点分区
spark.sql("SET spark.sql.shuffle.partitions=800")  # 默认 200，太少加剧倾斜

# AQE 下可动态调整
spark.sql("SET spark.sql.adaptive.coalescePartitions.minPartitionSize=1MB")
```

**原则：** 分区数 ≈ 并行任务数，通常设为 core 数的 2-4 倍。

### 4.8 动态分区与 bucket

```sql
-- Hive：开启 bucket 表，按 key 分桶存储，JOIN 时避免全量 shuffle
SET hive.enforce.bucketing=true;
SET hive.optimize.bucketmapjoin=true;

CREATE TABLE orders_bucketed (
    order_id STRING,
    amount DOUBLE
) CLUSTERED BY (order_id) INTO 256 BUCKETS;
```

### 4.9 处理方案对比

| 方案 | 适用场景 | 成本 | 推荐度 |
|------|---------|------|--------|
| AQE | 通用，自动检测 | 配置修改 | ⭐⭐⭐⭐⭐ |
| 广播 JOIN | 小表 JOIN 大表 | 内存充足 | ⭐⭐⭐⭐⭐ |
| 打散 key | 热点 key 明确 | SQL 改写 | ⭐⭐⭐⭐ |
| 两阶段聚合 | GROUP BY 倾斜 | SQL 改写 | ⭐⭐⭐⭐ |
| 预聚合 | 重复聚合场景 | 额外存储 | ⭐⭐⭐ |
| bucket 表 | 已知倾斜 key | 建表改写 | ⭐⭐⭐ |
| 增加分区数 | 临时缓解 | 参数调整 | ⭐⭐⭐ |
| 业务数据治理 | 根本解决 | 跨团队协作 | ⭐⭐ |

---

## 5. 业务级规避

### 5.1 建表规范

```
设计良好的分区/分桶策略，是成本最低的倾斜预防
```

- **分桶表**：对高频 JOIN 字段分桶，相同 key 落入同一桶，减少 shuffle
- **合理分区**：按时间 + 业务维度双重分区，避免数据集中在一个分区
- **避免 NULL 热点**：NULL 值用随机 UUID 替代，或单独分区

### 5.2 数据质量检查

```sql
-- 上线前检查 key 分布
SELECT key, COUNT(*), COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as ratio
FROM table
GROUP BY key
HAVING COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () > 1  -- 占比超过 1% 预警
ORDER BY ratio DESC
LIMIT 10;
```

### 5.3 监控告警

```
倾斜监控 → 任务耗时 > 预估 3 倍 → 告警 → 自动触发 AQE 优化
```

---

## 6. 面试高频问题

### Q1: 如何发现数据倾斜？

**回答框架：**
> 1. **表象判断**：Spark UI 看 Task 耗时柱状图，右偏严重 + 大多数任务秒完成但少数极慢
> 2. **定位 Stage**：确定是哪个 Stage 倾斜（Shuffle 边界）
> 3. **定位 Key**：从慢 Task 日志采样，或跑 `GROUP BY key ORDER BY COUNT(*) DESC` 找热点 key
> 4. **确认根因**：是业务数据不均还是 SQL 写法问题

### Q2: JOIN 倾斜怎么处理？

**满分回答：**
> - **方案一（推荐）**：广播小表 `F.broadcast(small_df)`，避免 Shuffle，O(1) 复杂度
> - **方案二**：打散热点 key，给热点 key 加随机前缀，分到不同分区后再 JOIN，最后合并
> - **方案三**：开启 AQE `spark.sql.adaptive.skewJoin.enabled=true`，自动检测并处理
> - **禁忌**：大表 JOIN 大表不要广播，会 OOM

### Q3: GROUP BY 倾斜怎么处理？

**满分回答：**
> 两阶段聚合 + 打散：
> 1. 给 key 加随机盐值，局部聚合：`GROUP BY key, RAND()*N`
> 2. 全局聚合：`GROUP BY key`
> 同时配合 `spark.sql.shuffle.partitions=800` 增加并行度

### Q4: AQE 和手动优化怎么选？

**满分回答：**
> - AQE 是兜底方案，适合未知倾斜（线上日常任务）
> - 已知热点 key 的固定场景，手动打散效果更稳定可控
> - 最佳实践：**先手动优化 + 开启 AQE 作为最终安全网**

### Q5: 如何从根本上解决倾斜？

**满分回答：**
> - **数据层**：预聚合写入、良好的分桶分区设计
> - **业务层**：与产品/数据治理团队协作，优化数据分布（如把"其他"类打散）
> - **架构层**：用好 Lambda/Kappa 架构，在数据入口做分布校验

---

## 7. 知识链

```
上游概念: Shuffle 机制 → Hash Partition → Task 并行度
本专题:   数据倾斜 → 发现 → 定位 → 处理 → 预防
下游应用: 场景题（异常任务排查/优化）
```

**相关笔记：**
- [[02-spark]] — Spark Shuffle 原理
- [[01-hive]] — Hive 优化参数
- [[06-changjing]] — 异常任务排查（含倾斜导致的 OOM/Hang）

---

> 🔗 上次整理：[[shuju-qingxie]]（基础版），本版为面试深度专题
