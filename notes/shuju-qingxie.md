---
title: 数据倾斜面试整理
created: 2026-04-12
updated: 2026-04-12
type: concept
tags: [Spark, 大数据, 面试]
sources: []
---

# 数据倾斜面试整理

## 什么是数据倾斜

数据分布不均匀，大量数据集中在少数节点/分区，导致任务拖慢整体进度。

**表现：** 1000个任务，999个几秒完成，1个跑了1小时。

## 常见原因

| 场景 | 倾斜原因 |
|------|----------|
| JOIN | 大表与小表join，小表key集中 |
| GROUP BY | 少数key数据量巨大（如"其他"） |
| COUNT DISTINCT | 单一key大量重复 |
| 窗口函数 | 分区键选择不当 |
| 原始数据 | 业务数据本身不均匀（地域、性别） |

## 解决方案

### 1. JOIN 倾斜
- **广播join**：小表广播到executor，避免shuffle
- **打散key**：热点key加随机前缀分散到不同分区
- **两阶段聚合**：先局部聚合再全局聚合

### 2. GROUP BY 倾斜
```sql
-- 两阶段聚合
SELECT key, SUM(c) FROM (
    SELECT CONCAT(key, '_', CAST(RAND()*10 AS INT)) as key, SUM(c) as c
    FROM table
    GROUP BY CONCAT(key, '_', CAST(RAND()*10 AS INT))
) t
GROUP BY key
```

### 3. AQE 自适应优化
```python
spark.sql("SET spark.sql.adaptive.enabled=true")
spark.sql("SET spark.sql.adaptive.coalescePartitions.enabled=true")
```

## 核心思路
1. 找热点key → 看日志/UI/监控
2. 打散 → 加随机前缀/盐值
3. 聚合 → 两阶段GROUP BY
4. 广播 → 小表JOIN大表用Broadcast
5. 调参 → shuffle.partitions, AQE

## 相关概念
- [[Spark优化]]
