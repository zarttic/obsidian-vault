# 面试专题

> 数据工程师面试复习笔记。聚焦 Hive + Spark 离线批处理。

## 目录

| 文件 | 内容 |
|------|------|
| [[01-hive]] | Hive DDL/DML/架构/优化 |
| [[02-spark]] | Spark 架构/Shuffle/SQL/优化 |
| [[03-hadoop]] | HDFS/YARN/核心原理 |
| [[04-shujicang-jianmo/]] | 数仓建模专题（分层/主题/指标/DQC）|
| [[05-shuju-qingxie]] | ⭐ **数据倾斜**（专题，深入）|
| [[06-changjing]] | 场景题（异常排查/GC/Shuffle/设计类）|

## 面试框架

```
提问方向          →  核心考点
─────────────────────────────
Hive 架构原理     →  Metastore / Tez / 执行引擎
HiveQL vs SQL    →  动态分区/分桶/窗口函数
Hive 优化         →  小文件/并行度/内存/cbo
Spark 架构        →  Driver / Executor / Shuffle
Spark SQL        →  DataFrame / Dataset / Tungsten
Spark 优化        →  倾斜/广播/内存/并行度
数据倾斜          →  发现/定位/处理/预防（专题）
异常任务排查       →  场景题综合应用
数仓建模          →  分层/主题/宽表/流水 vs 快照
```

## 待整理

- [ ] 03-hadoop.md（HDFS/YARN 原理）
- [ ] 02-spark.md（补充完整 Spark SQL / Shuffle 章节）
- [ ] 04-shujicang-jianmo 子文件（04-01～04-05）
