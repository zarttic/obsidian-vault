---
title: Vault Schema
created: 2026-04-12
updated: 2026-04-12
type: meta
---

# Vault 规范

## 目录结构
- `notes/` - 知识笔记（概念、主题）
- `daily/` - 每日笔记
- `raw/` - 原始资料（不修改）

## 命名规范
- 文件名全小写，中文用拼音或英文
- 空格用 `-` 连接
- 示例：`shuju-qingxie.md`、`spark-optimization.md`

## Frontmatter 必填字段
```yaml
---
title: 标题
created: YYYY-MM-DD
updated: YYYY-MM-DD
type: concept | entity | daily | raw
tags: [标签1, 标签2]
---
```

## 链接
- 使用 `[[wikilink]]` 双向链接
- 每页至少 2 个外部链接
