# ADR-0007: 集群 slot 控制采用 key 变换策略函数（HashTag: TagFunc）

- 状态：已接受
- 日期：2026-11-17

## 背景

Redis Cluster 以 key 中首个 `{` 到首个 `}` 之间的文本计算 slot。默认整键散列时，同租户/同分片组的 key 散落到不同 slot，无法单 pipeline 操作。库需要在"聚合"（同 slot 低延迟）与"打散"（负载均衡）之间给业务双向选型，同时与 Shard/FieldShard 子 key、多机房隔离等场景兼容。

常见做法是在定义上配置一个固定的 hash-tag 字符串（整键包裹），表达力有限：无法按占位符聚合、无法只聚合本键分片、无法支持前缀隔离等场景。

## 决策

定义级 `HashTag` 为 key 变换策略函数：`nil` 表示不变换（默认整键散列，子 key 天然打散）；预设 `TagFromPlaceholder("tenant")`（占位符段原地包裹）与 `TagWhole`（整键包裹）；自定义 `TagFunc(TagInput) (TagOp, error)` 支持四类变换：

| Kind | 行为 | 典型用途 |
|------|------|----------|
| `TagWrap` | 将 `At` 定位的子串原地包上 `{}` | 聚合同 slot（同租户、同分片组） |
| `TagPrefix` | 头部注入 `Value` | 多机房/多环境前缀隔离、头部 tag 聚合 |
| `TagSuffix` | 尾部注入 `Value` | 追加分段标识 |
| `TagInsert` | 于 `At` 匹配处之前注入 `Value` | key 中段插入分隔内容/tag |

契约：

1. 纯函数确定性：同入参必同变换，否则 slot 漂移；
2. Wrap 定位合法：`At` 必须为 `LogicalKey` 非空子串，片段应取完整占位符值整段；
3. Prefix/Suffix/Insert 的 `Value` 可含自闭合 `{...}` 参与 slot 控制；`At` 为纯定位子串不得含大括号；禁止嵌套 `{}`；`Value`/`At` 不得含空白字符；
4. 单 op：每次返回一个变换，序列组合待有真实需求再扩展；
5. 违反契约在 key 构建时报错，不静默降级。

与 Shard/FieldShard 子 key 叠加：子 key 后缀 `:s{i}`/`:h{i}` 追加在变换之后，slot 由 tag 段决定。选型结论：声明产生 `{...}` 的变换 → 全部分片同 slot（单 pipeline，热点集中）；不变换（默认）→ 整键散列天然打散（ClusterClient 自动 scatter-gather），大值分片默认建议打散。

## 备选方案

- **固定 hash-tag 字符串配置**：只能整键包裹，无法覆盖按占位符聚合、前缀隔离等场景。被否决。
- **仅支持 Wrap 一种变换**：无法表达多机房隔离、中段插入需求。被否决。

## 后果

- 聚合与打散双向可选，变换模型统一覆盖聚合/隔离/注入场景。
- 函数式风格与 `Splitter`/`Hasher` 可插拔风格一致。
- 跨定义共用同一占位符即共享 slot 归属，为将来跨缓存多 key 原子操作预留基础（当前 API 未提供跨 key 原子操作）。
- 契约约束依赖业务自觉，错误在 key 构建时显式报错，无静默降级。
