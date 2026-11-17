# ADR-0001: 强类型缓存 API 采用泛型路线（TypedCache[T] + 可插拔序列化器）

- 状态：已接受
- 日期：2026-11-17

## 背景

go_biz_cache 定位为强类型业务 Redis 缓存库：业务不接触裸 `[]byte`/`string`。在 Go 中实现强类型缓存有两条主流路线：

1. 运行时泛型：`TypedCache[T]`，序列化/反序列化在运行时完成，依赖可插拔 `Serializer` 接口（json/msgpack/proto）。
2. 代码生成：为每个类型生成专用编解码代码，消除反射/序列化运行时开销。

库要求支持多数据结构（Object/Hash/Set/List/ZSet），且 K/V/field 类型组合多样（hash 还需独立的 `FieldCodec` 处理 field 类型 F），代码生成路线的结构复杂度与维护成本显著更高。

## 决策

泛型起步：提供 `TypedCache[T]` 泛型句柄，序列化经可插拔 `Serializer` 接口（默认 JSON）；hash field 类型 F 经 `FieldCodec`（string 直通，int/enum 经转换函数映射为 Redis field 名）。v1 中 K 恒为 `string`（业务键即模板展开后的字符串），定义层 `Define[V]` 不含 K。

代码生成路径作为性能不够时的后续演进方向（design.md §12），不进入 v1。

## 备选方案

- **代码生成**：无序列化运行时开销，但类型组合（5 种结构 × K/V/F 组合 × 序列化器）导致生成矩阵庞大，接入成本高，初期收益不成比例。被否决：性能需求未证实前不承担该复杂度。

## 后果

- 业务侧全程强类型，杜绝裸字节操作带来的类型错误。
- 序列化开销存在，但可经更换序列化器（msgpack/proto）缓解；瓶颈确认后再评估代码生成。
- 泛型句柄按数据结构拆分（`TypedObjectCache[K,V]` / `TypedHashCache[K,F,V]` 等），API 面随结构类型自然扩展。
