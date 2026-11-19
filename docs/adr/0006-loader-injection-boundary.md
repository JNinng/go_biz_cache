# ADR-0006: 库边界为纯缓存层，源数据访问由业务注入 Loader 契约

- 状态：已接受
- 日期：2026-11-17

## 背景

read-through 缓存需要"缓存未命中时取源数据"。取源方式有两种：

1. 库内置 DAO/ORM 集成，感知数据库；
2. 库只定义回源契约，由业务实现并注入。

方案 1 会让库绑架业务的 ORM/存储选型，与"单库接入、聚焦业务缓存"的轻量定位冲突。

## 决策

库为纯缓存层，不做 DAO 层。源数据访问由业务按结构实现 Loader 契约并注入，契约均为批量函数：

| 结构 | 回源契约 |
|------|----------|
| Object | `Loader: func(ctx, []K) (map[K]V, error)` |
| Hash | `HashLoader: func(ctx, key K, fields []F) (map[F]V, error)` |
| Set | `SetLoader: func(ctx, K) ([]M, error)` |
| List | `ListLoader: func(ctx, K) ([]V, error)` |
| ZSet | `ZSetLoader: func(ctx, K) ([]Scored[V], error)` |

Object 的 Loader 以 key 列表为入参（MultiGet 批量契约，不循环单查）；hash 的 field 级回源中，缺失的 fields 一次性传给 `HashLoader`；集合结构只支持整 key 回源。库不关心 DB 类型与访问方式。

## 备选方案

- **内置 DAO/DB 集成**：绑架 ORM/存储选型，且 DB 访问的连接管理、事务语义不属于缓存库职责。被否决。

## 后果

- 库与存储完全解耦，测试可用任意 Loader 实现（含内存 stub）。
- 回源行为（含错误处理）由业务掌控；Loader 返回的 map 中缺失 key 即按未命中处理（触发负缓存）。
- 分布式锁仅库内部用于回源保护，不对外暴露通用锁 API。

> 现状：回源保护当前仅进程内 singleflight（Get 单 key 合并、MultiGet 组内 leader 一次批量加载），分布式回源锁未实现，为后续可选项（design.md §12）；「不对外暴露通用锁 API」的边界不变。
