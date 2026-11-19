# ADR-0004: 事件通道抽象为可插拔接口，默认进程内总线 + Redis Pub/Sub 适配器

- 状态：已接受
- 日期：2026-11-17

## 背景

多实例部署下，L1 本地缓存的失效/更新需要跨实例通知。通知通道有多种实现：进程内总线（单机/单实例）、Redis Pub/Sub、MQ（Kafka/NATS）。库定位为轻量单库接入、零基础设施要求，不能强制依赖 MQ；但也不应把通道实现写死，堵死后续演进。

## 决策

事件通道抽象为 `EventBus` 接口（`Publish / Subscribe / Close`）：

- 默认实现：进程内总线（单机/单实例模式）；
- 跨实例：Redis Pub/Sub 适配器（channel 按 namespace 分），仅依赖已有 Redis，不引入新基础设施；
- MQ 适配器（Kafka/NATS 等）后续以同一接口接入，业务零改动。

事件结构 `Event{Type, DefName, Keys, Instance}`，类型为 `Updated / Invalidated / GenerationBumped`；`Instance` 为发布者实例 ID，订阅方据此过滤自身事件。

> 现状：接口定名 `event.Bus`（`Publish / Subscribe / Close`，Subscribe 返回取消函数）；`Event` 结构在上述字段基础上扩展 `Namespace`（Pub/Sub channel 按 namespace 分）与 `Gen`（GenerationBumped 携带新世代号），见 event/bus.go。

## 备选方案

- **硬编码 Redis Pub/Sub**：实现简单，但换 MQ 时需改动库与业务。被否决。
- **默认接入 MQ**：违反"零基础设施要求"定位。被否决。

## 后果

- 单机模式零依赖开箱即用；跨实例仅依赖 Redis。
- 事件保证 at-most-once，L1 一致性由后续兜底机制补足（见 ADR-0005）。
- 扩展新通道只需实现 `EventBus` 接口。
