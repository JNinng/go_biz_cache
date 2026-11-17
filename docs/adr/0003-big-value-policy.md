# ADR-0003: 大 key 处理采用明确策略四选一（None / Zstd / Shard / FieldShard）

- 状态：已接受
- 日期：2026-11-17

## 背景

业务缓存中存在两类大 key：单个大值（Object 的 value 过大）与成员多的大集合（Hash/Set 成员数量大）。库需要提供大 key 处理能力，但自动检测并升级策略（如超阈值自动压缩/自动分片）会引入不可预期行为，且多策略叠加的组合空间复杂。

可选策略：

- `None`：无策略，原样存取；
- `Zstd`：压缩，作用单元为序列化后的单值（Object 整值、Hash 逐 field 值、集合逐 member），带 1 字节 header 标记压缩/原始，`CompressThreshold` 控制起压门槛，`Level` 控制压缩率与 CPU 取舍；
- `Shard`：内容切分（仅 Object 单个大值），默认固定均匀切分，可注入自定义 `Splitter`（纯函数、分片数恒定、无损可还原）；子 key 后缀 `:s{0..N-1}`，读侧按静态 `ShardCount` 推算全部物理 key 一次 MGET；
- `FieldShard`：成员哈希分片（仅 Hash/Set），子 key = `{key}:h{fnv1a(member) % N}`，单点读精确定位到单个物理 key，与内容切分正交互补而非替代；ZSet/List 为有序结构，成员哈希打散后无法保序，不支持。

## 决策

定义级 `BigValuePolicy` 显式声明一种策略及专属参数（四选一），**不做自动升级/叠加**；不配置（零值）即 `None`。策略按数据结构分流：`Shard` 解决单个大值（Object），`FieldShard` 解决成员多的大集合（Hash/Set），`Zstd` 通用。`Shard` 与 `FieldShard` 互斥，选了分片就不压缩。

非法参数（`ShardCount < 2`、FieldShard 声明于 Object/ZSet/List 等）在定义注册时校验报错；切分器/哈希器行为契约（返回长度 != shardCount、非子串定位等）无法在注册期验证，由写路径运行时断言兜底：不符即报错，不静默截断。指标中暴露超大值写入次数供治理参考。

## 备选方案

- **自动策略升级**（超阈值自动压缩/分片）：行为不可预期，掩盖问题；策略选择权应留给业务。被否决。
- **策略叠加**（如 Shard + Zstd）：组合空间复杂，单片写入仍超限时直接报错不做隐式压缩兜底。被否决。

## 后果

- 不配置即零开销，业务按值大小与数据结构自行选型。
- 修改 `ShardCount`/`Splitter`/`Hasher` 后旧分片读不到，沿用自愈重写 + TTL 过期机制，不保证迁移期零回源。
- 读路径不调用切分器，只按 ShardCount 静态推算 MGET，读性能与切分算法解耦。
