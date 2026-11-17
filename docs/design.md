# go_biz_cache 设计文档

> 强类型业务 Redis 缓存库。定位：用不上专用大型缓存体系（消息队列驱动等）场景下的轻量替代方案——单库接入、零基础设施要求、聚焦业务缓存日常需求。

## 1. 目标与非目标

**目标**

- 强类型 API：`TypedCache[T]`，业务不接触裸 `[]byte`/`string`
- 统一 key 构建：声明式定义，一改全生效
- 多数据结构支持（string/hash/set/zset/list 语义层）
- Redis 集群适配（基于 go-redis ClusterClient）
- 单个/批次获取、单个/批次更新（传值/读源）、批次全量失效
- 缓存 + 源数据多级获取（read-through）
- 本地缓存（L1）接口抽象，可独立启用（无 Redis 环境）
- 事件驱动更新/失效接口，多实例 L1 一致性走事件广播
- 大 key 可选处理（明确策略四选一：无策略 / zstd 压缩 / 内容切分 / 成员哈希分片）
- 防击穿/防穿透/防雪崩等业务缓存基础能力

**非目标**

- 不做 DAO 层：源数据访问由业务注入 Loader 契约，库不关心 DB
- 不做分布式锁库（仅内部用于回源保护，不对外暴露通用锁 API）
- 不做 write-behind / 写合并（一致性风险高，需要 MQ 体系时请上专用方案）
- 不做多后端适配（memcached 等），Redis 为唯一 L2 后端

## 2. 关键设计决策（已拍板）

| # | 决策点 | 结论 | 理由 |
|---|--------|------|------|
| D1 | 强类型路线 | 泛型起步：`TypedCache[T]` + 可插拔序列化器 | 简单可靠；性能不够时再上代码生成 |
| D2 | 批次全量失效 | 世代号递增：key 内嵌 `v{gen}` 段 | 干净、集群友好；"一改全生效"= `InvalidateAll` 对元 key `INCR`；旧 key 靠 TTL 自然过期 |
| D3 | 大 key 策略 | 明确策略四选一：`None`（零值默认）/ `Zstd` / `Shard`（内容切分，仅 Object）/ `FieldShard`（成员哈希分片，仅 Hash/Set），不做自动叠加；`Shard` 可注入自定义 `Splitter`（见 §7） | 不配置即无策略；业务按值大小与数据结构自行选型；FieldShard 使 hash/set 大集合的单点读定位到单个物理 key，与内容切分正交互补而非替代 |
| D4 | 事件通道 | 默认进程内总线（单机/单实例），跨实例经 Redis Pub/Sub 适配器；通道抽象为可插拔接口 | 后续可换 MQ 适配器而不动业务代码 |
| D5 | L1 一致性 | 跨实例一致性通过事件驱动（失效/更新广播）实现 | 见 D4 |
| D6 | 库边界 | 纯缓存层，Loader 由业务注入 | 不绑架 ORM/存储选型 |
| D7 | 集群 hash-tag | `HashTag` 为 key 变换策略函数：nil 不变换（默认整键散列，子 key 天然打散）；预设 `TagFromPlaceholder`（占位符段原地包裹）/ `TagWhole`（整键包裹）；自定义 `TagFunc(TagInput) (TagOp, error)` 支持 Wrap（原地包裹）/ Prefix / Suffix / Insert 四类变换（契约见 §3.3） | 聚合与打散双向可选：Wrap 聚合同 slot 低延迟、无大括号散列负载均衡；变换模型统一覆盖聚合/隔离/注入场景；函数式与 Splitter/Hasher 可插拔风格一致 |

## 3. 核心概念

```
CacheDefinition   声明式缓存定义（注册表条目）
TypedCache[T]     强类型缓存句柄，业务持有并使用
Loader            源数据访问契约，业务实现，库注入
Serializer        序列化器接口（json / msgpack / proto 可插拔）
FieldCodec        hash field 编解码（field 类型 F ↔ Redis field 名）
L1Cache           本地缓存接口（默认进程内 LRU，可换 freecache/otter/bigcache）
EventBus          事件总线接口（进程内 / Redis Pub/Sub 适配器）
BigValuePolicy    大 key 策略枚举（None / Zstd / Shard / FieldShard，按定义四选一）
```

### 3.1 缓存定义（声明式）

每个缓存在包级定义一次，集中管理：

```go
var userProfileCache = gocache.Define[gocache.UserProfile](
    "biz",              // namespace（命名空间，租户隔离靠 key 模板实现）
    "user_profile",     // 逻辑名
    gocache.Definition[gocache.UserProfile]{
        KeyTemplate:   "t:{tenant}:u:{uid}",       // 命名占位符
        HashTag:       gocache.TagFromPlaceholder("tenant"), // key 变换策略：Wrap tenant 占位符段，同租户同 slot（nil = 不变换，整键散列）
        TTL:           30 * time.Minute,
        TTLJitter:     0.1,                        // ±10% 打散，防雪崩
        Serializer:    gocache.JSON,               // 默认 JSON，可换
        NegativeCache: true,                       // 空值缓存，防穿透
        BigValue: gocache.BigValuePolicy{       // 明确策略四选一: None / Zstd / Shard / FieldShard，各带专属参数
            Type: gocache.BigValueZstd,
            Zstd: gocache.ZstdOptions{CompressThreshold: 16 << 10},
        },
        L1: gocache.L1Options{Enabled: true, Size: 4096},
        L2: gocache.L2Options{Enabled: true},       // 关闭（或未配置 Redis）则纯本地模式（§9）
    },
)
```

### 3.2 Key 构建

物理 key 统一格式，由库生成，业务禁止手拼：

```
{ns}:{name}:v{gen}:{keyTemplate 展开后的业务键}
示例: biz:user_profile:v3:t:acme:u:10001
声明 Wrap（tenant）变换后: biz:user_profile:v3:t:{acme}:u:10001
```

- `gen`（世代号）由库管理，存储于 Redis 元 key（见下），旧世代 key 靠 TTL 过期
- **世代号解析机制**：
  - 世代号存储在 Redis 元 key（`{ns}:{name}:__gen`，不占用业务 key 命名空间）；
  - `InvalidateAll` 对元 key 执行 `INCR`（原子，避免多实例并发失效覆盖），新世代号随 `GenerationBumped` 事件广播；
  - 读取端：进程内按定义缓存当前世代号，首次使用 GET 一次后摊销为 0 次额外 RTT；收到 `GenerationBumped` 事件即时更新本地世代号；
  - 兜底：事件 at-most-once 可能丢失，本地世代号带短 TTL（默认 5s 惰性刷新）；窗口内最多读到旧世代旧值（stale 窗口秒级，与 L1 一致性定位一致）；
  - Redis 不可用时沿用最后已知世代号，不阻塞读取。
- **为何用世代号而非 SCAN+DEL 删除**（批次全量失效的取舍）：
  - SCAN 成本 O(keyspace)，集群下需逐 master 节点执行，大库时失效耗时分钟级并持续冲击线上延迟；世代号 INCR 为 O(1)；
  - 删除窗口与并发写存在竞态（删后写回旧值），需二次扫描兜底；世代号机制下新写入天然进新世代，无竞态；
  - 跨实例 L1 清理：世代号靠单个 `GenerationBumped` 事件，SCAN 方案需广播海量 key，载荷不可行；
  - 代价：旧世代 key 占用内存直至 TTL 过期；如需提前回收，可叠加深度的后台低频增量清理（按旧世代前缀分批 SCAN+DEL，仅加速回收、不影响正确性，见 §12 后续）。
- 所有 key 构建走 `KeyBuilder`，提供 `With(key, val)` 链式展开模板

### 3.3 集群 slot 控制（key 变换 / hash-tag）

Redis Cluster 以 key 中**首个 `{` 到首个 `}` 之间**的文本计算 slot（大括号是 key 内容的一部分，Redis 不剥离）。默认整键散列时，同租户/同分片组的 key 会散落到不同 slot，无法单 pipeline 操作。定义级 `HashTag` 为 key 变换策略函数，让业务在"聚合"与"打散"之间显式选型；`nil` 表示不变换（默认，整键散列）。

```go
type TagInput struct {
    LogicalKey string             // 模板展开后的逻辑键
    Parts      map[string]string  // 占位符值
    NS, Name   string             // 命名空间/定义名（支持按定义级聚合等场景）
}

type TagOp struct {
    Kind  TagKind // TagNone（不变换）/ TagWrap / TagPrefix / TagSuffix / TagInsert
    Value string  // Prefix/Suffix/Insert 的注入内容（可含自闭合 {...} 控制 slot）；Wrap 忽略
    At    string  // Wrap/Insert 的定位子串（首次出现处）；空 = 整键包裹 / 头部插入；Insert 插入于 At 匹配处之前
}

type TagFunc func(TagInput) (TagOp, error)
```

四类变换：

| Kind | 行为 | 典型用途 |
|------|------|----------|
| `TagWrap` | 将 `At` 定位的子串原地包上 `{}`（`At` 空 = 整键包裹），不新增其他字节 | 聚合同 slot：同租户、同分片组 |
| `TagPrefix` | 头部注入 `Value`（可含自闭合 `{...}` 控制 slot） | 多机房/多环境前缀隔离；头部 tag 聚合 |
| `TagSuffix` | 尾部注入 `Value`（可含自闭合 `{...}`） | 追加分段标识 |
| `TagInsert` | 于 `At` 匹配处之前注入 `Value`（`At` 空 = 头部；可含自闭合 `{...}`） | 在 key 中段插入分隔内容 / tag |

预设：`TagFromPlaceholder("tenant")` = Wrap tenant 占位符段（KeyBuilder 按占位符替换位置精确包裹）；`TagWhole` = Wrap 整键。跨定义共用同一占位符即共享 slot 归属，为将来跨缓存多 key 原子操作预留同 slot 基础（当前 API 未提供跨 key 原子操作）。

`TagInput.LogicalKey` 为逻辑键全段 `{ns}:{name}:v{gen}:{业务键}`，`At` 可定位其中任意子串——包括业务键段与 ns/name/gen 头部段，因此包裹粒度由业务自选。

示例走查（§3.1 定义，tenant=acme，逻辑键 = `biz:user_profile:v3:t:acme:u:10001`）：

```
TagWrap（At="acme"，即 TagFromPlaceholder("tenant")）:
  biz:user_profile:v3:t:{acme}:u:10001        slot 取 "acme"，同租户 key 全部同 slot

TagWrap（At="t:acme:u:10001"，包裹业务键整段）:
  biz:user_profile:v3:{t:acme:u:10001}        slot 取业务键整段；仅聚合本键的分片，不跨键聚合

TagWrap（At=""，即 TagWhole）:
  {biz:user_profile:v3:t:acme:u:10001}        整键作 tag

TagPrefix（Value="dc1:"）:
  dc1:biz:user_profile:v3:t:acme:u:10001      多机房隔离；不含大括号 → 仍整键散列

TagSuffix（Value=":r2"）:
  biz:user_profile:v3:t:acme:u:10001:r2       追加分段标识；不含大括号 → 仍整键散列

TagInsert（Value=":r2"，At=":t:"）:
  biz:user_profile:v3:r2:t:acme:u:10001       在首个 ":t:" 之前插入 ":r2"

nil（默认）:
  biz:user_profile:v3:t:acme:u:10001          整键散列
```

Prefix/Suffix/Insert 的 `Value` 也可携带 `{...}`（如 `TagPrefix(Value="{acme}")`），此时 slot 由注入段决定——等价于头部插入 tag，但推荐优先用 `TagWrap` 原地包裹（零字节开销）。

与 Shard/FieldShard 子 key 的叠加：子 key 后缀 `:s{i}`/`:h{i}` 追加在变换之后，slot 由 tag 段决定。以 Shard（ShardCount=16）为例：

```
不变换（默认，打散）:
  biz:user_profile:v3:t:acme:u:10001:s0
  biz:user_profile:v3:t:acme:u:10001:s1
  ...                                        16 个子 key 整键散列 → 天然落到不同 slot

TagFromPlaceholder("tenant"):
  biz:user_profile:v3:t:{acme}:u:10001:s0
  biz:user_profile:v3:t:{acme}:u:10001:s1
  ...                                        slot 全部取 "acme" → 16 片同 slot，单 pipeline
```

FieldShard 同理：`HGet(key, field="nickname")` → 子 key = 变换后逻辑 key + `:h{fnv1a(nickname) % 16}`（如 `...:u:10001:h7`），单 key 单次 HGET，slot 同样由 tag 段决定。

选型结论：

- **包裹粒度决定聚合范围**：Wrap 占位符段（跨键聚合，粒度最粗）→ Wrap 业务键整段（仅聚合本键的分片）→ Wrap 全键（同左，但在同一定义内与业务键整段 slot 等价——ns/name/vgen 恒定；差异在跨定义：全键 tag 携带定义身份、跨定义隔离，业务键段 tag 允许不同定义下相同业务键同 slot，即跨定义聚合预留）
- 声明 Wrap（或任何产生 `{...}` 的变换）→ 全部分片**同 slot**：读写单 pipeline，低延迟，代价是热点集中
- 不变换（默认）→ 各子 key 整键散列**天然打散**到不同 slot：读写由 ClusterClient 自动按 slot 分组 scatter-gather，负载均衡；大值分片默认建议打散（§7）

契约：

1. **纯函数确定性**：同入参必同变换，否则 slot 漂移
2. **Wrap 定位合法**：`At` 必须为 `LogicalKey` 非空子串；自定义函数按首次出现处包裹，片段应取完整占位符值整段，避免短片段误命中非预期位置
3. **大括号与空白规则**：Prefix/Suffix/Insert 的 `Value` 可含自闭合 `{...}`（参与 slot 控制）；`At` 为纯定位子串，不得含大括号。禁止嵌套 `{}`（Redis 取首个 `{` 到首个 `}` 计算 slot，嵌套会导致路由不可预期）；`Value`/`At` 不得含空白字符。注意仅**首个**大括号对决定 slot——key 中已有 tag 段时，后续注入的大括号只是普通内容、不参与路由
4. **单 op**：每次返回一个变换，序列组合待有真实需求再扩展
5. 违反上述契约在 key 构建时报错，不静默降级

## 4. 读取路径（多级获取）

```
Get(ctx, key) 
  → L1 命中？返回
  → L2 (Redis) 命中？
       → 检测到负缓存标记？回填 L1 负缓存条目（短 TTL）→ 返回 ErrNotFound（不回源）
       → 回填 L1，返回
  → singleflight 合并并发回源
  → Loader(ctx, keys)（契约: func(ctx, []K) (map[K]V, error)）
  → 命中：序列化 → 大 key 策略（压缩/分片）→ 写 L2 + 回填 L1
  → 未命中：NegativeCache 开启时写短 TTL 占位值（L2 + L1 负缓存条目），返回 ErrNotFound

MultiGet(ctx, keys)
  → L1 批量探测 → 命中直接取
  → 未命中 keys 批量探测 L2（Shard key 按各自分片组 MGET，跨 slot 由 ClusterClient 分组）
       → 命中：回填 L1，返回
       → 负缓存标记命中：回填 L1 负缓存条目，记缺失
  → 仍未命中 keys：singleflight 按 key 粒度合并 → Loader 批量
       → 命中：序列化 → 大 key 策略 → 写 L2 + 回填 L1
       → 未命中：NegativeCache 开启时写占位（L2 + L1 负缓存条目）
  → 返回 map：负缓存/未命中 key 不出现在 map 中，调用方按缺失处理（不返回 ErrNotFound）
```

- **singleflight**：进程内 singleflight 合并同 key 并发回源；可选分布式锁（SET NX + TTL）保护多实例同时回源，默认关闭（回源幂等时无需）
- **TTL jitter**：写入 L2 时按 `TTLJitter` 打散过期时间
- **降级**：Redis 不可用时按定义配置选择——`SourceOnly`（直连源）/ `StaleL1`（只用 L1 旧值）/ `Fail`（默认，返回错误）

## 5. 更新与失效语义

| API | 语义 | 行为 |
|-----|------|------|
| `Set(ctx, key, val)` | 传值更新（写穿） | 写 L2（含大 key 策略）→ 更新本实例 L1 → 发布 `Updated` 事件，订阅方**删除**各自 L1 对应条目 |
| `SetMulti(ctx, pairs)` | 批次传值更新 | pipeline 批量写 L2（含大 key 策略）→ 逐 key 更新本实例 L1 → 发布 `Updated` 事件（Keys 为各物理 key），订阅方删除各自 L1 对应条目 |
| `Refresh(ctx, key)` | 读源更新 | singleflight 回源后走 `Set` 路径 |
| `Delete(ctx, key)` | 失效（下次读回源） | 删 L2 + 删本实例 L1 → 发布 `Invalidated` 事件，订阅方删各自 L1 |
| `InvalidateAll(ctx)` | 批次全量失效 | 对世代号元 key `INCR` 递增 + 发布 `GenerationBumped` 事件；旧 key 靠 TTL 过期 |
| `MultiGet(ctx, keys)` | 批次获取 | 按 §4 MultiGet 流程：L1 批量探测 → 未命中 keys 批量探测 L2 → 剩余 keys singleflight 合并后一次性传给 Loader（批量契约）→ 逐级回填；负缓存/未命中 key 不出现在返回 map |

双删语义：业务在"先写库后失效"场景用 `Delete`；需要更强一致时由业务自行组合（规划提供 `Delete` 后短延迟再删一次的 `DeleteTwice` 可选方法，见 §12 后续）。

## 6. 多数据结构（强类型操作矩阵）

每种结构一个泛型句柄，读/写/回源全部强类型。`K` 在 v1 恒为 `string`（业务键即模板展开后的字符串），定义层 `Define[V]` 不含 K；`V` 为值类型（走 Serializer），hash 的 field 类型 `F` 走 FieldCodec（string 直通；int/enum 经转换函数映射为 Redis field 名）。

| 结构 | 句柄 | 读操作 | 写操作 | 回源契约 |
|------|------|--------|--------|----------|
| Object（string） | `TypedObjectCache[K, V]` | `Get` / `MultiGet` | `Set` / `SetMulti` / `Delete` | `Loader: func(ctx, []K) (map[K]V, error)` |
| Hash | `TypedHashCache[K, F, V]` | `HGet(key, field)` / `HMGet(key, fields...)` / `HGetAll(key)` | `HSet` / `HSetMulti` / `HDel` | `HashLoader: func(ctx, key K, fields []F) (map[F]V, error)` |
| Set | `TypedSetCache[K, M]` | `Members(key)` / `IsMember(key, m)` | `Add` / `Remove` | `SetLoader: func(ctx, K) ([]M, error)` |
| List | `TypedListCache[K, V]` | `Range(key, start, stop)` / `Len(key)` | `LPush` / `RPush` / `Trim` | `ListLoader: func(ctx, K) ([]V, error)` |
| ZSet | `TypedZSetCache[K, V]` | `Range(key, ...)` / `RangeByScore(...)` / `Score(key, v)` | `Add`（带 score）/ `Remove` | `ZSetLoader: func(ctx, K) ([]Scored[V], error)` |

**hash field 级语义细节（重点）**：

- `HGet/HMGet` 未命中 field 走 field 级回源：Redis 命中的 fields 直接取，缺失的 fields 一次性传给 `HashLoader`（批量契约，不循环单查），结果 `HMSET` 回填并刷新 key TTL
- singleflight 与负缓存按 `(key, field)` 粒度合并/占位，而非整个 key
- 负缓存标记为**独立短 TTL string key**，不写入真实数据结构（避免污染）：Object 为 `{物理key}:__neg`；hash field 为 `{物理key}:__neg:{fnv1a(field)}`（与 FieldShard 正交，独立于子 hash）；`HMGet` 读路径检查对应标记，`HGetAll` 不读标记（全量语义，见上）
- `HGetAll` 整 hash 读：hash 存在则直接返回；不存在走 `HashLoader(ctx, key, nil)`（fields 为空表示全量回源）；返回 `map[F]V` 中未写入的 field 静默缺失——`HGetAll` 是全量语义，不存在即不存在，不做负缓存占位；负缓存仅作用于单 field 的 `HGet/HMGet`
- ZSet/List/Set 为集合语义，不支持 field 级部分回源，只支持整 key 回源（key 未命中 → Loader 全量重建后整写）；**回源-整写窗口**内的并发写（Add/Remove/LPush 等）可能被重建写覆盖，语义为 last-write-win——集合结构读多写少场景下可接受，需更强一致由业务在写侧重试；Set/ZSet/List 均不做负缓存占位（防穿透仅适用于单 key / 单 field 语义），FieldShard 仅影响成员定位、不引入负缓存
- 世代号在 key 层生效于所有结构；hash 的 field 更新不触发世代号，整 hash 失效走 `Delete(key)` 或 `InvalidateAll`
- 写 field 时 TTL 刷新策略由定义控制：`HashOptions{RefreshTTLOnWrite: bool}`
- 大 hash 场景建议声明 `FieldShard` 策略（§7）：field 级操作定位到单个子 key，避免整 hash 大 key

里程碑安排见 §12。

## 7. 大 key 处理（明确策略，四选一，各带专属参数）

定义上显式声明一种策略及其参数，不做自动升级/叠加。**不配置该字段（零值）即 `None`**——无策略、原样存取。策略按数据结构分流：`Shard` 解决"单个大值"（Object），`FieldShard` 解决"成员多的大集合"（Hash/Set），`Zstd` 通用。策略类型 + 各策略独立参数块，参数仅在对应策略下生效：

```go
// 1) 无策略：零值默认，原样存取（不配置 BigValue 字段时等效）
BigValue: gocache.BigValuePolicy{Type: gocache.BigValueNone}

// 2) 压缩（ZstdOptions 专属参数）
BigValue: gocache.BigValuePolicy{
    Type: gocache.BigValueZstd,
    Zstd: gocache.ZstdOptions{
        CompressThreshold: 16 << 10,        // 序列化后 ≥ 16KB 才压缩；0 = 全压缩
        Level:             gocache.Speed,   // 压缩级别: Speed / Default / High
    },
}

// 3) 分片拆 key（ShardOptions 专属参数）
BigValue: gocache.BigValuePolicy{
    Type: gocache.BigValueShard,
    Shard: gocache.ShardOptions{
        ShardCount: 16,                     // 分片数（= 物理子 key 数量），读侧靠它静态推算全部分片 key 做 MGET
        Splitter:   nil,                    // nil = 默认均匀切分；可注入自定义切分算法
    },
}

// 4) 成员哈希分片（FieldShardOptions 专属参数，仅 Hash/Set 声明有效）
BigValue: gocache.BigValuePolicy{
    Type: gocache.BigValueFieldShard,
    FieldShard: gocache.FieldShardOptions{
        ShardCount: 16,                     // 分片数（= 子 key 数量），读侧静态可推算
        Hasher:     nil,                    // nil = 默认 fnv1a(member) % N；可注入自定义成员哈希
    },
}
```

1. **`Zstd`（压缩）**：物理值带 1 字节 header 标记压缩/原始（`CompressThreshold` > 0 时小值原样存），读时透明解压；`Level` 控制压缩率与 CPU 开销的取舍。**作用单元为序列化后的单值**：Object 整值、Hash 逐 field 值、Set/ZSet/List 逐 member 序列化值；集合结构本身不整体序列化压缩，保持原生集合语义
2. **`Shard`（内容切分，仅适用于 Object 单个大值）**：
   - **切分算法**：默认为固定均匀切分——`chunkSize = ceil(len / ShardCount)`，第 i 片为 `[i*chunkSize, min((i+1)*chunkSize, len))`，写读两侧同一算法，确定性可推算；可通过 `Splitter` 注入自定义算法
   - **自定义切分器契约**（`ShardSplitter`）：
     ```go
     // 入参：逻辑业务键（模板展开后，如 "t:acme:u:10001"）、序列化后的完整值、分片数；
     // 返回恰好 shardCount 个分片（按序拼接须还原原值）。
     type ShardSplitter func(logicalKey string, value []byte, shardCount int) ([][]byte, error)
     ```
     业务键作为入参传递，使算法可利用 key 结构做语义化切分（如按记录边界对齐）；硬性契约：
     1. **纯函数确定性**：同入参必得同输出（禁随机/时间/外部状态），写读两侧靠此对称
     2. **分片数量恒定**：返回长度必须等于 `shardCount`（读侧无值，靠静态 ShardCount 推算全部物理 key 一次 MGET）；空分片允许
     3. **无损可还原**：按序拼接全部分片必须还原原始值
     4. **物理 key 命名不外泄**：库负责 `{key}:s{i}` 命名与 hash-tag，切分器只决定各片内容
     > 注意：切分器**仅写路径调用**（此时值已就绪）；读路径不调用切分器——只按 ShardCount 推算物理 key 一次 MGET，命中后顺序拼接还原，无需感知切分细节。
   - **写**：按算法切分后，对变换后逻辑 key 追加 `:s{0..N-1}` 后缀 pipeline 写入（TTL 相同）；集群 slot 策略（§3.3）：未声明产生 `{...}` 的变换策略（默认）→ 子 key 整键散列**天然打散**到不同 slot，读写由 ClusterClient 自动按 slot 分组，大值负载分散到多节点；声明 Wrap → 全部分片同 slot（单 pipeline 低延迟，代价热点集中）——大值分片默认建议打散，除非业务明确要聚合
   - **读**：`ShardCount` 为定义级静态参数，按同一算法推算全部 N 个物理 key 一次 MGET（跨 slot 由 ClusterClient 自动分组）；全部命中 → 顺序拼接后反序列化；任一缺失 → 视为未命中走回源（写一半失败或修改 `ShardCount`/`Splitter` 后旧分片读不到，均自愈重写，旧分片靠 TTL 过期）
   - 单片写入仍超限时报错，不做隐式压缩兜底（策略选择权留给业务）
3. **`FieldShard`（成员哈希分片，仅 Hash/Set）**：按成员（hash 的 field / set 的 member）哈希分散到 N 个子 key，单点读精确定位到单个物理 key——与内容切分正交的另一种分片维度，服务"结构化大集合"而非"单个大值"：
   - **定位算法**：子 key = `{key}:h{fnv1a(member) % N}`；默认成员哈希 fnv1a，可注入自定义 `Hasher`（契约同切分器：纯函数、确定性、无状态）；子 key 数量恒等于 `ShardCount`，读侧静态可推算
   - **Hash 读路径**：`HGet(key, field)` → 单 key 单次 HGET；`HMGet` → fields 按子 key 分组后 pipeline（声明 Wrap 变换则单 pipeline，未声明由 ClusterClient 自动 scatter-gather）；`HGetAll` → pipeline HGETALL × N 合并，全空视为 key 不存在走全量回源（不做独立 EXISTS 判断，避免额外 RTT）
   - **Hash 写路径**：`HSet` 落到对应子 key；field 级回源回填自动落到正确子 key；`RefreshTTLOnWrite` 刷新被触碰的子 key；`Delete(key)` → DEL 全部 N 个子 key
   - **Set**：`Add/Remove/IsMember` 按 member 定位单子 key；`Members` → pipeline SMEMBERS × N 合并
   - **ZSet/List 不支持**：两者为有序结构，按成员哈希打散后 Range/RangeByScore 无法保序（跨子 key 归并读放大且复杂），声明即注册报错；大 ZSet 用 `Zstd` 或后续专项设计
   - **世代号/负缓存**：世代号作用于逻辑 key 前缀，对所有子 key 生效；单 field 负缓存仍按 `(key, field)` 粒度，与子 key 定位正交；修改 `ShardCount`/`Hasher` 后旧子 key 读不到，沿用自愈重写 + TTL 过期机制
4. 策略不叠加（选了 Shard/FieldShard 就不压缩；Shard 与 FieldShard 互斥）；非法参数（ShardCount < 2、FieldShard 声明于 Object/ZSet/List 等）在定义注册时校验报错；切分器/哈希器行为契约（返回长度 != shardCount、非子串定位等）无法在注册期验证，由写路径运行时断言兜底：不符即报错，不静默截断；指标中暴露超大值写入次数供治理参考

## 8. 事件驱动

```go
type Event struct {
    Type      EventType  // Updated / Invalidated / GenerationBumped
    DefName   string
    Keys      []string   // Updated/Invalidated: 变换后物理 key（不含分片子 key）；GenerationBumped: 空（订阅方按 DefName 清空 L1）
    Instance  string     // 发布者实例 ID（过滤自身事件）
}
```

- 接口：`EventBus`（`Publish / Subscribe / Close`）
- 默认实现：进程内总线（单机/单实例模式）；跨实例：Redis Pub/Sub 适配器（channel 按 namespace 分）
- 订阅方收到 `Updated/Invalidated` 后**删除** L1 对应条目，不回源；事件**不带 value 载荷**（大值不经 Pub/Sub，与轻量定位一致，删除后下次读走 L2 即新值）；收到 `GenerationBumped` 后整代 L1 清空
- 发起 `InvalidateAll` 的实例不依赖自身事件：`INCR` 成功后同步清空自身 L1 并更新本地世代号；`Instance` 过滤作用于全部事件类型（发起实例均已同步处理自身状态，处理自身事件只会造成冗余操作）
- MQ 适配器（Kafka/NATS 等）后续以同一接口接入，业务零改动
- 事件保证 at-most-once，L1 一致性定位为"最终、秒级"，不承诺强一致

## 9. 本地缓存（L1）

- 接口 `L1Cache`：`Get / Set / Delete / Clear / Size`（`Set` 携带 TTL 参数，负缓存条目的短 TTL 过期依赖此），默认实现为进程内分片 LRU
- **实现注入**：`L1Options{Enabled, Size, Cache}`——`Cache` 字段为 `L1Cache` 接口，非 nil 时使用注入实现（freecache/otter/bigcache 等），nil 时用默认 LRU。自定义实现须满足契约：并发安全；按 TTL 过期；键粒度遵循上文（逻辑键，hash 为 `逻辑键+field` 复合键）；值为序列化后的字节（L1 不参与大 key 策略，原样存取）
- **键粒度**：L1 键 = 逻辑键（不含 ns/name 前缀、hash-tag 大括号与世代号段）；hash 场景为 `逻辑键 + field` 复合键。事件 `Keys` 为变换后物理 key，订阅方由库**逆变换**映射回逻辑键后删 L1；世代号变更不进 L1 键，靠 `GenerationBumped` 整代清空兜底
- **独立启用**：定义关闭 L2（不配置 Redis 连接）时整体退化为"纯本地缓存库"（无 Redis 依赖运行），事件仅进程内；世代号改存进程内存，重启归零等价全量失效（L1 随进程消亡，可接受），`InvalidateAll` 仅清本进程
- 一致性：同进程写入即生效；跨实例经事件广播（§8）

### 9.1 Redis 客户端注入形态

L2 的 Redis 客户端以**提供者函数**注入，库不持有具体实例的创建与生命周期：

```go
// 提供者契约：返回当前可用的客户端实例（返回即已连接就绪）
type ClientProvider func(ctx context.Context) (RedisCmdable, error)
```

- **每次 L2 操作原子获取当前客户端**，操作间不保证同一实例——缓存单操作幂等，正确性由世代号与 TTL 兜底，不依赖请求级实例一致性
- **返回即可用契约**：提供者返回的实例必须已连接就绪（失败应返回 error 而非不可用实例）；持续返回同一实例（实例未变更）合法
- **请求级实例配对**非库的职责，需要时由提供者实现方自行保证（如返回实例 + 世代快照的组合）
- **Pub/Sub 订阅与客户端实例绑定**：适配器检测到提供者返回的实例变更后，以新实例重建订阅；重建窗口内丢失的事件由世代号短 TTL 惰性刷新兜底（§3.2）
- **进程内状态与实例生命周期解耦**：世代号缓存、负缓存、singleflight、L1 均不依赖具体客户端实例，实例更替对其透明

## 10. 可观测性与容错

- 指标（Prometheus 接口 + 内置实现）：逐级命中率（L1/L2/负缓存/源）、回源耗时、singleflight 合并率、序列化/压缩开销、超大值写入次数（§7）、错误计数（按定义打标签）
- 日志钩子：`Logger` 接口注入，慢查询/降级/大 key 告警点
- 超时：L2 读写独立超时；超时即触发降级策略

## 11. 包结构

```
gocache/
├── object.go           # TypedObjectCache（string 值语义）
├── hash.go             # TypedHashCache
├── set.go              # TypedSetCache
├── list.go             # TypedListCache
├── zset.go             # TypedZSetCache
├── definition.go       # Define / Definition / 注册表
├── key.go              # KeyBuilder / 世代号 / TagOp key 变换
├── loader.go           # Loader 契约与批量加载器
├── codec/
│   ├── codec.go        # Serializer / FieldCodec 接口 + 注册
│   ├── json.go / msgpack.go / proto.go
├── l1/
│   ├── l1.go           # L1Cache 接口
│   └── lru.go          # 默认分片 LRU
├── l2/
│   ├── l2.go           # Redis 抽象（单机/集群统一，客户端经提供者注入，§9.1）
│   └── redis.go        # go-redis ClusterClient 实现
├── bigvalue/
│   ├── compress.go     # zstd
│   ├── shard.go        # 内容切分（Object 大值）
│   └── fieldshard.go   # 成员哈希分片（Hash/Set 大集合）
├── event/
│   ├── bus.go          # EventBus 接口 + 进程内实现
│   └── redispubsub.go
├── singleflight.go
├── metrics.go
└── degrade.go          # 降级策略
```

## 12. 里程碑

| 阶段 | 内容 |
|------|------|
| v0.1 | 定义注册表、key 构建 + 世代号、TypedObjectCache（string 值语义）、Get/Set/Delete/MultiGet/InvalidateAll、TTL jitter、singleflight、负缓存、Prometheus 指标、内存实现（测试用） |
| v0.2 | L1 接口 + 默认 LRU、事件总线（进程内 + Redis Pub/Sub）、独立本地模式、降级策略 |
| v0.3 | 大 key 压缩 + 内容切分 + 成员哈希分片（FieldShard，Hash/Set）、hash/set/list/zset 结构化句柄、可插拔序列化器（msgpack/proto）、分布式回源锁（可选） |
| 后续 | 代码生成路径（若性能需要）、MQ 事件适配器、DeleteTwice、热点探测、旧世代 key 后台增量清理（可选，提前回收内存） |
