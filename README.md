# go_biz_cache

强类型业务 Redis 缓存库：泛型 API、声明式定义、多级读取（L1 → L2 → 回源）、事件驱动跨实例一致性、大 key 处理。单库接入、零额外基础设施，聚焦业务缓存日常需求。

```go
got, err := userCache.Get(ctx, "t:acme:u:10001") // 强类型，业务不接触裸 []byte/string
```

## 特性

- **强类型句柄**：五种结构（object / hash / set / list / zset），读写回源全程泛型，业务不接触字节
- **声明式定义**：key 模板 + 选项集中注册，一改全生效；业务禁止手拼物理 key
- **多级读取**：L1（进程内分片 LRU）→ L2（Redis）→ singleflight 合并回源（业务注入 Loader）
- **防击穿/穿透/雪崩**：singleflight 合并、负缓存占位（短 TTL）、TTL jitter 打散
- **批次全量失效**：世代号 INCR（O(1)，无 SCAN、无删除竞态），旧 key 靠 TTL 过期
- **事件驱动 L1 一致性**：进程内 / Redis Pub/Sub 事件总线，订阅方删除对应 L1 条目
- **大 key 四选一**：`None`（默认）/ `Zstd` 压缩 / `Shard` 内容切分（仅 object）/ `FieldShard` 成员哈希分片（仅 hash/set）
- **集群友好**：hash-tag key 变换（Wrap/Prefix/Suffix/Insert）、批量按 slot 分组、子 key 天然打散
- **可观测**：Prometheus 指标（逐级命中率/回源耗时/singleflight 合并率/编解码开销/超大值写入）+ 日志钩子
- **纯本地模式**：不配置 Redis 时整体退化为进程内缓存库，无 Redis 依赖运行

## 环境要求

- Go ≥ 1.26
- 使用 L2 时需 **Redis ≥ 7.0**（TTL 语义依赖 `EXPIRE ... NX`）；纯本地模式无 Redis 要求

## 安装

```bash
go get github.com/jninng/go_biz_cache
```

## 快速开始

```go
package main

import (
	"context"
	"time"

	"github.com/jninng/go_biz_cache"
	"github.com/redis/go-redis/v9"
)

type UserProfile struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	// 全局注入：客户端提供者（返回即已连接就绪）与指标（跨实例事件总线见下节）
	gocache.UseClientProvider(func(ctx context.Context) (gocache.RedisCmdable, error) { return rdb, nil })
	gocache.UseMetrics(gocache.NewPrometheusMetrics(nil))

	// 声明式定义：包级定义一次，集中管理
	userCache := gocache.MustDefine("biz", "user_profile",
		gocache.Definition[UserProfile]{
			KeyTemplate:   "t:{tenant}:u:{uid}",                 // 命名占位符模板
			HashTag:       gocache.TagFromPlaceholder("tenant"), // 同租户同 slot（集群）
			TTL:           30 * time.Minute,
			TTLJitter:     0.1,    // ±10% 打散过期时间，防雪崩
			NegativeCache: true,   // 空值缓存，防穿透
			L1:            gocache.L1Options{Enabled: true, Size: 4096},
			L2:            gocache.L2Options{Enabled: true},
			Loader: func(ctx context.Context, keys []string) (map[string]UserProfile, error) {
				// 业务注入源数据访问（批量契约，不循环单查）
				return loadUsersFromDB(ctx, keys)
			},
		})

	ctx := context.Background()
	_ = userCache.Set(ctx, "t:acme:u:10001", UserProfile{ID: "10001", Name: "alice"})
	got, err := userCache.Get(ctx, "t:acme:u:10001") // L1 → L2 → 回源
	_, _ = userCache.MultiGet(ctx, "t:acme:u:10001", "t:acme:u:10002")
	_ = userCache.Delete(ctx, "t:acme:u:10001")      // 失效 + 广播 Invalidated
	_ = userCache.InvalidateAll(ctx)                 // 世代号 INCR，批次全量失效
	_ = got
}
```

跨实例 L1 一致性（可选，单机部署无需）：

```go
import "github.com/jninng/go_biz_cache/event"

provider := func(ctx context.Context) (gocache.RedisCmdable, error) { return rdb, nil }
gocache.UseEventBus(event.NewRedisPubSub(provider))
```

## 五种结构句柄

| 结构 | 构造 | 读 | 写 | 回源契约 |
|------|------|----|----|----------|
| Object | `Define` / `MustDefine`（ns, name, def） | `Get` / `MultiGet` / `Refresh` | `Set` / `SetMulti` / `Delete` | `Loader: func(ctx, []string) (map[string]V, error)` |
| Hash | `DefineHash` / `MustDefineHash`（ns, name, def, opts） | `HGet` / `HMGet` / `HGetAll` | `HSet` / `HSetMulti` / `HDel` / `Delete` | `HashLoader: func(ctx, key, fields []F) (map[F]V, error)` |
| Set | `DefineSet` / `MustDefineSet`（ns, name, def, opts） | `Members` / `IsMember` | `Add` / `Remove` / `Delete` | `SetLoader: func(ctx, key) ([]M, error)` |
| List | `DefineList` / `MustDefineList`（ns, name, def, opts） | `Range` / `Len` | `LPush` / `RPush` / `Trim` / `Delete` | `ListLoader: func(ctx, key) ([]V, error)` |
| ZSet | `DefineZSet` / `MustDefineZSet`（ns, name, def, opts） | `Range` / `RangeByScore` / `Score` | `Add` / `Remove` / `Delete` | `ZSetLoader: func(ctx, key) ([]Scored[V], error)` |

全部句柄均有 `InvalidateAll`（批次全量失效）。hash 的 field 类型 `F` 经 `FieldCodec` 编解码：`F = string` 直通；int/enum 用 `FieldCodecFunc` / `FieldCodecMap` 转换，未提供且 `F` 非 string 时注册报错。

hash field 级语义：未命中 field 走 field 级回源（缺失 fields 一次批量传给 `HashLoader`）、`(key, field)` 粒度 singleflight 与负缓存；`HGetAll` 为全量语义，不做负缓存占位。

## 大 key 策略（四选一，定义级声明）

```go
// Zstd 压缩：序列化后 ≥ 阈值才压缩，读侧透明解压
gocache.BigValuePolicy{Type: gocache.BigValueZstd,
	Zstd: gocache.ZstdOptions{CompressThreshold: 16 << 10, Level: gocache.Speed}}

// Shard 内容切分（仅 Object）：物理子 key = 逻辑 key + :s{i}，读侧静态推算 MGET 拼接，
// 任一片缺失/长度帧不符 → 视为未命中回源自愈
gocache.BigValuePolicy{Type: gocache.BigValueShard,
	Shard: gocache.ShardOptions{ShardCount: 16}} // 可注入自定义 Splitter

// FieldShard 成员哈希分片（仅 Hash/Set）：field/member 定位到单个子 key :h{i}
gocache.BigValuePolicy{Type: gocache.BigValueFieldShard,
	FieldShard: gocache.FieldShardOptions{ShardCount: 16}} // 可注入自定义 Hasher
```

不配置即 `None`（零值默认，原样存取）。策略不叠加。`FieldShard` 的 `ShardCount`/`Hasher` 变更前需 `InvalidateAll`（旧子 key 在旧 TTL 内可能返回不完整集合，见 `FieldShardOptions` 注释）；`Shard`/`Zstd` 参数变更按单键自愈重写，旧数据靠 TTL 过期。

## 本地模式（无 Redis）

`L2Options{Enabled: false}`（或不配置客户端）时整体退化为纯本地缓存库：L1 存取、世代号存进程内存（重启归零等价全量失效）、事件仅进程内。

```go
h := gocache.MustDefine("app", "kv", gocache.Definition[Kv]{
	KeyTemplate: "k:{k}",
	L1:          gocache.L1Options{Enabled: true},
	Loader:      loadFromSource,
})
```

## 读取语义与降级

- 返回 `ErrNotFound` 的情形：L1/L2 均未命中且回源缺失（负缓存命中同样返回 `ErrNotFound`）；`MultiGet` 中缺失 key 不出现在返回 map
- L2 不可用时按定义选择：`DegradeFail`（默认，报错）/ `DegradeSourceOnly`（跳过 L2 直连源）/ `DegradeStaleL1`（只用 L1 旧值）

## 测试

```bash
go test ./...               # 单元测试（MemStore 注入，无需 Redis）
go test -race ./...         # 竞态检测
go test -tags integration ./...  # Redis 集成测试（需本机 Redis 127.0.0.1:6379）
```

测试注入 L2 用 `L2Options{Enabled: true, Store: l2.NewMemStore()}`，无需真实 Redis。

## 文档

- [docs/design.md](docs/design.md) —— 完整设计（key 构建、读取路径、更新失效、大 key 策略、事件、L1/L2）
- [docs/adr/](docs/adr) —— 关键决策记录（世代号失效、事件驱动 L1、Loader 边界、key 变换等）
- 包文档：`go doc github.com/jninng/go_biz_cache`

## License

[MIT](LICENSE)
