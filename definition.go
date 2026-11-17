package gocache

import (
	"time"

	"github.com/jninng/go_biz_cache/bigvalue"
	"github.com/jninng/go_biz_cache/codec"
	"github.com/jninng/go_biz_cache/event"
	"github.com/jninng/go_biz_cache/l1"
	"github.com/jninng/go_biz_cache/l2"
)

// ---------- 别名导出 ----------

type (
	// Serializer 序列化器接口（json / msgpack 可插拔，proto 由业务实现接口注入）。
	Serializer = codec.Serializer
	// FieldCodec hash field 编解码器（field 类型 F ↔ Redis field 名）。
	FieldCodec[F any] = codec.FieldCodec[F]
	// RedisCmdable L2 Redis 客户端最小命令面（Client/ClusterClient 均满足）。
	RedisCmdable = l2.RedisCmdable
	// ClientProvider Redis 客户端提供者契约（返回即已连接就绪，§9.1）。
	ClientProvider = l2.ClientProvider
	// Z 有序集合成员。
	Z = l2.Z
	// ZRangeBy ZRANGEBYSCORE 参数。
	ZRangeBy = l2.ZRangeBy
	// ShardSplitter 自定义内容切分器（契约见 §7；仅写路径调用）。
	ShardSplitter = bigvalue.Splitter
	// MemberHasher 自定义成员哈希（契约：纯函数、确定性、无状态）。
	MemberHasher = bigvalue.Hasher
	// CompressLevel 压缩级别（压缩率与 CPU 开销取舍）。
	CompressLevel = bigvalue.CompressLevel
)

// 压缩级别常量（零值按 Default 处理）。
const (
	Speed   = bigvalue.Speed   // 最快
	Default = bigvalue.Default // 默认
	High    = bigvalue.High    // 高压缩率
)

// 内置序列化器。
var (
	JSON    = codec.JSON
	MsgPack = codec.MsgPack
)

// FieldCodecString string 直通 field 编解码器便捷构造。
func FieldCodecString() FieldCodec[string] { return codec.FieldCodecString() }

// FieldCodecFunc 由转换函数构造 field 编解码器。
func FieldCodecFunc[F any](enc func(F) (string, error), dec func(string) (F, error)) FieldCodec[F] {
	return codec.FieldCodecFunc(enc, dec)
}

// FieldCodecMap 由双向映射表构造 field 编解码器（适用于枚举类型）。
func FieldCodecMap[F comparable](enc map[F]string, dec map[string]F) FieldCodec[F] {
	return codec.FieldCodecMap(enc, dec)
}

// ---------- 大 key 策略（§7）----------

// BigValueType 大 key 策略枚举：明确四选一，不叠加；零值 = None（无策略，原样存取）。
type BigValueType uint8

const (
	BigValueNone      BigValueType = iota // 无策略（零值默认）
	BigValueZstd                          // 压缩（通用）
	BigValueShard                         // 内容切分（仅 Object 单个大值）
	BigValueFieldShard                    // 成员哈希分片（仅 Hash/Set 大集合）
)

// ZstdOptions 压缩策略参数。
type ZstdOptions struct {
	CompressThreshold int                    // 序列化后 ≥ 该字节数才压缩；0 = 全压缩
	Level             bigvalue.CompressLevel // Speed / Default / High；零值 = Default
}

// ShardOptions 内容切分参数（仅 Object）。
type ShardOptions struct {
	ShardCount int           // 分片数（= 物理子 key 数量），读侧静态推算；≥ 2
	Splitter   ShardSplitter // nil = 默认均匀切分（SplitEven）
}

// FieldShardOptions 成员哈希分片参数（仅 Hash/Set）。
// 运维约束：修改 ShardCount/Hasher 前必须先 InvalidateAll —— 参数变更后旧子 key 读不到，
// 单 field/点查可自愈重写，但批量全量读（HGetAll/Members）在旧 TTL 内可能返回不完整集合。
type FieldShardOptions struct {
	ShardCount int          // 分片数（= 子 key 数量），读侧静态可推算；≥ 2
	Hasher     MemberHasher // nil = 默认 fnv1a(member) % N
}

// BigValuePolicy 大 key 策略 + 各策略独立参数块（参数仅在对应策略下生效）。
type BigValuePolicy struct {
	Type       BigValueType
	Zstd       ZstdOptions
	Shard      ShardOptions
	FieldShard FieldShardOptions
}

// ---------- L1 / L2 选项 ----------

// L1Options 本地缓存选项（Enabled 关闭则读写路径不经过 L1）。
type L1Options struct {
	Enabled bool
	Size    int        // LRU 容量（条目数）；默认 4096
	Cache   l1.L1Cache // 注入实现（freecache/otter/bigcache 等），nil 时用默认分片 LRU
}

// L2Options Redis 选项。关闭 Enabled（或未配置客户端）则纯本地模式（§9）。
type L2Options struct {
	Enabled        bool
	ClientProvider ClientProvider // nil 时用全局 UseClientProvider 注入的提供者
	Store          l2.Store       // 直接注入 Store 实现（优先于 ClientProvider；测试/自实现后端用）
	Timeout        time.Duration  // 单次 L2 操作独立超时，默认 3s
}

// ---------- 缓存定义（§3.1）----------

// Definition 声明式缓存定义（包级定义一次，集中管理）。
type Definition[V any] struct {
	KeyTemplate   string        // 命名占位符模板，如 "t:{tenant}:u:{uid}"
	HashTag       TagFunc       // key 变换策略：nil = 不变换（默认整键散列）
	TTL           time.Duration // 0 = 默认 30m
	TTLJitter     float64       // ± 比例打散过期时间，防雪崩；建议 ≤ 0.5；打散后下限钳制为 1s
	Serializer    Serializer    // nil = JSON
	NegativeCache bool          // 空值缓存，防穿透
	NegativeTTL   time.Duration // 负缓存 TTL，默认 10s
	BigValue      BigValuePolicy
	L1            L1Options
	L2            L2Options
	Degrade       DegradePolicy // L2 不可用时读降级，默认 Fail

	// Loader Object 读源契约；nil 时 Get/MultiGet 未命中直接返回缺失。
	Loader Loader[string, V]

	// 以下为定义级覆盖（nil 时用全局注入）。
	EventBus event.Bus
	Metrics  Metrics
	Logger   Logger
}

// HashOptions hash 句柄专属定义。
type HashOptions[F comparable, V any] struct {
	FieldCodec        FieldCodec[F] // nil 时 F 必须为 string（直通）
	HashLoader        HashLoader[string, F, V]
	RefreshTTLOnWrite bool // 写 field 时刷新 key TTL（默认 ExpireNX：仅 key 无 TTL 时设置）
}

// SetOptions set 句柄专属定义。
type SetOptions[M any] struct {
	Loader SetLoader[string, M]
}

// ListOptions list 句柄专属定义。
type ListOptions[V any] struct {
	Loader ListLoader[string, V]
}

// ZSetOptions zset 句柄专属定义。
type ZSetOptions[V any] struct {
	Loader ZSetLoader[string, V]
}

type structKind uint8

const (
	kindObject structKind = iota
	kindHash
	kindSet
	kindList
	kindZSet
)

func (k structKind) String() string {
	switch k {
	case kindObject:
		return "object"
	case kindHash:
		return "hash"
	case kindSet:
		return "set"
	case kindList:
		return "list"
	case kindZSet:
		return "zset"
	}
	return "unknown"
}
