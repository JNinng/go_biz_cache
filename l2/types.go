// Package l2 提供 L2 缓存的 Redis 抽象：单机/集群统一（go-redis 客户端经提供者注入，docs/design.md §9.1），
// 以及测试用内存实现（MemStore）。批量操作由实现内部 pipeline / 分组。
//
// Redis 服务端要求 ≥ 7.0：TTL 语义依赖 EXPIRE ... NX 选项（Redis 7.0 引入）。
package l2

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrNotFound key/field 在 L2 中不存在（含 redis.Nil）。
var ErrNotFound = errors.New("l2: not found")

// RedisCmdable L2 客户端最小命令面。
// go-redis 的 Client / ClusterClient / UniversalClient 均满足该接口。
type RedisCmdable interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Incr(ctx context.Context, key string) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	ExpireNX(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd

	HGet(ctx context.Context, key, field string) *redis.StringCmd
	HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd

	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
	SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd

	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	LLen(ctx context.Context, key string) *redis.IntCmd
	LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd

	ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	ZScore(ctx context.Context, key, member string) *redis.FloatCmd
	ZCard(ctx context.Context, key string) *redis.IntCmd
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd
	ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd

	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	PSubscribe(ctx context.Context, patterns ...string) *redis.PubSub
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd

	Pipeline() redis.Pipeliner
}

// ClientProvider 客户端提供者契约：返回当前可用的客户端实例（返回即已连接就绪）。
// 每次操作原子获取当前客户端，操作间不保证同一实例；持续返回同一实例合法。
type ClientProvider func(ctx context.Context) (RedisCmdable, error)

// Z 有序集合成员（member 为序列化后的字节）。
type Z struct {
	Score  float64
	Member []byte
}

// ZRangeBy ZRANGEBYSCORE 参数（语义同 redis：支持 -inf/+inf、"(x" 开区间、limit）。
type ZRangeBy struct {
	Min, Max string
	Offset   int64
	Count    int64 // <= 0 表示不限制
}

// Store L2 缓存操作抽象（单机/集群统一）。所有批量操作由实现内部 pipeline。
// ttl <= 0 表示不设置/不刷新过期；SAdd/LPush/RPush/ZAdd 的 ttl 语义为 ExpireNX（仅 key 无 TTL 时设置），
// 用于“新建 key 必须带 TTL，而已有 TTL 的 key 不因追加写被续期”。
type Store interface {
	// string
	Get(ctx context.Context, key string) ([]byte, error) // 未命中返回 ErrNotFound
	MGet(ctx context.Context, keys ...string) (map[string][]byte, error)
	MSet(ctx context.Context, ttl time.Duration, items map[string][]byte) error
	Del(ctx context.Context, keys ...string) error
	Incr(ctx context.Context, key string) (int64, error)

	// hash
	HGet(ctx context.Context, key, field string) ([]byte, error) // 未命中返回 ErrNotFound
	HMGet(ctx context.Context, key string, fields ...string) (map[string][]byte, error)
	HGetAll(ctx context.Context, key string) (map[string][]byte, error)
	HMSet(ctx context.Context, key string, fields map[string][]byte, ttl time.Duration, refresh bool) error
	HDel(ctx context.Context, key string, fields ...string) error

	// set
	SMembers(ctx context.Context, key string) ([][]byte, error)
	SAdd(ctx context.Context, key string, ttl time.Duration, members ...[]byte) error
	SIsMember(ctx context.Context, key string, member []byte) (bool, error)
	SRem(ctx context.Context, key string, members ...[]byte) error

	// list
	LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error)
	LLen(ctx context.Context, key string) (int64, error)
	LPush(ctx context.Context, key string, ttl time.Duration, values ...[]byte) error
	RPush(ctx context.Context, key string, ttl time.Duration, values ...[]byte) error
	LTrim(ctx context.Context, key string, start, stop int64) error

	// zset
	ZAdd(ctx context.Context, key string, ttl time.Duration, members ...Z) error
	ZRem(ctx context.Context, key string, members ...[]byte) error
	ZScore(ctx context.Context, key string, member []byte) (float64, error) // 未命中返回 ErrNotFound
	ZCard(ctx context.Context, key string) (int64, error)
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]Z, error)
	ZRangeByScoreWithScores(ctx context.Context, key string, by ZRangeBy) ([]Z, error)

	// 批量（FieldShard 子 key 合并；实现内部单 pipeline，集群模式由 ClusterClient 按 slot 分组）
	HGetAllBatch(ctx context.Context, keys ...string) (map[string]map[string][]byte, error)
	SMembersBatch(ctx context.Context, keys ...string) (map[string][][]byte, error)
	HMSetBatch(ctx context.Context, ttl time.Duration, refresh bool, items map[string]map[string][]byte) error
	SAddBatch(ctx context.Context, ttl time.Duration, items map[string][][]byte) error
}
