package l2

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore 基于 go-redis 的 Store 实现（Client/ClusterClient 统一，客户端经提供者注入）。
// 集群模式下跨 slot 的批量操作由 ClusterClient 自动按 slot 分组（scatter-gather）。
type RedisStore struct {
	provider ClientProvider
	timeout  time.Duration // 单次操作独立超时；超时即触发降级策略（由调用方处理）
}

// NewRedisStore 创建 Redis Store；timeout <= 0 时取默认 3s。
func NewRedisStore(provider ClientProvider, timeout time.Duration) *RedisStore {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &RedisStore{provider: provider, timeout: timeout}
}

func (s *RedisStore) withClient(ctx context.Context) (context.Context, RedisCmdable, context.CancelFunc, error) {
	c, err := s.provider(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, s.timeout)
	return cctx, c, cancel, nil
}

func (s *RedisStore) Get(ctx context.Context, key string) ([]byte, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	b, err := c.Get(cctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

// MGet pipeline GET；结果仅含命中 key。
func (s *RedisStore) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	pipe := c.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.Get(cctx, k)
	}
	_, _ = pipe.Exec(cctx) // 逐命令判定错误（GET 未命中为 redis.Nil，Exec 汇总无意义）
	for i, cmd := range cmds {
		b, err := cmd.Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return nil, err
		}
		out[keys[i]] = b
	}
	return out, nil
}

func (s *RedisStore) MSet(ctx context.Context, ttl time.Duration, items map[string][]byte) error {
	if len(items) == 0 {
		return nil
	}
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	pipe := c.Pipeline()
	for k, v := range items {
		pipe.Set(cctx, k, v, ttl)
	}
	_, err = pipe.Exec(cctx)
	return err
}

func (s *RedisStore) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return c.Del(cctx, keys...).Err()
}

func (s *RedisStore) Incr(ctx context.Context, key string) (int64, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	return c.Incr(cctx, key).Result()
}

func (s *RedisStore) HGet(ctx context.Context, key, field string) ([]byte, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	b, err := c.HGet(cctx, key, field).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (s *RedisStore) HMGet(ctx context.Context, key string, fields ...string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(fields))
	if len(fields) == 0 {
		return out, nil
	}
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	vals, err := c.HMGet(cctx, key, fields...).Result()
	if err != nil {
		return nil, err
	}
	for i, v := range vals {
		if str, ok := v.(string); ok {
			out[fields[i]] = []byte(str)
		}
	}
	return out, nil
}

func (s *RedisStore) HGetAll(ctx context.Context, key string) (map[string][]byte, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	m, err := c.HGetAll(cctx, key).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(m))
	for f, v := range m {
		out[f] = []byte(v)
	}
	return out, nil
}

func (s *RedisStore) HMSet(ctx context.Context, key string, fields map[string][]byte, ttl time.Duration, refresh bool) error {
	if len(fields) == 0 {
		return nil
	}
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	pipe := c.Pipeline()
	vals := make(map[string]interface{}, len(fields))
	for f, v := range fields {
		vals[f] = string(v)
	}
	pipe.HSet(cctx, key, vals)
	if ttl > 0 {
		if refresh {
			pipe.Expire(cctx, key, ttl)
		} else {
			pipe.ExpireNX(cctx, key, ttl)
		}
	}
	_, err = pipe.Exec(cctx)
	return err
}

func (s *RedisStore) HDel(ctx context.Context, key string, fields ...string) error {
	if len(fields) == 0 {
		return nil
	}
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return c.HDel(cctx, key, fields...).Err()
}

func (s *RedisStore) SMembers(ctx context.Context, key string) ([][]byte, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	vals, err := c.SMembers(cctx, key).Result()
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(vals))
	for i, v := range vals {
		out[i] = []byte(v)
	}
	return out, nil
}

func (s *RedisStore) SAdd(ctx context.Context, key string, ttl time.Duration, members ...[]byte) error {
	if len(members) == 0 {
		return nil
	}
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	pipe := c.Pipeline()
	vals := make([]interface{}, len(members))
	for i, m := range members {
		vals[i] = string(m)
	}
	pipe.SAdd(cctx, key, vals...)
	if ttl > 0 {
		pipe.ExpireNX(cctx, key, ttl)
	}
	_, err = pipe.Exec(cctx)
	return err
}

func (s *RedisStore) SIsMember(ctx context.Context, key string, member []byte) (bool, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return false, err
	}
	defer cancel()
	return c.SIsMember(cctx, key, string(member)).Result()
}

func (s *RedisStore) SRem(ctx context.Context, key string, members ...[]byte) error {
	if len(members) == 0 {
		return nil
	}
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	vals := make([]interface{}, len(members))
	for i, m := range members {
		vals[i] = string(m)
	}
	return c.SRem(cctx, key, vals...).Err()
}

func (s *RedisStore) LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	vals, err := c.LRange(cctx, key, start, stop).Result()
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(vals))
	for i, v := range vals {
		out[i] = []byte(v)
	}
	return out, nil
}

func (s *RedisStore) LLen(ctx context.Context, key string) (int64, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	return c.LLen(cctx, key).Result()
}

func (s *RedisStore) push(ctx context.Context, key string, ttl time.Duration, left bool, values ...[]byte) error {
	if len(values) == 0 {
		return nil
	}
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	pipe := c.Pipeline()
	vals := make([]interface{}, len(values))
	for i, v := range values {
		vals[i] = string(v)
	}
	if left {
		pipe.LPush(cctx, key, vals...)
	} else {
		pipe.RPush(cctx, key, vals...)
	}
	if ttl > 0 {
		pipe.ExpireNX(cctx, key, ttl)
	}
	_, err = pipe.Exec(cctx)
	return err
}

func (s *RedisStore) LPush(ctx context.Context, key string, ttl time.Duration, values ...[]byte) error {
	return s.push(ctx, key, ttl, true, values...)
}

func (s *RedisStore) RPush(ctx context.Context, key string, ttl time.Duration, values ...[]byte) error {
	return s.push(ctx, key, ttl, false, values...)
}

func (s *RedisStore) LTrim(ctx context.Context, key string, start, stop int64) error {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return c.LTrim(cctx, key, start, stop).Err()
}

func (s *RedisStore) ZAdd(ctx context.Context, key string, ttl time.Duration, members ...Z) error {
	if len(members) == 0 {
		return nil
	}
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	pipe := c.Pipeline()
	zs := make([]redis.Z, len(members))
	for i, m := range members {
		zs[i] = redis.Z{Score: m.Score, Member: string(m.Member)}
	}
	pipe.ZAdd(cctx, key, zs...)
	if ttl > 0 {
		pipe.ExpireNX(cctx, key, ttl)
	}
	_, err = pipe.Exec(cctx)
	return err
}

func (s *RedisStore) ZRem(ctx context.Context, key string, members ...[]byte) error {
	if len(members) == 0 {
		return nil
	}
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	vals := make([]interface{}, len(members))
	for i, m := range members {
		vals[i] = string(m)
	}
	return c.ZRem(cctx, key, vals...).Err()
}

func (s *RedisStore) ZScore(ctx context.Context, key string, member []byte) (float64, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	score, err := c.ZScore(cctx, key, string(member)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return score, nil
}

func (s *RedisStore) ZCard(ctx context.Context, key string) (int64, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	return c.ZCard(cctx, key).Result()
}

func toZSlice(zs []redis.Z) []Z {
	out := make([]Z, len(zs))
	for i, z := range zs {
		member, _ := z.Member.(string)
		out[i] = Z{Score: z.Score, Member: []byte(member)}
	}
	return out
}

func (s *RedisStore) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]Z, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	zs, err := c.ZRangeWithScores(cctx, key, start, stop).Result()
	if err != nil {
		return nil, err
	}
	return toZSlice(zs), nil
}

func (s *RedisStore) ZRangeByScoreWithScores(ctx context.Context, key string, by ZRangeBy) ([]Z, error) {
	cctx, c, cancel, err := s.withClient(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	opt := &redis.ZRangeBy{Min: by.Min, Max: by.Max, Offset: by.Offset, Count: by.Count}
	zs, err := c.ZRangeByScoreWithScores(cctx, key, opt).Result()
	if err != nil {
		return nil, err
	}
	return toZSlice(zs), nil
}
