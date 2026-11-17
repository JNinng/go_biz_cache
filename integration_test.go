//go:build integration

// 集成测试：验证 RedisStore + ClientProvider 真实接线（单机 redis.Client）。
// 需本机 Redis（默认 127.0.0.1:6379）；运行：go test -tags integration ./...
package gocache_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	gocache "github.com/jninng/go_biz_cache"
	"github.com/redis/go-redis/v9"
)

type integrationUser struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func newIntegrationClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// requireRedis7 跳过依赖 ExpireNX 语义（Redis 7.0+）的用例。
func requireRedis7(t *testing.T, client *redis.Client) {
	t.Helper()
	info := client.Info(context.Background(), "server").Val()
	var major int
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "redis_version:") {
			v := strings.TrimPrefix(line, "redis_version:")
			fmt.Sscanf(v, "%d", &major)
			break
		}
	}
	if major < 7 {
		t.Skipf("redis %d.x < 7.0：ExpireNX（EXPIRE ... NX）语义不可用，升级 Redis 后运行本用例", major)
	}
}

func TestIntegrationRedisObject(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	_ = client.Del(ctx, "itest:users:v0:u:1", "itest:users:__gen").Err()

	provider := func(ctx context.Context) (gocache.RedisCmdable, error) { return client, nil }
	loads := 0
	h := gocache.MustDefine[integrationUser]("itest", "users", gocache.Definition[integrationUser]{
		KeyTemplate:   "u:{id}",
		TTL:           time.Minute,
		NegativeCache: true,
		L2:            gocache.L2Options{Enabled: true, ClientProvider: provider},
		Loader: func(ctx context.Context, keys []string) (map[string]integrationUser, error) {
			loads++
			m := map[string]integrationUser{}
			for _, k := range keys {
				if strings.Contains(k, "ghost") {
					continue // 源缺失 → 负缓存路径
				}
				m[k] = integrationUser{ID: k, Name: "loaded"}
			}
			return m, nil
		},
	})

	want := integrationUser{ID: "u:1", Name: "alice"}
	if err := h.Set(ctx, "u:1", want); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// L2 真实存在
	if _, err := client.Get(ctx, "itest:users:v0:u:1").Result(); err != nil {
		t.Fatalf("phys key missing in redis: %v", err)
	}
	got, err := h.Get(ctx, "u:1")
	if err != nil || got.Name != "alice" {
		t.Fatalf("Get = %v,%v", got, err)
	}
	// 清 L1 后 L2 命中
	// L1 默认未启用（L1Options 零值），Get 直接走 L2
	if got, err = h.Get(ctx, "u:1"); err != nil || got.Name != "alice" || loads != 0 {
		t.Fatalf("Get via redis = %v,%v loads=%d", got, err, loads)
	}
	// 负缓存穿透路径
	if _, err := h.Get(ctx, "u:ghost"); !errors.Is(err, gocache.ErrNotFound) {
		t.Fatalf("ghost = %v", err)
	}
	// Delete 后回源（ghost 的 Get 已消耗一次回源，计 2）
	if err := h.Delete(ctx, "u:1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err = h.Get(ctx, "u:1")
	if err != nil || got.Name != "loaded" || loads != 2 {
		t.Fatalf("Get after delete = %v,%v loads=%d", got, err, loads)
	}
	// InvalidateAll 世代号生效
	if err := h.InvalidateAll(ctx); err != nil {
		t.Fatalf("InvalidateAll: %v", err)
	}
	if n, err := client.Get(ctx, "itest:users:__gen").Int64(); err != nil || n != 1 {
		t.Fatalf("gen meta key = %d,%v", n, err)
	}
}

func TestIntegrationRedisHash(t *testing.T) {
	client := newIntegrationClient(t)
	requireRedis7(t, client) // HMSet 的 ExpireNX 语义需要 Redis 7.0+
	ctx := context.Background()
	_ = client.Del(ctx, "itest:hobjs:v0:h:a").Err()

	provider := func(ctx context.Context) (gocache.RedisCmdable, error) { return client, nil }
	h := gocache.MustDefineHash[string, integrationUser]("itest", "hobjs",
		gocache.Definition[integrationUser]{
			KeyTemplate: "h:{k}",
			TTL:         time.Minute,
			L2:          gocache.L2Options{Enabled: true, ClientProvider: provider},
		},
		gocache.HashOptions[string, integrationUser]{})
	if err := h.HSet(ctx, "h:a", "nickname", integrationUser{ID: "n", Name: "nick"}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	// redis hash 语义真实存在
	if v, err := client.HGet(ctx, "itest:hobjs:v0:h:a", "nickname").Result(); err != nil || len(v) == 0 {
		t.Fatalf("redis HGET = %q,%v", v, err)
	}
	got, err := h.HGet(ctx, "h:a", "nickname")
	if err != nil || got.Name != "nick" {
		t.Fatalf("HGet = %v,%v", got, err)
	}
	if m, err := h.HGetAll(ctx, "h:a"); err != nil || len(m) != 1 {
		t.Fatalf("HGetAll = %v,%v", m, err)
	}
	if err := h.Delete(ctx, "h:a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n, err := client.HLen(ctx, "itest:hobjs:v0:h:a").Result(); err == nil && n > 0 {
		t.Fatalf("hash not deleted, fields=%d", n)
	}
}
