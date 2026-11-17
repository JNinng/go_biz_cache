package gocache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jninng/go_biz_cache/l2"
)

// ---------- 测试共享设施 ----------

type userProfile struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

// recStore 包装 MemStore 记录写操作（供物理 key / TTL 断言）。
type recStore struct {
	l2.Store
	mu           sync.Mutex
	setKeys      []string
	setTTLs      []time.Duration
	hmsetRefresh []bool // 每次 HMSet 的 refresh 实参（验证 RefreshTTLOnWrite 接线）
}

func newRecStore() *recStore { return &recStore{Store: l2.NewMemStore()} }

func (s *recStore) MSet(ctx context.Context, ttl time.Duration, items map[string][]byte) error {
	s.mu.Lock()
	for k := range items {
		s.setKeys = append(s.setKeys, k)
		s.setTTLs = append(s.setTTLs, ttl)
	}
	s.mu.Unlock()
	return s.Store.MSet(ctx, ttl, items)
}

func (s *recStore) HMSet(ctx context.Context, key string, fields map[string][]byte, ttl time.Duration, refresh bool) error {
	s.mu.Lock()
	s.hmsetRefresh = append(s.hmsetRefresh, refresh)
	s.mu.Unlock()
	return s.Store.HMSet(ctx, key, fields, ttl, refresh)
}

func (s *recStore) recordedRefreshFlags() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bool(nil), s.hmsetRefresh...)
}

func (s *recStore) recordedKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.setKeys...)
}

func (s *recStore) recordedTTLs() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.setTTLs...)
}

// flakyStore 可切换故障的 Store（降级策略测试用）。
type flakyStore struct {
	l2.Store
	fail atomic.Bool
}

func newFlakyStore() *flakyStore { return &flakyStore{Store: l2.NewMemStore()} }

func (s *flakyStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s.fail.Load() {
		return nil, context.DeadlineExceeded
	}
	return s.Store.Get(ctx, key)
}

func (s *flakyStore) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	if s.fail.Load() {
		return nil, context.DeadlineExceeded
	}
	return s.Store.MGet(ctx, keys...)
}

func (s *flakyStore) MSet(ctx context.Context, ttl time.Duration, items map[string][]byte) error {
	if s.fail.Load() {
		return context.DeadlineExceeded
	}
	return s.Store.MSet(ctx, ttl, items)
}

// countingLoader 计数 Loader（记录每次调用的 key 集合）。
type countingLoader struct {
	mu     sync.Mutex
	calls  int
	loaded [][]string
}

func (l *countingLoader) load(ctx context.Context, keys []string) (map[string]userProfile, error) {
	l.mu.Lock()
	l.calls++
	l.loaded = append(l.loaded, append([]string(nil), keys...))
	l.mu.Unlock()
	out := make(map[string]userProfile, len(keys))
	for _, k := range keys {
		out[k] = userProfile{ID: k, Name: "n-" + k, Age: 18}
	}
	return out, nil
}

func (l *countingLoader) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func (l *countingLoader) lastCall() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.loaded) == 0 {
		return nil
	}
	return l.loaded[len(l.loaded)-1]
}

func newTestObject(store l2.Store, loader Loader[string, userProfile], mutate func(*Definition[userProfile])) *TypedObjectCache[string, userProfile] {
	def := Definition[userProfile]{
		KeyTemplate:   "t:{tenant}:u:{uid}",
		TTL:           30 * time.Minute,
		NegativeCache: true,
		L1:            L1Options{Enabled: true, Size: 1024},
		L2:            L2Options{Enabled: true, Store: store},
		Loader:        loader,
	}
	if mutate != nil {
		mutate(&def)
	}
	return MustDefine[userProfile]("test", "users", def)
}
