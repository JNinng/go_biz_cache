package gocache

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/go_biz_cache/event"
	"github.com/jninng/go_biz_cache/l2"
)

func TestObjectSetGet(t *testing.T) {
	Reset()
	defer Reset()
	store := newRecStore()
	ld := &countingLoader{}
	h := newTestObject(store, ld.load, nil)
	ctx := context.Background()

	want := userProfile{ID: "u1", Name: "alice", Age: 30}
	if err := h.Set(ctx, "t:acme:u:1", want); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// 物理 key 形如 {ns}:{name}:v0:{biz}
	keys := store.recordedKeys()
	if len(keys) != 1 || keys[0] != "test:users:v0:t:acme:u:1" {
		t.Fatalf("phys keys = %v", keys)
	}
	got, err := h.Get(ctx, "t:acme:u:1")
	if err != nil || got != want {
		t.Fatalf("Get = %v,%v", got, err)
	}
	if ld.callCount() != 0 {
		t.Fatalf("Set/Get must not load source, calls=%d", ld.callCount())
	}
}

func TestObjectGetLoadThrough(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	ld := &countingLoader{}
	h := newTestObject(store, ld.load, nil)
	ctx := context.Background()

	got, err := h.Get(ctx, "t:acme:u:1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "n-t:acme:u:1" {
		t.Fatalf("loaded value = %v", got)
	}
	// L1 命中：不再回源
	if _, err = h.Get(ctx, "t:acme:u:1"); err != nil || ld.callCount() != 1 {
		t.Fatalf("second Get must hit L1, calls=%d err=%v", ld.callCount(), err)
	}
	// 清 L1 后 L2 命中：仍不回源
	h.core.l1.Delete("t:acme:u:1")
	if _, err = h.Get(ctx, "t:acme:u:1"); err != nil || ld.callCount() != 1 {
		t.Fatalf("third Get must hit L2, calls=%d err=%v", ld.callCount(), err)
	}
}

func TestObjectNegativeCache(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	h := newTestObject(store, func(ctx context.Context, keys []string) (map[string]userProfile, error) {
		return map[string]userProfile{}, nil // 全部缺失
	}, nil)
	ctx := context.Background()

	if _, err := h.Get(ctx, "t:acme:u:ghost"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("first Get = %v", err)
	}
	// 负缓存生效：不再回源
	if _, err := h.Get(ctx, "t:acme:u:ghost"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Get = %v", err)
	}
	// L2 负标记存在
	if _, err := store.Get(ctx, "test:users:v0:t:acme:u:ghost:__neg"); err != nil {
		t.Fatalf("neg marker missing: %v", err)
	}
	// Set 后负标记清除、可读新值
	want := userProfile{ID: "ghost", Name: "now", Age: 1}
	if err := h.Set(ctx, "t:acme:u:ghost", want); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, err := h.Get(ctx, "t:acme:u:ghost"); err != nil || got != want {
		t.Fatalf("Get after Set = %v,%v", got, err)
	}
}

func TestObjectNoLoaderMiss(t *testing.T) {
	Reset()
	defer Reset()
	h := newTestObject(l2.NewMemStore(), nil, nil) // Loader 未配置
	if _, err := h.Get(t.Context(), "t:acme:u:1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get without loader = %v", err)
	}
	if err := h.Refresh(t.Context(), "t:acme:u:1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Refresh without loader = %v", err)
	}
}

func TestObjectDelete(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	ld := &countingLoader{}
	h := newTestObject(store, ld.load, nil)
	ctx := context.Background()

	if err := h.Set(ctx, "t:acme:u:1", userProfile{ID: "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := h.Delete(ctx, "t:acme:u:1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, "test:users:v0:t:acme:u:1"); !errors.Is(err, l2.ErrNotFound) {
		t.Fatalf("phys key must be deleted, got %v", err)
	}
	// L1 已删 + L2 已删 → 回源
	if _, err := h.Get(ctx, "t:acme:u:1"); err != nil || ld.callCount() != 1 {
		t.Fatalf("Get after Delete must reload, calls=%d err=%v", ld.callCount(), err)
	}
}

func TestObjectMultiGet(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	ld := &countingLoader{}
	h := newTestObject(store, ld.load, nil)
	ctx := context.Background()

	// f1 预写 L2
	if err := h.Set(ctx, "t:acme:u:1", userProfile{ID: "1", Name: "one"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := h.MultiGet(ctx, "t:acme:u:1", "t:acme:u:2", "t:acme:u:2", "t:acme:u:3")
	if err != nil {
		t.Fatalf("MultiGet: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("MultiGet = %v", got)
	}
	if got["t:acme:u:1"].Name != "one" {
		t.Fatalf("L2 hit value = %v", got["t:acme:u:1"])
	}
	// 剩余 keys 一次批量回源（去重后；批次顺序不定，按集合比较）
	if ld.callCount() != 1 {
		t.Fatalf("loader calls = %d, want 1", ld.callCount())
	}
	last := ld.lastCall()
	if len(last) != 2 {
		t.Fatalf("batch loader keys = %v", last)
	}
	sort.Strings(last)
	if last[0] != "t:acme:u:2" || last[1] != "t:acme:u:3" {
		t.Fatalf("batch loader keys = %v", last)
	}
	// 全 L1 命中路径
	h.core.l1.Clear()
	got2, err := h.MultiGet(ctx, "t:acme:u:2", "t:acme:u:3")
	if err != nil || len(got2) != 2 {
		t.Fatalf("MultiGet after warm = %v,%v", got2, err)
	}
	if ld.callCount() != 1 {
		t.Fatalf("warm MultiGet must not reload, calls=%d", ld.callCount())
	}
}

func TestObjectRefresh(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	calls := 0
	h := newTestObject(store, func(ctx context.Context, keys []string) (map[string]userProfile, error) {
		calls++
		return map[string]userProfile{"t:acme:u:1": {ID: "1", Name: "v" + string(rune('0'+calls))}}, nil
	}, nil)
	ctx := context.Background()

	if err := h.Set(ctx, "t:acme:u:1", userProfile{ID: "1", Name: "stale"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := h.Refresh(ctx, "t:acme:u:1"); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	got, err := h.Get(ctx, "t:acme:u:1")
	if err != nil || got.Name != "v1" {
		t.Fatalf("Get after Refresh = %v,%v", got, err)
	}
}

func TestObjectInvalidateAllGeneration(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	ld := &countingLoader{}
	h := newTestObject(store, ld.load, nil)
	ctx := context.Background()

	if err := h.Set(ctx, "t:acme:u:1", userProfile{ID: "1", Name: "old"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := h.InvalidateAll(ctx); err != nil {
		t.Fatalf("InvalidateAll: %v", err)
	}
	// 新世代 key 生效：旧值不再可见（L1 已清 + 物理 key 进入 v1）
	got, err := h.Get(ctx, "t:acme:u:1")
	if err != nil {
		t.Fatalf("Get after InvalidateAll: %v", err)
	}
	// 回源值写穿到新世代 key
	if _, err := store.Get(ctx, "test:users:v1:t:acme:u:1"); err != nil {
		t.Fatalf("new-gen key missing: %v", err)
	}
	if got.Name != "n-t:acme:u:1" {
		t.Fatalf("loaded value = %v", got)
	}
	// 再次 Set 落在 v1
	if err := h.Set(ctx, "t:acme:u:2", userProfile{ID: "2"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := store.Get(ctx, "test:users:v1:t:acme:u:2"); err != nil {
		t.Fatalf("Set must land in new generation: %v", err)
	}
}

func TestObjectTTLJitter(t *testing.T) {
	Reset()
	defer Reset()
	store := newRecStore()
	h := newTestObject(store, nil, func(d *Definition[userProfile]) {
		d.TTL = 10 * time.Minute
		d.TTLJitter = 0.1
		d.L1 = L1Options{Enabled: false}
	})
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := h.Set(ctx, "t:acme:u:"+string(rune('a'+i)), userProfile{ID: "x"}); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	ttls := store.recordedTTLs()
	if len(ttls) != 20 {
		t.Fatalf("recorded %d ttls", len(ttls))
	}
	lo, hi := 9*time.Minute, 11*time.Minute
	distinct := map[time.Duration]bool{}
	for _, ttl := range ttls {
		if ttl < lo || ttl > hi {
			t.Fatalf("ttl %v out of jitter range [%v,%v]", ttl, lo, hi)
		}
		distinct[ttl] = true
	}
	// 离散性：jitter 生效则 20 个样本应出现多个不同值（全相等的概率可忽略）
	if len(distinct) < 2 {
		t.Fatalf("jitter must disperse TTLs, got %d distinct values", len(distinct))
	}
}

// 负缓存占位按 NegativeTTL 短 TTL 过期，过期后恢复回源。
func TestObjectNegativeTTLExpiry(t *testing.T) {
	Reset()
	defer Reset()
	h := newTestObject(l2.NewMemStore(), func(ctx context.Context, keys []string) (map[string]userProfile, error) {
		return map[string]userProfile{}, nil // 恒缺失
	}, func(d *Definition[userProfile]) {
		d.NegativeTTL = 80 * time.Millisecond
	})
	ctx := context.Background()
	biz := "t:acme:u:ghost"

	if _, err := h.Get(ctx, biz); !errors.Is(err, ErrNotFound) {
		t.Fatalf("first = %v", err)
	}
	// 窗口内负缓存命中，不回源
	if _, err := h.Get(ctx, biz); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cached = %v", err)
	}
	time.Sleep(200 * time.Millisecond) // > NegativeTTL，L1 nil 条目与 L2 标记均过期
	// 过期后恢复回源
	if _, err := h.Get(ctx, biz); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after expiry = %v", err)
	}
}

// SetMulti 一次广播 Updated（Keys 为各物理 key）并清除旧负标记。
func TestObjectSetMultiEventAndNegClear(t *testing.T) {
	Reset()
	defer Reset()
	bus := event.NewInProcess()
	defer bus.Close()
	UseEventBus(bus)
	defer UseEventBus(nil)
	events := make(chan event.Event, 8)
	if _, err := bus.Subscribe(context.Background(), func(e event.Event) { events <- e }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	store := l2.NewMemStore()
	h := newTestObject(store, func(ctx context.Context, keys []string) (map[string]userProfile, error) {
		return map[string]userProfile{}, nil // 缺失 → 写负标记
	}, nil)
	ctx := context.Background()
	if _, err := h.Get(ctx, "t:acme:u:1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("warm negative: %v", err)
	}
	if _, err := store.Get(ctx, "test:users:v0:t:acme:u:1:__neg"); err != nil {
		t.Fatalf("neg marker must exist: %v", err)
	}
	// SetMulti：写穿 + 清负标记 + 一次 Updated
	err := h.SetMulti(ctx, map[string]userProfile{
		"t:acme:u:1": {ID: "1", Name: "one"},
		"t:acme:u:2": {ID: "2", Name: "two"},
	})
	if err != nil {
		t.Fatalf("SetMulti: %v", err)
	}
	if _, err := store.Get(ctx, "test:users:v0:t:acme:u:1:__neg"); !errors.Is(err, l2.ErrNotFound) {
		t.Fatalf("neg marker must be cleared by SetMulti: %v", err)
	}
	select {
	case e := <-events:
		if e.Type != event.Updated || len(e.Keys) != 2 {
			t.Fatalf("event = %+v", e)
		}
		sort.Strings(e.Keys)
		if e.Keys[0] != "test:users:v0:t:acme:u:1" || e.Keys[1] != "test:users:v0:t:acme:u:2" {
			t.Fatalf("SetMulti event keys = %v", e.Keys)
		}
	case <-time.After(time.Second):
		t.Fatal("SetMulti Updated event not received")
	}
}

func TestObjectSingleflightMerge(t *testing.T) {
	Reset()
	defer Reset()
	var calls atomic.Int32
	h := newTestObject(l2.NewMemStore(), func(ctx context.Context, keys []string) (map[string]userProfile, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond) // 放大并发窗口
		return map[string]userProfile{"t:acme:u:1": {ID: "1", Name: "once"}}, nil
	}, nil)
	ctx := context.Background()
	const n = 50
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := h.Get(ctx, "t:acme:u:1")
			if err == nil && v.Name != "once" {
				err = errors.New("bad value")
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	// 同 key 并发回源由 singleflight 合并为一次
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}
}

func TestObjectDegradeFail(t *testing.T) {
	Reset()
	defer Reset()
	fs := newFlakyStore()
	called := 0
	h := newTestObject(fs, func(ctx context.Context, keys []string) (map[string]userProfile, error) {
		called++
		return map[string]userProfile{}, nil
	}, func(d *Definition[userProfile]) { d.Degrade = DegradeFail })
	ctx := context.Background()
	fs.fail.Store(true)
	if _, err := h.Get(ctx, "t:acme:u:1"); err == nil {
		t.Fatal("DegradeFail must propagate error")
	}
	if called != 0 {
		t.Fatal("DegradeFail must not fall through to loader")
	}
}

func TestObjectDegradeSourceOnly(t *testing.T) {
	Reset()
	defer Reset()
	fs := newFlakyStore()
	ld := &countingLoader{}
	h := newTestObject(fs, ld.load, func(d *Definition[userProfile]) { d.Degrade = DegradeSourceOnly })
	ctx := context.Background()
	fs.fail.Store(true)
	got, err := h.Get(ctx, "t:acme:u:1")
	if err != nil || got.Name != "n-t:acme:u:1" {
		t.Fatalf("SourceOnly must load from source: %v,%v", got, err)
	}
}

func TestObjectDegradeStaleL1(t *testing.T) {
	Reset()
	defer Reset()
	fs := newFlakyStore()
	called := 0
	h := newTestObject(fs, func(ctx context.Context, keys []string) (map[string]userProfile, error) {
		called++
		return map[string]userProfile{}, nil
	}, func(d *Definition[userProfile]) { d.Degrade = DegradeStaleL1 })
	ctx := context.Background()
	// L1 有旧值：L2 故障下仍可读（L1 优先）
	if err := h.Set(ctx, "t:acme:u:1", userProfile{ID: "1", Name: "stale-ok"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	fs.fail.Store(true)
	got, err := h.Get(ctx, "t:acme:u:1")
	if err != nil || got.Name != "stale-ok" {
		t.Fatalf("StaleL1 with warm L1 = %v,%v", got, err)
	}
	// L1 无值：不回源、直接缺失
	if _, err := h.Get(ctx, "t:acme:u:2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("StaleL1 cold = %v", err)
	}
	if called != 0 {
		t.Fatalf("StaleL1 must not load source, calls=%d", called)
	}
}

func TestObjectShardRoundTrip(t *testing.T) {
	Reset()
	defer Reset()
	store := newRecStore()
	ld := &countingLoader{}
	h := newTestObject(store, ld.load, func(d *Definition[userProfile]) {
		d.BigValue = BigValuePolicy{Type: BigValueShard, Shard: ShardOptions{ShardCount: 4}}
		d.NegativeCache = false
	})
	ctx := context.Background()
	want := userProfile{ID: "1", Name: strings.Repeat("long-value-", 100), Age: 7}
	if err := h.Set(ctx, "t:acme:u:1", want); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// 4 个分片子 key（:s0..:s3），整 key 不存在
	keys := store.recordedKeys()
	if len(keys) != 4 {
		t.Fatalf("shard keys = %v", keys)
	}
	sort.Strings(keys)
	for i, k := range keys {
		if !strings.HasSuffix(k, ":s"+string(rune('0'+i))) {
			t.Fatalf("key %d = %q missing :s%d suffix", i, k, i)
		}
	}
	if _, err := store.Get(ctx, "test:users:v0:t:acme:u:1"); !errors.Is(err, l2.ErrNotFound) {
		t.Fatal("whole key must not exist under Shard policy")
	}
	// 读路径端到端：清 L1 强制走 L2 分片 MGET → 拼接 → 去长度帧 → 反序列化
	h.core.l1.Delete("t:acme:u:1")
	got, err := h.Get(ctx, "t:acme:u:1")
	if err != nil || got != want {
		t.Fatalf("Get via L2 shards = %v,%v", got, err)
	}
	// 任一分片缺失 → 视为未命中自愈重写
	if err := store.Del(ctx, "test:users:v0:t:acme:u:1:s2"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	h.core.l1.Delete("t:acme:u:1")
	if _, err := h.Get(ctx, "t:acme:u:1"); err != nil || ld.callCount() != 1 {
		t.Fatalf("missing shard must trigger reload, calls=%d err=%v", ld.callCount(), err)
	}
	// Delete 清理全部子 key
	if err := h.Delete(ctx, "t:acme:u:1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err := store.Get(ctx, "test:users:v0:t:acme:u:1:s"+string(rune('0'+i))); !errors.Is(err, l2.ErrNotFound) {
			t.Fatalf("shard %d not deleted", i)
		}
	}
}

// ShardCount 调小后旧分片全命中会拼出截断值：长度帧校验必须将其判为未命中自愈，而非反序列化报错。
func TestObjectShardShrinkSelfHeal(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	ld := &countingLoader{}
	h := newTestObject(store, ld.load, func(d *Definition[userProfile]) {
		d.BigValue = BigValuePolicy{Type: BigValueShard, Shard: ShardOptions{ShardCount: 4}}
		d.NegativeCache = false
		d.L1 = L1Options{Enabled: false}
	})
	ctx := context.Background()
	phys := "test:users:v0:t:acme:u:1"

	// 模拟旧大分片数残留：无长度帧的 4 片数据（拼接后长度头校验必然不符）
	junk := bytes.Repeat([]byte("x"), 100)
	for i := 0; i < 4; i++ {
		chunk := junk[i*25 : (i+1)*25]
		if err := store.MSet(ctx, time.Minute, map[string][]byte{phys + ":s" + string(rune('0'+i)): chunk}); err != nil {
			t.Fatalf("seed old shards: %v", err)
		}
	}
	got, err := h.Get(ctx, "t:acme:u:1")
	if err != nil {
		t.Fatalf("stale shards must self-heal via loader, got error: %v", err)
	}
	if got.Name != "n-t:acme:u:1" || ld.callCount() != 1 {
		t.Fatalf("self-heal value = %v, calls=%d", got, ld.callCount())
	}
	// 自愈重写后带长度帧，二次读直接 L2 命中
	got, err = h.Get(ctx, "t:acme:u:1")
	if err != nil || got.Name != "n-t:acme:u:1" || ld.callCount() != 1 {
		t.Fatalf("after rewrite = %v, calls=%d err=%v", got, ld.callCount(), err)
	}
}

func TestObjectZstdCompress(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	h := newTestObject(store, nil, func(d *Definition[userProfile]) {
		d.BigValue = BigValuePolicy{Type: BigValueZstd, Zstd: ZstdOptions{CompressThreshold: 0, Level: Speed}}
	})
	ctx := context.Background()
	want := userProfile{ID: "1", Name: strings.Repeat("compress-me-", 500), Age: 9}
	if err := h.Set(ctx, "t:acme:u:1", want); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// L2 值应为压缩字节（zstd header = 0x01）
	data, err := store.Get(ctx, "test:users:v0:t:acme:u:1")
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if data[0] != 0x01 {
		t.Fatalf("L2 value must be zstd-framed, header=%d", data[0])
	}
	// 解压读路径：清 L1 强制走 L2 → 解压 → 反序列化
	h.core.l1.Delete("t:acme:u:1")
	got, err := h.Get(ctx, "t:acme:u:1")
	if err != nil || got != want {
		t.Fatalf("Get via L2 (decompress) = %v,%v", got, err)
	}
}

func TestObjectTaggedPhysicalKey(t *testing.T) {
	Reset()
	defer Reset()
	store := newRecStore()
	h := newTestObject(store, nil, func(d *Definition[userProfile]) {
		d.HashTag = TagFromPlaceholder("tenant")
	})
	if err := h.Set(t.Context(), "t:acme:u:1", userProfile{ID: "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := store.recordedKeys(); len(got) != 1 || got[0] != "test:users:v0:t:{acme}:u:1" {
		t.Fatalf("tagged phys keys = %v", got)
	}
}

func TestObjectEventsPublished(t *testing.T) {
	Reset()
	defer Reset()
	bus := event.NewInProcess()
	defer bus.Close()
	UseEventBus(bus)
	defer UseEventBus(nil)

	events := make(chan event.Event, 8)
	_, err := bus.Subscribe(context.Background(), func(e event.Event) { events <- e })
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	h := newTestObject(l2.NewMemStore(), nil, nil)
	ctx := context.Background()
	if err := h.Set(ctx, "t:acme:u:1", userProfile{ID: "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	select {
	case e := <-events:
		if e.Type != event.Updated || e.DefName != "users" || e.Namespace != "test" {
			t.Fatalf("event = %+v", e)
		}
		if len(e.Keys) != 1 || e.Keys[0] != "test:users:v0:t:acme:u:1" {
			t.Fatalf("event keys = %v", e.Keys)
		}
	case <-time.After(time.Second):
		t.Fatal("Updated event not received")
	}
	if err := h.Delete(ctx, "t:acme:u:1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	select {
	case e := <-events:
		if e.Type != event.Invalidated {
			t.Fatalf("event = %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("Invalidated event not received")
	}
}

func TestObjectSetMulti(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	h := newTestObject(store, nil, nil)
	ctx := context.Background()
	err := h.SetMulti(ctx, map[string]userProfile{
		"t:acme:u:1": {ID: "1", Name: "one"},
		"t:acme:u:2": {ID: "2", Name: "two"},
	})
	if err != nil {
		t.Fatalf("SetMulti: %v", err)
	}
	for biz, name := range map[string]string{"t:acme:u:1": "one", "t:acme:u:2": "two"} {
		got, err := h.Get(ctx, biz)
		if err != nil || got.Name != name {
			t.Fatalf("Get %s = %v,%v", biz, got, err)
		}
	}
}

func TestObjectNegativeAbsentFromMultiGet(t *testing.T) {
	Reset()
	defer Reset()
	h := newTestObject(l2.NewMemStore(), func(ctx context.Context, keys []string) (map[string]userProfile, error) {
		return map[string]userProfile{"t:acme:u:1": {ID: "1"}}, nil // 2 缺失 → 负缓存
	}, nil)
	ctx := context.Background()
	got, err := h.MultiGet(ctx, "t:acme:u:1", "t:acme:u:2")
	if err != nil {
		t.Fatalf("MultiGet: %v", err)
	}
	if len(got) != 1 || got["t:acme:u:1"].ID != "1" {
		t.Fatalf("MultiGet = %v", got)
	}
	// 负缓存命中不回源
	got, err = h.MultiGet(ctx, "t:acme:u:1", "t:acme:u:2")
	if err != nil || len(got) != 1 {
		t.Fatalf("MultiGet with negative = %v,%v", got, err)
	}
}

func TestDefineValidationErrors(t *testing.T) {
	Reset()
	defer Reset()
	// Shard 仅 Object
	if _, err := DefineHash[string, userProfile]("v", "bad_shard", Definition[userProfile]{
		KeyTemplate: "k:{k}",
		BigValue:    BigValuePolicy{Type: BigValueShard, Shard: ShardOptions{ShardCount: 4}},
	}, HashOptions[string, userProfile]{}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("shard on hash = %v", err)
	}
	// FieldShard 仅 Hash/Set
	if _, err := Define[userProfile]("v", "bad_fs", Definition[userProfile]{
		KeyTemplate: "k:{k}",
		BigValue:    BigValuePolicy{Type: BigValueFieldShard, FieldShard: FieldShardOptions{ShardCount: 4}},
	}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("fieldshard on object = %v", err)
	}
	// ShardCount >= 2
	if _, err := Define[userProfile]("v", "bad_cnt", Definition[userProfile]{
		KeyTemplate: "k:{k}",
		BigValue:    BigValuePolicy{Type: BigValueShard, Shard: ShardOptions{ShardCount: 1}},
	}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("shard count 1 = %v", err)
	}
	// 重复注册
	_, _ = Define[userProfile]("v", "dup", Definition[userProfile]{KeyTemplate: "k:{k}"})
	if _, err := Define[userProfile]("v", "dup", Definition[userProfile]{KeyTemplate: "k:{k}"}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("duplicate = %v", err)
	}
	// 空模板
	if _, err := Define[userProfile]("v", "bad_tpl", Definition[userProfile]{}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("empty template = %v", err)
	}
}

func TestPureLocalMode(t *testing.T) {
	Reset()
	defer Reset()
	h := MustDefine[userProfile]("local", "objs", Definition[userProfile]{
		KeyTemplate: "k:{k}",
		L1:          L1Options{Enabled: true},
		L2:          L2Options{Enabled: false},
		Loader: func(ctx context.Context, keys []string) (map[string]userProfile, error) {
			m := map[string]userProfile{}
			for _, k := range keys {
				m[k] = userProfile{ID: k}
			}
			return m, nil
		},
	})
	ctx := t.Context()
	if err := h.Set(ctx, "a", userProfile{ID: "a", Name: "x"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, err := h.Get(ctx, "a"); err != nil || got.Name != "x" {
		t.Fatalf("Get = %v,%v", got, err)
	}
	// 本地模式世代号内存递增
	if err := h.InvalidateAll(ctx); err != nil {
		t.Fatalf("InvalidateAll: %v", err)
	}
	if _, _, err := h.core.buildKey(ctx, "a"); err != nil {
		t.Fatalf("buildKey: %v", err)
	}
	_, phys, _ := h.core.buildKey(ctx, "a")
	if phys != "local:objs:v1:a" {
		t.Fatalf("local gen phys = %q", phys)
	}
}
