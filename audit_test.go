package gocache

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/jninng/go_biz_cache/bigvalue"
	"github.com/jninng/go_biz_cache/event"
	"github.com/jninng/go_biz_cache/l2"
)

// ---------- 降级分支（审查遗留：hash/集合/MultiGet 此前未测）----------

func TestObjectMultiGetDegrade(t *testing.T) {
	Reset()
	defer Reset()
	ctx := context.Background()
	mk := func(policy DegradePolicy, fs *flakyStore, ld *countingLoader) *TypedObjectCache[string, userProfile] {
		Reset() // 同 ns/name，每个子用例重新注册
		return newTestObject(fs, ld.load, func(d *Definition[userProfile]) { d.Degrade = policy })
	}

	// Fail：任一 key L2 读失败整体报错
	fs1 := newFlakyStore()
	ld1 := &countingLoader{}
	h := mk(DegradeFail, fs1, ld1)
	if err := h.Set(ctx, "t:acme:u:1", userProfile{ID: "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	fs1.fail.Store(true)
	if _, err := h.MultiGet(ctx, "t:acme:u:1", "t:acme:u:2"); err == nil {
		t.Fatal("Fail must propagate error")
	}
	if ld1.callCount() != 0 {
		t.Fatal("Fail must not fall through to loader")
	}

	// StaleL1：只保留 L1 命中，不回源
	fs2 := newFlakyStore()
	ld2 := &countingLoader{}
	h = mk(DegradeStaleL1, fs2, ld2)
	if err := h.Set(ctx, "t:acme:u:1", userProfile{ID: "1", Name: "warm"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	fs2.fail.Store(true)
	got, err := h.MultiGet(ctx, "t:acme:u:1", "t:acme:u:2")
	if err != nil || len(got) != 1 || got["t:acme:u:1"].Name != "warm" {
		t.Fatalf("StaleL1 = %v,%v", got, err)
	}
	if ld2.callCount() != 0 {
		t.Fatal("StaleL1 must not load source")
	}

	// SourceOnly：L1 命中 + 剩余走回源
	fs3 := newFlakyStore()
	ld3 := &countingLoader{}
	h = mk(DegradeSourceOnly, fs3, ld3)
	if err := h.Set(ctx, "t:acme:u:1", userProfile{ID: "1", Name: "warm"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	fs3.fail.Store(true)
	got, err = h.MultiGet(ctx, "t:acme:u:1", "t:acme:u:2")
	if err != nil || len(got) != 2 {
		t.Fatalf("SourceOnly = %v,%v", got, err)
	}
	if ld3.callCount() != 1 || len(ld3.lastCall()) != 1 || ld3.lastCall()[0] != "t:acme:u:2" {
		t.Fatalf("SourceOnly batch = %v calls=%d", ld3.lastCall(), ld3.callCount())
	}
}

func TestHashDegrade(t *testing.T) {
	Reset()
	defer Reset()
	ctx := context.Background()
	loader := func(ctx context.Context, key string, fields []string) (map[string]hashProfile, error) {
		m := map[string]hashProfile{}
		if fields == nil {
			m["a"], m["b"] = hashProfile{Name: "fa"}, hashProfile{Name: "fb"}
			return m, nil
		}
		for _, f := range fields {
			m[f] = hashProfile{Name: "v-" + f}
		}
		return m, nil
	}

	// Fail：HGet/HMGet/HGetAll 报错
	fs := newFlakyStore()
	h := newTestHash(fs, loader, func(d *Definition[hashProfile], o *HashOptions[string, hashProfile]) {
		d.Degrade = DegradeFail
	})
	fs.fail.Store(true)
	if _, err := h.HGet(ctx, "k", "f"); err == nil {
		t.Fatal("HGet Fail must error")
	}
	if _, err := h.HMGet(ctx, "k", "f"); err == nil {
		t.Fatal("HMGet Fail must error")
	}
	if _, err := h.HGetAll(ctx, "k"); err == nil {
		t.Fatal("HGetAll Fail must error")
	}

	// StaleL1：L1 命中可读；冷数据缺失
	Reset() // 同 ns/name，重新注册
	fs2 := newFlakyStore()
	h2 := newTestHash(fs2, loader, func(d *Definition[hashProfile], o *HashOptions[string, hashProfile]) {
		d.Degrade = DegradeStaleL1
	})
	if err := h2.HSet(ctx, "k", "f", hashProfile{Name: "warm"}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	fs2.fail.Store(true)
	if v, err := h2.HGet(ctx, "k", "f"); err != nil || v.Name != "warm" {
		t.Fatalf("HGet StaleL1 warm = %v,%v", v, err)
	}
	if _, err := h2.HGet(ctx, "k", "cold"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("HGet StaleL1 cold = %v", err)
	}

	// SourceOnly：L1 命中 + 冷数据走回源
	Reset()
	fs3 := newFlakyStore()
	h3 := newTestHash(fs3, loader, func(d *Definition[hashProfile], o *HashOptions[string, hashProfile]) {
		d.Degrade = DegradeSourceOnly
	})
	fs3.fail.Store(true)
	if v, err := h3.HGet(ctx, "k", "g"); err != nil || v.Name != "v-g" {
		t.Fatalf("HGet SourceOnly = %v,%v", v, err)
	}
	m, err := h3.HGetAll(ctx, "k2")
	if err != nil || len(m) != 2 {
		t.Fatalf("HGetAll SourceOnly = %v,%v", m, err)
	}
}

func TestCollectionsDegrade(t *testing.T) {
	Reset()
	defer Reset()
	ctx := context.Background()
	mkDef := func(policy DegradePolicy, store l2.Store) Definition[string] {
		return Definition[string]{
			KeyTemplate: "x:{k}",
			Degrade:     policy,
			L2:          L2Options{Enabled: true, Store: store},
		}
	}

	// Fail
	fs := newFlakyStore()
	s := MustDefineSet[string]("dg", "set", mkDef(DegradeFail, fs), SetOptions[string]{Loader: func(ctx context.Context, key string) ([]string, error) {
		return []string{"a"}, nil
	}})
	l := MustDefineList[string]("dg", "list", mkDef(DegradeFail, fs), ListOptions[string]{Loader: func(ctx context.Context, key string) ([]string, error) {
		return []string{"a"}, nil
	}})
	z := MustDefineZSet[string]("dg", "zset", mkDef(DegradeFail, fs), ZSetOptions[string]{Loader: func(ctx context.Context, key string) ([]Scored[string], error) {
		return []Scored[string]{{Value: "a", Score: 1}}, nil
	}})
	fs.fail.Store(true)
	if _, err := s.Members(ctx, "x:k"); err == nil {
		t.Fatal("set Members Fail must error")
	}
	if _, err := l.Range(ctx, "x:k", 0, -1); err == nil {
		t.Fatal("list Range Fail must error")
	}
	if _, err := l.Len(ctx, "x:k"); err == nil {
		t.Fatal("list Len Fail must error")
	}
	if _, err := z.Range(ctx, "x:k", 0, -1); err == nil {
		t.Fatal("zset Range Fail must error")
	}
	if _, err := z.RangeByScore(ctx, "x:k", ZRangeBy{Min: "-inf", Max: "+inf"}); err == nil {
		t.Fatal("zset RangeByScore Fail must error")
	}

	// StaleL1 冷数据 → 缺失
	fs2 := newFlakyStore()
	s2 := MustDefineSet[string]("dg2", "set", mkDef(DegradeStaleL1, fs2), SetOptions[string]{Loader: func(ctx context.Context, key string) ([]string, error) {
		return []string{"a"}, nil
	}})
	fs2.fail.Store(true)
	if _, err := s2.Members(ctx, "x:k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("set Members StaleL1 cold = %v", err)
	}
}

// ---------- 解码失败自愈（脏数据 → 删条目走回源，读不报错）----------

func TestObjectDecodeSelfHeal(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	ld := &countingLoader{}
	h := newTestObject(store, ld.load, nil)
	ctx := context.Background()
	phys := "test:users:v0:t:acme:u:1"

	// 直接污染 L2（绕过句柄写入，模拟脏数据/格式演进）
	if err := store.MSet(ctx, time.Minute, map[string][]byte{phys: []byte("{corrupt")}); err != nil {
		t.Fatalf("pollute: %v", err)
	}
	got, err := h.Get(ctx, "t:acme:u:1")
	if err != nil || got.Name != "n-t:acme:u:1" || ld.callCount() != 1 {
		t.Fatalf("corrupt must self-heal = %v,%v calls=%d", got, err, ld.callCount())
	}
	// 重写后为合法值：二次读走 L2 不回源
	if _, err := h.Get(ctx, "t:acme:u:1"); err != nil || ld.callCount() != 1 {
		t.Fatalf("after rewrite = %v calls=%d", err, ld.callCount())
	}
}

func TestHashDecodeSelfHeal(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	loads := 0
	h := newTestHash(store, func(ctx context.Context, key string, fields []string) (map[string]hashProfile, error) {
		loads++
		return map[string]hashProfile{"f": {Name: "fresh"}}, nil
	}, nil)
	ctx := context.Background()
	// 污染 L2 field 值
	if err := store.HMSet(ctx, "test:hobjs:v0:a", map[string][]byte{"f": []byte("{corrupt")}, time.Minute, false); err != nil {
		t.Fatalf("pollute: %v", err)
	}
	v, err := h.HGet(ctx, "a", "f")
	if err != nil || v.Name != "fresh" || loads != 1 {
		t.Fatalf("corrupt field must self-heal = %v,%v loads=%d", v, err, loads)
	}
}

// ---------- HSetMulti 全覆盖（读写、负标记清理、单次事件、FieldShard 分组）----------

func TestHashHSetMulti(t *testing.T) {
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
	h := newTestHash(store, func(ctx context.Context, key string, fields []string) (map[string]hashProfile, error) {
		return map[string]hashProfile{}, nil // 缺失 → 负标记
	}, nil)
	ctx := context.Background()

	// 预置 f1 负标记
	if _, err := h.HGet(ctx, "a", "f1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("warm negative: %v", err)
	}
	// HSetMulti 覆盖 f1/f2，一次广播
	if err := h.HSetMulti(ctx, "a", map[string]hashProfile{
		"f1": {Name: "one"},
		"f2": {Name: "two"},
	}); err != nil {
		t.Fatalf("HSetMulti: %v", err)
	}
	if v, err := h.HGet(ctx, "a", "f1"); err != nil || v.Name != "one" {
		t.Fatalf("HGet f1 = %v,%v", v, err) // 负标记已被清除
	}
	if _, err := store.Get(ctx, "test:hobjs:v0:a:__neg:"+hashFieldProbe("f1")); !errors.Is(err, l2.ErrNotFound) {
		t.Fatal("f1 negative marker must be cleared")
	}
	select {
	case e := <-events:
		if e.Type != event.Updated || len(e.Keys) != 1 || e.Keys[0] != "test:hobjs:v0:a" {
			t.Fatalf("event = %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("HSetMulti Updated event not received")
	}

	// FieldShard：分组写落入各自子 key，HGetAll 合并
	Reset() // 同 ns/name，重新注册
	h2 := newTestHash(l2.NewMemStore(), nil, func(d *Definition[hashProfile], o *HashOptions[string, hashProfile]) {
		d.BigValue = BigValuePolicy{Type: BigValueFieldShard, FieldShard: FieldShardOptions{ShardCount: 4}}
	})
	if err := h2.HSetMulti(ctx, "big", map[string]hashProfile{
		"x": {Name: "x"}, "y": {Name: "y"}, "z": {Name: "z"},
	}); err != nil {
		t.Fatalf("HSetMulti shard: %v", err)
	}
	m, err := h2.HGetAll(ctx, "big")
	if err != nil || len(m) != 3 {
		t.Fatalf("HGetAll merged = %v,%v", m, err)
	}
}

// hashFieldProbe 测试侧 field 负标记 key 的 fnv 段（与 defCore.fieldNegKey 一致）。
func hashFieldProbe(field string) string {
	return strconv.FormatUint(bigvalue.FNV1a([]byte(field)), 16)
}

// ---------- 带 Tag 的跨实例事件逆变换成功路径（精确删除对应 L1 条目）----------

func TestOnEventInverseDeletesExactL1(t *testing.T) {
	Reset()
	defer Reset()
	h := newTestObject(l2.NewMemStore(), nil, func(d *Definition[userProfile]) {
		d.HashTag = TagFromPlaceholder("tenant")
	})
	ctx := context.Background()
	if err := h.Set(ctx, "t:acme:u:1", userProfile{ID: "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := h.Set(ctx, "t:acme:u:2", userProfile{ID: "2"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// 另一实例更新 u:1：逆变换成功 → 仅删 u:1 的 L1 条目
	h.core.onEvent(event.Event{
		Type: event.Updated, Namespace: "test", DefName: "users",
		Keys: []string{"test:users:v0:t:{acme}:u:1"}, Instance: "other-instance",
	})
	if _, ok := h.core.l1.Get("t:acme:u:1"); ok {
		t.Fatal("u:1 L1 entry must be deleted")
	}
	if _, ok := h.core.l1.Get("t:acme:u:2"); !ok {
		t.Fatal("u:2 L1 entry must remain")
	}
}

// ---------- 世代号刷新摊销与失败退避 ----------

func TestGenTrackerRefreshAmortizedAndBackoff(t *testing.T) {
	Reset()
	defer Reset()
	origTTL, origBackoff := genRefreshTTL, genFailBackoff
	genRefreshTTL, genFailBackoff = 60*time.Millisecond, 300*time.Millisecond
	t.Cleanup(func() { genRefreshTTL, genFailBackoff = origTTL, origBackoff })

	cs := &countingStore{Store: l2.NewMemStore()}
	h := MustDefine[userProfile]("gt", "objs", Definition[userProfile]{
		KeyTemplate: "k:{k}",
		L2:          L2Options{Enabled: true, Store: cs},
	})
	ctx := context.Background()
	build := func() string {
		_, phys, err := h.core.buildKey(ctx, "k")
		if err != nil {
			t.Fatalf("buildKey: %v", err)
		}
		return phys
	}

	// 首次 GET；元 key 不存在（等价 gen 0）→ 缓存摊销
	build()
	if got := cs.gets.Load(); got != 1 {
		t.Fatalf("gets = %d, want 1", got)
	}
	build()
	build()
	if got := cs.gets.Load(); got != 1 {
		t.Fatalf("ErrNotFound must be amortized, gets = %d", got)
	}

	// TTL 过期后再 GET 一次
	time.Sleep(120 * time.Millisecond)
	build()
	if got := cs.gets.Load(); got != 2 {
		t.Fatalf("expired must refresh once, gets = %d", got)
	}

	// 故障：GET 失败 → 退避窗口内沿用旧世代号、不再打 Redis
	cs.fail.Store(true)
	time.Sleep(120 * time.Millisecond)
	build()
	if got := cs.gets.Load(); got != 3 {
		t.Fatalf("failing refresh attempted once, gets = %d", got)
	}
	build()
	build()
	if got := cs.gets.Load(); got != 3 {
		t.Fatalf("backoff must short-circuit, gets = %d", got)
	}

	// 退避窗口过后恢复重试
	time.Sleep(350 * time.Millisecond)
	build()
	if got := cs.gets.Load(); got != 4 {
		t.Fatalf("after backoff window must retry, gets = %d", got)
	}
	_ = build
}
