package gocache

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/jninng/go_biz_cache/bigvalue"
	"github.com/jninng/go_biz_cache/l2"
)

type hashProfile struct {
	Name  string `json:"name"`
	Level int    `json:"level"`
}

func newTestHash(store l2.Store, loader HashLoader[string, string, hashProfile], mutate func(*Definition[hashProfile], *HashOptions[string, hashProfile])) *TypedHashCache[string, string, hashProfile] {
	def := Definition[hashProfile]{
		KeyTemplate:   "{k}",
		TTL:           30 * time.Minute,
		NegativeCache: true,
		L1:          L1Options{Enabled: true, Size: 1024},
		L2:          L2Options{Enabled: true, Store: store},
	}
	opts := HashOptions[string, hashProfile]{HashLoader: loader}
	if mutate != nil {
		mutate(&def, &opts)
	}
	return MustDefineHash[string, hashProfile]("test", "hobjs", def, opts)
}

func TestHashHSetHGet(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	h := newTestHash(store, nil, nil)
	ctx := context.Background()

	if err := h.HSet(ctx, "a", "nickname", hashProfile{Name: "nick", Level: 2}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	got, err := h.HGet(ctx, "a", "nickname")
	if err != nil || got.Name != "nick" {
		t.Fatalf("HGet = %v,%v", got, err)
	}
	// L2 中真实存在（redis hash 语义）
	data, err := store.HGet(ctx, "test:hobjs:v0:a", "nickname")
	if err != nil {
		t.Fatalf("store HGet: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("field value empty")
	}
	// L2 命中读路径：清 L1 强制走 HGET → 解压 → 反序列化
	h.core.l1.Clear()
	if got, err = h.HGet(ctx, "a", "nickname"); err != nil || got.Name != "nick" {
		t.Fatalf("HGet via L2 = %v,%v", got, err)
	}
	// L1 复合键命中（清 L2 后仍可读）
	if err := store.Del(ctx, "test:hobjs:v0:a"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if got, err = h.HGet(ctx, "a", "nickname"); err != nil || got.Name != "nick" {
		t.Fatalf("HGet via L1 = %v,%v", got, err)
	}
}

func TestHashFieldLevelLoadAndNegative(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	loads := 0
	var lastFields []string
	h := newTestHash(store, func(ctx context.Context, key string, fields []string) (map[string]hashProfile, error) {
		loads++
		lastFields = fields
		m := map[string]hashProfile{}
		for _, f := range fields {
			if f == "ghost" {
				continue // 缺失 → 负缓存
			}
			m[f] = hashProfile{Name: "v-" + f, Level: 1}
		}
		return m, nil
	}, nil)
	ctx := context.Background()

	got, err := h.HGet(ctx, "a", "nickname")
	if err != nil || got.Name != "v-nickname" {
		t.Fatalf("HGet = %v,%v", got, err)
	}
	if loads != 1 || len(lastFields) != 1 || lastFields[0] != "nickname" {
		t.Fatalf("loads=%d fields=%v", loads, lastFields)
	}
	// 二次读命中缓存
	if _, err = h.HGet(ctx, "a", "nickname"); err != nil || loads != 1 {
		t.Fatalf("cached HGet loads=%d err=%v", loads, err)
	}
	// 缺失 field → 负缓存
	if _, err = h.HGet(ctx, "a", "ghost"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ghost = %v", err)
	}
	if _, err = h.HGet(ctx, "a", "ghost"); !errors.Is(err, ErrNotFound) || loads != 2 {
		t.Fatalf("ghost cached loads=%d err=%v", loads, err)
	}
	// L2 负标记为独立短 TTL string key
	if _, err := store.Get(ctx, "test:hobjs:v0:a:__neg:"+strconv.FormatUint(bigvalue.FNV1a([]byte("ghost")), 16)); err != nil {
		t.Fatalf("field neg marker missing: %v", err)
	}
	// 真实 hash 结构未被污染
	if _, err := store.HGet(ctx, "test:hobjs:v0:a", "ghost"); !errors.Is(err, l2.ErrNotFound) {
		t.Fatal("neg marker must not pollute the hash")
	}
}

func TestHashHMGetPartial(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	loads := 0
	var lastFields []string
	h := newTestHash(store, func(ctx context.Context, key string, fields []string) (map[string]hashProfile, error) {
		loads++
		lastFields = fields
		m := map[string]hashProfile{}
		for _, f := range fields {
			m[f] = hashProfile{Name: "v-" + f}
		}
		return m, nil
	}, nil)
	ctx := context.Background()

	// f1 预写 L2
	if err := h.HSet(ctx, "a", "f1", hashProfile{Name: "one"}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	got, err := h.HMGet(ctx, "a", "f1", "f2", "f3", "f1")
	if err != nil {
		t.Fatalf("HMGet: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("HMGet = %v", got)
	}
	if got["f1"].Name != "one" || got["f2"].Name != "v-f2" {
		t.Fatalf("HMGet values = %v", got)
	}
	// 缺失的 fields 一次性批量回源
	if loads != 1 || len(lastFields) != 2 {
		t.Fatalf("loads=%d fields=%v", loads, lastFields)
	}
}

func TestHashHGetAll(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	fullLoads := 0
	h := newTestHash(store, func(ctx context.Context, key string, fields []string) (map[string]hashProfile, error) {
		if fields == nil { // 全量回源
			fullLoads++
			if key != "b" {
				return map[string]hashProfile{}, nil
			}
			return map[string]hashProfile{
				"a": {Name: "va"},
				"b": {Name: "vb"},
			}, nil
		}
		return map[string]hashProfile{}, nil
	}, nil)
	ctx := context.Background()

	// L2 存在 → 直接返回
	if err := h.HSet(ctx, "a", "x", hashProfile{Name: "x"}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	m, err := h.HGetAll(ctx, "a")
	if err != nil || len(m) != 1 || m["x"].Name != "x" {
		t.Fatalf("HGetAll = %v,%v", m, err)
	}
	if fullLoads != 0 {
		t.Fatal("existing hash must not reload")
	}
	// L2 不存在 → 全量回源整写
	m, err = h.HGetAll(ctx, "b")
	if err != nil || len(m) != 2 {
		t.Fatalf("HGetAll rebuild = %v,%v", m, err)
	}
	if fullLoads != 1 {
		t.Fatalf("full loads = %d", fullLoads)
	}
	// 整写后再次读不再回源
	m, err = h.HGetAll(ctx, "b")
	if err != nil || len(m) != 2 || fullLoads != 1 {
		t.Fatalf("HGetAll after rebuild = %v,%v loads=%d", m, err, fullLoads)
	}
	// 空 hash → 空 map（全量语义，不存在即不存在）
	m, err = h.HGetAll(ctx, "empty")
	if err != nil || len(m) != 0 {
		t.Fatalf("HGetAll empty = %v,%v", m, err)
	}
}

func TestHashHDelAndDelete(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	h := newTestHash(store, nil, nil)
	ctx := context.Background()
	for _, f := range []string{"a", "b"} {
		if err := h.HSet(ctx, "k", f, hashProfile{Name: f}); err != nil {
			t.Fatalf("HSet: %v", err)
		}
	}
	if err := h.HDel(ctx, "k", "a"); err != nil {
		t.Fatalf("HDel: %v", err)
	}
	if _, err := store.HGet(ctx, "test:hobjs:v0:k", "a"); !errors.Is(err, l2.ErrNotFound) {
		t.Fatal("field a must be deleted")
	}
	if _, err := store.HGet(ctx, "test:hobjs:v0:k", "b"); err != nil {
		t.Fatal("field b must remain")
	}
	if err := h.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.HGet(ctx, "test:hobjs:v0:k", "b"); !errors.Is(err, l2.ErrNotFound) {
		t.Fatal("whole hash must be deleted")
	}
}

func TestHashFieldShard(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	h := newTestHash(store, nil, func(d *Definition[hashProfile], o *HashOptions[string, hashProfile]) {
		d.BigValue = BigValuePolicy{Type: BigValueFieldShard, FieldShard: FieldShardOptions{ShardCount: 4}}
	})
	ctx := context.Background()
	phys := "test:hobjs:v0:big"

	// 每个字段落到自己的子 key
	for _, f := range []string{"f0", "f1", "f2", "f3", "f4"} {
		if err := h.HSet(ctx, "big", f, hashProfile{Name: f}); err != nil {
			t.Fatalf("HSet %s: %v", f, err)
		}
		idx := bigvalue.FieldShardIndex([]byte(f), 4, nil)
		data, err := store.HGet(ctx, phys+":h"+strconv.Itoa(idx), f)
		if err != nil || len(data) == 0 {
			t.Fatalf("field %s must land in subkey :h%d: %v", f, idx, err)
		}
	}
	// HGetAll 合并全部子 key
	m, err := h.HGetAll(ctx, "big")
	if err != nil || len(m) != 5 {
		t.Fatalf("HGetAll merged = %v,%v", m, err)
	}
	// Delete 清理全部子 key
	if err := h.Delete(ctx, "big"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for i := 0; i < 4; i++ {
		m, err := store.HGetAll(ctx, phys+":h"+strconv.Itoa(i))
		if err != nil || len(m) != 0 {
			t.Fatalf("subkey :h%d not cleared: %v,%v", i, m, err)
		}
	}
}

func TestHashFieldCodec(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	// int field + 转换函数 codec
	h := MustDefineHash[int, hashProfile]("test", "hint", Definition[hashProfile]{
		KeyTemplate: "{k}",
		L2:          L2Options{Enabled: true, Store: store},
	}, HashOptions[int, hashProfile]{
		FieldCodec: FieldCodecFunc[int](
			func(i int) (string, error) { return strconv.Itoa(i * 2), nil },
			func(s string) (int, error) { return strconv.Atoi(s) },
		),
	})
	ctx := context.Background()
	if err := h.HSet(ctx, "a", 21, hashProfile{Name: "answer"}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	// 编码后的物理 field 名为 42
	if _, err := store.HGet(ctx, "test:hint:v0:a", "42"); err != nil {
		t.Fatalf("encoded field missing: %v", err)
	}
	got, err := h.HGet(ctx, "a", 21)
	if err != nil || got.Name != "answer" {
		t.Fatalf("HGet = %v,%v", got, err)
	}
	// F 非 string 且无 codec → 注册报错
	if _, err := DefineHash[int, hashProfile]("test", "hnocodec", Definition[hashProfile]{
		KeyTemplate: "{k}",
	}, HashOptions[int, hashProfile]{}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("missing codec = %v", err)
	}
}

// RefreshTTLOnWrite 接线：句柄必须把定义开关透传给 Store.HMSet 的 refresh 实参。
func TestHashHMSetTTLRefresh(t *testing.T) {
	Reset()
	defer Reset()
	store := newRecStore()
	h := newTestHash(store, nil, func(d *Definition[hashProfile], o *HashOptions[string, hashProfile]) {
		d.TTL = 10 * time.Minute
		o.RefreshTTLOnWrite = false
	})
	ctx := context.Background()
	if err := h.HSet(ctx, "a", "f", hashProfile{Name: "x"}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if flags := store.recordedRefreshFlags(); len(flags) != 1 || flags[0] {
		t.Fatalf("RefreshTTLOnWrite=false must pass refresh=false to HMSet, flags=%v", flags)
	}

	Reset()
	store2 := newRecStore()
	h2 := newTestHash(store2, nil, func(d *Definition[hashProfile], o *HashOptions[string, hashProfile]) {
		d.TTL = 10 * time.Minute
		o.RefreshTTLOnWrite = true
	})
	if err := h2.HSet(ctx, "a", "f", hashProfile{Name: "x"}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if flags := store2.recordedRefreshFlags(); len(flags) != 1 || flags[0] != true {
		t.Fatalf("RefreshTTLOnWrite=true must pass refresh=true to HMSet, flags=%v", flags)
	}
}
