package gocache

import (
	"context"
	"errors"
	"testing"

	"github.com/jninng/go_biz_cache/bigvalue"
	"github.com/jninng/go_biz_cache/l2"
)

// ---------- Set ----------

func TestSetMembersAndRebuild(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	loads := 0
	h := MustDefineSet[string]("test", "tags", Definition[string]{
		KeyTemplate: "s:{k}",
		L1:          L1Options{Enabled: true},
		L2:          L2Options{Enabled: true, Store: store},
	}, SetOptions[string]{Loader: func(ctx context.Context, key string) ([]string, error) {
		loads++
		return []string{"go", "rust"}, nil
	}})
	ctx := context.Background()

	members, err := h.Members(ctx, "s:langs")
	if err != nil || len(members) != 2 {
		t.Fatalf("Members = %v,%v", members, err)
	}
	if loads != 1 {
		t.Fatalf("loads = %d", loads)
	}
	// L2 命中（回源整写过）
	members, err = h.Members(ctx, "s:langs")
	if err != nil || len(members) != 2 || loads != 1 {
		t.Fatalf("Members cached = %v,%v loads=%d", members, err, loads)
	}
	// Add 更新后可读
	if err := h.Add(ctx, "s:langs", "zig"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	members, err = h.Members(ctx, "s:langs")
	if err != nil || len(members) != 3 {
		t.Fatalf("Members after Add = %v,%v", members, err)
	}
	// IsMember
	ok, err := h.IsMember(ctx, "s:langs", "go")
	if err != nil || !ok {
		t.Fatalf("IsMember go = %v,%v", ok, err)
	}
	if ok, err = h.IsMember(ctx, "s:langs", "java"); err != nil || ok {
		t.Fatalf("IsMember java = %v,%v", ok, err)
	}
	// Remove
	if err := h.Remove(ctx, "s:langs", "rust"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	members, _ = h.Members(ctx, "s:langs")
	if len(members) != 2 {
		t.Fatalf("Members after Remove = %v", members)
	}
	// Delete → 回源重建（残留被清理）
	if err := h.Delete(ctx, "s:langs"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	members, err = h.Members(ctx, "s:langs")
	if err != nil || len(members) != 2 || loads != 2 {
		t.Fatalf("Members after Delete = %v,%v loads=%d", members, err, loads)
	}
}

func TestSetFieldShard(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	h := MustDefineSet[string]("test", "bigtags", Definition[string]{
		KeyTemplate: "s:{k}",
		L2:          L2Options{Enabled: true, Store: store},
		BigValue:    BigValuePolicy{Type: BigValueFieldShard, FieldShard: FieldShardOptions{ShardCount: 4}},
	}, SetOptions[string]{Loader: func(ctx context.Context, key string) ([]string, error) {
		return nil, errors.New("not loaded")
	}})
	ctx := context.Background()
	phys := "test:bigtags:v0:s:big"

	if err := h.Add(ctx, "s:big", "m0", "m1", "m2", "m3", "m4"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// 成员落到 :h{i} 子 key
	for _, m := range []string{"m0", "m1", "m2", "m3", "m4"} {
		idx := bigvalue.FieldShardIndex([]byte(`"`+m+`"`), 4, nil)
		ok, err := store.SIsMember(ctx, phys+":h"+string(rune('0'+idx)), []byte(`"`+m+`"`))
		if err != nil || !ok {
			t.Fatalf("member %s must land in :h%d (%v,%v)", m, idx, ok, err)
		}
	}
	// Members 合并全部子 key（不回源）
	members, err := h.Members(ctx, "s:big")
	if err != nil || len(members) != 5 {
		t.Fatalf("Members merged = %v,%v", members, err)
	}
	// IsMember 点查定位单子 key
	ok, err := h.IsMember(ctx, "s:big", "m2")
	if err != nil || !ok {
		t.Fatalf("IsMember = %v,%v", ok, err)
	}
}

// ---------- List ----------

func TestListOps(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	loads := 0
	h := MustDefineList[int]("test", "queue", Definition[int]{
		KeyTemplate: "l:{k}",
		L1:          L1Options{Enabled: true},
		L2:          L2Options{Enabled: true, Store: store},
	}, ListOptions[int]{Loader: func(ctx context.Context, key string) ([]int, error) {
		loads++
		return []int{10, 20, 30}, nil
	}})
	ctx := context.Background()

	// 空列表 Range → 回源重建
	items, err := h.Range(ctx, "l:jobs", 0, -1)
	if err != nil || len(items) != 3 || loads != 1 {
		t.Fatalf("Range rebuild = %v,%v loads=%d", items, err, loads)
	}
	// L2 命中读取（含负索引切片）
	items, err = h.Range(ctx, "l:jobs", -2, -1)
	if err != nil || len(items) != 2 || items[0] != 20 {
		t.Fatalf("Range -2,-1 = %v,%v", items, err)
	}
	// Len
	n, err := h.Len(ctx, "l:jobs")
	if err != nil || n != 3 {
		t.Fatalf("Len = %d,%v", n, err)
	}
	// LPush / RPush
	if err := h.LPush(ctx, "l:jobs", 5); err != nil {
		t.Fatalf("LPush: %v", err)
	}
	if err := h.RPush(ctx, "l:jobs", 99); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	items, err = h.Range(ctx, "l:jobs", 0, -1)
	if err != nil || len(items) != 5 || items[0] != 5 || items[4] != 99 {
		t.Fatalf("Range after push = %v,%v", items, err)
	}
	// Trim（LRANGE 闭区间语义：[5,10,20,30,99] 保留 1..3 → [10,20,30]）
	if err := h.Trim(ctx, "l:jobs", 1, 3); err != nil {
		t.Fatalf("Trim: %v", err)
	}
	items, _ = h.Range(ctx, "l:jobs", 0, -1)
	if len(items) != 3 || items[0] != 10 || items[2] != 30 {
		t.Fatalf("Range after trim = %v", items)
	}
	// Delete → 重建
	if err := h.Delete(ctx, "l:jobs"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	items, err = h.Range(ctx, "l:jobs", 0, -1)
	if err != nil || len(items) != 3 || loads != 2 {
		t.Fatalf("Range after delete = %v,%v loads=%d", items, err, loads)
	}
}

// ---------- ZSet ----------

func TestZSetOps(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	loads := 0
	h := MustDefineZSet[string]("test", "rank", Definition[string]{
		KeyTemplate: "z:{k}",
		L1:          L1Options{Enabled: true},
		L2:          L2Options{Enabled: true, Store: store},
	}, ZSetOptions[string]{Loader: func(ctx context.Context, key string) ([]Scored[string], error) {
		loads++
		return []Scored[string]{
			{Value: "alice", Score: 90},
			{Value: "bob", Score: 80},
			{Value: "carol", Score: 70},
		}, nil
	}})
	ctx := context.Background()

	// 空集合 Range → 回源重建
	items, err := h.Range(ctx, "z:board", 0, -1)
	if err != nil || len(items) != 3 || loads != 1 {
		t.Fatalf("Range rebuild = %v,%v loads=%d", items, err, loads)
	}
	if items[0].Value != "carol" || items[2].Value != "alice" {
		t.Fatalf("Range order = %v", items)
	}
	// Add 更新
	if err := h.Add(ctx, "z:board", Scored[string]{Value: "dave", Score: 85}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// RangeByScore：开区间 + 闭区间
	items, err = h.RangeByScore(ctx, "z:board", ZRangeBy{Min: "(80", Max: "90"})
	if err != nil || len(items) != 2 {
		t.Fatalf("RangeByScore = %v,%v", items, err)
	}
	if items[0].Value != "dave" || items[1].Value != "alice" {
		t.Fatalf("RangeByScore order = %v", items)
	}
	// offset/count
	items, err = h.RangeByScore(ctx, "z:board", ZRangeBy{Min: "-inf", Max: "+inf", Offset: 1, Count: 2})
	if err != nil || len(items) != 2 || items[0].Value != "bob" {
		t.Fatalf("RangeByScore offset = %v,%v", items, err)
	}
	// 空窗口 + key 存在 → 合法空查询，不回源
	items, err = h.RangeByScore(ctx, "z:board", ZRangeBy{Min: "(100000", Max: "+inf"})
	if err != nil || len(items) != 0 || loads != 1 {
		t.Fatalf("RangeByScore empty window = %v,%v loads=%d", items, err, loads)
	}
	// Score 点查
	score, err := h.Score(ctx, "z:board", "bob")
	if err != nil || score != 80 {
		t.Fatalf("Score = %v,%v", score, err)
	}
	if _, err = h.Score(ctx, "z:board", "nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Score missing = %v", err)
	}
	// Remove
	if err := h.Remove(ctx, "z:board", "bob"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	score, err = h.Score(ctx, "z:board", "bob")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Score after Remove = %v,%v", score, err)
	}
	// Delete → 重建
	if err := h.Delete(ctx, "z:board"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	items, err = h.Range(ctx, "z:board", 0, -1)
	if err != nil || len(items) != 3 || loads != 2 {
		t.Fatalf("Range after delete = %v,%v loads=%d", items, err, loads)
	}
}

func TestZSetRangeByScoreRebuildOnUncached(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	loads := 0
	h := MustDefineZSet[int]("test", "zr", Definition[int]{
		KeyTemplate: "z:{k}",
		L1:          L1Options{Enabled: false},
		L2:          L2Options{Enabled: true, Store: store},
	}, ZSetOptions[int]{Loader: func(ctx context.Context, key string) ([]Scored[int], error) {
		loads++
		if key == "z:none" {
			return nil, nil // 空集合
		}
		return []Scored[int]{{Value: 1, Score: 1}, {Value: 2, Score: 2}}, nil
	}})
	// 未缓存 key：RangeByScore 先判空（ZCard==0）→ 重建 → 过滤返回
	items, err := h.RangeByScore(t.Context(), "z:new", ZRangeBy{Min: "(1", Max: "+inf"})
	if err != nil || len(items) != 1 || items[0].Value != 2 || loads != 1 {
		t.Fatalf("RangeByScore rebuild = %v,%v loads=%d", items, err, loads)
	}
	// 空集合 key：重建返回空
	items, err = h.RangeByScore(t.Context(), "z:none", ZRangeBy{Min: "-inf", Max: "+inf"})
	if err != nil || len(items) != 0 || loads != 2 {
		t.Fatalf("RangeByScore empty rebuild = %v,%v loads=%d", items, err, loads)
	}
}

// ---------- 集合句柄无 Loader ----------

func TestCollectionNoLoader(t *testing.T) {
	Reset()
	defer Reset()
	h := MustDefineSet[string]("test", "nold", Definition[string]{
		KeyTemplate: "s:{k}",
		L2:          L2Options{Enabled: true, Store: l2.NewMemStore()},
	}, SetOptions[string]{})
	if _, err := h.Members(t.Context(), "s:x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Members without loader = %v", err)
	}
	lh := MustDefineList[string]("test", "noldl", Definition[string]{
		KeyTemplate: "l:{k}",
		L2:          L2Options{Enabled: true, Store: l2.NewMemStore()},
	}, ListOptions[string]{})
	if _, err := lh.Range(t.Context(), "l:x", 0, -1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Range without loader = %v", err)
	}
	zh := MustDefineZSet[string]("test", "noldz", Definition[string]{
		KeyTemplate: "z:{k}",
		L2:          L2Options{Enabled: true, Store: l2.NewMemStore()},
	}, ZSetOptions[string]{})
	if _, err := zh.Range(t.Context(), "z:x", 0, -1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("zset Range without loader = %v", err)
	}
}
