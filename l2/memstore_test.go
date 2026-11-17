package l2

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemStoreStrings(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	if _, err := s.Get(ctx, "miss"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get miss = %v", err)
	}
	if err := s.MSet(ctx, time.Minute, map[string][]byte{"a": []byte("1"), "b": []byte("2")}); err != nil {
		t.Fatalf("MSet: %v", err)
	}
	if v, _ := s.Get(ctx, "a"); string(v) != "1" {
		t.Fatalf("Get a = %q", v)
	}
	got, err := s.MGet(ctx, "a", "b", "miss")
	if err != nil || len(got) != 2 {
		t.Fatalf("MGet = %v,%v", got, err)
	}
	// Incr 保留 TTL
	n, err := s.Incr(ctx, "ctr")
	if err != nil || n != 1 {
		t.Fatalf("Incr = %d,%v", n, err)
	}
	n, _ = s.Incr(ctx, "ctr")
	if n != 2 {
		t.Fatalf("Incr again = %d", n)
	}
	// Del 跨类型删除
	if err := s.Del(ctx, "a"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if _, err := s.Get(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after Del = %v", err)
	}
}

func TestMemStoreTTL(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	s.MSet(ctx, 20*time.Millisecond, map[string][]byte{"k": []byte("v")})
	if _, err := s.Get(ctx, "k"); err != nil {
		t.Fatalf("fresh: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := s.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired = %v", err)
	}
}

func TestMemStoreHash(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	if _, err := s.HGet(ctx, "h", "f"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("HGet miss = %v", err)
	}
	if err := s.HMSet(ctx, "h", map[string][]byte{"f1": []byte("1"), "f2": []byte("2")}, time.Minute, false); err != nil {
		t.Fatalf("HMSet: %v", err)
	}
	// ExpireNX 语义：refresh=false 且已有 TTL → 不续期
	if err := s.HMSet(ctx, "h", map[string][]byte{"f3": []byte("3")}, time.Minute, false); err != nil {
		t.Fatalf("HMSet: %v", err)
	}
	m, err := s.HMGet(ctx, "h", "f1", "f2", "f3", "miss")
	if err != nil || len(m) != 3 {
		t.Fatalf("HMGet = %v,%v", m, err)
	}
	all, err := s.HGetAll(ctx, "h")
	if err != nil || len(all) != 3 {
		t.Fatalf("HGetAll = %v,%v", all, err)
	}
	if err := s.HDel(ctx, "h", "f1", "f2", "f3"); err != nil {
		t.Fatalf("HDel: %v", err)
	}
	all, _ = s.HGetAll(ctx, "h")
	if len(all) != 0 {
		t.Fatalf("after HDel = %v", all)
	}
}

// ExpireNX 语义：refresh=false 时已有 TTL 的 key 不被续期（首个短 TTL 到期后整 hash 消失）。
func TestMemStoreHMSetExpireNX(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	s.HMSet(ctx, "h", map[string][]byte{"f1": []byte("1")}, 60*time.Millisecond, false)
	s.HMSet(ctx, "h", map[string][]byte{"f2": []byte("2")}, 10*time.Minute, false)
	// 若第二次 HMSet 错误续期，key 将存活 10 分钟而非 60ms
	time.Sleep(150 * time.Millisecond)
	all, err := s.HGetAll(ctx, "h")
	if err != nil || len(all) != 0 {
		t.Fatalf("ExpireNX violated: hash still alive after first TTL: %v,%v", all, err)
	}
	// refresh=true 语义：续期生效
	s.HMSet(ctx, "r", map[string][]byte{"f": []byte("1")}, 60*time.Millisecond, false)
	s.HMSet(ctx, "r", map[string][]byte{"g": []byte("2")}, 10*time.Minute, true)
	time.Sleep(150 * time.Millisecond)
	all, _ = s.HGetAll(ctx, "r")
	if len(all) != 2 {
		t.Fatalf("refresh=true must renew TTL, got %v", all)
	}
}

func TestMemStoreSet(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	if err := s.SAdd(ctx, "s", time.Minute, []byte("a"), []byte("b")); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	ok, err := s.SIsMember(ctx, "s", []byte("a"))
	if err != nil || !ok {
		t.Fatalf("SIsMember = %v,%v", ok, err)
	}
	if ok, _ = s.SIsMember(ctx, "s", []byte("c")); ok {
		t.Fatal("c must not be member")
	}
	mem, err := s.SMembers(ctx, "s")
	if err != nil || len(mem) != 2 {
		t.Fatalf("SMembers = %v,%v", mem, err)
	}
	if err := s.SRem(ctx, "s", []byte("a")); err != nil {
		t.Fatalf("SRem: %v", err)
	}
	mem, _ = s.SMembers(ctx, "s")
	if len(mem) != 1 {
		t.Fatalf("after SRem = %v", mem)
	}
}

func TestMemStoreList(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	s.RPush(ctx, "l", time.Minute, []byte("1"), []byte("2"), []byte("3"))
	s.LPush(ctx, "l", time.Minute, []byte("0"))
	// LPush 后：[0 1 2 3]
	items, err := s.LRange(ctx, "l", 0, -1)
	if err != nil || len(items) != 4 {
		t.Fatalf("LRange = %v,%v", items, err)
	}
	// 负索引
	items, _ = s.LRange(ctx, "l", -2, -1)
	if len(items) != 2 || string(items[0]) != "2" {
		t.Fatalf("LRange -2,-1 = %v", items)
	}
	// 越界收敛
	items, _ = s.LRange(ctx, "l", 2, 100)
	if len(items) != 2 {
		t.Fatalf("LRange overflow = %v", items)
	}
	if items, _ = s.LRange(ctx, "l", 5, 10); len(items) != 0 {
		t.Fatalf("LRange out-of-range = %v", items)
	}
	n, _ := s.LLen(ctx, "l")
	if n != 4 {
		t.Fatalf("LLen = %d", n)
	}
	if err := s.LTrim(ctx, "l", 1, 2); err != nil {
		t.Fatalf("LTrim: %v", err)
	}
	items, _ = s.LRange(ctx, "l", 0, -1)
	if len(items) != 2 || string(items[0]) != "1" {
		t.Fatalf("after LTrim = %v", items)
	}
	// LTrim 全部裁掉 → 空
	if err := s.LTrim(ctx, "l", 5, 9); err != nil {
		t.Fatalf("LTrim: %v", err)
	}
	n, _ = s.LLen(ctx, "l")
	if n != 0 {
		t.Fatalf("after full trim LLen = %d", n)
	}
}

func TestMemStoreZSet(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	zs := []Z{{Score: 3, Member: []byte("c")}, {Score: 1, Member: []byte("a")}, {Score: 2, Member: []byte("b")}, {Score: 2, Member: []byte("b2")}}
	if err := s.ZAdd(ctx, "z", time.Minute, zs...); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	// score 升序，同分按成员字典序
	items, err := s.ZRangeWithScores(ctx, "z", 0, -1)
	if err != nil || len(items) != 4 {
		t.Fatalf("ZRange = %v,%v", items, err)
	}
	wantOrder := []string{"a", "b", "b2", "c"}
	for i, w := range wantOrder {
		if string(items[i].Member) != w {
			t.Fatalf("order[%d] = %q, want %q", i, items[i].Member, w)
		}
	}
	score, err := s.ZScore(ctx, "z", []byte("b"))
	if err != nil || score != 2 {
		t.Fatalf("ZScore = %v,%v", score, err)
	}
	if _, err = s.ZScore(ctx, "z", []byte("none")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ZScore miss = %v", err)
	}
	card, _ := s.ZCard(ctx, "z")
	if card != 4 {
		t.Fatalf("ZCard = %d", card)
	}
	// ZRangeByScore：开闭区间 + offset/count
	items, err = s.ZRangeByScoreWithScores(ctx, "z", ZRangeBy{Min: "(1", Max: "3"})
	if err != nil || len(items) != 3 {
		t.Fatalf("ZRangeByScore = %v,%v", items, err)
	}
	items, err = s.ZRangeByScoreWithScores(ctx, "z", ZRangeBy{Min: "-inf", Max: "+inf", Offset: 1, Count: 2})
	if err != nil || len(items) != 2 || string(items[0].Member) != "b" {
		t.Fatalf("ZRangeByScore offset = %v,%v", items, err)
	}
	// 非法边界
	if _, err = s.ZRangeByScoreWithScores(ctx, "z", ZRangeBy{Min: "abc", Max: "+inf"}); err == nil {
		t.Fatal("bad bound must error")
	}
	// ZRem 清空后 key 消失
	if err := s.ZRem(ctx, "z", []byte("a"), []byte("b"), []byte("b2"), []byte("c")); err != nil {
		t.Fatalf("ZRem: %v", err)
	}
	card, _ = s.ZCard(ctx, "z")
	if card != 0 {
		t.Fatalf("ZCard after ZRem = %d", card)
	}
}

func TestMemStoreIncrPreservesTTL(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	s.MSet(ctx, time.Hour, map[string][]byte{"c": []byte("5")})
	n, err := s.Incr(ctx, "c")
	if err != nil || n != 6 {
		t.Fatalf("Incr = %d,%v", n, err)
	}
	// TTL 仍在（未过期）
	if _, err = s.Get(ctx, "c"); err != nil {
		t.Fatalf("ttl lost: %v", err)
	}
}
