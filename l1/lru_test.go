package l1

import (
	"testing"
	"time"
)

func TestLRUBasics(t *testing.T) {
	c := NewLRU(100)
	if _, ok := c.Get("missing"); ok {
		t.Fatal("empty cache must miss")
	}
	c.Set("a", []byte("1"), time.Minute)
	c.Set("b", []byte("2"), time.Minute)
	if v, ok := c.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("Get a = %v,%v", v, ok)
	}
	if c.Size() != 2 {
		t.Fatalf("Size = %d", c.Size())
	}
	c.Delete("a")
	if _, ok := c.Get("a"); ok {
		t.Fatal("deleted key must miss")
	}
	// nil 值 = 负缓存条目
	c.Set("neg", nil, time.Minute)
	if v, ok := c.Get("neg"); !ok || v != nil {
		t.Fatalf("negative entry = %v,%v", v, ok)
	}
	c.Clear()
	if c.Size() != 0 {
		t.Fatalf("after Clear Size = %d", c.Size())
	}
}

func TestLRUTTLExpiry(t *testing.T) {
	c := NewLRU(100)
	c.Set("k", []byte("v"), 20*time.Millisecond)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("fresh entry must hit")
	}
	time.Sleep(40 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("expired entry must miss")
	}
	// ttl <= 0 = 不过期
	c.Set("perm", []byte("v"), 0)
	time.Sleep(10 * time.Millisecond)
	if _, ok := c.Get("perm"); !ok {
		t.Fatal("no-ttl entry must not expire")
	}
}

func TestLRUEviction(t *testing.T) {
	// 分片 LRU：容量按 shard 均分（每分片 ceil(max/16)，此处 max=64 → 每分片 4，总上界 64）
	c := NewLRU(64)
	const total = 100
	misses := 0
	for i := 0; i < total; i++ {
		c.Set(string(rune('a'+i)), []byte{byte(i)}, time.Minute)
	}
	if c.Size() > 64 {
		t.Fatalf("Size must be bounded by maxEntries, got %d", c.Size())
	}
	for i := 0; i < total; i++ {
		if _, ok := c.Get(string(rune('a'+i))); !ok {
			misses++
		}
	}
	if misses < total-64 {
		t.Fatalf("evicted %d, want at least %d", misses, total-64)
	}
	// 覆盖不改变容量上界
	for i := 0; i < total; i++ {
		c.Set(string(rune('a'+i)), []byte{byte(i)}, time.Minute)
	}
	if c.Size() > 64 {
		t.Fatalf("Size after overwrite = %d", c.Size())
	}
}

func TestLRUOverwrite(t *testing.T) {
	c := NewLRU(4)
	c.Set("k", []byte("v1"), time.Minute)
	c.Set("k", []byte("v2"), time.Minute)
	if v, ok := c.Get("k"); !ok || string(v) != "v2" {
		t.Fatalf("overwrite = %v,%v", v, ok)
	}
	if c.Size() != 1 {
		t.Fatalf("Size after overwrite = %d", c.Size())
	}
	// 覆盖为更短 TTL 后过期
	c.Set("k", []byte("v3"), 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("overwritten ttl must apply")
	}
}
