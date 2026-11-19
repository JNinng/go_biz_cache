package l1

import (
	"sync"
	"time"
)

const lruShards = 16

const sweepInterval = 1024 // 每 shard 每隔 1024 次 Set 触发一次过期清扫

func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

type lruEntry struct {
	key        string
	value      []byte
	expireAt   int64 // unix nano，0 = 不过期
	prev, next *lruEntry
}

type lruShard struct {
	mu    sync.Mutex
	items map[string]*lruEntry
	head  *lruEntry // 伪头部
	tail  *lruEntry // 伪尾部
	max   int
	sets  uint32
}

// ShardedLRU 默认进程内分片 LRU，实现 L1Cache。
// 并发安全；过期在读时惰性淘汰 + 写时低频清扫。
type ShardedLRU struct {
	shards [lruShards]*lruShard
}

// NewLRU 创建默认 L1 实现；maxEntries 为总容量（条目数），<= 0 时取 4096。
func NewLRU(maxEntries int) *ShardedLRU {
	if maxEntries <= 0 {
		maxEntries = 4096
	}
	s := &ShardedLRU{}
	per := (maxEntries + lruShards - 1) / lruShards
	for i := range s.shards {
		sh := &lruShard{items: make(map[string]*lruEntry, per+1), max: per}
		head, tail := &lruEntry{}, &lruEntry{}
		head.next, tail.prev = tail, head
		sh.head, sh.tail = head, tail
		s.shards[i] = sh
	}
	return s
}

func (s *ShardedLRU) shardOf(key string) *lruShard { return s.shards[fnv32(key)%lruShards] }

func expireAtOf(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return time.Now().Add(ttl).UnixNano()
}

func (sh *lruShard) expired(e *lruEntry, now int64) bool {
	return e.expireAt > 0 && now >= e.expireAt
}

func (sh *lruShard) unlink(e *lruEntry) {
	e.prev.next, e.next.prev = e.next, e.prev
	e.prev, e.next = nil, nil
}

func (sh *lruShard) pushFront(e *lruEntry) {
	e.prev, e.next = sh.head, sh.head.next
	sh.head.next.prev = e
	sh.head.next = e
}

func (sh *lruShard) moveToFront(e *lruEntry) {
	sh.unlink(e)
	sh.pushFront(e)
}

func (sh *lruShard) evictOldest() {
	e := sh.tail.prev
	if e == sh.head {
		return
	}
	sh.unlink(e)
	delete(sh.items, e.key)
}

func (sh *lruShard) sweep(now int64) {
	for _, e := range sh.items {
		if sh.expired(e, now) {
			sh.unlink(e)
			delete(sh.items, e.key)
		}
	}
}

func (s *ShardedLRU) Get(key string) ([]byte, bool) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := sh.items[key]
	if !ok {
		return nil, false
	}
	now := time.Now().UnixNano()
	if sh.expired(e, now) {
		sh.unlink(e)
		delete(sh.items, key)
		return nil, false
	}
	sh.moveToFront(e)
	return e.value, true
}

func (s *ShardedLRU) Set(key string, value []byte, ttl time.Duration) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	// 防御拷贝：调用方可能复用缓冲区，存储后内部状态与外部解耦
	if len(value) > 0 {
		cp := make([]byte, len(value))
		copy(cp, value)
		value = cp
	}
	sh.sets++
	if sh.sets%sweepInterval == 0 {
		sh.sweep(time.Now().UnixNano())
	}
	if e, ok := sh.items[key]; ok {
		e.value = value
		e.expireAt = expireAtOf(ttl)
		sh.moveToFront(e)
		return
	}
	if len(sh.items) >= sh.max {
		sh.evictOldest()
	}
	e := &lruEntry{key: key, value: value, expireAt: expireAtOf(ttl)}
	sh.items[key] = e
	sh.pushFront(e)
}

func (s *ShardedLRU) Delete(keys ...string) {
	for _, key := range keys {
		sh := s.shardOf(key)
		sh.mu.Lock()
		if e, ok := sh.items[key]; ok {
			sh.unlink(e)
			delete(sh.items, key)
		}
		sh.mu.Unlock()
	}
}

func (s *ShardedLRU) Clear() {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.items = make(map[string]*lruEntry)
		head, tail := sh.head, sh.tail
		head.next, tail.prev = tail, head
		tail.prev, head.next = head, tail
		sh.mu.Unlock()
	}
}

func (s *ShardedLRU) Size() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		n += len(sh.items)
		sh.mu.Unlock()
	}
	return n
}
