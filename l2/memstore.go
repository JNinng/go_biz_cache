package l2

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"
)

// MemStore 测试用内存 Store 实现（单机语义，TTL 惰性过期）。
type MemStore struct {
	mu      sync.Mutex
	strings map[string]memVal
	hashes  map[string]*memHash
	sets    map[string]*memSet
	lists   map[string]*memList
	zsets   map[string]*memZSet
}

// NewMemStore 创建内存 Store。
func NewMemStore() *MemStore {
	return &MemStore{
		strings: make(map[string]memVal),
		hashes:  make(map[string]*memHash),
		sets:    make(map[string]*memSet),
		lists:   make(map[string]*memList),
		zsets:   make(map[string]*memZSet),
	}
}

type memVal struct {
	data     []byte
	expireAt int64
}

type memHash struct {
	fields   map[string][]byte
	expireAt int64
}

type memSet struct {
	members  map[string]struct{}
	expireAt int64
}

type memList struct {
	items    [][]byte
	expireAt int64
}

type memZSet struct {
	members  map[string]float64
	expireAt int64
}

func ttlAt(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return time.Now().Add(ttl).UnixNano()
}

func alive(expireAt int64) bool {
	return expireAt == 0 || time.Now().UnixNano() < expireAt
}

func (m *MemStore) getStr(key string) (memVal, bool) {
	v, ok := m.strings[key]
	if ok && !alive(v.expireAt) {
		delete(m.strings, key)
		return memVal{}, false
	}
	return v, ok
}

func (m *MemStore) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.getStr(key)
	if !ok {
		return nil, ErrNotFound
	}
	return v.data, nil
}

func (m *MemStore) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		if v, ok := m.getStr(k); ok {
			out[k] = v.data
		}
	}
	return out, nil
}

func (m *MemStore) MSet(ctx context.Context, ttl time.Duration, items map[string][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	at := ttlAt(ttl)
	for k, v := range items {
		cp := make([]byte, len(v))
		copy(cp, v)
		m.strings[k] = memVal{data: cp, expireAt: at}
	}
	return nil
}

func (m *MemStore) Del(ctx context.Context, keys ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.strings, k)
		delete(m.hashes, k)
		delete(m.sets, k)
		delete(m.lists, k)
		delete(m.zsets, k)
	}
	return nil
}

func (m *MemStore) Incr(ctx context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, expire := int64(0), int64(0)
	if v, ok := m.getStr(key); ok {
		expire = v.expireAt
		cur, _ = strconv.ParseInt(string(v.data), 10, 64)
	}
	cur++
	m.strings[key] = memVal{data: []byte(strconv.FormatInt(cur, 10)), expireAt: expire}
	return cur, nil
}

func (m *MemStore) HGet(ctx context.Context, key, field string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hashes[key]
	if ok && !alive(h.expireAt) {
		delete(m.hashes, key)
		ok = false
	}
	if !ok {
		return nil, ErrNotFound
	}
	v, found := h.fields[field]
	if !found {
		return nil, ErrNotFound
	}
	return v, nil
}

func (m *MemStore) HMGet(ctx context.Context, key string, fields ...string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(fields))
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hashes[key]
	if ok && !alive(h.expireAt) {
		delete(m.hashes, key)
		ok = false
	}
	if !ok {
		return out, nil
	}
	for _, f := range fields {
		if v, found := h.fields[f]; found {
			out[f] = v
		}
	}
	return out, nil
}

func (m *MemStore) HGetAll(ctx context.Context, key string) (map[string][]byte, error) {
	out := make(map[string][]byte)
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hashes[key]
	if ok && !alive(h.expireAt) {
		delete(m.hashes, key)
		ok = false
	}
	if !ok {
		return out, nil
	}
	for f, v := range h.fields {
		out[f] = v
	}
	return out, nil
}

func (m *MemStore) HMSet(ctx context.Context, key string, fields map[string][]byte, ttl time.Duration, refresh bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hashes[key]
	if !ok || !alive(h.expireAt) {
		h = &memHash{fields: make(map[string][]byte)}
		m.hashes[key] = h
	}
	for f, v := range fields {
		cp := make([]byte, len(v))
		copy(cp, v)
		h.fields[f] = cp
	}
	if ttl > 0 && (refresh || h.expireAt == 0) {
		h.expireAt = ttlAt(ttl)
	}
	return nil
}

func (m *MemStore) HDel(ctx context.Context, key string, fields ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hashes[key]
	if !ok || !alive(h.expireAt) {
		return nil
	}
	for _, f := range fields {
		delete(h.fields, f)
	}
	if len(h.fields) == 0 {
		delete(m.hashes, key)
	}
	return nil
}

func (m *MemStore) SMembers(ctx context.Context, key string) ([][]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sets[key]
	if ok && !alive(s.expireAt) {
		delete(m.sets, key)
		ok = false
	}
	if !ok {
		return nil, nil
	}
	names := make([]string, 0, len(s.members))
	for name := range s.members {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([][]byte, len(names))
	for i, n := range names {
		out[i] = []byte(n)
	}
	return out, nil
}

func (m *MemStore) SAdd(ctx context.Context, key string, ttl time.Duration, members ...[]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sets[key]
	if !ok || !alive(s.expireAt) {
		s = &memSet{members: make(map[string]struct{})}
		m.sets[key] = s
	}
	for _, mem := range members {
		s.members[string(mem)] = struct{}{}
	}
	if ttl > 0 && s.expireAt == 0 {
		s.expireAt = ttlAt(ttl)
	}
	return nil
}

func (m *MemStore) SIsMember(ctx context.Context, key string, member []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sets[key]
	if ok && !alive(s.expireAt) {
		delete(m.sets, key)
		ok = false
	}
	if !ok {
		return false, nil
	}
	_, found := s.members[string(member)]
	return found, nil
}

func (m *MemStore) SRem(ctx context.Context, key string, members ...[]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sets[key]
	if !ok || !alive(s.expireAt) {
		return nil
	}
	for _, mem := range members {
		delete(s.members, string(mem))
	}
	if len(s.members) == 0 {
		delete(m.sets, key)
	}
	return nil
}

func (m *MemStore) getList(key string) (*memList, bool) {
	l, ok := m.lists[key]
	if ok && !alive(l.expireAt) {
		delete(m.lists, key)
		ok = false
	}
	return l, ok
}

func (m *MemStore) LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error) {
	m.mu.Lock()
	l, ok := m.getList(key)
	if !ok {
		m.mu.Unlock()
		return nil, nil
	}
	items := l.items
	m.mu.Unlock()
	s, e, okr := normalizeRange(int(start), int(stop), len(items))
	if !okr {
		return nil, nil
	}
	out := make([][]byte, 0, e-s+1)
	for i := s; i <= e; i++ {
		out = append(out, items[i])
	}
	return out, nil
}

func (m *MemStore) LLen(ctx context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.getList(key)
	if !ok {
		return 0, nil
	}
	return int64(len(l.items)), nil
}

func (m *MemStore) push(ctx context.Context, key string, ttl time.Duration, left bool, values ...[]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.getList(key)
	if !ok {
		l = &memList{}
		m.lists[key] = l
	}
	cp := make([][]byte, len(values))
	for i, v := range values {
		c := make([]byte, len(v))
		copy(c, v)
		cp[i] = c
	}
	if left {
		l.items = append(cp, l.items...)
	} else {
		l.items = append(l.items, cp...)
	}
	if ttl > 0 && l.expireAt == 0 {
		l.expireAt = ttlAt(ttl)
	}
	return nil
}

func (m *MemStore) LPush(ctx context.Context, key string, ttl time.Duration, values ...[]byte) error {
	return m.push(ctx, key, ttl, true, values...)
}

func (m *MemStore) RPush(ctx context.Context, key string, ttl time.Duration, values ...[]byte) error {
	return m.push(ctx, key, ttl, false, values...)
}

func (m *MemStore) LTrim(ctx context.Context, key string, start, stop int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.getList(key)
	if !ok {
		return nil
	}
	s, e, okr := normalizeRange(int(start), int(stop), len(l.items))
	if !okr {
		l.items = nil
	} else {
		l.items = append([][]byte{}, l.items[s:e+1]...)
	}
	return nil
}

// normalizeRange redis LRANGE 语义：负索引、闭区间；第三返回值表示是否非空。
func normalizeRange(start, stop, n int) (int, int, bool) {
	if start < 0 {
		start += n
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += n
	}
	if n == 0 || start >= n || stop < 0 || start > stop {
		return 0, 0, false
	}
	if stop >= n {
		stop = n - 1
	}
	return start, stop, true
}

func (m *MemStore) getZSet(key string) (*memZSet, bool) {
	z, ok := m.zsets[key]
	if ok && !alive(z.expireAt) {
		delete(m.zsets, key)
		ok = false
	}
	return z, ok
}

func (m *MemStore) ZAdd(ctx context.Context, key string, ttl time.Duration, members ...Z) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	z, ok := m.getZSet(key)
	if !ok {
		z = &memZSet{members: make(map[string]float64)}
		m.zsets[key] = z
	}
	for _, mem := range members {
		z.members[string(mem.Member)] = mem.Score
	}
	if ttl > 0 && z.expireAt == 0 {
		z.expireAt = ttlAt(ttl)
	}
	return nil
}

func (m *MemStore) ZRem(ctx context.Context, key string, members ...[]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	z, ok := m.getZSet(key)
	if !ok {
		return nil
	}
	for _, mem := range members {
		delete(z.members, string(mem))
	}
	if len(z.members) == 0 {
		delete(m.zsets, key)
	}
	return nil
}

func (m *MemStore) ZScore(ctx context.Context, key string, member []byte) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	z, ok := m.getZSet(key)
	if !ok {
		return 0, ErrNotFound
	}
	score, found := z.members[string(member)]
	if !found {
		return 0, ErrNotFound
	}
	return score, nil
}

func (m *MemStore) ZCard(ctx context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	z, ok := m.getZSet(key)
	if !ok {
		return 0, nil
	}
	return int64(len(z.members)), nil
}

func (m *MemStore) zSorted(key string) ([]Z, bool) {
	z, ok := m.getZSet(key)
	if !ok {
		return nil, false
	}
	names := make([]string, 0, len(z.members))
	for name := range z.members {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		si, sj := z.members[names[i]], z.members[names[j]]
		if si != sj {
			return si < sj
		}
		return names[i] < names[j]
	})
	out := make([]Z, len(names))
	for i, n := range names {
		out[i] = Z{Score: z.members[n], Member: []byte(n)}
	}
	return out, true
}

func (m *MemStore) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]Z, error) {
	m.mu.Lock()
	zs, ok := m.zSorted(key)
	m.mu.Unlock()
	if !ok {
		return nil, nil
	}
	s, e, okr := normalizeRange(int(start), int(stop), len(zs))
	if !okr {
		return nil, nil
	}
	return zs[s : e+1], nil
}

var errBadBound = errors.New("l2: bad zrangebyscore bound")

// parseZBound 解析 ZRANGEBYSCORE 边界：-inf/+inf、"(x" 开区间、"x" 闭区间。
func parseZBound(s string) (float64, bool, bool, error) {
	if s == "-inf" || s == "+inf" {
		return 0, true, false, nil
	}
	excl := false
	if len(s) > 0 && s[0] == '(' {
		excl = true
		s = s[1:]
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false, false, errBadBound
	}
	return v, false, excl, nil
}

func inBound(score float64, v float64, inf bool, excl bool, isMax bool) bool {
	if inf {
		return true
	}
	if isMax {
		if excl {
			return score < v
		}
		return score <= v
	}
	if excl {
		return score > v
	}
	return score >= v
}

func (m *MemStore) ZRangeByScoreWithScores(ctx context.Context, key string, by ZRangeBy) ([]Z, error) {
	minV, minInf, minExcl, err := parseZBound(by.Min)
	if err != nil {
		return nil, err
	}
	maxV, maxInf, maxExcl, err := parseZBound(by.Max)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	zs, ok := m.zSorted(key)
	m.mu.Unlock()
	if !ok {
		return nil, nil
	}
	var out []Z
	for _, z := range zs {
		if !inBound(z.Score, minV, minInf, minExcl, false) {
			continue
		}
		if !inBound(z.Score, maxV, maxInf, maxExcl, true) {
			continue
		}
		out = append(out, z)
	}
	if by.Offset > 0 {
		if int64(len(out)) <= by.Offset {
			return nil, nil
		}
		out = out[by.Offset:]
	}
	if by.Count > 0 && int64(len(out)) > by.Count {
		out = out[:by.Count]
	}
	return out, nil
}
