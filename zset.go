package gocache

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/jninng/go_biz_cache/event"
)

// TypedZSetCache 强类型 zset 缓存句柄。有序结构不支持 FieldShard（注册期校验）；
// Zstd 作用单元为逐 member；L1 缓存整个 zset（成员按 score 序）。
// Range/RangeByScore 空结果以 ZCard 判空区分越界窗口、合法空查询与 key 缺失。
type TypedZSetCache[K comparable, V any] struct {
	core   *defCore
	ser    Serializer
	loader ZSetLoader[K, V]
	sf     *Group[loadResult[[]Scored[V]]]
}

// DefineZSet 定义并注册 ZSet 缓存。
func DefineZSet[V any](ns, name string, def Definition[V], opts ZSetOptions[V]) (*TypedZSetCache[string, V], error) {
	core, err := register(kindZSet, ns, name, def, false)
	if err != nil {
		return nil, err
	}
	return &TypedZSetCache[string, V]{
		core:   core,
		ser:    resolveSerializer(def.Serializer),
		loader: opts.Loader,
		sf:     &Group[loadResult[[]Scored[V]]]{},
	}, nil
}

// MustDefineZSet 同 DefineZSet，注册失败 panic。
func MustDefineZSet[V any](ns, name string, def Definition[V], opts ZSetOptions[V]) *TypedZSetCache[string, V] {
	h, err := DefineZSet(ns, name, def, opts)
	if err != nil {
		panic(err)
	}
	return h
}

// ---------- 编解码 ----------

func (h *TypedZSetCache[K, V]) encMember(v V) ([]byte, error) {
	raw, err := h.core.serialize(h.ser, v)
	if err != nil {
		return nil, err
	}
	return h.core.compress(raw)
}

func (h *TypedZSetCache[K, V]) decMember(data []byte) (v V, err error) {
	raw, err := h.core.decompress(data)
	if err != nil {
		return
	}
	err = h.core.deserialize(h.ser, raw, &v)
	return
}

func (h *TypedZSetCache[K, V]) decScored(zs []Z) ([]Scored[V], error) {
	out := make([]Scored[V], 0, len(zs))
	for _, z := range zs {
		v, err := h.decMember(z.Member)
		if err != nil {
			return nil, err
		}
		out = append(out, Scored[V]{Value: v, Score: z.Score})
	}
	return out, nil
}

// ---------- 写入 ----------

// Add 写入带 score 成员：ZAdd（ExpireNX 语义）→ 删本实例 L1 → 广播 Updated。
func (h *TypedZSetCache[K, V]) Add(ctx context.Context, key K, scored ...Scored[V]) error {
	if len(scored) == 0 {
		return nil
	}
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	zs := make([]Z, 0, len(scored))
	for _, s := range scored {
		data, err := h.encMember(s.Value)
		if err != nil {
			return err
		}
		if err := c.checkOversize(phys, data); err != nil {
			return err
		}
		zs = append(zs, Z{Score: s.Score, Member: data})
	}
	ttl := c.ttlJittered()
	if c.store != nil {
		if err := c.store.ZAdd(ctx, phys, ttl, zs...); err != nil {
			c.metrics.Error("l2_write", c.name)
			return err
		}
	}
	if c.l1 != nil {
		c.l1.Delete(biz)
	}
	c.publish(ctx, event.Updated, []string{phys})
	return nil
}

// Remove 删成员：ZRem → 删本实例 L1 → 广播 Updated。
func (h *TypedZSetCache[K, V]) Remove(ctx context.Context, key K, values ...V) error {
	if len(values) == 0 {
		return nil
	}
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	members := make([][]byte, 0, len(values))
	for _, v := range values {
		data, err := h.encMember(v)
		if err != nil {
			return err
		}
		members = append(members, data)
	}
	if c.store != nil {
		if err := c.store.ZRem(ctx, phys, members...); err != nil {
			c.metrics.Error("l2_write", c.name)
			return err
		}
	}
	if c.l1 != nil {
		c.l1.Delete(biz)
	}
	c.publish(ctx, event.Updated, []string{phys})
	return nil
}

// ---------- 读取 ----------

// Range 按 index 区间读取（score 升序）：L1 整集合切片 → L2 → 空结果以 ZCard 判空（越界窗口返回空，key 缺失重建）。
func (h *TypedZSetCache[K, V]) Range(ctx context.Context, key K, start, stop int64) ([]Scored[V], error) {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return nil, err
	}
	if c.l1 != nil {
		if raw, ok := c.l1.Get(biz); ok {
			var items []Scored[V]
			if err := c.deserialize(h.ser, raw, &items); err != nil {
				return nil, err
			}
			c.metrics.Hit(LevelL1, c.name)
			return sliceRange(items, int(start), int(stop)), nil
		}
		c.metrics.Miss(LevelL1, c.name)
	}
	if c.store != nil && c.degrade != DegradeSourceOnly {
		zs, err := c.store.ZRangeWithScores(ctx, phys, start, stop)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return nil, err
			}
			return nil, ErrNotFound // StaleL1：L1 已查无
		}
		if len(zs) > 0 {
			c.metrics.Hit(LevelL2, c.name)
			return h.decScored(zs)
		}
		// 空结果：key 存在（ZCard>0）→ 越界区间为合法空查询，不回源
		card, err := c.store.ZCard(ctx, phys)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return nil, err
			}
			return nil, ErrNotFound
		}
		if card > 0 {
			c.metrics.Hit(LevelL2, c.name)
			return []Scored[V]{}, nil
		}
		c.metrics.Miss(LevelL2, c.name)
	}
	items, err := h.rebuild(ctx, key, biz, phys)
	if err != nil {
		return nil, err
	}
	return sliceRange(items, int(start), int(stop)), nil
}

// RangeByScore 按分值区间读取。空结果可能是合法查询：ZCard 判空区分"空集合"与"未缓存"。
func (h *TypedZSetCache[K, V]) RangeByScore(ctx context.Context, key K, by ZRangeBy) ([]Scored[V], error) {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return nil, err
	}
	if c.store != nil && c.degrade != DegradeSourceOnly {
		zs, err := c.store.ZRangeByScoreWithScores(ctx, phys, by)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return nil, err
			}
			return nil, ErrNotFound // StaleL1：L1 已查无
		}
		if len(zs) > 0 {
			c.metrics.Hit(LevelL2, c.name)
			return h.decScored(zs)
		}
		// 空结果：key 存在（ZCard>0）→ 合法空查询，不回源
		card, err := c.store.ZCard(ctx, phys)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return nil, err
			}
			return nil, ErrNotFound
		}
		if card > 0 {
			c.metrics.Hit(LevelL2, c.name)
			return []Scored[V]{}, nil
		}
		c.metrics.Miss(LevelL2, c.name)
	}
	items, err := h.rebuild(ctx, key, biz, phys)
	if err != nil {
		return nil, err
	}
	return filterZByScore(items, by), nil
}

// Score 点查分值：L2 直查（不触发回源、不走 L1）；本地模式 / SourceOnly 退化为整载判定。
func (h *TypedZSetCache[K, V]) Score(ctx context.Context, key K, v V) (float64, error) {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return 0, err
	}
	vEnc, err := h.encMember(v)
	if err != nil {
		return 0, err
	}
	if c.store != nil && c.degrade != DegradeSourceOnly {
		score, err := c.store.ZScore(ctx, phys, vEnc)
		if err == nil {
			c.metrics.Hit(LevelL2, c.name)
			return score, nil
		}
		if errors.Is(err, ErrNotFound) {
			c.metrics.Miss(LevelL2, c.name)
			return 0, ErrNotFound
		}
		c.metrics.Error("l2_read", c.name)
		if c.degrade == DegradeFail {
			return 0, err
		}
		return 0, ErrNotFound
	}
	items, err := h.membersViaLoader(ctx, key, biz, phys)
	if err != nil {
		return 0, err
	}
	for _, s := range items {
		e, err := h.encMember(s.Value)
		if err == nil && bytes.Equal(e, vEnc) {
			return s.Score, nil
		}
	}
	return 0, ErrNotFound
}

// ---------- 失效 ----------

// Delete 失效整 zset：DEL 物理 key → 删本实例 L1 → 广播 Invalidated。
func (h *TypedZSetCache[K, V]) Delete(ctx context.Context, key K) error {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	if c.store != nil {
		if err := c.store.Del(ctx, phys); err != nil {
			c.metrics.Error("delete", c.name)
			return err
		}
	}
	if c.l1 != nil {
		c.l1.Delete(biz)
	}
	c.publish(ctx, event.Invalidated, []string{phys})
	return nil
}

// InvalidateAll 批次全量失效：世代号 INCR + 清本实例 L1 + 广播 GenerationBumped。
func (h *TypedZSetCache[K, V]) InvalidateAll(ctx context.Context) error {
	return h.core.invalidateAll(ctx)
}

// ---------- 内部路径 ----------

// membersViaLoader 经 singleflight 整 key 回源（Score 本地判定用）。
func (h *TypedZSetCache[K, V]) membersViaLoader(ctx context.Context, key K, biz, phys string) ([]Scored[V], error) {
	res, err, shared := h.sf.Do(sfLoadPrefix+biz, func() (loadResult[[]Scored[V]], error) {
		items, err := h.rebuild(ctx, key, biz, phys)
		if err != nil {
			return loadResult[[]Scored[V]]{}, err
		}
		return loadResult[[]Scored[V]]{val: items, ok: true}, nil
	})
	if shared {
		h.core.metrics.SingleflightShared(h.core.name, 1)
	}
	return res.val, err
}

// rebuild 整 key 回源重建：回源前 DEL 旧 key 再 ZAdd 整写；空 zset 同样回填 L1。
func (h *TypedZSetCache[K, V]) rebuild(ctx context.Context, key K, biz, phys string) ([]Scored[V], error) {
	c := h.core
	if h.loader == nil {
		return nil, ErrNotFound
	}
	start := time.Now()
	items, err := h.loader(ctx, key)
	c.metrics.ObserveSourceLoad(c.name, time.Since(start))
	if err != nil {
		c.metrics.Error("loader", c.name)
		return nil, err
	}
	// redis ZSET 语义：读取按 score 升序（同分按成员序），重建结果排序后写与返回
	h.sortScored(items)
	if len(items) > 0 {
		c.metrics.Hit(LevelSource, c.name)
	} else {
		c.metrics.Miss(LevelSource, c.name)
	}
	ttl := c.ttlJittered()
	if c.store != nil {
		// 回源-整写窗口内并发写可能被重建覆盖，语义 last-write-win（设计 §6）；
		// L2 回写失败不影响本次读结果（降级路径下属预期），Del 失败时跳过整写防重复叠加
		if err := c.store.Del(ctx, phys); err != nil {
			c.metrics.Error("l2_write", c.name)
			c.logger.Warnf("gocache: rebuild clear %s for %s:%s: %v", biz, c.ns, c.name, err)
		} else if len(items) > 0 {
			zs := make([]Z, 0, len(items))
			for _, s := range items {
				data, err := h.encMember(s.Value)
				if err != nil {
					return nil, err
				}
				if err := c.checkOversize(phys, data); err != nil {
					return nil, err
				}
				zs = append(zs, Z{Score: s.Score, Member: data})
			}
			if err := c.store.ZAdd(ctx, phys, ttl, zs...); err != nil {
				c.metrics.Error("l2_write", c.name)
				c.logger.Warnf("gocache: rebuild write %s for %s:%s: %v", biz, c.ns, c.name, err)
			}
		}
	}
	if c.l1 != nil {
		if raw, serr := c.serialize(h.ser, items); serr == nil {
			c.l1.Set(biz, raw, ttl)
		}
	}
	return items, nil
}

// filterZByScore ZRANGEBYSCORE 内存过滤（本地模式 / 重建后查询用；语义同 redis：-inf/+inf、"(x" 开区间、offset/count）。
func filterZByScore[V any](items []Scored[V], by ZRangeBy) []Scored[V] {
	minV, minInf, minExcl, err := parseZBound(by.Min)
	if err != nil {
		return nil
	}
	maxV, maxInf, maxExcl, err := parseZBound(by.Max)
	if err != nil {
		return nil
	}
	out := make([]Scored[V], 0, len(items))
	for _, s := range items {
		if !inZBound(s.Score, minV, minInf, minExcl, false) || !inZBound(s.Score, maxV, maxInf, maxExcl, true) {
			continue
		}
		out = append(out, s)
	}
	if by.Offset > 0 {
		if int64(len(out)) <= by.Offset {
			return nil
		}
		out = out[by.Offset:]
	}
	if by.Count > 0 && int64(len(out)) > by.Count {
		out = out[:by.Count]
	}
	return out
}

func parseZBound(s string) (v float64, inf, excl bool, err error) {
	if s == "-inf" || s == "+inf" {
		return 0, true, false, nil
	}
	if len(s) > 0 && s[0] == '(' {
		excl = true
		s = s[1:]
	}
	v, err = strconv.ParseFloat(s, 64)
	return
}

func inZBound(score, v float64, inf, excl, isMax bool) bool {
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

// sortScored 按 score 升序；同分按物理存储（序列化+压缩后）成员字节序，与 L2 集合内排序一致（确定性）。
func (h *TypedZSetCache[K, V]) sortScored(items []Scored[V]) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score < items[j].Score
		}
		a, aerr := h.encMember(items[i].Value)
		b, berr := h.encMember(items[j].Value)
		if aerr != nil || berr != nil {
			return false
		}
		return string(a) < string(b)
	})
}
