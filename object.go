package gocache

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/jninng/go_biz_cache/event"
)

// TypedObjectCache 强类型对象缓存句柄（redis string 结构整值存取；K 在 v1 恒为 string）。
// 读取路径：L1（nil 值 = 负缓存）→ L2（负标记 / Shard 拼接）→ 降级策略 → singleflight 回源。
type TypedObjectCache[K comparable, V any] struct {
	core   *defCore
	ser    Serializer
	loader Loader[K, V]
	sf     *Group[loadResult[V]]
}

// Define 定义并注册 Object 缓存（包级定义一次，集中管理；重复注册报错）。
func Define[V any](ns, name string, def Definition[V]) (*TypedObjectCache[string, V], error) {
	core, err := register(kindObject, ns, name, def, false)
	if err != nil {
		return nil, err
	}
	return &TypedObjectCache[string, V]{
		core:   core,
		ser:    resolveSerializer(def.Serializer),
		loader: def.Loader,
		sf:     &Group[loadResult[V]]{},
	}, nil
}

// MustDefine 同 Define，注册失败 panic（包级定义场景配置错误应尽早暴露）。
func MustDefine[V any](ns, name string, def Definition[V]) *TypedObjectCache[string, V] {
	h, err := Define(ns, name, def)
	if err != nil {
		panic(err)
	}
	return h
}

// ---------- 读取 ----------

// Get 多级获取；未命中（含负缓存命中）返回 ErrNotFound。
func (h *TypedObjectCache[K, V]) Get(ctx context.Context, key K) (V, error) {
	var zero V
	biz := keyString(key)
	v, ok, err := h.fetch(ctx, key, biz)
	if err != nil {
		return zero, err
	}
	if !ok {
		return zero, ErrNotFound
	}
	return v, nil
}

func (h *TypedObjectCache[K, V]) fetch(ctx context.Context, key K, biz string) (V, bool, error) {
	c := h.core
	var zero V
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return zero, false, err
	}
	// L1（nil 值 = 负缓存条目）
	if c.l1 != nil {
		if raw, ok := c.l1.Get(biz); ok {
			if raw == nil {
				c.metrics.Hit(LevelNegative, c.name)
				return zero, false, nil
			}
			v, err := h.decodeRaw(raw)
			if err != nil {
				return zero, false, err
			}
			c.metrics.Hit(LevelL1, c.name)
			return v, true, nil
		}
		c.metrics.Miss(LevelL1, c.name)
	}
	// L2（降级 SourceOnly 跳过读；读失败 Fail 报错 / StaleL1 只认 L1 旧值）
	if c.store != nil && c.degrade != DegradeSourceOnly {
		data, neg, err := h.readL2(ctx, phys)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return zero, false, err
			}
			return zero, false, nil // StaleL1：L1 已查无
		}
		if neg {
			c.metrics.Hit(LevelNegative, c.name)
			if c.l1 != nil {
				c.l1.Set(biz, nil, c.negTTL)
			}
			return zero, false, nil
		}
		if data != nil {
			raw, v, err := h.decodeCompressed(data)
			if err != nil {
				return zero, false, err
			}
			c.metrics.Hit(LevelL2, c.name)
			if c.l1 != nil {
				c.l1.Set(biz, raw, c.ttlJittered())
			}
			return v, true, nil
		}
		c.metrics.Miss(LevelL2, c.name)
	}
	// 回源（singleflight 合并同 key 并发）
	res, err, shared := h.sf.Do(sfLoadPrefix+biz, func() (loadResult[V], error) {
		return h.loadOne(ctx, key, biz)
	})
	if shared {
		c.metrics.SingleflightShared(c.name, 1)
	}
	if err != nil {
		return zero, false, err
	}
	return res.val, res.ok, nil
}

// MultiGet 批量获取：L1 批量 → L2 批量（含 Shard 子键与负标记）→ 剩余 singleflight 合并后
// 一次性批量回源；负缓存/未命中 key 不出现在返回 map。
func (h *TypedObjectCache[K, V]) MultiGet(ctx context.Context, keys ...K) (map[K]V, error) {
	out := make(map[K]V, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	c := h.core
	seen := make(map[string]struct{}, len(keys))
	var pending []K
	for _, k := range keys {
		biz := keyString(k)
		if _, dup := seen[biz]; dup {
			continue
		}
		seen[biz] = struct{}{}
		if c.l1 != nil {
			if raw, ok := c.l1.Get(biz); ok {
				if raw == nil {
					c.metrics.Hit(LevelNegative, c.name)
					continue
				}
				v, err := h.decodeRaw(raw)
				if err != nil {
					return nil, err
				}
				c.metrics.Hit(LevelL1, c.name)
				out[k] = v
				continue
			}
			c.metrics.Miss(LevelL1, c.name)
		}
		pending = append(pending, k)
	}
	if len(pending) == 0 {
		return out, nil
	}
	// L2 批量（跨 slot 由 ClusterClient 分组）
	if c.store != nil && c.degrade != DegradeSourceOnly {
		var err error
		pending, err = h.multiReadL2(ctx, pending, out)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return nil, err
			}
			return out, nil // StaleL1：只保留 L1 命中
		}
		if len(pending) == 0 {
			return out, nil
		}
	}
	// 批量回源：per-key singleflight 合并 + 组内 leader 一次 Loader 调用
	results, err := h.loadPending(ctx, pending)
	if err != nil {
		return nil, err
	}
	for _, k := range pending {
		if r := results[keyString(k)]; r.ok {
			out[k] = r.val
		}
	}
	return out, nil
}

// ---------- 写入 / 失效 ----------

// Set 传值更新（写穿）：写 L2（含大 key 策略）→ 更新本实例 L1 → 广播 Updated。
func (h *TypedObjectCache[K, V]) Set(ctx context.Context, key K, val V) error {
	biz := keyString(key)
	_, phys, err := h.core.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	if err := h.storeValue(ctx, biz, phys, val); err != nil {
		return err
	}
	h.core.publish(ctx, event.Updated, []string{phys})
	return nil
}

// SetMulti 批次传值更新：pipeline 批量写 L2 → 逐 key 更新本实例 L1 → 一次广播 Updated。
func (h *TypedObjectCache[K, V]) SetMulti(ctx context.Context, pairs map[K]V) error {
	if len(pairs) == 0 {
		return nil
	}
	c := h.core
	type entry struct {
		biz, phys string
		raw, data []byte
	}
	entries := make([]entry, 0, len(pairs))
	for k, v := range pairs {
		biz := keyString(k)
		_, phys, err := c.buildKey(ctx, biz)
		if err != nil {
			return err
		}
		raw, err := c.serialize(h.ser, v)
		if err != nil {
			return err
		}
		data, err := c.compress(raw)
		if err != nil {
			return err
		}
		if err := c.checkOversize(phys, data); err != nil {
			return err
		}
		entries = append(entries, entry{biz: biz, phys: phys, raw: raw, data: data})
	}
	ttl := c.ttlJittered()
	if c.store != nil {
		items := make(map[string][]byte, len(entries)*2)
		var negs []string
		for _, e := range entries {
			if c.big.Type == BigValueShard {
				parts, err := c.splitObject(e.biz, frameShards(e.data))
				if err != nil {
					return err
				}
				for i, p := range parts {
					sk := shardKey(e.phys, i)
					if err := c.checkOversize(sk, p); err != nil {
						return err
					}
					items[sk] = p
				}
			} else {
				items[e.phys] = e.data
			}
			if c.negative {
				negs = append(negs, c.negKey(e.phys))
			}
		}
		if err := c.store.MSet(ctx, ttl, items); err != nil {
			c.metrics.Error("l2_write", c.name)
			return err
		}
		if len(negs) > 0 {
			if err := c.store.Del(ctx, negs...); err != nil {
				c.metrics.Error("l2_write", c.name)
				return err
			}
		}
	}
	physKeys := make([]string, 0, len(entries))
	if c.l1 != nil {
		for _, e := range entries {
			c.l1.Set(e.biz, e.raw, ttl)
		}
	}
	for _, e := range entries {
		physKeys = append(physKeys, e.phys)
	}
	c.publish(ctx, event.Updated, physKeys)
	return nil
}

// Refresh 读源更新：singleflight 回源后走 Set 路径（Loader 未配置返回 ErrNotFound）。
func (h *TypedObjectCache[K, V]) Refresh(ctx context.Context, key K) error {
	biz := keyString(key)
	_, phys, err := h.core.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	c := h.core
	_, err, _ = h.sf.Do(sfRefreshPrefix+biz, func() (loadResult[V], error) {
		v, ok, err := h.loadFromSource(ctx, key, biz)
		if err != nil {
			return loadResult[V]{}, err
		}
		if !ok {
			if c.negative {
				if nerr := h.writeNegative(ctx, biz, phys); nerr != nil {
					return loadResult[V]{}, nerr
				}
			}
			return loadResult[V]{}, ErrNotFound
		}
		if serr := h.storeValue(ctx, biz, phys, v); serr != nil {
			return loadResult[V]{}, serr
		}
		c.publish(ctx, event.Updated, []string{phys})
		return loadResult[V]{val: v, ok: true}, nil
	})
	return err
}

// Delete 失效：DEL 物理键 + Shard 子键 + 负标记 → 删本实例 L1 → 广播 Invalidated。
func (h *TypedObjectCache[K, V]) Delete(ctx context.Context, key K) error {
	biz := keyString(key)
	_, phys, err := h.core.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	c := h.core
	if c.store != nil {
		dels := make([]string, 0, c.big.Shard.ShardCount+2)
		dels = append(dels, phys, c.negKey(phys))
		if c.big.Type == BigValueShard {
			for i := 0; i < c.big.Shard.ShardCount; i++ {
				dels = append(dels, shardKey(phys, i))
			}
		}
		if err := c.store.Del(ctx, dels...); err != nil {
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

// InvalidateAll 批次全量失效：世代号 INCR + 清本实例 L1 + 广播 GenerationBumped；旧 key 靠 TTL 过期。
func (h *TypedObjectCache[K, V]) InvalidateAll(ctx context.Context) error {
	return h.core.invalidateAll(ctx)
}

// ---------- 内部路径 ----------

// readL2 读物理值与负标记（Shard 按静态 ShardCount 推算全部子 key；任一片缺失或长度帧不符均视为未命中自愈重写）。
func (h *TypedObjectCache[K, V]) readL2(ctx context.Context, phys string) ([]byte, bool, error) {
	c := h.core
	base := []string{phys}
	if c.big.Type == BigValueShard {
		base = make([]string, 0, c.big.Shard.ShardCount)
		for i := 0; i < c.big.Shard.ShardCount; i++ {
			base = append(base, shardKey(phys, i))
		}
	}
	negK := c.negKey(phys)
	keys := append(append(make([]string, 0, len(base)+1), base...), negK)
	res, err := c.store.MGet(ctx, keys...)
	if err != nil {
		return nil, false, err
	}
	if _, ok := res[negK]; ok {
		return nil, true, nil
	}
	if c.big.Type == BigValueShard {
		buf, ok := h.concatShards(res, phys)
		if !ok {
			return nil, false, nil
		}
		data, ok := unframeShards(buf)
		if !ok {
			return nil, false, nil
		}
		return data, false, nil
	}
	d, ok := res[phys]
	if !ok {
		return nil, false, nil
	}
	return d, false, nil
}

// multiReadL2 MultiGet 的 L2 批量读：全部物理键与负标记合并为一次 MGET。
// 返回仍需回源的 keys；命中的写入 out（含 L1 回填与负缓存处理）。
func (h *TypedObjectCache[K, V]) multiReadL2(ctx context.Context, pending []K, out map[K]V) ([]K, error) {
	c := h.core
	mget := make([]string, 0, len(pending)*2)
	physOf := make(map[string]string, len(pending))
	for _, k := range pending {
		biz := keyString(k)
		_, phys, err := c.buildKey(ctx, biz)
		if err != nil {
			return nil, err
		}
		physOf[biz] = phys
		if c.big.Type == BigValueShard {
			for i := 0; i < c.big.Shard.ShardCount; i++ {
				mget = append(mget, shardKey(phys, i))
			}
		} else {
			mget = append(mget, phys)
		}
		mget = append(mget, c.negKey(phys))
	}
	res, err := c.store.MGet(ctx, mget...)
	if err != nil {
		return nil, err
	}
	var rest []K
	for _, k := range pending {
		biz := keyString(k)
		phys := physOf[biz]
		if _, ok := res[c.negKey(phys)]; ok {
			c.metrics.Hit(LevelNegative, c.name)
			if c.l1 != nil {
				c.l1.Set(biz, nil, c.negTTL)
			}
			continue
		}
		var data []byte
		if c.big.Type == BigValueShard {
			buf, ok := h.concatShards(res, phys)
			if !ok {
				c.metrics.Miss(LevelL2, c.name)
				rest = append(rest, k)
				continue
			}
			if data, ok = unframeShards(buf); !ok {
				c.metrics.Miss(LevelL2, c.name)
				rest = append(rest, k)
				continue
			}
		} else {
			data = res[phys]
			if len(data) == 0 {
				c.metrics.Miss(LevelL2, c.name)
				rest = append(rest, k)
				continue
			}
		}
		raw, v, err := h.decodeCompressed(data)
		if err != nil {
			return nil, err
		}
		c.metrics.Hit(LevelL2, c.name)
		out[k] = v
		if c.l1 != nil {
			c.l1.Set(biz, raw, c.ttlJittered())
		}
	}
	return rest, nil
}

// concatShards 拼接全部分片；任一片缺失返回 nil,false。
func (h *TypedObjectCache[K, V]) concatShards(res map[string][]byte, phys string) ([]byte, bool) {
	var buf []byte
	for i := 0; i < h.core.big.Shard.ShardCount; i++ {
		p, ok := res[shardKey(phys, i)]
		if !ok {
			return nil, false
		}
		buf = append(buf, p...)
	}
	return buf, true
}

const shardFrameLen = 8 // Shard 长度帧字节数（大端 uint64 总长度前缀）

// frameShards Shard 物理格式 = 8 字节大端总长度 + 值。
// 读侧校验总长度，防 ShardCount 调小后旧分片全命中拼接出截断值（设计 §7 自愈承诺）。
func frameShards(data []byte) []byte {
	framed := make([]byte, shardFrameLen+len(data))
	binary.BigEndian.PutUint64(framed, uint64(len(data)))
	copy(framed[shardFrameLen:], data)
	return framed
}

// unframeShards 校验并剥离长度帧；长度不符（ShardCount 变更 / 旧格式 / 损坏）→ 视为未命中自愈。
func unframeShards(buf []byte) ([]byte, bool) {
	if len(buf) < shardFrameLen {
		return nil, false
	}
	total := binary.BigEndian.Uint64(buf[:shardFrameLen])
	if total != uint64(len(buf)-shardFrameLen) {
		return nil, false
	}
	return buf[shardFrameLen:], true
}

// loadPending 批量回源：per-key singleflight 合并（跨调用同 key 复用），组内 leader 一次批量加载。
func (h *TypedObjectCache[K, V]) loadPending(ctx context.Context, pending []K) (map[string]loadResult[V], error) {
	items := make([]string, len(pending))
	for i, k := range pending {
		items[i] = keyString(k)
	}
	lb := newLoadBatch[V](items)
	out := make(map[string]loadResult[V], len(pending))
	shared, firstErr := 0, error(nil)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(pending))
	for _, k := range pending {
		go func(k K) {
			defer wg.Done()
			biz := keyString(k)
			res, err, sh := h.sf.Do(sfLoadPrefix+biz, func() (loadResult[V], error) {
				return lb.run(biz, func(batch []string) error {
					res, err := h.loadBatch(ctx, batch)
					if err != nil {
						return err
					}
					lb.record(res)
					return nil
				})
			})
			mu.Lock()
			out[biz] = res
			if sh {
				shared++
			}
			if err != nil && firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
		}(k)
	}
	wg.Wait()
	if shared > 0 {
		h.core.metrics.SingleflightShared(h.core.name, shared)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

// loadOne Get 路径单 key 回源（批量契约按单元素调用，与 MultiGet 同 sf key 合并）。
func (h *TypedObjectCache[K, V]) loadOne(ctx context.Context, key K, biz string) (loadResult[V], error) {
	res, err := h.loadBatch(ctx, []string{biz})
	if err != nil {
		return loadResult[V]{}, err
	}
	return res[biz], nil
}

// loadBatch 批量回源（单次 Loader 调用）+ 命中写穿回填 / 缺失负缓存占位。
func (h *TypedObjectCache[K, V]) loadBatch(ctx context.Context, batch []string) (map[string]loadResult[V], error) {
	c := h.core
	if h.loader == nil {
		// 无回源契约：未命中即缺失，不写负缓存占位（占位仅由回源结果产生）
		out := make(map[string]loadResult[V], len(batch))
		for _, biz := range batch {
			out[biz] = loadResult[V]{}
		}
		return out, nil
	}
	out := make(map[string]loadResult[V], len(batch))
	keys := make([]K, len(batch))
	for i, biz := range batch {
		keys[i] = keyOf[K](biz)
	}
	start := time.Now()
	m, err := h.loader(ctx, keys)
	c.metrics.ObserveSourceLoad(c.name, time.Since(start))
	if err != nil {
		c.metrics.Error("loader", c.name)
		return nil, err
	}
	for i, biz := range batch {
		_, phys, perr := c.buildKey(ctx, biz)
		if perr != nil {
			return nil, perr
		}
		if v, ok := m[keys[i]]; ok {
			c.metrics.Hit(LevelSource, c.name)
			// 回源写穿失败不影响本次读结果（降级路径下 L2 不可用属预期），记日志交由指标观测
			if serr := h.storeValue(ctx, biz, phys, v); serr != nil {
				c.logger.Warnf("gocache: backfill %s for %s:%s: %v", biz, c.ns, c.name, serr)
			}
			out[biz] = loadResult[V]{val: v, ok: true}
			continue
		}
		c.metrics.Miss(LevelSource, c.name)
		if c.negative {
			// 负标记写失败不影响本次读结果（缺失语义仍正确返回），降级路径下属预期
			if nerr := h.writeNegative(ctx, biz, phys); nerr != nil {
				c.logger.Warnf("gocache: write negative %s for %s:%s: %v", biz, c.ns, c.name, nerr)
			}
		}
		out[biz] = loadResult[V]{}
	}
	return out, nil
}

// loadFromSource 单 key 仅回源（Refresh 用，不写穿）。
func (h *TypedObjectCache[K, V]) loadFromSource(ctx context.Context, key K, biz string) (V, bool, error) {
	if h.loader == nil {
		var zero V
		return zero, false, ErrNotFound
	}
	c := h.core
	start := time.Now()
	m, err := h.loader(ctx, []K{key})
	c.metrics.ObserveSourceLoad(c.name, time.Since(start))
	if err != nil {
		c.metrics.Error("loader", c.name)
		var zero V
		return zero, false, err
	}
	v, ok := m[key]
	if !ok {
		c.metrics.Miss(LevelSource, c.name)
		var zero V
		return zero, false, nil
	}
	c.metrics.Hit(LevelSource, c.name)
	return v, true, nil
}

// storeValue 写穿：序列化 → 压缩 → 写 L2（Zstd/Shard）→ 清旧负标记 → 回填本实例 L1（序列化原字节，L1 不参与大 key 策略）。
func (h *TypedObjectCache[K, V]) storeValue(ctx context.Context, biz, phys string, v V) error {
	c := h.core
	raw, err := c.serialize(h.ser, v)
	if err != nil {
		return err
	}
	data, err := c.compress(raw)
	if err != nil {
		return err
	}
	if err := c.checkOversize(phys, data); err != nil {
		return err
	}
	ttl := c.ttlJittered()
	if c.store != nil {
		if c.big.Type == BigValueShard {
			parts, err := c.splitObject(biz, frameShards(data))
			if err != nil {
				return err
			}
			items := make(map[string][]byte, len(parts))
			for i, p := range parts {
				sk := shardKey(phys, i)
				if err := c.checkOversize(sk, p); err != nil {
					return err
				}
				items[sk] = p
			}
			if err := c.store.MSet(ctx, ttl, items); err != nil {
				c.metrics.Error("l2_write", c.name)
				return err
			}
		} else {
			if err := c.store.MSet(ctx, ttl, map[string][]byte{phys: data}); err != nil {
				c.metrics.Error("l2_write", c.name)
				return err
			}
		}
		if c.negative {
			if err := c.store.Del(ctx, c.negKey(phys)); err != nil {
				c.metrics.Error("l2_write", c.name)
				return err
			}
		}
	}
	if c.l1 != nil {
		c.l1.Set(biz, raw, ttl)
	}
	return nil
}

// writeNegative 写负缓存占位：L2 独立标记 key + 本实例 L1 nil 条目。
func (h *TypedObjectCache[K, V]) writeNegative(ctx context.Context, biz, phys string) error {
	c := h.core
	if c.store != nil {
		if err := c.store.MSet(ctx, c.negTTL, map[string][]byte{c.negKey(phys): negMarker}); err != nil {
			c.metrics.Error("l2_write", c.name)
			return err
		}
	}
	if c.l1 != nil {
		c.l1.Set(biz, nil, c.negTTL)
	}
	return nil
}

// decodeRaw 反序列化（L1 值为序列化原字节）。
func (h *TypedObjectCache[K, V]) decodeRaw(raw []byte) (V, error) {
	var v V
	if err := h.core.deserialize(h.ser, raw, &v); err != nil {
		var zero V
		return zero, err
	}
	return v, nil
}

// decodeCompressed 解压 + 反序列化（L2 值）；返回序列化原字节供 L1 回填。
func (h *TypedObjectCache[K, V]) decodeCompressed(data []byte) ([]byte, V, error) {
	raw, err := h.core.decompress(data)
	if err != nil {
		var zero V
		return nil, zero, err
	}
	v, err := h.decodeRaw(raw)
	if err != nil {
		var zero V
		return nil, zero, err
	}
	return raw, v, nil
}
