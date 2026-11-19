package gocache

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/jninng/go_biz_cache/event"
)

// TypedHashCache 强类型 hash 缓存句柄。
// field 级语义：HGet/HMGet 未命中 field 走 field 级回源（批量契约），
// singleflight 与负缓存按 (key, field) 粒度；HGetAll 为全量语义（不做负缓存占位）。
// L1 键粒度为 业务键+\x1f+fieldEnc 复合键。
type TypedHashCache[K comparable, F comparable, V any] struct {
	core   *defCore
	ser    Serializer
	fc     FieldCodec[F]
	loader HashLoader[K, F, V]
	sf     *Group[loadResult[V]]       // field 级回源
	hall   *Group[loadResult[map[F]V]] // HGetAll 全量回源
}

// DefineHash 定义并注册 Hash 缓存；F 非 string 且未提供 FieldCodec 时注册报错。
func DefineHash[F comparable, V any](ns, name string, def Definition[V], opts HashOptions[F, V]) (*TypedHashCache[string, F, V], error) {
	core, err := register(kindHash, ns, name, def, opts.RefreshTTLOnWrite)
	if err != nil {
		return nil, err
	}
	fc := opts.FieldCodec
	if fc == nil {
		if reflect.TypeOf((*F)(nil)).Elem().Kind() != reflect.String {
			return nil, fmt.Errorf("%w: hash field type %T requires FieldCodec", ErrInvalidDefinition, *new(F))
		}
		fc = stringFieldCodec[F]()
	}
	return &TypedHashCache[string, F, V]{
		core:   core,
		ser:    resolveSerializer(def.Serializer),
		fc:     fc,
		loader: opts.HashLoader,
		sf:     &Group[loadResult[V]]{},
		hall:   &Group[loadResult[map[F]V]]{},
	}, nil
}

// MustDefineHash 同 DefineHash，注册失败 panic。
func MustDefineHash[F comparable, V any](ns, name string, def Definition[V], opts HashOptions[F, V]) *TypedHashCache[string, F, V] {
	h, err := DefineHash(ns, name, def, opts)
	if err != nil {
		panic(err)
	}
	return h
}

// ---------- 编解码 / 定位 ----------

func (h *TypedHashCache[K, F, V]) encField(f F) (string, error) {
	s, err := h.fc.EncodeField(f)
	if err != nil {
		return "", fmt.Errorf("gocache: encode field %v for %s:%s: %w", f, h.core.ns, h.core.name, err)
	}
	return s, nil
}

func (h *TypedHashCache[K, F, V]) decField(s string) (F, error) {
	f, err := h.fc.DecodeField(s)
	if err != nil {
		var zero F
		return zero, fmt.Errorf("gocache: decode field %q for %s:%s: %w", s, h.core.ns, h.core.name, err)
	}
	return f, nil
}

// encValue 序列化 + 压缩（Zstd 作用单元为单 field 值）；raw 为 L1 回填字节。
func (h *TypedHashCache[K, F, V]) encValue(v V) (raw, data []byte, err error) {
	if raw, err = h.core.serialize(h.ser, v); err != nil {
		return
	}
	data, err = h.core.compress(raw)
	return
}

// decCompressed 解压 + 反序列化；raw 为 L1 回填字节。
func (h *TypedHashCache[K, F, V]) decCompressed(data []byte) (raw []byte, v V, err error) {
	if raw, err = h.core.decompress(data); err != nil {
		return
	}
	err = h.core.deserialize(h.ser, raw, &v)
	return
}

func (h *TypedHashCache[K, F, V]) decRaw(raw []byte) (V, error) {
	var v V
	err := h.core.deserialize(h.ser, raw, &v)
	return v, err
}

// stringFieldCodec string 底层类型（含命名类型）的直通 field 编解码器。
func stringFieldCodec[F comparable]() FieldCodec[F] {
	rt := reflect.TypeOf((*F)(nil)).Elem()
	st := reflect.TypeOf("")
	return FieldCodecFunc[F](
		func(f F) (string, error) { return reflect.ValueOf(f).Convert(st).String(), nil },
		func(s string) (F, error) {
			v, ok := reflect.ValueOf(s).Convert(rt).Interface().(F)
			if !ok {
				var zero F
				return zero, fmt.Errorf("gocache: field decode %q to %s", s, rt)
			}
			return v, nil
		},
	)
}

// fieldPhys field 的物理定位 key（FieldShard → :h{i} 子 key）。
func (h *TypedHashCache[K, F, V]) fieldPhys(phys, fEnc string) string {
	if h.core.big.Type == BigValueFieldShard {
		return fieldShardKey(phys, h.core.memberIndex([]byte(fEnc)))
	}
	return phys
}

func l1FieldKey(biz, fEnc string) string { return biz + l1FieldSep + fEnc }

// ---------- 读取 ----------

// HGet 单 field 读取：L1 复合键 → 单 key HGET → field 负标记 → field 级 singleflight 回源。
func (h *TypedHashCache[K, F, V]) HGet(ctx context.Context, key K, field F) (V, error) {
	var zero V
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return zero, err
	}
	fEnc, err := h.encField(field)
	if err != nil {
		return zero, err
	}
	lk := l1FieldKey(biz, fEnc)
	// L1 复合键（解码失败视为脏数据删条目走 L2/回源自愈）
	if c.l1 != nil {
		if raw, ok := c.l1.Get(lk); ok {
			if raw == nil {
				c.metrics.Hit(LevelNegative, c.name)
				return zero, ErrNotFound
			}
			v, err := h.decRaw(raw)
			if err == nil {
				c.metrics.Hit(LevelL1, c.name)
				return v, nil
			}
			c.decodeError("l1:"+lk, err)
			c.l1.Delete(lk)
		} else {
			c.metrics.Miss(LevelL1, c.name)
		}
	}
	if c.store != nil && c.degrade != DegradeSourceOnly {
		data, err := c.store.HGet(ctx, h.fieldPhys(phys, fEnc), fEnc)
		if err == nil {
			raw, v, derr := h.decCompressed(data)
			if derr == nil {
				c.metrics.Hit(LevelL2, c.name)
				if c.l1 != nil {
					c.l1.Set(lk, raw, c.ttlJittered())
				}
				return v, nil
			}
			// 脏 field 值：删该 field 走回源自愈重写
			c.decodeError("l2:"+phys+"/"+fEnc, derr)
			if hderr := c.store.HDel(ctx, h.fieldPhys(phys, fEnc), fEnc); hderr != nil {
				c.logger.Warnf("gocache: purge corrupt field %s/%s for %s:%s: %v", biz, fEnc, c.ns, c.name, hderr)
			}
			c.metrics.Miss(LevelL2, c.name)
		} else if !errors.Is(err, ErrNotFound) {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return zero, err
			}
			return zero, ErrNotFound // StaleL1：L1 已查无
		} else {
			c.metrics.Miss(LevelL2, c.name)
		}
		// field 负标记（独立短 TTL string key；NegativeCache 关闭时不探测）
		if c.negative {
			if mk, gerr := c.store.Get(ctx, c.fieldNegKey(phys, fEnc)); gerr == nil && mk != nil {
				c.metrics.Hit(LevelNegative, c.name)
				if c.l1 != nil {
					c.l1.Set(lk, nil, c.negTTL)
				}
				return zero, ErrNotFound
			}
		}
	}
	// field 级 singleflight 回源
	res, err, shared := h.sf.Do(sfLoadPrefix+biz+l1FieldSep+fEnc, func() (loadResult[V], error) {
		m, err := h.loadFields(ctx, key, biz, phys, []string{fEnc})
		if err != nil {
			return loadResult[V]{}, err
		}
		return m[fEnc], nil
	})
	if shared {
		c.metrics.SingleflightShared(c.name, 1)
	}
	if err != nil {
		return zero, err
	}
	if !res.ok {
		return zero, ErrNotFound
	}
	return res.val, nil
}

// HMGet 批量 field 读取：L1 逐 field → L2 按子 key 分组 HMGet → 负标记 → 剩余 fields 一次批量回源。
func (h *TypedHashCache[K, F, V]) HMGet(ctx context.Context, key K, fields ...F) (map[F]V, error) {
	out := make(map[F]V, len(fields))
	if len(fields) == 0 {
		return out, nil
	}
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(fields))
	fieldOf := make(map[string]F, len(fields)) // fEnc → F
	var pending []string
	for _, f := range fields {
		fEnc, err := h.encField(f)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[fEnc]; dup {
			continue
		}
		seen[fEnc] = struct{}{}
		fieldOf[fEnc] = f
		lk := l1FieldKey(biz, fEnc)
		if c.l1 != nil {
			if raw, ok := c.l1.Get(lk); ok {
				if raw == nil {
					c.metrics.Hit(LevelNegative, c.name)
					continue
				}
				v, err := h.decRaw(raw)
				if err != nil {
					return nil, err
				}
				c.metrics.Hit(LevelL1, c.name)
				out[f] = v
				continue
			}
			c.metrics.Miss(LevelL1, c.name)
		}
		pending = append(pending, fEnc)
	}
	// L2：fields 按子 key 分组后 HMGet（跨 slot 由 ClusterClient 分组）
	if len(pending) > 0 && c.store != nil && c.degrade != DegradeSourceOnly {
		groups := make(map[string][]string) // 子 key → fEncs
		for _, fEnc := range pending {
			sub := h.fieldPhys(phys, fEnc)
			groups[sub] = append(groups[sub], fEnc)
		}
		for sub, encs := range groups {
			res, err := c.store.HMGet(ctx, sub, encs...)
			if err != nil {
				c.metrics.Error("l2_read", c.name)
				if c.degrade == DegradeFail {
					return nil, err
				}
				return out, nil // StaleL1：只保留 L1 命中
			}
			for _, fEnc := range encs {
				data, ok := res[fEnc]
				if !ok {
					continue
				}
				raw, v, derr := h.decCompressed(data)
				if derr != nil {
					// 脏 field 值：删该 field，剩余交由回源自愈
					c.decodeError("l2:"+sub+"/"+fEnc, derr)
					if hderr := c.store.HDel(ctx, sub, fEnc); hderr != nil {
						c.logger.Warnf("gocache: purge corrupt field %s/%s for %s:%s: %v", biz, fEnc, c.ns, c.name, hderr)
					}
					continue
				}
				c.metrics.Hit(LevelL2, c.name)
				out[fieldOf[fEnc]] = v
				if c.l1 != nil {
					c.l1.Set(l1FieldKey(biz, fEnc), raw, c.ttlJittered())
				}
			}
		}
		// 负标记批量检查（标记读取失败按无标记处理，交由回源兜底）
		pending = h.filterNegative(ctx, biz, phys, pending, out, fieldOf)
	}
	if len(pending) == 0 {
		return out, nil
	}
	if h.loader == nil {
		return out, nil
	}
	// 剩余 fields 批量回源：per-field singleflight 合并 + 组内 leader 一次 HashLoader 调用
	results, shared, err := h.loadPendingFields(ctx, key, biz, phys, pending)
	if shared > 0 {
		c.metrics.SingleflightShared(c.name, shared)
	}
	if err != nil {
		return nil, err
	}
	for fEnc, r := range results {
		if r.ok {
			out[fieldOf[fEnc]] = r.val
		}
	}
	return out, nil
}

// filterNegative 过滤已负缓存的 fields（L1 nil 回填并从 pending 剔除）。
func (h *TypedHashCache[K, F, V]) filterNegative(ctx context.Context, biz, phys string, pending []string, out map[F]V, fieldOf map[string]F) []string {
	c := h.core
	if !c.negative || len(pending) == 0 {
		return pending
	}
	negKeys := make([]string, len(pending))
	for i, fEnc := range pending {
		negKeys[i] = c.fieldNegKey(phys, fEnc)
	}
	res, err := c.store.MGet(ctx, negKeys...)
	if err != nil {
		c.metrics.Error("l2_read", c.name)
		return pending // 交由回源兜底
	}
	var rest []string
	for _, fEnc := range pending {
		if _, hit := res[c.fieldNegKey(phys, fEnc)]; hit {
			c.metrics.Hit(LevelNegative, c.name)
			if c.l1 != nil {
				c.l1.Set(l1FieldKey(biz, fEnc), nil, c.negTTL)
			}
			continue
		}
		rest = append(rest, fEnc)
	}
	return rest
}

// HGetAll 全量读取：hash 存在直接返回；不存在走 HashLoader(ctx,key,nil) 全量回源整写。
// 不做负缓存占位；未写入的 field 静默缺失。
func (h *TypedHashCache[K, F, V]) HGetAll(ctx context.Context, key K) (map[F]V, error) {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return nil, err
	}
	res, err, shared := h.hall.Do("hall:"+biz, func() (loadResult[map[F]V], error) {
		return h.loadAll(ctx, key, biz, phys)
	})
	if shared {
		c.metrics.SingleflightShared(c.name, 1)
	}
	if err != nil {
		return nil, err
	}
	if !res.ok {
		return nil, ErrNotFound
	}
	return res.val, nil
}

// ---------- 写入 / 失效 ----------

// HSet 写单 field（写穿）：清旧负标记 → HMSet 到定位子 key → 更新本实例 L1 → 广播 Updated。
func (h *TypedHashCache[K, F, V]) HSet(ctx context.Context, key K, field F, val V) error {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	fEnc, err := h.encField(field)
	if err != nil {
		return err
	}
	raw, data, err := h.encValue(val)
	if err != nil {
		return err
	}
	sub := h.fieldPhys(phys, fEnc)
	if err := c.checkOversize(sub, data); err != nil {
		return err
	}
	ttl := c.ttlJittered()
	if c.store != nil {
		// 先清旧负标记后写数据：中途失败只少缓存，不会留下遮蔽新值的标记
		if c.negative {
			if err := c.store.Del(ctx, c.fieldNegKey(phys, fEnc)); err != nil {
				c.metrics.Error("l2_write", c.name)
				return err
			}
		}
		if err := c.store.HMSet(ctx, sub, map[string][]byte{fEnc: data}, ttl, c.refreshTTL); err != nil {
			c.metrics.Error("l2_write", c.name)
			return err
		}
	}
	if c.l1 != nil {
		c.l1.Set(l1FieldKey(biz, fEnc), raw, ttl)
	}
	c.publish(ctx, event.Updated, []string{phys})
	return nil
}

// HSetMulti 批量写 fields：逐 field 编码并更新本实例 L1 → 清旧负标记 → 按子 key 分组 HMSet → 一次广播 Updated。
func (h *TypedHashCache[K, F, V]) HSetMulti(ctx context.Context, key K, fields map[F]V) error {
	if len(fields) == 0 {
		return nil
	}
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	groups := make(map[string]map[string][]byte) // 子 key → {fEnc: data}
	ttl := c.ttlJittered()
	var negs []string
	for f, v := range fields {
		fEnc, err := h.encField(f)
		if err != nil {
			return err
		}
		raw, data, err := h.encValue(v)
		if err != nil {
			return err
		}
		sub := h.fieldPhys(phys, fEnc)
		if err := c.checkOversize(sub, data); err != nil {
			return err
		}
		w, ok := groups[sub]
		if !ok {
			w = make(map[string][]byte, len(fields))
			groups[sub] = w
		}
		w[fEnc] = data
		if c.negative {
			negs = append(negs, c.fieldNegKey(phys, fEnc))
		}
		if c.l1 != nil {
			c.l1.Set(l1FieldKey(biz, fEnc), raw, ttl)
		}
	}
	if c.store != nil {
		// 先清旧负标记后批量写（单 pipeline）：中途失败只少缓存，不会留下遮蔽新值的标记
		if len(negs) > 0 {
			if err := c.store.Del(ctx, negs...); err != nil {
				c.metrics.Error("l2_write", c.name)
				return err
			}
		}
		if err := c.store.HMSetBatch(ctx, ttl, c.refreshTTL, groups); err != nil {
			c.metrics.Error("l2_write", c.name)
			return err
		}
	}
	c.publish(ctx, event.Updated, []string{phys})
	return nil
}

// HDel 删 fields：HDel 定位子 key → 清负标记 → 删本实例 L1 复合键 → 广播 Invalidated。
func (h *TypedHashCache[K, F, V]) HDel(ctx context.Context, key K, fields ...F) error {
	if len(fields) == 0 {
		return nil
	}
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	groups := make(map[string][]string)
	var negs []string
	l1Keys := make([]string, 0, len(fields))
	for _, f := range fields {
		fEnc, err := h.encField(f)
		if err != nil {
			return err
		}
		sub := h.fieldPhys(phys, fEnc)
		groups[sub] = append(groups[sub], fEnc)
		negs = append(negs, c.fieldNegKey(phys, fEnc))
		l1Keys = append(l1Keys, l1FieldKey(biz, fEnc))
	}
	if c.store != nil {
		for sub, encs := range groups {
			if err := c.store.HDel(ctx, sub, encs...); err != nil {
				c.metrics.Error("delete", c.name)
				return err
			}
		}
		if err := c.store.Del(ctx, negs...); err != nil {
			c.metrics.Error("delete", c.name)
			return err
		}
	}
	if c.l1 != nil {
		c.l1.Delete(l1Keys...)
	}
	c.publish(ctx, event.Invalidated, []string{phys})
	return nil
}

// Delete 失效整 hash：DEL 全部子 key → 清本实例 L1（复合键不可枚举，退化为整定义清理）→ 广播 Invalidated。
// 残留 field 负标记无法枚举，由其短 TTL 自然过期兜底。
func (h *TypedHashCache[K, F, V]) Delete(ctx context.Context, key K) error {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	if c.store != nil {
		if err := c.store.Del(ctx, h.subkeysAll(phys)...); err != nil {
			c.metrics.Error("delete", c.name)
			return err
		}
	}
	if c.l1 != nil {
		c.l1.Clear()
	}
	c.publish(ctx, event.Invalidated, []string{phys})
	return nil
}

// InvalidateAll 批次全量失效：世代号 INCR + 清本实例 L1 + 广播 GenerationBumped。
func (h *TypedHashCache[K, F, V]) InvalidateAll(ctx context.Context) error {
	return h.core.invalidateAll(ctx)
}

// ---------- 内部路径 ----------

// loadPendingFields 批量 field 回源：per-field singleflight（跨调用同 (key,field) 复用），
// 组内 leader 一次 HashLoader 调用。返回各 field 加载结果。
func (h *TypedHashCache[K, F, V]) loadPendingFields(ctx context.Context, key K, biz, phys string, fEncs []string) (map[string]loadResult[V], int, error) {
	lb := newLoadBatch[V](fEncs)
	out := make(map[string]loadResult[V], len(fEncs))
	shared, firstErr := 0, error(nil)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(fEncs))
	for _, fEnc := range fEncs {
		go func(fEnc string) {
			defer wg.Done()
			res, err, sh := h.sf.Do(sfLoadPrefix+biz+l1FieldSep+fEnc, func() (loadResult[V], error) {
				return lb.run(fEnc, func(batch []string) error {
					res, err := h.loadFields(ctx, key, biz, phys, batch)
					if err != nil {
						return err
					}
					lb.record(res)
					return nil
				})
			})
			mu.Lock()
			out[fEnc] = res
			if sh {
				shared++
			}
			if err != nil && firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
		}(fEnc)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, 0, firstErr
	}
	return out, shared, nil
}

// loadFields field 级批量回源（单次 HashLoader）+ 写穿回填 / 缺失负缓存占位。
func (h *TypedHashCache[K, F, V]) loadFields(ctx context.Context, key K, biz, phys string, fEncs []string) (map[string]loadResult[V], error) {
	c := h.core
	if h.loader == nil {
		out := make(map[string]loadResult[V], len(fEncs))
		for _, fEnc := range fEncs {
			out[fEnc] = loadResult[V]{}
		}
		return out, nil
	}
	fields := make([]F, len(fEncs))
	for i, fEnc := range fEncs {
		f, err := h.decField(fEnc)
		if err != nil {
			return nil, err
		}
		fields[i] = f
	}
	start := time.Now()
	m, err := h.loader(ctx, key, fields)
	c.metrics.ObserveSourceLoad(c.name, time.Since(start))
	if err != nil {
		c.metrics.Error("loader", c.name)
		return nil, err
	}
	out := make(map[string]loadResult[V], len(fEncs))
	writes := make(map[string]map[string][]byte) // 子 key → {fEnc: data}
	var negs []string
	l1TTL := c.ttlJittered()
	for i, fEnc := range fEncs {
		f := fields[i]
		if v, ok := m[f]; ok {
			c.metrics.Hit(LevelSource, c.name)
			raw, data, verr := h.encValue(v)
			if verr != nil {
				return nil, verr
			}
			sub := h.fieldPhys(phys, fEnc)
			if serr := c.checkOversize(sub, data); serr != nil {
				return nil, serr
			}
			w, exists := writes[sub]
			if !exists {
				w = make(map[string][]byte, len(fEncs))
				writes[sub] = w
			}
			w[fEnc] = data
			out[fEnc] = loadResult[V]{val: v, ok: true}
			if c.l1 != nil {
				c.l1.Set(l1FieldKey(biz, fEnc), raw, l1TTL)
			}
			if c.negative {
				negs = append(negs, c.fieldNegKey(phys, fEnc))
			}
			continue
		}
		c.metrics.Miss(LevelSource, c.name)
		if c.negative {
			if c.store != nil {
				// 负标记写失败不影响本次读结果（缺失语义仍正确返回）
				if nerr := c.store.MSet(ctx, c.negTTL, map[string][]byte{c.fieldNegKey(phys, fEnc): negMarker}); nerr != nil {
					c.metrics.Error("l2_write", c.name)
					c.logger.Warnf("gocache: write field negative %s/%s for %s:%s: %v", biz, fEnc, c.ns, c.name, nerr)
				}
			}
			if c.l1 != nil {
				c.l1.Set(l1FieldKey(biz, fEnc), nil, c.negTTL)
			}
		}
		out[fEnc] = loadResult[V]{}
	}
	if c.store != nil {
		// 先清旧负标记后批量回写（单 pipeline）；回写失败不影响本次读结果（降级路径下属预期）
		if len(negs) > 0 {
			if err := c.store.Del(ctx, negs...); err != nil {
				c.metrics.Error("l2_write", c.name)
				c.logger.Warnf("gocache: clear field negatives %s for %s:%s: %v", biz, c.ns, c.name, err)
			}
		}
		if err := c.store.HMSetBatch(ctx, l1TTL, c.refreshTTL, writes); err != nil {
			c.metrics.Error("l2_write", c.name)
			c.logger.Warnf("gocache: backfill %s for %s:%s: %v", biz, c.ns, c.name, err)
		}
	}
	return out, nil
}

// loadAll HGetAll 全量语义：hash 存在直接返回（FieldShard 合并 HGETALL×N，全空视为不存在）；
// 不存在全量回源（fields=nil）整写。不做负缓存占位。
func (h *TypedHashCache[K, F, V]) loadAll(ctx context.Context, key K, biz, phys string) (loadResult[map[F]V], error) {
	c := h.core
	if c.store != nil && c.degrade != DegradeSourceOnly {
		raws, err := h.hGetAllRaw(ctx, phys)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return loadResult[map[F]V]{}, err
			}
			return loadResult[map[F]V]{}, ErrNotFound // StaleL1
		}
		if len(raws) > 0 {
			m, derr := h.decodeAll(raws, biz)
			if derr == nil {
				c.metrics.Hit(LevelL2, c.name)
				return loadResult[map[F]V]{val: m, ok: true}, nil
			}
			// 脏数据/格式演进：清全部子 key，走全量回源自愈重写
			c.decodeError("l2:"+phys, derr)
			if serr := c.store.Del(ctx, h.subkeysAll(phys)...); serr != nil {
				c.logger.Warnf("gocache: purge corrupt %s for %s:%s: %v", biz, c.ns, c.name, serr)
			}
		}
		c.metrics.Miss(LevelL2, c.name)
	}
	if h.loader == nil {
		return loadResult[map[F]V]{}, ErrNotFound
	}
	start := time.Now()
	m, err := h.loader(ctx, key, nil)
	c.metrics.ObserveSourceLoad(c.name, time.Since(start))
	if err != nil {
		c.metrics.Error("loader", c.name)
		return loadResult[map[F]V]{}, err
	}
	if len(m) > 0 {
		c.metrics.Hit(LevelSource, c.name)
		writes := make(map[string]map[string][]byte)
		l1TTL := c.ttlJittered()
		for f, v := range m {
			fEnc, ferr := h.encField(f)
			if ferr != nil {
				return loadResult[map[F]V]{}, ferr
			}
			raw, data, verr := h.encValue(v)
			if verr != nil {
				return loadResult[map[F]V]{}, verr
			}
			sub := h.fieldPhys(phys, fEnc)
			if serr := c.checkOversize(sub, data); serr != nil {
				return loadResult[map[F]V]{}, serr
			}
			w, exists := writes[sub]
			if !exists {
				w = make(map[string][]byte, len(m))
				writes[sub] = w
			}
			w[fEnc] = data
			if c.l1 != nil {
				c.l1.Set(l1FieldKey(biz, fEnc), raw, l1TTL)
			}
		}
		if c.store != nil {
			// 整写失败不影响本次读结果（降级路径下属预期）
			if werr := c.store.HMSetBatch(ctx, l1TTL, c.refreshTTL, writes); werr != nil {
				c.metrics.Error("l2_write", c.name)
				c.logger.Warnf("gocache: backfill %s for %s:%s: %v", biz, c.ns, c.name, werr)
			}
		}
	} else {
		c.metrics.Miss(LevelSource, c.name)
	}
	return loadResult[map[F]V]{val: m, ok: true}, nil
}

// hGetAllRaw 整 hash 原始值：HGETALL（FieldShard 子 key 经 HGetAllBatch 单 pipeline 合并），全空视为 key 不存在。
func (h *TypedHashCache[K, F, V]) hGetAllRaw(ctx context.Context, phys string) (map[string][]byte, error) {
	mm, err := h.core.store.HGetAllBatch(ctx, h.subkeysAll(phys)...)
	if err != nil {
		return nil, err
	}
	merged := make(map[string][]byte)
	for _, m := range mm {
		for f, v := range m {
			merged[f] = v
		}
	}
	return merged, nil
}

// subkeysAll 全部物理子 key（FieldShard → N 个 :h{i}，否则整 key）。
func (h *TypedHashCache[K, F, V]) subkeysAll(phys string) []string {
	if h.core.big.Type == BigValueFieldShard {
		keys := make([]string, 0, h.core.big.FieldShard.ShardCount)
		for i := 0; i < h.core.big.FieldShard.ShardCount; i++ {
			keys = append(keys, fieldShardKey(phys, i))
		}
		return keys
	}
	return []string{phys}
}

// decodeAll 解码整 hash（L2 值解压 + 反序列化）并回填 L1 复合键。
func (h *TypedHashCache[K, F, V]) decodeAll(raws map[string][]byte, biz string) (map[F]V, error) {
	c := h.core
	m := make(map[F]V, len(raws))
	l1TTL := c.ttlJittered()
	for fEnc, data := range raws {
		f, err := h.decField(fEnc)
		if err != nil {
			return nil, err
		}
		raw, v, err := h.decCompressed(data)
		if err != nil {
			return nil, err
		}
		m[f] = v
		if c.l1 != nil {
			c.l1.Set(l1FieldKey(biz, fEnc), raw, l1TTL)
		}
	}
	return m, nil
}
