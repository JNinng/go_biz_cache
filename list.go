package gocache

import (
	"context"
	"time"

	"github.com/jninng/go_biz_cache/event"
)

// TypedListCache 强类型 list 缓存句柄。整 key 回源（ListLoader）；L1 缓存整列表，
// 读取按 redis LRANGE 语义切片；Range 空结果以 LLEN 判空区分越界窗口与缺失，Len==0 视为缺失触发重建。
type TypedListCache[K comparable, V any] struct {
	core   *defCore
	ser    Serializer
	loader ListLoader[K, V]
	sf     *Group[loadResult[[]V]]
}

// DefineList 定义并注册 List 缓存。
func DefineList[V any](ns, name string, def Definition[V], opts ListOptions[V]) (*TypedListCache[string, V], error) {
	core, err := register(kindList, ns, name, def, false)
	if err != nil {
		return nil, err
	}
	return &TypedListCache[string, V]{
		core:   core,
		ser:    resolveSerializer(def.Serializer),
		loader: opts.Loader,
		sf:     &Group[loadResult[[]V]]{},
	}, nil
}

// MustDefineList 同 DefineList，注册失败 panic。
func MustDefineList[V any](ns, name string, def Definition[V], opts ListOptions[V]) *TypedListCache[string, V] {
	h, err := DefineList(ns, name, def, opts)
	if err != nil {
		panic(err)
	}
	return h
}

// ---------- 编解码 ----------

// encValue 单元素序列化 + 压缩（Zstd 作用单元为逐 member）。
func (h *TypedListCache[K, V]) encValue(v V) ([]byte, error) {
	raw, err := h.core.serialize(h.ser, v)
	if err != nil {
		return nil, err
	}
	return h.core.compress(raw)
}

func (h *TypedListCache[K, V]) decValue(data []byte) (v V, err error) {
	raw, err := h.core.decompress(data)
	if err != nil {
		return
	}
	err = h.core.deserialize(h.ser, raw, &v)
	return
}

func (h *TypedListCache[K, V]) decValues(datas [][]byte) ([]V, error) {
	out := make([]V, 0, len(datas))
	for _, d := range datas {
		v, err := h.decValue(d)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// ---------- 写入 ----------

// LPush 头部插入：写 L2 → 删本实例 L1 → 广播 Updated。
func (h *TypedListCache[K, V]) LPush(ctx context.Context, key K, values ...V) error {
	return h.push(ctx, true, key, values...)
}

// RPush 尾部追加：写 L2 → 删本实例 L1 → 广播 Updated。
func (h *TypedListCache[K, V]) RPush(ctx context.Context, key K, values ...V) error {
	return h.push(ctx, false, key, values...)
}

func (h *TypedListCache[K, V]) push(ctx context.Context, left bool, key K, values ...V) error {
	if len(values) == 0 {
		return nil
	}
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	datas := make([][]byte, 0, len(values))
	for _, v := range values {
		data, err := h.encValue(v)
		if err != nil {
			return err
		}
		if err := c.checkOversize(phys, data); err != nil {
			return err
		}
		datas = append(datas, data)
	}
	ttl := c.ttlJittered()
	if c.store != nil {
		if left {
			err = c.store.LPush(ctx, phys, ttl, datas...)
		} else {
			err = c.store.RPush(ctx, phys, ttl, datas...)
		}
		if err != nil {
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

// Trim 裁剪区间（LRANGE 语义闭区间）：写 L2 → 删本实例 L1 → 广播 Updated。
func (h *TypedListCache[K, V]) Trim(ctx context.Context, key K, start, stop int64) error {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	if c.store != nil {
		if err := c.store.LTrim(ctx, phys, start, stop); err != nil {
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

// Range 区间读取：L1 整列表切片 → L2 LRANGE → 空结果以 LLEN 判空（越界窗口返回空，key 缺失重建）后切片。
func (h *TypedListCache[K, V]) Range(ctx context.Context, key K, start, stop int64) ([]V, error) {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return nil, err
	}
	if c.l1 != nil {
		if raw, ok := c.l1.Get(biz); ok {
			var items []V
			if err := c.deserialize(h.ser, raw, &items); err != nil {
				return nil, err
			}
			c.metrics.Hit(LevelL1, c.name)
			return sliceRange(items, int(start), int(stop)), nil
		}
		c.metrics.Miss(LevelL1, c.name)
	}
	if c.store != nil && c.degrade != DegradeSourceOnly {
		datas, err := c.store.LRange(ctx, phys, start, stop)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return nil, err
			}
			return nil, ErrNotFound // StaleL1：L1 已查无
		}
		if len(datas) > 0 {
			c.metrics.Hit(LevelL2, c.name)
			return h.decValues(datas)
		}
		// 空结果：key 存在（LLEN>0）→ 越界区间为合法空查询，不回源
		n, err := c.store.LLen(ctx, phys)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return nil, err
			}
			return nil, ErrNotFound
		}
		if n > 0 {
			c.metrics.Hit(LevelL2, c.name)
			return []V{}, nil
		}
		c.metrics.Miss(LevelL2, c.name)
	}
	// 空结果视为 key 缺失 → 整 key 回源重建后切片
	items, err := h.rebuild(ctx, key, biz, phys)
	if err != nil {
		return nil, err
	}
	return sliceRange(items, int(start), int(stop)), nil
}

// Len 长度：L1 → L2 LLEN → 空视为缺失整 key 回源重建。
func (h *TypedListCache[K, V]) Len(ctx context.Context, key K) (int64, error) {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return 0, err
	}
	if c.l1 != nil {
		if raw, ok := c.l1.Get(biz); ok {
			var items []V
			if err := c.deserialize(h.ser, raw, &items); err != nil {
				return 0, err
			}
			c.metrics.Hit(LevelL1, c.name)
			return int64(len(items)), nil
		}
		c.metrics.Miss(LevelL1, c.name)
	}
	if c.store != nil && c.degrade != DegradeSourceOnly {
		n, err := c.store.LLen(ctx, phys)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return 0, err
			}
			return 0, ErrNotFound
		}
		if n > 0 {
			c.metrics.Hit(LevelL2, c.name)
			return n, nil
		}
		c.metrics.Miss(LevelL2, c.name)
	}
	items, err := h.rebuild(ctx, key, biz, phys)
	if err != nil {
		return 0, err
	}
	return int64(len(items)), nil
}

// ---------- 失效 ----------

// Delete 失效整 list：DEL 物理 key → 删本实例 L1 → 广播 Invalidated。
func (h *TypedListCache[K, V]) Delete(ctx context.Context, key K) error {
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
func (h *TypedListCache[K, V]) InvalidateAll(ctx context.Context) error {
	return h.core.invalidateAll(ctx)
}

// ---------- 内部路径 ----------

// rebuild 整 key 回源重建：回源前 DEL 旧 key 再 RPush 整写；空列表同样回填 L1。
func (h *TypedListCache[K, V]) rebuild(ctx context.Context, key K, biz, phys string) ([]V, error) {
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
	if len(items) > 0 {
		c.metrics.Hit(LevelSource, c.name)
	} else {
		c.metrics.Miss(LevelSource, c.name)
	}
	ttl := c.ttlJittered()
	if c.store != nil {
		// 回源-整写窗口内并发写可能被重建覆盖，语义 last-write-win（设计 §6）；
		// L2 回写失败不影响本次读结果（降级路径下属预期），Del 失败时跳过整写防重复追加
		if err := c.store.Del(ctx, phys); err != nil {
			c.metrics.Error("l2_write", c.name)
			c.logger.Warnf("gocache: rebuild clear %s for %s:%s: %v", biz, c.ns, c.name, err)
		} else if len(items) > 0 {
			datas := make([][]byte, 0, len(items))
			for _, v := range items {
				data, err := h.encValue(v)
				if err != nil {
					return nil, err
				}
				if err := c.checkOversize(phys, data); err != nil {
					return nil, err
				}
				datas = append(datas, data)
			}
			if err := c.store.RPush(ctx, phys, ttl, datas...); err != nil {
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
