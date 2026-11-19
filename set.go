package gocache

import (
	"bytes"
	"context"
	"time"

	"github.com/jninng/go_biz_cache/event"
)

// TypedSetCache 强类型 set 缓存句柄。集合语义整 key 回源（SetLoader），回源前 DEL 子键再写
// 避免残留；无负缓存占位（防穿透仅适用于单 key / 单 field 语义）。
// FieldShard 按 member 定位 :h{i} 子 key；L1 缓存整集合序列化字节。
type TypedSetCache[K comparable, M any] struct {
	core   *defCore
	ser    Serializer
	loader SetLoader[K, M]
	sf     *Group[loadResult[[]M]]
}

// DefineSet 定义并注册 Set 缓存。
func DefineSet[M any](ns, name string, def Definition[M], opts SetOptions[M]) (*TypedSetCache[string, M], error) {
	core, err := register(kindSet, ns, name, def, false)
	if err != nil {
		return nil, err
	}
	return &TypedSetCache[string, M]{
		core:   core,
		ser:    resolveSerializer(def.Serializer),
		loader: opts.Loader,
		sf:     &Group[loadResult[[]M]]{},
	}, nil
}

// MustDefineSet 同 DefineSet，注册失败 panic。
func MustDefineSet[M any](ns, name string, def Definition[M], opts SetOptions[M]) *TypedSetCache[string, M] {
	h, err := DefineSet(ns, name, def, opts)
	if err != nil {
		panic(err)
	}
	return h
}

// ---------- 编解码 / 定位 ----------

// encMember 单 member 序列化 + 压缩（Zstd 作用单元为逐 member）。
func (h *TypedSetCache[K, M]) encMember(m M) ([]byte, error) {
	raw, err := h.core.serialize(h.ser, m)
	if err != nil {
		return nil, err
	}
	return h.core.compress(raw)
}

// decMember 解压 + 反序列化单 member。
func (h *TypedSetCache[K, M]) decMember(data []byte) (m M, err error) {
	raw, err := h.core.decompress(data)
	if err != nil {
		return
	}
	err = h.core.deserialize(h.ser, raw, &m)
	return
}

func (h *TypedSetCache[K, M]) decMembers(datas [][]byte) ([]M, error) {
	out := make([]M, 0, len(datas))
	for _, d := range datas {
		m, err := h.decMember(d)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// subkeys 全部物理子 key（FieldShard → N 个 :h{i}，否则整 key）。
func (h *TypedSetCache[K, M]) subkeys(phys string) []string {
	if h.core.big.Type == BigValueFieldShard {
		keys := make([]string, 0, h.core.big.FieldShard.ShardCount)
		for i := 0; i < h.core.big.FieldShard.ShardCount; i++ {
			keys = append(keys, fieldShardKey(phys, i))
		}
		return keys
	}
	return []string{phys}
}

// ---------- 读取 ----------

// Members 全成员读取：L1 整集合 → L2（FieldShard 合并 SMEMBERS×N）→ 整 key 回源重建。
func (h *TypedSetCache[K, M]) Members(ctx context.Context, key K) ([]M, error) {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return nil, err
	}
	// L1 整集合（解码失败视为脏数据删条目走 L2/回源自愈）
	if c.l1 != nil {
		if raw, ok := c.l1.Get(biz); ok {
			var members []M
			if err := c.deserialize(h.ser, raw, &members); err == nil {
				c.metrics.Hit(LevelL1, c.name)
				return members, nil
			} else {
				c.decodeError("l1:"+biz, err)
				c.l1.Delete(biz)
			}
		} else {
			c.metrics.Miss(LevelL1, c.name)
		}
	}
	if c.store != nil && c.degrade != DegradeSourceOnly {
		datas, err := h.sMembers(ctx, phys)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return nil, err
			}
			return nil, ErrNotFound // StaleL1：L1 已查无
		}
		if len(datas) > 0 {
			c.metrics.Hit(LevelL2, c.name)
			members, derr := h.decMembers(datas)
			if derr == nil {
				if c.l1 != nil {
					if raw, serr := c.serialize(h.ser, members); serr == nil {
						c.l1.Set(biz, raw, c.ttlJittered())
					}
				}
				return members, nil
			}
			// 脏数据：清全部子 key，走回源自愈重写
			c.decodeError("l2:"+phys, derr)
			if serr := c.store.Del(ctx, h.subkeys(phys)...); serr != nil {
				c.logger.Warnf("gocache: purge corrupt %s for %s:%s: %v", biz, c.ns, c.name, serr)
			}
		} else {
			c.metrics.Miss(LevelL2, c.name)
		}
	}
	// 未缓存 → 整 key 回源重建
	res, err, shared := h.sf.Do(sfLoadPrefix+biz, func() (loadResult[[]M], error) {
		members, err := h.loadAll(ctx, key, biz, phys)
		if err != nil {
			return loadResult[[]M]{}, err
		}
		return loadResult[[]M]{val: members, ok: true}, nil
	})
	if shared {
		c.metrics.SingleflightShared(c.name, 1)
	}
	if err != nil {
		return nil, err
	}
	return res.val, nil
}

// IsMember 成员判定：L2 点查定位子 key（不触发回源、不走 L1）；
// 本地模式 / SourceOnly 退化为整 key 回源后内存判定。
func (h *TypedSetCache[K, M]) IsMember(ctx context.Context, key K, m M) (bool, error) {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return false, err
	}
	mEnc, err := h.encMember(m)
	if err != nil {
		return false, err
	}
	if c.store != nil && c.degrade != DegradeSourceOnly {
		ok, err := c.store.SIsMember(ctx, c.memberPhysKey(phys, mEnc), mEnc)
		if err != nil {
			c.metrics.Error("l2_read", c.name)
			if c.degrade == DegradeFail {
				return false, err
			}
			return false, nil // StaleL1：按陈旧缺失处理
		}
		c.metrics.Hit(LevelL2, c.name)
		return ok, nil
	}
	members, err := h.membersViaLoader(ctx, key, biz, phys)
	if err != nil {
		return false, err
	}
	for _, m2 := range members {
		e, err := h.encMember(m2)
		if err == nil && bytes.Equal(e, mEnc) {
			return true, nil
		}
	}
	return false, nil
}

// ---------- 写入 / 失效 ----------

// Add 加成员：按 member 定位子 key SAdd（ExpireNX 语义）→ 删本实例 L1 → 广播 Updated。
func (h *TypedSetCache[K, M]) Add(ctx context.Context, key K, members ...M) error {
	if len(members) == 0 {
		return nil
	}
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	groups := make(map[string][][]byte)
	for _, m := range members {
		data, err := h.encMember(m)
		if err != nil {
			return err
		}
		sub := c.memberPhysKey(phys, data)
		if err := c.checkOversize(sub, data); err != nil {
			return err
		}
		groups[sub] = append(groups[sub], data)
	}
	ttl := c.ttlJittered()
	if c.store != nil {
		if err := c.store.SAddBatch(ctx, ttl, groups); err != nil {
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

// Remove 删成员：SRem 定位子 key → 删本实例 L1 → 广播 Updated。
func (h *TypedSetCache[K, M]) Remove(ctx context.Context, key K, members ...M) error {
	if len(members) == 0 {
		return nil
	}
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	groups := make(map[string][][]byte)
	for _, m := range members {
		data, err := h.encMember(m)
		if err != nil {
			return err
		}
		sub := c.memberPhysKey(phys, data)
		groups[sub] = append(groups[sub], data)
	}
	if c.store != nil {
		for sub, datas := range groups {
			if err := c.store.SRem(ctx, sub, datas...); err != nil {
				c.metrics.Error("l2_write", c.name)
				return err
			}
		}
	}
	if c.l1 != nil {
		c.l1.Delete(biz)
	}
	c.publish(ctx, event.Updated, []string{phys})
	return nil
}

// Delete 失效整 set：DEL 全部子 key → 删本实例 L1 → 广播 Invalidated。
func (h *TypedSetCache[K, M]) Delete(ctx context.Context, key K) error {
	c := h.core
	biz := keyString(key)
	_, phys, err := c.buildKey(ctx, biz)
	if err != nil {
		return err
	}
	if c.store != nil {
		if err := c.store.Del(ctx, h.subkeys(phys)...); err != nil {
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
func (h *TypedSetCache[K, M]) InvalidateAll(ctx context.Context) error {
	return h.core.invalidateAll(ctx)
}

// ---------- 内部路径 ----------

// sMembers 全成员原始值：SMEMBERS（FieldShard 子 key 经 SMembersBatch 单 pipeline 合并）。
func (h *TypedSetCache[K, M]) sMembers(ctx context.Context, phys string) ([][]byte, error) {
	c := h.core
	keys := []string{phys}
	if c.big.Type == BigValueFieldShard {
		keys = keys[:0]
		for i := 0; i < c.big.FieldShard.ShardCount; i++ {
			keys = append(keys, fieldShardKey(phys, i))
		}
	}
	mm, err := c.store.SMembersBatch(ctx, keys...)
	if err != nil {
		return nil, err
	}
	var merged [][]byte
	for _, datas := range mm {
		merged = append(merged, datas...)
	}
	return merged, nil
}

// membersViaLoader 经 singleflight 整 key 回源（IsMember 本地判定用）。
func (h *TypedSetCache[K, M]) membersViaLoader(ctx context.Context, key K, biz, phys string) ([]M, error) {
	res, err, shared := h.sf.Do(sfLoadPrefix+biz, func() (loadResult[[]M], error) {
		members, err := h.loadAll(ctx, key, biz, phys)
		if err != nil {
			return loadResult[[]M]{}, err
		}
		return loadResult[[]M]{val: members, ok: true}, nil
	})
	if shared {
		h.core.metrics.SingleflightShared(h.core.name, 1)
	}
	return res.val, err
}

// loadAll 整 key 回源重建：回源前 DEL 子键再写避免残留；空集合同样回填 L1（后续读命中空值不再回源）。
func (h *TypedSetCache[K, M]) loadAll(ctx context.Context, key K, biz, phys string) ([]M, error) {
	c := h.core
	if h.loader == nil {
		return nil, ErrNotFound
	}
	start := time.Now()
	members, err := h.loader(ctx, key)
	c.metrics.ObserveSourceLoad(c.name, time.Since(start))
	if err != nil {
		c.metrics.Error("loader", c.name)
		return nil, err
	}
	// 集合语义：loader 成功即命中源（空集合是合法源数据，与"源缺失"不混淆）
	c.metrics.Hit(LevelSource, c.name)
	ttl := c.ttlJittered()
	if c.store != nil {
		// 回源-整写窗口内并发写可能被重建覆盖，语义 last-write-win（设计 §6）；
		// L2 回写失败不影响本次读结果（降级路径下属预期），Del 失败时跳过整写防残留叠加
		if err := c.store.Del(ctx, h.subkeys(phys)...); err != nil {
			c.metrics.Error("l2_write", c.name)
			c.logger.Warnf("gocache: rebuild clear %s for %s:%s: %v", biz, c.ns, c.name, err)
		} else {
			groups := make(map[string][][]byte)
			for _, m := range members {
				data, err := h.encMember(m)
				if err != nil {
					return nil, err
				}
				sub := c.memberPhysKey(phys, data)
				if err := c.checkOversize(sub, data); err != nil {
					return nil, err
				}
				groups[sub] = append(groups[sub], data)
			}
			if err := c.store.SAddBatch(ctx, ttl, groups); err != nil {
				c.metrics.Error("l2_write", c.name)
				c.logger.Warnf("gocache: rebuild write %s for %s:%s: %v", biz, c.ns, c.name, err)
			}
		}
	}
	if c.l1 != nil {
		if raw, serr := c.serialize(h.ser, members); serr == nil {
			c.l1.Set(biz, raw, ttl)
		}
	}
	return members, nil
}
