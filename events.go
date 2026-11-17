package gocache

import (
	"context"

	"github.com/jninng/go_biz_cache/event"
)

// publish 发布事件（无总线时跳过；发布失败仅记日志）。
func (c *defCore) publish(ctx context.Context, t event.Type, physKeys []string) {
	if c.bus == nil {
		return
	}
	e := event.Event{Type: t, Namespace: c.ns, DefName: c.name, Keys: physKeys, Instance: c.instance}
	if err := c.bus.Publish(ctx, e); err != nil {
		c.logger.Warnf("gocache: publish %s for %s:%s: %v", t, c.ns, c.name, err)
	}
}

// publishGen 广播 GenerationBumped 事件（无总线时跳过；发布失败仅记日志）。
func (c *defCore) publishGen(ctx context.Context, gen int64) {
	if c.bus == nil {
		return
	}
	e := event.Event{Type: event.GenerationBumped, Namespace: c.ns, DefName: c.name, Gen: gen, Instance: c.instance}
	if err := c.bus.Publish(ctx, e); err != nil {
		c.logger.Warnf("gocache: publish generation_bumped for %s:%s: %v", c.ns, c.name, err)
	}
}

// invalidateAll 批次全量失效：世代号 INCR + 清本实例 L1 + 广播 GenerationBumped；旧 key 靠 TTL 过期。
func (c *defCore) invalidateAll(ctx context.Context) error {
	gen, err := c.gen.bump(ctx)
	if err != nil {
		c.metrics.Error("invalidate_all", c.name)
		return err
	}
	if c.l1 != nil {
		c.l1.Clear()
	}
	c.publishGen(ctx, gen)
	return nil
}

// onEvent 订阅方（§8）：Updated/Invalidated 删除对应 L1 条目（物理 key 逆变换回业务键）；
// GenerationBumped 即时更新本地世代号并整代清空 L1。自身事件过滤（发起实例已同步处理）。
func (c *defCore) onEvent(e event.Event) {
	if e.Namespace != c.ns || e.DefName != c.name || e.Instance == c.instance {
		return
	}
	switch e.Type {
	case event.GenerationBumped:
		c.gen.external(e.Gen)
		if c.l1 != nil {
			c.l1.Clear()
		}
	case event.Updated, event.Invalidated:
		if c.l1 == nil || len(e.Keys) == 0 {
			return
		}
		if c.kind == kindHash {
			// hash L1 为 业务键+\x1f+field 复合键，事件仅携带整 key → 退化为整定义清理
			c.l1.Clear()
			return
		}
		keys := make([]string, 0, len(e.Keys))
		for _, pk := range e.Keys {
			biz, ok := c.toBizKey(pk)
			if !ok {
				c.logger.Warnf("gocache: cannot inverse-map key %q for %s:%s (clear L1)", pk, c.ns, c.name)
				c.l1.Clear()
				return
			}
			keys = append(keys, biz)
		}
		c.l1.Delete(keys...)
	}
}
