package gocache

import (
	"fmt"

	"github.com/jninng/go_biz_cache/l1"
	"github.com/jninng/go_biz_cache/l2"
)

func validateBigValue(kind structKind, def BigValuePolicy) error {
	if def.Type == BigValueShard && kind != kindObject {
		return fmt.Errorf("%w: BigValueShard only applies to object", ErrInvalidDefinition)
	}
	if def.Type == BigValueFieldShard && kind != kindHash && kind != kindSet {
		return fmt.Errorf("%w: BigValueFieldShard only applies to hash/set (zset/list are ordered, member sharding cannot keep order)", ErrInvalidDefinition)
	}
	if def.Type == BigValueShard && def.Shard.ShardCount < 2 {
		return fmt.Errorf("%w: shard count must be >= 2", ErrInvalidDefinition)
	}
	if def.Type == BigValueFieldShard && def.FieldShard.ShardCount < 2 {
		return fmt.Errorf("%w: field shard count must be >= 2", ErrInvalidDefinition)
	}
	if def.Type == BigValueZstd && def.Zstd.CompressThreshold < 0 {
		return fmt.Errorf("%w: compress threshold must be >= 0", ErrInvalidDefinition)
	}
	return nil
}

func clampJitter(j float64) float64 {
	if j < 0 {
		return 0
	}
	if j > 0.5 {
		return 0.5
	}
	return j
}

// resolveL1 组装 L1（默认分片 LRU 或注入实现）。
func resolveL1(opt L1Options) l1.L1Cache {
	if !opt.Enabled {
		return nil
	}
	if opt.Cache != nil {
		return opt.Cache
	}
	size := opt.Size
	if size <= 0 {
		size = defaultL1Size
	}
	return l1.NewLRU(size)
}

// resolveL2 组装 L2：Enabled 且有客户端（或直接注入 Store）才启用，否则纯本地模式。
func resolveL2(opt L2Options) l2.Store {
	if !opt.Enabled {
		return nil
	}
	if opt.Store != nil {
		return opt.Store
	}
	glMu.Lock()
	p := opt.ClientProvider
	if p == nil {
		p = glProvider
	}
	glMu.Unlock()
	if p == nil {
		return nil
	}
	timeout := opt.Timeout
	if timeout <= 0 {
		timeout = defaultL2Timeout
	}
	return l2.NewRedisStore(p, timeout)
}
