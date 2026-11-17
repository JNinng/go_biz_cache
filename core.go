package gocache

import (
	"context"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/jninng/go_biz_cache/bigvalue"
)

// buildKey 构建逻辑键（含 ns/name/gen 头）与变换后物理 key。
func (c *defCore) buildKey(ctx context.Context, bizKey string) (logical, phys string, err error) {
	var parts map[string]string
	if c.tag != nil {
		if parts, err = extractParts(c.template, bizKey); err != nil {
			return "", "", err
		}
	}
	gen := c.gen.current(ctx)
	logical = c.ns + ":" + c.name + ":v" + strconv.FormatInt(gen, 10) + ":" + bizKey
	if c.tag != nil {
		op, terr := c.tag(TagInput{LogicalKey: logical, Parts: parts, NS: c.ns, Name: c.name})
		if terr != nil {
			return "", "", terr
		}
		if phys, err = applyTag(op, logical); err != nil {
			return "", "", err
		}
	} else {
		phys = logical
	}
	return logical, phys, nil
}

func (c *defCore) ttlJittered() time.Duration {
	if c.jitter <= 0 {
		return c.ttl
	}
	f := 1 + (rand.Float64()*2-1)*c.jitter
	d := time.Duration(float64(c.ttl) * f)
	if d < time.Second {
		d = time.Second
	}
	return d
}

func (c *defCore) negKey(phys string) string { return phys + ":__neg" }

// fieldNegKey hash field 负缓存标记：{物理key}:__neg:{fnv1a(field)}（与 FieldShard 正交，独立于子 hash）。
func (c *defCore) fieldNegKey(phys, field string) string {
	return phys + ":__neg:" + strconv.FormatUint(bigvalue.FNV1a([]byte(field)), 16)
}

func shardKey(phys string, i int) string      { return phys + ":s" + strconv.Itoa(i) }
func fieldShardKey(phys string, i int) string { return phys + ":h" + strconv.Itoa(i) }

// memberIndex 成员 → 分片下标。
func (c *defCore) memberIndex(member []byte) int {
	return bigvalue.FieldShardIndex(member, c.big.FieldShard.ShardCount, c.big.FieldShard.Hasher)
}

// memberPhysKey set 成员定位的物理 key（FieldShard → :h{i}）。
func (c *defCore) memberPhysKey(phys string, member []byte) string {
	if c.big.Type == BigValueFieldShard {
		return fieldShardKey(phys, c.memberIndex(member))
	}
	return phys
}

// toBizKey 事件订阅方：物理 key → 业务键（逆变换 + 剥离 ns/name/gen 头）。
func (c *defCore) toBizKey(physical string) (string, bool) {
	logical := physical
	if c.tag != nil {
		if !c.probeOK {
			return "", false
		}
		l, ok := inverseTag(c.probe, physical)
		if !ok {
			return "", false
		}
		logical = l
	}
	prefix := c.ns + ":" + c.name + ":v"
	if !strings.HasPrefix(logical, prefix) {
		return "", false
	}
	rest := logical[len(prefix):]
	j := strings.IndexByte(rest, ':')
	if j < 0 {
		return "", false
	}
	return rest[j+1:], true
}
