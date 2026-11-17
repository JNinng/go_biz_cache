package gocache

import (
	"fmt"
	"time"

	"github.com/jninng/go_biz_cache/bigvalue"
)

// serialize / deserialize：序列化开销计入指标。
func (c *defCore) serialize(ser Serializer, v any) ([]byte, error) {
	start := time.Now()
	data, err := ser.Marshal(v)
	c.metrics.ObserveCodec("serialize", c.name, time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("gocache: serialize %s:%s: %w", c.ns, c.name, err)
	}
	return data, nil
}

func (c *defCore) deserialize(ser Serializer, data []byte, out any) error {
	start := time.Now()
	err := ser.Unmarshal(data, out)
	c.metrics.ObserveCodec("deserialize", c.name, time.Since(start))
	if err != nil {
		return fmt.Errorf("gocache: deserialize %s:%s: %w", c.ns, c.name, err)
	}
	return nil
}

// compress / decompress：Zstd 作用于序列化后的单值。
func (c *defCore) compress(data []byte) ([]byte, error) {
	if c.big.Type != BigValueZstd {
		return data, nil
	}
	level := c.big.Zstd.Level
	if level == 0 {
		level = bigvalue.Default
	}
	start := time.Now()
	out, err := bigvalue.Compress(data, level, c.big.Zstd.CompressThreshold)
	c.metrics.ObserveCodec("compress", c.name, time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("gocache: compress %s:%s: %w", c.ns, c.name, err)
	}
	return out, nil
}

func (c *defCore) decompress(data []byte) ([]byte, error) {
	if c.big.Type != BigValueZstd {
		return data, nil
	}
	start := time.Now()
	out, err := bigvalue.Decompress(data)
	c.metrics.ObserveCodec("decompress", c.name, time.Since(start))
	if err != nil {
		return nil, fmt.Errorf("gocache: decompress %s:%s: %w", c.ns, c.name, err)
	}
	return out, nil
}

// checkOversize 单片写入仍超限时报错，不做隐式压缩兜底；超大值写入计入指标。
func (c *defCore) checkOversize(key string, data []byte) error {
	if len(data) > maxPhysicalValue {
		c.metrics.OversizeWrite(c.name)
		return fmt.Errorf("%w: physical value %d bytes exceeds %d limit: %s", ErrContract, len(data), maxPhysicalValue, key)
	}
	if len(data) > oversizeReport {
		c.metrics.OversizeWrite(c.name)
	}
	return nil
}

// splitObject Object 内容切分（仅写路径调用；读路径按 ShardCount 静态推算 MGET）。
func (c *defCore) splitObject(bizKey string, value []byte) ([][]byte, error) {
	n := c.big.Shard.ShardCount
	splitter := c.big.Shard.Splitter
	if splitter == nil {
		splitter = bigvalue.SplitEven
	}
	parts, err := splitter(bizKey, value, n)
	if err != nil {
		return nil, fmt.Errorf("%w: splitter: %v", ErrContract, err)
	}
	if len(parts) != n {
		return nil, fmt.Errorf("%w: splitter returned %d shards, want %d", ErrContract, len(parts), n)
	}
	return parts, nil
}
