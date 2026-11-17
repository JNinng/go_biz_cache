// Package bigvalue 实现大 key 处理策略：zstd 压缩、内容切分与成员哈希分片。
// 明确策略四选一，不做自动叠加；非法参数在定义注册时校验，行为契约由写路径运行时断言兜底。
package bigvalue

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

// CompressLevel 压缩级别：压缩率与 CPU 开销的取舍。零值按 Default 处理。
type CompressLevel int

const (
	Speed   CompressLevel = 1 // 最快
	Default CompressLevel = 2 // 默认
	High    CompressLevel = 3 // 高压缩率
)

const (
	headerRaw  byte = 0 // 未压缩
	headerZstd byte = 1 // zstd 压缩
)

func zstdLevel(l CompressLevel) zstd.EncoderLevel {
	switch l {
	case Speed:
		return zstd.SpeedFastest
	case High:
		return zstd.SpeedBetterCompression
	default:
		return zstd.SpeedDefault
	}
}

var (
	encOnce sync.Once
	encs    [4]*zstd.Encoder
	decOnce sync.Once
	dec     *zstd.Decoder
)

func encoderFor(l CompressLevel) *zstd.Encoder {
	encOnce.Do(func() {
		opts := []zstd.EOption{zstd.WithEncoderConcurrency(1)}
		for lv := Speed; lv <= High; lv++ {
			e, err := zstd.NewWriter(nil, append(opts, zstd.WithEncoderLevel(zstdLevel(lv)))...)
			if err != nil {
				panic(err)
			}
			encs[lv] = e
		}
	})
	return encs[l]
}

func decoder() *zstd.Decoder {
	decOnce.Do(func() {
		d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			panic(err)
		}
		dec = d
	})
	return dec
}

// Compress 带标记头的透明压缩：序列化后字节数 ≥ threshold 才压缩（threshold <= 0 全压缩），否则原样存。
// 作用单元为序列化后的单值：Object 整值、Hash 逐 field 值、集合逐 member。
// level 零值（或越界）按 Default 处理。
func Compress(data []byte, level CompressLevel, threshold int) ([]byte, error) {
	if level < Speed || level > High {
		level = Default
	}
	if threshold > 0 && len(data) < threshold {
		out := make([]byte, len(data)+1)
		out[0] = headerRaw
		copy(out[1:], data)
		return out, nil
	}
	z := encoderFor(level).EncodeAll(data, nil)
	out := make([]byte, len(z)+1)
	out[0] = headerZstd
	copy(out[1:], z)
	return out, nil
}

// Decompress 按标记头透明解压。
func Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}
	switch data[0] {
	case headerRaw:
		return data[1:], nil
	case headerZstd:
		return decoder().DecodeAll(data[1:], nil)
	default:
		return data[1:], nil
	}
}
