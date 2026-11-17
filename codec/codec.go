// Package codec 提供缓存值序列化器（Serializer）与 hash field 编解码器（FieldCodec）。
package codec

import (
	"encoding/json"
	"fmt"
)

// Serializer 序列化器接口：缓存值类型 V 经其编码为字节。
type Serializer interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	Name() string
}

// FieldCodec hash field 编解码器：field 类型 F ↔ Redis field 名。
type FieldCodec[F any] interface {
	EncodeField(F) (string, error)
	DecodeField(string) (F, error)
}

type jsonSerializer struct{}

func (jsonSerializer) Marshal(v any) ([]byte, error)          { return json.Marshal(v) }
func (jsonSerializer) Unmarshal(data []byte, v any) error     { return json.Unmarshal(data, v) }
func (jsonSerializer) Name() string                           { return "json" }

// JSON 默认 JSON 序列化器。
var JSON Serializer = jsonSerializer{}

// FieldCodecString string 直通 field 编解码器（F = string）。
func FieldCodecString() FieldCodec[string] {
	return fieldCodecFunc[string]{
		enc: func(s string) (string, error) { return s, nil },
		dec: func(s string) (string, error) { return s, nil },
	}
}

// FieldCodecFunc 由转换函数构造 field 编解码器。
func FieldCodecFunc[F any](enc func(F) (string, error), dec func(string) (F, error)) FieldCodec[F] {
	return fieldCodecFunc[F]{enc: enc, dec: dec}
}

// FieldCodecMap 由双向映射表构造 field 编解码器（适用于枚举类型）。
func FieldCodecMap[F comparable](enc map[F]string, dec map[string]F) FieldCodec[F] {
	return fieldCodecFunc[F]{
		enc: func(f F) (string, error) {
			s, ok := enc[f]
			if !ok {
				return "", fmt.Errorf("codec: no field encoding for %v", f)
			}
			return s, nil
		},
		dec: func(s string) (F, error) {
			f, ok := dec[s]
			if !ok {
				var zero F
				return zero, fmt.Errorf("codec: no field decoding for %q", s)
			}
			return f, nil
		},
	}
}

type fieldCodecFunc[F any] struct {
	enc func(F) (string, error)
	dec func(string) (F, error)
}

func (c fieldCodecFunc[F]) EncodeField(f F) (string, error) { return c.enc(f) }
func (c fieldCodecFunc[F]) DecodeField(s string) (F, error) { return c.dec(s) }
