package bigvalue

import (
	"bytes"
	"testing"
)

func TestCompressRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("compress-me-repeated-content-"), 500)
	for _, level := range []CompressLevel{Speed, Default, High, 0} {
		data, err := Compress(payload, level, 0)
		if err != nil {
			t.Fatalf("Compress level=%d: %v", level, err)
		}
		if data[0] != headerZstd {
			t.Fatalf("level=%d: expect zstd header, got %d", level, data[0])
		}
		if len(data) >= len(payload) {
			t.Fatalf("level=%d: no compression gain (%d >= %d)", level, len(data), len(payload))
		}
		got, err := Decompress(data)
		if err != nil {
			t.Fatalf("Decompress: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("level=%d: roundtrip mismatch", level)
		}
	}
}

func TestCompressThreshold(t *testing.T) {
	small := []byte("tiny")
	data, err := Compress(small, Default, 1024)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	if data[0] != headerRaw {
		t.Fatalf("below threshold must store raw, header=%d", data[0])
	}
	if !bytes.Equal(data[1:], small) {
		t.Fatal("raw payload mismatch")
	}
	got, err := Decompress(data)
	if err != nil || !bytes.Equal(got, small) {
		t.Fatalf("Decompress raw = %v", err)
	}
	// threshold <= 0 全压缩
	data, err = Compress(small, Default, 0)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	if data[0] != headerZstd {
		t.Fatal("threshold<=0 must compress")
	}
	// Decompress 空输入透传
	got, err = Decompress(nil)
	if err != nil || got != nil {
		t.Fatalf("Decompress(nil) = %v,%v", got, err)
	}
	// 未知标记头 = 数据损坏 → 报错（调用方走脏数据自愈）
	if _, err = Decompress(append([]byte{0xFF}, small...)); err == nil {
		t.Fatal("unknown header must error")
	}
}

func TestSplitEven(t *testing.T) {
	payload := []byte("0123456789abcdef") // 16 字节 → 4 片各 4 字节
	parts, err := SplitEven("k", payload, 4)
	if err != nil {
		t.Fatalf("SplitEven: %v", err)
	}
	if len(parts) != 4 {
		t.Fatalf("parts = %d", len(parts))
	}
	var joined []byte
	for _, p := range parts {
		joined = append(joined, p...)
	}
	if !bytes.Equal(joined, payload) {
		t.Fatalf("concat mismatch: %q", joined)
	}
	// 非整除：17 字节 4 片 → ceil = 5，第 4 片 2 字节
	parts, err = SplitEven("k", append(payload, 'z'), 4)
	if err != nil || len(parts) != 4 {
		t.Fatalf("parts = %v,%v", parts, err)
	}
	if len(parts[3]) != 2 {
		t.Fatalf("last shard len = %d", len(parts[3]))
	}
	// 空值：全部空分片，数量恒定
	parts, err = SplitEven("k", nil, 3)
	if err != nil || len(parts) != 3 {
		t.Fatalf("empty value parts = %v,%v", parts, err)
	}
	// 确定性
	a, _ := SplitEven("k", payload, 4)
	b, _ := SplitEven("k", payload, 4)
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Fatal("SplitEven must be deterministic")
		}
	}
	// 非法分片数
	if _, err := SplitEven("k", payload, 0); err == nil {
		t.Fatal("shard count 0 must error")
	}
}

func TestFieldShardIndex(t *testing.T) {
	// 默认 fnv1a 确定性
	a := FieldShardIndex([]byte("nickname"), 16, nil)
	b := FieldShardIndex([]byte("nickname"), 16, nil)
	if a != b || a < 0 || a >= 16 {
		t.Fatalf("index = %d,%d", a, b)
	}
	// 自定义哈希
	custom := func(member []byte) uint64 { return 42 }
	if got := FieldShardIndex([]byte("x"), 16, custom); got != 42%16 {
		t.Fatalf("custom hasher index = %d", got)
	}
	// FNV1a 已知向量（空串 offset basis）
	if FNV1a(nil) != 14695981039346656037 {
		t.Fatalf("FNV1a(nil) = %d", FNV1a(nil))
	}
	// 分布冒烟：100 个成员覆盖多数分片
	seen := map[int]bool{}
	for i := 0; i < 100; i++ {
		seen[FieldShardIndex([]byte(string(rune('a'+i%26))+string(rune('a'+i/26))), 8, nil)] = true
	}
	if len(seen) < 4 {
		t.Fatalf("distribution too narrow: %v", seen)
	}
}
