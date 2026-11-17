package bigvalue

// Hasher 自定义成员哈希（契约同切分器：纯函数、确定性、无状态）。
type Hasher func(member []byte) uint64

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// FNV1a 默认成员哈希（FNV-1a 64 位）。
func FNV1a(b []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

// FieldShardIndex 成员 → 子 key 分片下标：hasher(member) % shardCount。
func FieldShardIndex(member []byte, shardCount int, hasher Hasher) int {
	if hasher == nil {
		hasher = FNV1a
	}
	return int(hasher(member) % uint64(shardCount))
}
