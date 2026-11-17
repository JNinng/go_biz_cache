// Package l1 提供进程内本地缓存（L1）抽象与默认实现。
package l1

import "time"

// L1Cache 本地缓存接口（Set 携带 TTL 参数，负缓存条目的短 TTL 过期依赖此）。
//
// 契约：
//   - 并发安全；
//   - 条目按 TTL 过期；
//   - value 为序列化后的字节，L1 不参与大 key 策略，原样存取；
//   - value 为 nil 且 ok 为 true 表示负缓存条目（短 TTL 过期）；
//   - Get 返回的切片为只读内部状态，调用方不得修改（实现可按需防御拷贝写入值）。
type L1Cache interface {
	Get(key string) (value []byte, ok bool)
	Set(key string, value []byte, ttl time.Duration)
	Delete(keys ...string)
	Clear()
	Size() int
}
