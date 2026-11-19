package gocache

import (
	"fmt"
	"sync"
)

// ---------- 句柄共享设施 ----------

// keyString 业务键归一（模板展开后的业务键，句柄方法入参即业务键）；导出句柄均固定 K = string，非 string 键经 fmt.Sprint 归一。
func keyString[K comparable](k K) string {
	if s, ok := any(k).(string); ok {
		return s
	}
	return fmt.Sprint(k)
}

// keyOf 业务键 → K（keyString 的逆；仅支持 K = string，其余类型返回零值）。
func keyOf[K comparable](biz string) K {
	var zero K
	if p, ok := any(&zero).(*string); ok {
		*p = biz
	}
	return zero
}

// loadResult 单条目加载结果；ok=false 表示源数据缺失（触发负缓存）。
type loadResult[V any] struct {
	val V
	ok  bool
}

const (
	// l1FieldSep hash L1 复合键分隔符：业务键+\x1f+fieldEnc（\x1f 为控制字符，约定 field 编码与业务键不含该字符）。
	l1FieldSep = "\x1f"

	sfLoadPrefix    = "load:"    // object/hash/set/zset 回源 singleflight
	sfRefreshPrefix = "refresh:" // object Refresh
)

// negMarker L2 负缓存标记值（独立短 TTL string key，存在即命中，内容无意义）。
var negMarker = []byte{1}

// loadBatch 批量回源协调器：同批次 pending 条目由首个到手的 leader 一次批量加载
// （批量契约，不循环单查），其余条目等待共享结果；跨调用同条目并发经外部 singleflight 合并。
type loadBatch[V any] struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	pending map[string]struct{}
	results map[string]loadResult[V]
	err     error
}

func newLoadBatch[V any](items []string) *loadBatch[V] {
	lb := &loadBatch[V]{
		pending: make(map[string]struct{}, len(items)),
		results: make(map[string]loadResult[V], len(items)),
	}
	for _, it := range items {
		lb.pending[it] = struct{}{}
	}
	return lb
}

// record 写入批量加载结果（leader 的 load 闭包调用；缺失条目须含 ok=false 条目）。
func (lb *loadBatch[V]) record(res map[string]loadResult[V]) {
	lb.mu.Lock()
	for s, r := range res {
		lb.results[s] = r
	}
	lb.mu.Unlock()
}

// run 单条目加载路径（经 singleflight 调用，同条目并发合并）：
// 本条目已被同批次其他 leader 认领时等待批量结果，否则作为 leader 认领全部 pending 一次加载。
func (lb *loadBatch[V]) run(item string, load func(batch []string) error) (loadResult[V], error) {
	lb.mu.Lock()
	if r, ok := lb.results[item]; ok {
		lb.mu.Unlock()
		return r, nil
	}
	if _, mine := lb.pending[item]; mine {
		batch := make([]string, 0, len(lb.pending))
		for it := range lb.pending {
			batch = append(batch, it)
		}
		lb.pending = nil
		lb.wg.Add(1)
		lb.mu.Unlock()

		func() {
			defer lb.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					lb.mu.Lock()
					lb.err = fmt.Errorf("gocache: batch load panic: %v", r)
					lb.mu.Unlock()
				}
			}()
			if err := load(batch); err != nil {
				lb.mu.Lock()
				lb.err = err
				lb.mu.Unlock()
			}
		}()
		lb.mu.Lock()
		err, r := lb.err, lb.results[item]
		lb.mu.Unlock()
		return r, err
	}
	// 已被认领：等待批量结果
	lb.mu.Unlock()
	lb.wg.Wait()
	if lb.err != nil {
		return loadResult[V]{}, lb.err
	}
	lb.mu.Lock()
	r := lb.results[item]
	lb.mu.Unlock()
	return r, nil
}

// resolveSerializer 序列化器缺省为 JSON。
func resolveSerializer(s Serializer) Serializer {
	if s == nil {
		return JSON
	}
	return s
}

// sliceRange redis LRANGE 语义切片（负索引、闭区间，越界收敛）。
func sliceRange[T any](items []T, start, stop int64) []T {
	n := int64(len(items))
	if start < 0 {
		start += n
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += n
	}
	if n == 0 || start >= n || stop < 0 || start > stop {
		return nil
	}
	if stop >= n {
		stop = n - 1
	}
	return items[start : stop+1]
}
