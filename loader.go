package gocache

import "context"

// Loader 源数据访问契约（Object 批量）。库不关心 DB，业务注入。
type Loader[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// HashLoader hash field 级回源契约（批量契约，不循环单查）；fields 为空表示整 hash 全量回源。
type HashLoader[K, F comparable, V any] func(ctx context.Context, key K, fields []F) (map[F]V, error)

// SetLoader set 整 key 回源契约。
type SetLoader[K comparable, M any] func(ctx context.Context, key K) ([]M, error)

// ListLoader list 整 key 回源契约。
type ListLoader[K comparable, V any] func(ctx context.Context, key K) ([]V, error)

// Scored zset 成员与分值。
type Scored[V any] struct {
	Value V
	Score float64
}

// ZSetLoader zset 整 key 回源契约。
type ZSetLoader[K comparable, V any] func(ctx context.Context, key K) ([]Scored[V], error)
