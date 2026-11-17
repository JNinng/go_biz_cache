// Package event 提供缓存事件总线抽象：进程内实现（单机/单实例）与 Redis Pub/Sub 适配器（跨实例）。
// 事件保证 at-most-once，L1 一致性定位为“最终、秒级”。
package event

import (
	"context"
	"errors"
	"sync"
)

// Type 事件类型。
type Type string

const (
	Updated          Type = "updated"           // 值更新：订阅方删除各自 L1 对应条目
	Invalidated      Type = "invalidated"       // 失效：订阅方删除各自 L1 对应条目
	GenerationBumped Type = "generation_bumped" // 世代号递增：订阅方整代清空 L1
)

// ErrBusClosed 总线已关闭。
var ErrBusClosed = errors.New("event: bus closed")

// Event 缓存事件。不携带 value 载荷（大值不经 Pub/Sub，删除后下次读走 L2 即新值）。
type Event struct {
	Type      Type     `json:"type"`
	Namespace string   `json:"ns,omitempty"`
	DefName   string   `json:"def,omitempty"`
	Keys      []string `json:"keys,omitempty"` // Updated/Invalidated: 变换后物理 key（不含分片子 key）
	Gen       int64    `json:"gen,omitempty"`  // GenerationBumped: 新世代号
	Instance  string   `json:"instance,omitempty"` // 发布者实例 ID（订阅方过滤自身事件）
}

// Handler 事件处理器。
type Handler func(Event)

// Bus 事件总线接口。MQ 适配器（Kafka/NATS 等）以同一接口接入，业务零改动。
type Bus interface {
	Publish(ctx context.Context, e Event) error
	Subscribe(ctx context.Context, h Handler) (cancel func(), err error)
	Close() error
}

// InProcessBus 进程内事件总线（单机/单实例模式）。
type InProcessBus struct {
	mu       sync.Mutex
	handlers map[int]Handler
	nextID   int
	closed   bool
}

// NewInProcess 创建进程内总线。
func NewInProcess() *InProcessBus {
	return &InProcessBus{handlers: make(map[int]Handler)}
}

// Publish 异步扇出到全部订阅者（at-most-once，处理器 panic 不影响总线）。
func (b *InProcessBus) Publish(ctx context.Context, e Event) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	hs := make([]Handler, 0, len(b.handlers))
	for _, h := range b.handlers {
		hs = append(hs, h)
	}
	b.mu.Unlock()
	for _, h := range hs {
		go func(h Handler) {
			defer func() { _ = recover() }()
			h(e)
		}(h)
	}
	return nil
}

// Subscribe 注册处理器，返回取消函数。
func (b *InProcessBus) Subscribe(ctx context.Context, h Handler) (func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrBusClosed
	}
	id := b.nextID
	b.nextID++
	b.handlers[id] = h
	return func() {
		b.mu.Lock()
		delete(b.handlers, id)
		b.mu.Unlock()
	}, nil
}

// Close 关闭总线。
func (b *InProcessBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.handlers = make(map[int]Handler)
	return nil
}
