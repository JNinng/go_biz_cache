package event

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/jninng/go_biz_cache/l2"
	"github.com/redis/go-redis/v9"
)

const (
	channelPrefix = "gocache:evt:" // channel 按 namespace 分：gocache:evt:{ns}
	patternAll    = "gocache:evt:*"
)

// RedisPubSub 基于 Redis Pub/Sub 的跨实例事件总线适配器。
//
// 客户端经提供者函数注入（§9.1）：每次发布原子获取当前客户端；检测到实例变更后
// 以新实例重建订阅（PSubscribe gocache:evt:*）。重建窗口内丢失的事件由世代号
// 短 TTL 惰性刷新兜底。提供者应返回指针类型客户端以便实例变更比较。
type RedisPubSub struct {
	provider l2.ClientProvider

	mu       sync.Mutex
	client   l2.RedisCmdable
	pubsub   *redis.PubSub
	handlers map[int]Handler
	nextID   int
	started  bool
	closed   bool
}

// NewRedisPubSub 创建 Redis Pub/Sub 总线适配器。
func NewRedisPubSub(provider l2.ClientProvider) *RedisPubSub {
	return &RedisPubSub{provider: provider, handlers: make(map[int]Handler)}
}

// Publish 将事件发布到按 namespace 分的 channel。
func (b *RedisPubSub) Publish(ctx context.Context, e Event) error {
	c, err := b.provider(ctx)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b.mu.Lock()
	changed := b.started && b.client != nil && !sameClient(c, b.client)
	b.mu.Unlock()
	if changed {
		b.reconnect(ctx, c)
	}
	return c.Publish(ctx, channelPrefix+e.Namespace, payload).Err()
}

// Subscribe 注册处理器；首个订阅触发 PSubscribe，后续复用同一订阅。
func (b *RedisPubSub) Subscribe(ctx context.Context, h Handler) (func(), error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrBusClosed
	}
	id := b.nextID
	b.nextID++
	b.handlers[id] = h
	needStart := !b.started
	b.started = true
	b.mu.Unlock()

	if needStart {
		c, err := b.provider(ctx)
		if err != nil {
			b.mu.Lock()
			delete(b.handlers, id)
			b.started = len(b.handlers) > 0
			b.mu.Unlock()
			return nil, err
		}
		b.connect(ctx, c)
	}
	return func() {
		b.mu.Lock()
		delete(b.handlers, id)
		b.mu.Unlock()
	}, nil
}

func sameClient(a, b l2.RedisCmdable) bool {
	defer func() { _ = recover() }() // 动态类型不可比较时退化为按已变更处理
	return a == b
}

func (b *RedisPubSub) connect(ctx context.Context, c l2.RedisCmdable) {
	b.mu.Lock()
	if b.closed || b.pubsub != nil {
		b.mu.Unlock()
		return
	}
	ps := c.PSubscribe(ctx, patternAll)
	b.client, b.pubsub = c, ps
	b.mu.Unlock()
	go b.readLoop(ps)
}

func (b *RedisPubSub) reconnect(ctx context.Context, c l2.RedisCmdable) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	old := b.pubsub
	ps := c.PSubscribe(ctx, patternAll)
	b.client, b.pubsub = c, ps
	b.mu.Unlock()
	go b.readLoop(ps)
	if old != nil {
		_ = old.Close()
	}
}

func (b *RedisPubSub) readLoop(ps *redis.PubSub) {
	ch := ps.Channel()
	for msg := range ch {
		var e Event
		if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
			log.Printf("[gocache/event] malformed event payload: %v", err)
			continue
		}
		b.dispatch(e)
	}
}

func (b *RedisPubSub) dispatch(e Event) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
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
}

// Close 关闭订阅与总线。
func (b *RedisPubSub) Close() error {
	b.mu.Lock()
	b.closed = true
	ps := b.pubsub
	b.pubsub = nil
	b.handlers = make(map[int]Handler)
	b.mu.Unlock()
	if ps != nil {
		return ps.Close()
	}
	return nil
}
