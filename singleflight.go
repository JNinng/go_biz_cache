package gocache

import (
	"fmt"
	"sync"
)

// ---------- 进程内 singleflight：合并同 key 并发回源 ----------

type sfCall[T any] struct {
	wg  sync.WaitGroup
	val T
	err error
}

// Group 泛型 singleflight。
type Group[T any] struct {
	mu    sync.Mutex
	calls map[string]*sfCall[T]
}

// Do 合并同 key 的并发调用；shared 表示本次是否复用了其他 goroutine 的结果（singleflight 合并率指标）。
func (g *Group[T]) Do(key string, fn func() (T, error)) (t T, err error, shared bool) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*sfCall[T])
	}
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := &sfCall[T]{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	func() {
		defer func() {
			if r := recover(); r != nil {
				var zero T
				c.val, c.err = zero, fmt.Errorf("gocache: singleflight panic: %v", r)
			}
		}()
		c.val, c.err = fn()
	}()
	c.wg.Done()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	return c.val, c.err, false
}
