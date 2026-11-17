package gocache

import (
	"context"
	"testing"
	"time"

	"github.com/jninng/go_biz_cache/event"
	"github.com/jninng/go_biz_cache/l2"
)

// 跨实例事件驱动（设计 §8）：InProcessBus 广播 + core.onEvent + 伪造 Event.Instance 驱动。

func TestOnEventInvalidationDeletesL1(t *testing.T) {
	Reset()
	defer Reset()
	bus := event.NewInProcess()
	defer bus.Close()
	UseEventBus(bus)
	defer UseEventBus(nil)

	h := newTestObject(l2.NewMemStore(), nil, nil)
	ctx := context.Background()
	if err := h.Set(ctx, "t:acme:u:1", userProfile{ID: "1", Name: "x"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// L1 有条目
	if _, ok := h.core.l1.Get("t:acme:u:1"); !ok {
		t.Fatal("L1 must be warm after Set")
	}
	// 伪造跨实例 Updated 事件（另一实例写了同 key）
	other := event.Event{
		Type:      event.Updated,
		Namespace: "test",
		DefName:   "users",
		Keys:      []string{"test:users:v0:t:acme:u:1"},
		Instance:  "other-instance",
	}
	h.core.onEvent(other)
	if _, ok := h.core.l1.Get("t:acme:u:1"); ok {
		t.Fatal("cross-instance Updated must delete L1 entry")
	}
	// 自身事件被过滤
	h.Set(ctx, "t:acme:u:2", userProfile{ID: "2"})
	h.core.onEvent(event.Event{
		Type: event.Updated, Namespace: "test", DefName: "users",
		Keys: []string{"test:users:v0:t:acme:u:2"}, Instance: h.core.instance,
	})
	if _, ok := h.core.l1.Get("t:acme:u:2"); !ok {
		t.Fatal("self event must be filtered out")
	}
	// 非本定义事件被过滤
	h.core.onEvent(event.Event{
		Type: event.Updated, Namespace: "test", DefName: "others",
		Keys: []string{"whatever"}, Instance: "other-instance",
	})
	if _, ok := h.core.l1.Get("t:acme:u:2"); !ok {
		t.Fatal("foreign def event must be ignored")
	}
}

func TestOnEventGenerationBumped(t *testing.T) {
	Reset()
	defer Reset()
	bus := event.NewInProcess()
	defer bus.Close()
	UseEventBus(bus)
	defer UseEventBus(nil)

	h := newTestObject(l2.NewMemStore(), nil, nil)
	ctx := context.Background()
	h.Set(ctx, "t:acme:u:1", userProfile{ID: "1"})
	h.Set(ctx, "t:acme:u:2", userProfile{ID: "2"})

	// 另一实例执行了 InvalidateAll → GenerationBumped 广播
	h.core.onEvent(event.Event{
		Type: event.GenerationBumped, Namespace: "test", DefName: "users",
		Gen: 7, Instance: "other-instance",
	})
	// 整代 L1 清空
	if h.core.l1.Size() != 0 {
		t.Fatalf("GenerationBumped must clear L1, size=%d", h.core.l1.Size())
	}
	// 本地世代号即时更新（缓存窗口内）
	if got := h.core.gen.current(ctx); got != 7 {
		t.Fatalf("gen = %d, want 7", got)
	}
	// 单调保护：迟到的旧事件不回拨世代号（并发 InvalidateAll 事件乱序场景）
	h.core.onEvent(event.Event{
		Type: event.GenerationBumped, Namespace: "test", DefName: "users",
		Gen: 5, Instance: "other-instance",
	})
	if got := h.core.gen.current(ctx); got != 7 {
		t.Fatalf("stale event must not roll back gen, got %d", got)
	}
	// 新写入进入新世代
	if err := h.Set(ctx, "t:acme:u:3", userProfile{ID: "3"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	logical, _, err := h.core.buildKey(ctx, "t:acme:u:3")
	if err != nil {
		t.Fatalf("buildKey: %v", err)
	}
	if logical != "test:users:v7:t:acme:u:3" {
		t.Fatalf("logical key after bump = %q", logical)
	}
}

func TestOnEventDegradedToClearWhenInverseFails(t *testing.T) {
	Reset()
	defer Reset()
	h := newTestObject(l2.NewMemStore(), nil, func(d *Definition[userProfile]) {
		// 自定义 TagFunc 前缀注入变换（Prefix/Suffix/Insert 经 probe 记录 Value 常量后可逆）
		d.HashTag = func(in TagInput) (TagOp, error) {
			return TagOp{Kind: TagPrefix, Value: "dc:" + in.Name}, nil
		}
	})
	h.Set(context.Background(), "t:acme:u:1", userProfile{ID: "1"})
	// 事件 key 前缀与 probe 记录的 Value 不符 → 逆变换失败 → 退化为整 L1 清理
	h.core.onEvent(event.Event{
		Type: event.Updated, Namespace: "test", DefName: "users",
		Keys: []string{"zz:test:users:v0:t:acme:u:1"}, Instance: "other",
	})
	if h.core.l1.Size() != 0 {
		t.Fatalf("uninverseable key must clear L1, size=%d", h.core.l1.Size())
	}
}

func TestInProcessBusBroadcast(t *testing.T) {
	Reset()
	defer Reset()
	bus := event.NewInProcess()
	defer bus.Close()
	UseEventBus(bus)
	defer UseEventBus(nil)

	got := make(chan event.Event, 4)
	if _, err := bus.Subscribe(context.Background(), func(e event.Event) { got <- e }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	h := newTestObject(l2.NewMemStore(), nil, nil)
	if err := h.Delete(context.Background(), "t:acme:u:9"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	select {
	case e := <-got:
		if e.Type != event.Invalidated || e.Instance != h.core.instance {
			t.Fatalf("event = %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("event not broadcast")
	}
}

func TestOnEventHashClearsL1(t *testing.T) {
	Reset()
	defer Reset()
	store := l2.NewMemStore()
	h := newTestHash(store, nil, nil)
	ctx := context.Background()
	if err := h.HSet(ctx, "a", "f", hashProfile{Name: "x"}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	// hash L1 为复合键，跨实例 field 更新退化为整定义清理
	if h.core.l1.Size() == 0 {
		t.Fatal("L1 must be warm")
	}
	h.core.onEvent(event.Event{
		Type: event.Updated, Namespace: "test", DefName: "hobjs",
		Keys: []string{"test:hobjs:v0:a"}, Instance: "other-instance",
	})
	if h.core.l1.Size() != 0 {
		t.Fatalf("hash Updated must clear L1, size=%d", h.core.l1.Size())
	}
}
