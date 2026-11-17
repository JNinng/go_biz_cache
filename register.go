package gocache

import (
	"context"
	"fmt"
	"strings"
)

func regKey(ns, name string) string { return ns + "\x00" + name }

// register 定义注册与校验（非法参数在定义注册时报错，不静默降级）。
func register[V any](kind structKind, ns, name string, def Definition[V], refreshTTL bool) (*defCore, error) {
	if ns == "" || name == "" {
		return nil, fmt.Errorf("%w: namespace/name must not be empty", ErrInvalidDefinition)
	}
	if strings.ContainsAny(ns+name, ":{}") {
		return nil, fmt.Errorf("%w: namespace/name must not contain ':{}'", ErrInvalidDefinition)
	}
	if def.KeyTemplate == "" {
		return nil, fmt.Errorf("%w: key template required", ErrInvalidDefinition)
	}
	if err := validateBigValue(kind, def.BigValue); err != nil {
		return nil, err
	}

	ttl := def.TTL
	if ttl <= 0 {
		ttl = defaultTTL
	}
	negTTL := def.NegativeTTL
	if negTTL <= 0 {
		negTTL = defaultNegTTL
	}

	core := &defCore{
		ns: ns, name: name, kind: kind,
		template:   def.KeyTemplate,
		tag:        def.HashTag,
		ttl:        ttl,
		jitter:     clampJitter(def.TTLJitter),
		negative:   def.NegativeCache,
		negTTL:     negTTL,
		big:        def.BigValue,
		degrade:    normalizeDegrade(def.Degrade),
		instance:   instanceID,
		refreshTTL: refreshTTL,
		l1:         resolveL1(def.L1),
		store:      resolveL2(def.L2),
	}

	// 事件 / 指标 / 日志（定义级覆盖 > 全局注入）
	glMu.Lock()
	bus := def.EventBus
	if bus == nil {
		bus = glBus
	}
	metrics := def.Metrics
	if metrics == nil {
		metrics = glMetrics
	}
	logger := def.Logger
	if logger == nil {
		logger = glLogger
	}
	glMu.Unlock()
	core.bus, core.metrics, core.logger = bus, metrics, logger

	core.gen = newGenTracker(ns+":"+name+":__gen", core.store, logger)
	probeTag(core)

	// 注册表 + 事件订阅
	regMu.Lock()
	key := regKey(ns, name)
	if _, exists := regIndex[key]; exists {
		regMu.Unlock()
		return nil, fmt.Errorf("%w: duplicate definition %s:%s", ErrInvalidDefinition, ns, name)
	}
	regIndex[key] = core
	if bus != nil {
		cancel, err := bus.Subscribe(context.Background(), core.onEvent)
		if err != nil {
			delete(regIndex, key)
			regMu.Unlock()
			return nil, fmt.Errorf("gocache: subscribe events for %s:%s: %w", ns, name, err)
		}
		regCancels = append(regCancels, cancel)
	}
	regMu.Unlock()

	core.logger.Debugf("gocache: defined %s:%s kind=%s l1=%v l2=%v", ns, name, kind, core.l1 != nil, core.store != nil)
	return core, nil
}

// probeTag 注册期探测一次 TagFunc，获取变换模板供事件逆变换（Prefix/Suffix/Insert 的 Value 常量）。
func probeTag(core *defCore) {
	if core.tag == nil {
		return
	}
	parts := map[string]string{}
	for _, name := range templatePlaceholders(core.template) {
		parts[name] = "probe"
	}
	op, err := core.tag(TagInput{LogicalKey: "probe", Parts: parts, NS: core.ns, Name: core.name})
	if err != nil {
		core.logger.Warnf("gocache: probe tag func for %s:%s failed: %v (event inverse mapping degraded to L1 clear)", core.ns, core.name, err)
		return
	}
	core.probe, core.probeOK = op, true
}

func templatePlaceholders(template string) []string {
	var names []string
	for i := 0; i < len(template); {
		if template[i] == '{' {
			j := strings.IndexByte(template[i+1:], '}')
			if j < 0 {
				break
			}
			names = append(names, template[i+1:i+1+j])
			i += j + 2
			continue
		}
		j := strings.IndexByte(template[i:], '{')
		if j < 0 {
			break
		}
		i += j
	}
	return names
}
