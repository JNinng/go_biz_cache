package gocache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jninng/go_biz_cache/l2"
)

// ---------- KeyBuilder（§3.2）----------

// KeyBuilder 声明式 key 模板构建器：With 链式展开占位符，业务禁止手拼物理 key。
type KeyBuilder struct {
	template string
	parts    map[string]string
}

// NewKeyBuilder 以定义的 KeyTemplate 创建构建器。
func NewKeyBuilder(template string) *KeyBuilder { return &KeyBuilder{template: template} }

// With 设置占位符值。
func (b *KeyBuilder) With(name, value string) *KeyBuilder {
	if b.parts == nil {
		b.parts = make(map[string]string, 2)
	}
	b.parts[name] = value
	return b
}

// Parts 返回占位符值副本。
func (b *KeyBuilder) Parts() map[string]string {
	out := make(map[string]string, len(b.parts))
	for k, v := range b.parts {
		out[k] = v
	}
	return out
}

// Build 展开模板；占位符缺失报错。
func (b *KeyBuilder) Build() (string, error) {
	var sb strings.Builder
	tpl := b.template
	for i := 0; i < len(tpl); {
		if tpl[i] == '{' {
			j := strings.IndexByte(tpl[i+1:], '}')
			if j < 0 {
				return "", fmt.Errorf("%w: template %q unbalanced '{'", ErrKeyBuild, tpl)
			}
			name := tpl[i+1 : i+1+j]
			v, ok := b.parts[name]
			if !ok {
				return "", fmt.Errorf("%w: placeholder %q missing in %q", ErrKeyBuild, name, tpl)
			}
			sb.WriteString(v)
			i += j + 2
			continue
		}
		sb.WriteByte(tpl[i])
		i++
	}
	return sb.String(), nil
}

// extractParts 按模板反推占位符值（首个匹配，确定性）：
// 模板交替由字面量段与 {name} 段构成，字面量固定匹配，{name} 捕获至下一字面量首次出现处。
func extractParts(template, bizKey string) (map[string]string, error) {
	type token struct {
		lit  string
		name string // 非空 = 占位符
	}
	var tokens []token
	for i := 0; i < len(template); {
		if template[i] == '{' {
			j := strings.IndexByte(template[i+1:], '}')
			if j < 0 {
				return nil, fmt.Errorf("%w: template %q unbalanced '{'", ErrKeyBuild, template)
			}
			tokens = append(tokens, token{name: template[i+1 : i+1+j]})
			i += j + 2
			continue
		}
		j := strings.IndexByte(template[i:], '{')
		if j < 0 {
			tokens = append(tokens, token{lit: template[i:]})
			i = len(template)
		} else {
			tokens = append(tokens, token{lit: template[i : i+j]})
			i += j
		}
	}
	parts := make(map[string]string, len(tokens))
	i := 0
	for idx, tk := range tokens {
		if tk.name == "" {
			if !strings.HasPrefix(bizKey[i:], tk.lit) {
				return nil, fmt.Errorf("%w: key %q does not match template %q", ErrKeyBuild, bizKey, template)
			}
			i += len(tk.lit)
			continue
		}
		var next string
		if idx+1 < len(tokens) {
			next = tokens[idx+1].lit
		}
		if next == "" {
			parts[tk.name] = bizKey[i:]
			i = len(bizKey)
			continue
		}
		j := strings.Index(bizKey[i:], next)
		if j < 0 {
			return nil, fmt.Errorf("%w: key %q does not match template %q", ErrKeyBuild, bizKey, template)
		}
		parts[tk.name] = bizKey[i : i+j]
		i += j
	}
	if i != len(bizKey) {
		return nil, fmt.Errorf("%w: key %q does not match template %q", ErrKeyBuild, bizKey, template)
	}
	return parts, nil
}

// ---------- key 变换 / 集群 slot 控制（§3.3）----------

// TagKind key 变换类型。
type TagKind uint8

const (
	TagNone  TagKind = iota // 不变换（默认整键散列）
	TagWrap                 // 将 At 定位的子串原地包上 {}（At 空 = 整键包裹）
	TagPrefix               // 头部注入 Value（可含自闭合 {...}）
	TagSuffix               // 尾部注入 Value（可含自闭合 {...}）
	TagInsert               // 于 At 匹配处之前注入 Value（At 空 = 头部）
)

// TagInput key 变换入参。
type TagInput struct {
	LogicalKey string            // 模板展开后的逻辑键全段 {ns}:{name}:v{gen}:{业务键}
	Parts      map[string]string // 占位符值
	NS, Name   string            // 命名空间/定义名
}

// TagOp key 变换操作（单 op；契约见 docs §3.3）。
type TagOp struct {
	Kind  TagKind
	Value string // Prefix/Suffix/Insert 的注入内容（可含自闭合 {...}）；Wrap 忽略
	At    string // Wrap/Insert 的定位子串（首次出现处）；空 = 整键包裹 / 头部插入
}

// TagFunc key 变换策略函数。nil = 不变换（默认整键散列）。
// 契约：纯函数确定性（同入参必同变换，否则 slot 漂移）；违反契约在 key 构建时报错。
type TagFunc func(TagInput) (TagOp, error)

// TagFromPlaceholder 预设：将指定占位符段原地包裹 {...}（同占位符值同 slot，跨键聚合）。
func TagFromPlaceholder(name string) TagFunc {
	return func(in TagInput) (TagOp, error) {
		v, ok := in.Parts[name]
		if !ok || v == "" {
			return TagOp{}, fmt.Errorf("%w: hash-tag placeholder %q missing", ErrKeyBuild, name)
		}
		return TagOp{Kind: TagWrap, At: v}, nil
	}
}

// TagWhole 预设：整键包裹 {...}。
func TagWhole() TagFunc {
	return func(TagInput) (TagOp, error) { return TagOp{Kind: TagWrap}, nil }
}

func hasWhitespace(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			return true
		}
	}
	return false
}

// checkBraces 校验注入内容的大括号：仅允许自闭合配对，禁止嵌套（Redis 取首个 { 到首个 } 计算 slot）。
func checkBraces(s, what string) error {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
			if depth > 1 {
				return fmt.Errorf("%w: nested '{' in %s %q", ErrKeyBuild, what, s)
			}
		case '}':
			depth--
			if depth < 0 {
				return fmt.Errorf("%w: unbalanced '}' in %s %q", ErrKeyBuild, what, s)
			}
		}
	}
	if depth != 0 {
		return fmt.Errorf("%w: unbalanced '{' in %s %q", ErrKeyBuild, what, s)
	}
	return nil
}

func checkAt(at string) error {
	if hasWhitespace(at) {
		return fmt.Errorf("%w: tag At %q contains whitespace", ErrKeyBuild, at)
	}
	if strings.ContainsAny(at, "{}") {
		return fmt.Errorf("%w: tag At %q must not contain braces", ErrKeyBuild, at)
	}
	return nil
}

// applyTag 应用 key 变换；违反契约报错，不静默降级。
func applyTag(op TagOp, logical string) (string, error) {
	switch op.Kind {
	case TagNone:
		return logical, nil
	case TagWrap:
		if op.At == "" {
			return "{" + logical + "}", nil
		}
		if err := checkAt(op.At); err != nil {
			return "", err
		}
		i := strings.Index(logical, op.At)
		if i < 0 {
			return "", fmt.Errorf("%w: tag At %q not found in %q", ErrKeyBuild, op.At, logical)
		}
		return logical[:i] + "{" + op.At + "}" + logical[i+len(op.At):], nil
	case TagPrefix, TagSuffix, TagInsert:
		if hasWhitespace(op.Value) {
			return "", fmt.Errorf("%w: tag Value %q contains whitespace", ErrKeyBuild, op.Value)
		}
		if err := checkBraces(op.Value, "tag Value"); err != nil {
			return "", err
		}
		if op.At != "" {
			if err := checkAt(op.At); err != nil {
				return "", err
			}
		}
		switch op.Kind {
		case TagPrefix:
			return op.Value + logical, nil
		case TagSuffix:
			return logical + op.Value, nil
		default: // TagInsert
			if op.At == "" {
				return op.Value + logical, nil
			}
			i := strings.Index(logical, op.At)
			if i < 0 {
				return "", fmt.Errorf("%w: tag At %q not found in %q", ErrKeyBuild, op.At, logical)
			}
			return logical[:i] + op.Value + logical[i:], nil
		}
	default:
		return "", fmt.Errorf("%w: unknown tag kind %d", ErrKeyBuild, op.Kind)
	}
}

// inverseTag 事件订阅方：物理 key → 变换前逻辑键（尽力而为）。
func inverseTag(op TagOp, physical string) (string, bool) {
	switch op.Kind {
	case TagNone:
		return physical, true
	case TagWrap:
		i := strings.IndexByte(physical, '{')
		j := strings.IndexByte(physical, '}')
		if i < 0 || j <= i || strings.Contains(physical[i+1:j], "{") {
			return "", false
		}
		return physical[:i] + physical[i+1:j] + physical[j+1:], true
	case TagPrefix:
		if strings.HasPrefix(physical, op.Value) {
			return physical[len(op.Value):], true
		}
	case TagSuffix:
		if strings.HasSuffix(physical, op.Value) {
			return physical[:len(physical)-len(op.Value)], true
		}
	case TagInsert:
		if op.At == "" {
			if strings.HasPrefix(physical, op.Value) {
				return physical[len(op.Value):], true
			}
			return "", false
		}
		i := strings.Index(physical, op.At)
		if i >= len(op.Value) && physical[i-len(op.Value):i] == op.Value {
			return physical[:i-len(op.Value)] + physical[i:], true
		}
	}
	return "", false
}

// ---------- 世代号（§3.2 D2）----------

const genRefreshTTL = 5 * time.Second // 世代号本地缓存短 TTL（事件丢失时的惰性刷新兜底）

// genTracker 世代号管理：存储于 Redis 元 key {ns}:{name}:__gen，进程内短 TTL 缓存摊销读取。
// 本地模式（无 L2）世代号存进程内存，重启归零等价全量失效。
type genTracker struct {
	mu      sync.RWMutex
	gen     int64
	expires time.Time
	metaKey string
	store   l2.Store // nil = 本地模式
	logger  Logger
}

func newGenTracker(metaKey string, store l2.Store, logger Logger) *genTracker {
	return &genTracker{metaKey: metaKey, store: store, logger: logger}
}

// current 当前世代号。Redis 不可用时沿用最后已知世代号，不阻塞读取。
func (g *genTracker) current(ctx context.Context) int64 {
	now := time.Now()
	g.mu.RLock()
	gen, valid := g.gen, now.Before(g.expires)
	g.mu.RUnlock()
	if valid {
		return gen
	}
	if g.store == nil { // 本地模式
		return gen
	}
	val, err := g.store.Get(ctx, g.metaKey)
	if err != nil {
		if !errors.Is(err, l2.ErrNotFound) && g.logger != nil {
			g.logger.Warnf("gocache: read generation %s: %v (stale gen %d used)", g.metaKey, err, gen)
		}
		return gen // 兜底：沿用最后已知世代号
	}
	n, _ := strconv.ParseInt(string(val), 10, 64)
	g.mu.Lock()
	g.gen, g.expires = n, time.Now().Add(genRefreshTTL)
	g.mu.Unlock()
	return n
}

// bump 世代号递增：Redis 元 key INCR（原子，避免多实例并发失效覆盖）；本地模式内存递增。
func (g *genTracker) bump(ctx context.Context) (int64, error) {
	if g.store == nil {
		g.mu.Lock()
		g.gen++
		g.expires = time.Now().Add(genRefreshTTL)
		n := g.gen
		g.mu.Unlock()
		return n, nil
	}
	n, err := g.store.Incr(ctx, g.metaKey)
	if err != nil {
		return 0, err
	}
	g.mu.Lock()
	g.gen, g.expires = n, time.Now().Add(genRefreshTTL)
	g.mu.Unlock()
	return n, nil
}

// external 订阅 GenerationBumped 事件后即时更新本地世代号。
// 单调保护：并发 InvalidateAll 下事件乱序到达时，迟到的旧事件不回拨世代号。
func (g *genTracker) external(gen int64) {
	g.mu.Lock()
	if gen > g.gen {
		g.gen, g.expires = gen, time.Now().Add(genRefreshTTL)
	}
	g.mu.Unlock()
}
