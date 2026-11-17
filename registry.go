package gocache

import (
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jninng/go_biz_cache/event"
	"github.com/jninng/go_biz_cache/l1"
	"github.com/jninng/go_biz_cache/l2"
)

const (
	defaultTTL       = 30 * time.Minute
	defaultNegTTL    = 10 * time.Second
	defaultL1Size    = 4096
	defaultL2Timeout = 3 * time.Second
	oversizeReport   = 64 << 20  // 超过 64MB 计入超大值写入指标（治理参考）
	maxPhysicalValue = 512 << 20 // redis 单 value 硬上限（不做隐式压缩兜底，直接报错）
)

// defCore 定义级非泛型核心（注册表条目）。
type defCore struct {
	ns, name   string
	kind       structKind
	template   string
	tag        TagFunc
	probe      TagOp // 注册期探测的变换模板（供事件逆变换）
	probeOK    bool  // 探测失败时逆变换不可用，事件退化为整 L1 清理
	ttl        time.Duration
	jitter     float64
	negative   bool
	negTTL     time.Duration
	big        BigValuePolicy
	l1         l1.L1Cache
	store      l2.Store // nil = 纯本地模式
	degrade    DegradePolicy
	bus        event.Bus
	metrics    Metrics
	logger     Logger
	instance   string
	gen        *genTracker
	refreshTTL bool // hash RefreshTTLOnWrite
}

var (
	regMu      sync.Mutex
	regIndex   = map[string]*defCore{}
	regCancels []func()
)

var (
	glMu       sync.Mutex
	glProvider l2.ClientProvider
	glBus      event.Bus
	glMetrics  Metrics = NoopMetrics
	glLogger   Logger  = nopLogger{}
)

var instanceID = newInstanceID()

func newInstanceID() string {
	host, _ := os.Hostname()
	var b [4]byte
	_, _ = rand.Read(b[:])
	id := uint64(b[0])<<24 | uint64(b[1])<<16 | uint64(b[2])<<8 | uint64(b[3])
	return fmt.Sprintf("%s-%d-%x", host, os.Getpid(), id)
}

// UseClientProvider 注入全局 L2 客户端提供者（定义未单独指定时生效）。
func UseClientProvider(p ClientProvider) { glMu.Lock(); glProvider = p; glMu.Unlock() }

// UseEventBus 注入全局事件总线（进程内 / Redis Pub/Sub 适配器，§8）。
func UseEventBus(b event.Bus) { glMu.Lock(); glBus = b; glMu.Unlock() }

// UseMetrics 注入全局指标实现。
func UseMetrics(m Metrics) {
	glMu.Lock()
	if m != nil {
		glMetrics = m
	}
	glMu.Unlock()
}

// UseLogger 注入全局日志钩子。
func UseLogger(l Logger) {
	glMu.Lock()
	if l != nil {
		glLogger = l
	}
	glMu.Unlock()
}

// Close 取消全部事件订阅（进程退出时调用；不关闭注入的总线）。
func Close() {
	regMu.Lock()
	defer regMu.Unlock()
	for _, c := range regCancels {
		c()
	}
	regCancels = nil
}

// Reset 清空注册表并取消订阅（仅供测试）。
func Reset() {
	Close()
	regMu.Lock()
	regIndex = map[string]*defCore{}
	regMu.Unlock()
}
