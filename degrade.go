package gocache

// Logger 日志钩子：慢查询/降级/大 key 告警点。注入 nil 时为静默。
type Logger interface {
	Debugf(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Debugf(string, ...any) {}
func (nopLogger) Warnf(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

// DegradePolicy L2 不可用（连接失败/超时）时的读降级策略。
type DegradePolicy string

const (
	// DegradeFail 默认：返回错误。
	DegradeFail DegradePolicy = "fail"
	// DegradeSourceOnly 直连源数据（跳过 L2 读，L1 仍可用）。
	DegradeSourceOnly DegradePolicy = "source_only"
	// DegradeStaleL1 只用 L1 旧值。
	DegradeStaleL1 DegradePolicy = "stale_l1"
)

func normalizeDegrade(p DegradePolicy) DegradePolicy {
	switch p {
	case DegradeSourceOnly, DegradeStaleL1:
		return p
	default:
		return DegradeFail
	}
}
