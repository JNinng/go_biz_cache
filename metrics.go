package gocache

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// 命中级别标签值。
const (
	LevelL1       = "l1"
	LevelL2       = "l2"
	LevelNegative = "negative"
	LevelSource   = "source"
)

// Metrics 可观测性接口；内置 Prometheus 实现（NewPrometheusMetrics），未注入时为 Noop。
type Metrics interface {
	Hit(level, def string)
	Miss(level, def string)
	ObserveSourceLoad(def string, d time.Duration)
	SingleflightShared(def string, n int)
	OversizeWrite(def string)                       // 超大值写入次数（治理参考）
	Error(op, def string)                           // 错误计数（按操作与定义打标签）
	ObserveCodec(kind, def string, d time.Duration) // kind: serialize/deserialize/compress/decompress
}

type noopMetrics struct{}

func (noopMetrics) Hit(string, string)                         {}
func (noopMetrics) Miss(string, string)                        {}
func (noopMetrics) ObserveSourceLoad(string, time.Duration)    {}
func (noopMetrics) SingleflightShared(string, int)             {}
func (noopMetrics) OversizeWrite(string)                       {}
func (noopMetrics) Error(string, string)                       {}
func (noopMetrics) ObserveCodec(string, string, time.Duration) {}

// NoopMetrics 空实现。
var NoopMetrics Metrics = noopMetrics{}

type promMetrics struct {
	hits       *prometheus.CounterVec
	misses     *prometheus.CounterVec
	sourceLoad *prometheus.HistogramVec
	sfShared   *prometheus.CounterVec
	oversize   *prometheus.CounterVec
	errs       *prometheus.CounterVec
	codec      *prometheus.HistogramVec
}

// NewPrometheusMetrics 创建 Prometheus 指标实现并注册到 reg（nil 时注册到 DefaultRegisterer）。
func NewPrometheusMetrics(reg prometheus.Registerer) Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &promMetrics{
		hits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gocache", Name: "hits_total", Help: "Cache hits by level.",
		}, []string{"level", "def"}),
		misses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gocache", Name: "misses_total", Help: "Cache misses by level.",
		}, []string{"level", "def"}),
		sourceLoad: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "gocache", Name: "source_load_seconds", Help: "Loader latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"def"}),
		sfShared: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gocache", Name: "singleflight_shared_total", Help: "Singleflight merged calls.",
		}, []string{"def"}),
		oversize: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gocache", Name: "oversize_writes_total", Help: "Oversize value writes.",
		}, []string{"def"}),
		errs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gocache", Name: "errors_total", Help: "Errors by op.",
		}, []string{"op", "def"}),
		codec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "gocache", Name: "codec_seconds", Help: "Serialization/compression overhead.",
			Buckets: prometheus.ExponentialBuckets(0.00001, 10, 7),
		}, []string{"kind", "def"}),
	}
	reg.MustRegister(m.hits, m.misses, m.sourceLoad, m.sfShared, m.oversize, m.errs, m.codec)
	return m
}

func (m *promMetrics) Hit(level, def string)  { m.hits.WithLabelValues(level, def).Inc() }
func (m *promMetrics) Miss(level, def string) { m.misses.WithLabelValues(level, def).Inc() }
func (m *promMetrics) ObserveSourceLoad(def string, d time.Duration) {
	m.sourceLoad.WithLabelValues(def).Observe(d.Seconds())
}
func (m *promMetrics) SingleflightShared(def string, n int) {
	m.sfShared.WithLabelValues(def).Add(float64(n))
}
func (m *promMetrics) OversizeWrite(def string) { m.oversize.WithLabelValues(def).Inc() }
func (m *promMetrics) Error(op, def string)     { m.errs.WithLabelValues(op, def).Inc() }
func (m *promMetrics) ObserveCodec(kind, def string, d time.Duration) {
	m.codec.WithLabelValues(kind, def).Observe(d.Seconds())
}
