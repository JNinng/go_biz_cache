// Package gocache 是强类型业务 Redis 缓存库（设计文档 docs/design.md）。
//
// 定位：单库接入、零基础设施要求、聚焦业务缓存日常需求的轻量方案。
//
// 用法：声明式定义缓存（MustDefine / MustDefineHash / MustDefineSet /
// MustDefineList / MustDefineZSet），全局注入基础设施（UseClientProvider /
// UseEventBus / UseMetrics / UseLogger）；完整定义示例见 docs/design.md §3.1，
// 跨实例 L1 一致性经 UseEventBus 注入事件总线（event 包适配器）。
package gocache
