// Package gocache 是强类型业务 Redis 缓存库（见 docs/design.md）。
//
// 定位：单库接入、零基础设施要求、聚焦业务缓存日常需求的轻量方案。
//
//	var userCache = gocache.MustDefine[gocache.UserProfile]("biz", "user_profile",
//		gocache.Definition[gocache.UserProfile]{
//			KeyTemplate:   "t:{tenant}:u:{uid}",
//			HashTag:       gocache.TagFromPlaceholder("tenant"),
//			TTL:           30 * time.Minute,
//			TTLJitter:     0.1,
//			Serializer:    gocache.JSON,
//			NegativeCache: true,
//			L1:            gocache.L1Options{Enabled: true, Size: 4096},
//			L2:            gocache.L2Options{Enabled: true},
//			Loader: func(ctx context.Context, keys []string) (map[string]gocache.UserProfile, error) {
//				// 业务注入源数据访问
//			},
//		})
//
// 全局注入 Redis 客户端提供者 / 事件总线 / 指标 / 日志：
//
//	gocache.UseClientProvider(func(ctx) (gocache.RedisCmdable, error) { return clusterClient, nil })
//	gocache.UseEventBus(gocache.NewRedisPubSub(provider)) // 跨实例 L1 一致性
package gocache
