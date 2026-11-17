package gocache

import (
	"errors"

	"github.com/jninng/go_biz_cache/l2"
)

var (
	// ErrNotFound key/field 未命中（含负缓存命中，调用方按缺失处理）。
	ErrNotFound = l2.ErrNotFound
	// ErrInvalidDefinition 定义注册校验失败（非法参数 / 结构不匹配 / 重复注册）。
	ErrInvalidDefinition = errors.New("gocache: invalid definition")
	// ErrKeyBuild key 构建失败（占位符缺失 / Tag 契约违反），不静默降级。
	ErrKeyBuild = errors.New("gocache: key build")
	// ErrContract 运行时契约断言失败（切分器/哈希器行为不符等），不静默截断。
	ErrContract = errors.New("gocache: contract violation")
)
