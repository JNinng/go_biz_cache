package bigvalue

import "errors"

// Splitter 自定义内容切分器（仅写路径调用，值已就绪；读路径只按 ShardCount 静态推算 MGET）。
//
// 硬性契约：
//  1. 纯函数确定性：同入参必得同输出（禁随机/时间/外部状态），写读两侧靠此对称；
//  2. 分片数量恒定：返回长度必须等于 shardCount，空分片允许；
//  3. 无损可还原：按序拼接全部分片必须还原原始值；
//  4. 物理 key 命名不外泄：库负责 ":s{i}" 命名与 hash-tag，切分器只决定各片内容。
type Splitter func(logicalKey string, value []byte, shardCount int) ([][]byte, error)

// SplitEven 默认固定均匀切分：chunkSize = ceil(len / shardCount)，
// 第 i 片为 [i*chunkSize, min((i+1)*chunkSize, len))，写读两侧同一算法，确定性可推算。
func SplitEven(logicalKey string, value []byte, shardCount int) ([][]byte, error) {
	if shardCount < 1 {
		return nil, errors.New("bigvalue: shard count must be positive")
	}
	out := make([][]byte, 0, shardCount)
	chunk := (len(value) + shardCount - 1) / shardCount
	if chunk > 0 {
		for i := 0; i < len(value); i += chunk {
			end := i + chunk
			if end > len(value) {
				end = len(value)
			}
			out = append(out, value[i:end])
		}
	}
	for len(out) < shardCount {
		out = append(out, nil)
	}
	return out, nil
}
