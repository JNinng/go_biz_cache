package codec

import msgpack "github.com/vmihailenco/msgpack/v5"

type msgpackSerializer struct{}

func (msgpackSerializer) Marshal(v any) ([]byte, error)      { return msgpack.Marshal(v) }
func (msgpackSerializer) Unmarshal(data []byte, v any) error { return msgpack.Unmarshal(data, v) }
func (msgpackSerializer) Name() string                       { return "msgpack" }

// MsgPack MessagePack 序列化器（比 JSON 更紧凑、更快）。
var MsgPack Serializer = msgpackSerializer{}
