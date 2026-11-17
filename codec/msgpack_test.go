package codec

import (
	"bytes"
	"reflect"
	"testing"
)

func TestMsgPackRoundTrip(t *testing.T) {
	type payload struct {
		ID    string
		Tags  []string
		Score float64
	}
	want := payload{ID: "u1", Tags: []string{"a", "b"}, Score: 3.14}
	data, err := MsgPack.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if bytes.Equal(data, nil) || len(data) == 0 {
		t.Fatal("empty marshal output")
	}
	var got payload
	if err := MsgPack.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("roundtrip = %+v, want %+v", got, want)
	}
	if MsgPack.Name() != "msgpack" {
		t.Fatalf("Name = %q", MsgPack.Name())
	}
}
