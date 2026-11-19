package gocache

import (
	"errors"
	"testing"
)

// ---------- KeyBuilder ----------

func TestKeyBuilderBuild(t *testing.T) {
	b := NewKeyBuilder("t:{tenant}:u:{uid}").With("tenant", "acme").With("uid", "10001")
	got, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got != "t:acme:u:10001" {
		t.Fatalf("Build = %q", got)
	}
	parts := b.Parts()
	if parts["tenant"] != "acme" || parts["uid"] != "10001" || len(parts) != 2 {
		t.Fatalf("Parts = %v", parts)
	}
	// 副本隔离
	parts["tenant"] = "x"
	if b.Parts()["tenant"] != "acme" {
		t.Fatal("Parts must return a copy")
	}
}

func TestKeyBuilderMissingPlaceholder(t *testing.T) {
	_, err := NewKeyBuilder("t:{tenant}:u:{uid}").With("uid", "1").Build()
	if !errors.Is(err, ErrKeyBuild) {
		t.Fatalf("want ErrKeyBuild, got %v", err)
	}
	_, err = NewKeyBuilder("bad{").Build()
	if !errors.Is(err, ErrKeyBuild) {
		t.Fatalf("unbalanced: want ErrKeyBuild, got %v", err)
	}
}

// ---------- extractParts ----------

func TestExtractParts(t *testing.T) {
	parts, err := extractParts("t:{tenant}:u:{uid}", "t:acme:u:10001")
	if err != nil {
		t.Fatalf("extractParts: %v", err)
	}
	if parts["tenant"] != "acme" || parts["uid"] != "10001" {
		t.Fatalf("parts = %v", parts)
	}
	// 尾部占位符捕获到末尾
	parts, err = extractParts("q:{q}", "q:hello:world")
	if err != nil {
		t.Fatalf("extractParts: %v", err)
	}
	if parts["q"] != "hello:world" {
		t.Fatalf("tail placeholder = %q", parts["q"])
	}
	// 不匹配报错
	if _, err := extractParts("t:{tenant}:u:{uid}", "x:other"); !errors.Is(err, ErrKeyBuild) {
		t.Fatalf("mismatch: want ErrKeyBuild, got %v", err)
	}
}

// ---------- Tag 变换走查（docs/design.md §3.3）----------

func TestApplyTagWalkthrough(t *testing.T) {
	const logical = "biz:user_profile:v3:t:acme:u:10001"
	cases := []struct {
		name string
		op   TagOp
		want string
	}{
		{"wrap placeholder", TagOp{Kind: TagWrap, At: "acme"}, "biz:user_profile:v3:t:{acme}:u:10001"},
		{"wrap biz segment", TagOp{Kind: TagWrap, At: "t:acme:u:10001"}, "biz:user_profile:v3:{t:acme:u:10001}"},
		{"wrap whole", TagOp{Kind: TagWrap}, "{biz:user_profile:v3:t:acme:u:10001}"},
		{"prefix", TagOp{Kind: TagPrefix, Value: "dc1:"}, "dc1:biz:user_profile:v3:t:acme:u:10001"},
		{"suffix", TagOp{Kind: TagSuffix, Value: ":r2"}, "biz:user_profile:v3:t:acme:u:10001:r2"},
		{"insert", TagOp{Kind: TagInsert, Value: ":r2", At: ":t:"}, "biz:user_profile:v3:r2:t:acme:u:10001"},
		{"none", TagOp{Kind: TagNone}, logical},
		{"prefix with tag", TagOp{Kind: TagPrefix, Value: "{acme}"}, "{acme}biz:user_profile:v3:t:acme:u:10001"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := applyTag(tc.op, logical)
			if err != nil {
				t.Fatalf("applyTag: %v", err)
			}
			if got != tc.want {
				t.Fatalf("applyTag = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestApplyTagContractErrors(t *testing.T) {
	const logical = "biz:user_profile:v3:t:acme:u:10001"
	cases := []struct {
		name string
		op   TagOp
	}{
		{"wrap at not found", TagOp{Kind: TagWrap, At: "nope"}},
		{"wrap at with brace", TagOp{Kind: TagWrap, At: "ac}me"}},
		{"wrap at whitespace", TagOp{Kind: TagWrap, At: "a b"}},
		{"prefix nested braces", TagOp{Kind: TagPrefix, Value: "{a{b}}"}},
		{"prefix unbalanced", TagOp{Kind: TagPrefix, Value: "{a"}},
		{"prefix whitespace", TagOp{Kind: TagPrefix, Value: "a b"}},
		{"suffix whitespace", TagOp{Kind: TagSuffix, Value: "a\tb"}},
		{"insert at not found", TagOp{Kind: TagInsert, Value: ":r2", At: "zzz"}},
		{"insert at braces", TagOp{Kind: TagInsert, Value: "x", At: "a{b"}},
		{"unknown kind", TagOp{Kind: TagKind(99)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := applyTag(tc.op, logical); !errors.Is(err, ErrKeyBuild) {
				t.Fatalf("want ErrKeyBuild, got %v", err)
			}
		})
	}
}

func TestInverseTag(t *testing.T) {
	cases := []struct {
		name   string
		op     TagOp
		phys   string
		want   string
		wantOK bool
	}{
		{"none", TagOp{Kind: TagNone}, "a:b", "a:b", true},
		{"wrap", TagOp{Kind: TagWrap, At: "acme"}, "b:v:t:{acme}:u:1", "b:v:t:acme:u:1", true},
		{"wrap whole", TagOp{Kind: TagWrap}, "{b:v:t:acme}", "b:v:t:acme", true},
		{"prefix", TagOp{Kind: TagPrefix, Value: "dc1:"}, "dc1:b:v", "b:v", true},
		{"prefix mismatch", TagOp{Kind: TagPrefix, Value: "dc1:"}, "dc2:b:v", "", false},
		{"suffix", TagOp{Kind: TagSuffix, Value: ":r2"}, "b:v:r2", "b:v", true},
		{"insert", TagOp{Kind: TagInsert, Value: ":r2", At: ":t:"}, "b:v:r2:t:a", "b:v:t:a", true},
		{"nested wrap", TagOp{Kind: TagWrap}, "a{b{c}d", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := inverseTag(tc.op, tc.phys)
			if ok != tc.wantOK || got != tc.want {
				t.Fatalf("inverseTag(%q) = (%q,%v), want (%q,%v)", tc.phys, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestTagPresets(t *testing.T) {
	in := TagInput{LogicalKey: "ns:name:v1:t:acme", Parts: map[string]string{"tenant": "acme"}}
	// TagFromPlaceholder
	op, err := TagFromPlaceholder("tenant")(in)
	if err != nil {
		t.Fatalf("TagFromPlaceholder: %v", err)
	}
	got, err := applyTag(op, in.LogicalKey)
	if err != nil || got != "ns:name:v1:t:{acme}" {
		t.Fatalf("wrapped = %q err=%v", got, err)
	}
	// 占位符缺失报错
	if _, err := TagFromPlaceholder("nope")(in); err == nil {
		t.Fatal("missing placeholder must error")
	}
	// TagWhole
	op, err = TagWhole()(in)
	if err != nil {
		t.Fatalf("TagWhole: %v", err)
	}
	if got, _ = applyTag(op, in.LogicalKey); got != "{ns:name:v1:t:acme}" {
		t.Fatalf("whole = %q", got)
	}
}

// ---------- buildKey / toBizKey ----------

func TestBuildKeyAndToBizKey(t *testing.T) {
	Reset()
	defer Reset()
	h := MustDefine[userProfile]("kk", "objs", Definition[userProfile]{
		KeyTemplate: "t:{tenant}:u:{uid}",
		HashTag:     TagFromPlaceholder("tenant"),
	})
	logical, phys, err := h.core.buildKey(t.Context(), "t:acme:u:10001")
	if err != nil {
		t.Fatalf("buildKey: %v", err)
	}
	if logical != "kk:objs:v0:t:acme:u:10001" {
		t.Fatalf("logical = %q", logical)
	}
	if phys != "kk:objs:v0:t:{acme}:u:10001" {
		t.Fatalf("phys = %q", phys)
	}
	biz, ok := h.core.toBizKey(phys)
	if !ok || biz != "t:acme:u:10001" {
		t.Fatalf("toBizKey = (%q,%v)", biz, ok)
	}
	// 非本定义 key 不可逆
	if _, ok = h.core.toBizKey("other:def:v0:x"); ok {
		t.Fatal("foreign key must not inverse-map")
	}
}
