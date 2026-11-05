package errors

import "testing"

func TestErrorMessageFormatting(t *testing.T) {
	tests := []struct {
		name   string
		format string
		args   []interface{}
		want   string
	}{
		{name: "plain message", format: "The number of ready pods is too less", want: "The number of ready pods is too less"},
		{name: "empty message", format: "", want: ""},
		{name: "literal percent without arguments", format: "progress 100%: unavailable %s", want: "progress 100%: unavailable %s"},
		{name: "formatted arguments", format: "ready=%d desired=%d", args: []interface{}{0, 4}, want: "ready=0 desired=4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := New(CODE_CLUSTER_FAILE, tt.format, tt.args...)
			if got.Code != CODE_CLUSTER_FAILE || got.Msg != tt.want {
				t.Fatalf("New: code=%d message=%q, want code=%d message=%q", got.Code, got.Msg, CODE_CLUSTER_FAILE, tt.want)
			}
			got = NewErrfMsgf(tt.format, tt.args...)
			if got.Code != CODE_ERR_UNKNOW || got.Msg != tt.want {
				t.Fatalf("NewErrfMsgf: code=%d message=%q, want code=%d message=%q", got.Code, got.Msg, CODE_ERR_UNKNOW, tt.want)
			}
		})
	}
}
