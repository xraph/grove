package kv_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/kv"
)

func TestCommandName(t *testing.T) {
	tests := []struct {
		op   hook.Operation
		want string
	}{
		{op: kv.OpGet, want: "GET"},
		{op: kv.OpSet, want: "SET"},
		{op: kv.OpDelete, want: "DEL"},
		{op: kv.OpExists, want: "EXISTS"},
		{op: kv.OpMGet, want: "MGET"},
		{op: kv.OpMSet, want: "MSET"},
		{op: kv.OpTTL, want: "TTL"},
		{op: kv.OpExpire, want: "EXPIRE"},
		{op: kv.OpScan, want: "SCAN"},
		{op: hook.Operation(999), want: "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, kv.CommandName(tt.op))
		})
	}
}
