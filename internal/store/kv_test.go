package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKVStore_Get(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name  string
		s     KVStore
		args  args
		want  string
		want1 bool
	}{
		{
			name:  "when kvstore is empty",
			s:     map[string]string{},
			args:  args{key: "key-1"},
			want:  "",
			want1: false,
		},
		{
			name:  "when kvstore is empty",
			s:     map[string]string{"key-1": "val-1"},
			args:  args{key: "key-1"},
			want:  "val-1",
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.s.Get(tt.args.key)
			if got != tt.want {
				t.Errorf("KVStore.Get() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("KVStore.Get() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestKVStore_Set(t *testing.T) {
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name     string
		s        KVStore
		args     args
		expected KVStore
	}{
		{
			name:     "when settings value",
			s:        map[string]string{},
			args:     args{key: "key-1", value: "val-1"},
			expected: map[string]string{"key-1": "val-1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.Set(tt.args.key, tt.args.value)

			assert.Equal(t, tt.s, tt.expected)
		})
	}
}
