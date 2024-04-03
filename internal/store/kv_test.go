package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKVStore_Get(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name  string
		s     *KVStore
		args  args
		want  string
		want1 bool
	}{
		{
			name:  "when kvstore is empty",
			s:     NewKVStore(),
			args:  args{key: "key-1"},
			want:  "",
			want1: false,
		},
		{
			name:  "when kvstore is not empty",
			s:     &KVStore{mu: &sync.Mutex{}, store: map[string]*Value{"key-1": {str: "val-1", perm: true}}},
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
		s        *KVStore
		args     args
		expected map[string]*Value
	}{
		{
			name:     "when settings value",
			s:        NewKVStore(),
			args:     args{key: "key-1", value: "val-1"},
			expected: map[string]*Value{"key-1": {str: "val-1", perm: true}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.Set(tt.args.key, tt.args.value, 0)

			for _, v := range tt.s.store {
				v.exp = 0
			}

			assert.Equal(t, tt.expected, tt.s.store)
		})
	}
}
