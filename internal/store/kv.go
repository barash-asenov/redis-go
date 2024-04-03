package store

import (
	"sync"
	"time"
)

type KVStore struct {
	store map[string]*Value
	mu    *sync.Mutex
}

type Value struct {
	str  string
	exp  int64 // unix milliseconds
	perm bool  // is permanent
}

func (v *Value) IsExpired() bool {
	if time.Now().UnixMilli() > v.exp {
		return true
	}

	return false
}

func (v *Value) IsPermanent() bool {
	if v == nil {
		return false
	}

	return v.perm
}

func NewKVStore() *KVStore {
	return &KVStore{
		store: map[string]*Value{},
		mu:    &sync.Mutex{},
	}
}

func (s *KVStore) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, exists := s.store[key]

	if !exists {
		return "", false
	}

	if !val.IsPermanent() && val.IsExpired() {
		return "", false
	}

	return val.str, exists
}

func (s *KVStore) Set(key, value string, exp int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[key] = &Value{
		str:  value,
		exp:  time.Now().UnixMilli() + exp,
		perm: exp == 0,
	}
}
