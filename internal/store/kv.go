package store

import "sync"

type KVStore struct {
	store map[string]string
	mu    *sync.Mutex
}

func NewKVStore() KVStore {
	return KVStore{
		store: map[string]string{},
		mu:    &sync.Mutex{},
	}
}

func (s KVStore) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, exists := s.store[key]

	return val, exists
}

func (s KVStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[key] = value
}
