package store

import (
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/payload"
)

type Stream struct {
	store map[string]map[string]string
	mu    *sync.Mutex
}

func NewStream() *Stream {
	return &Stream{
		store: map[string]map[string]string{},
		mu:    &sync.Mutex{},
	}
}

func (s *Stream) XAdd(key string, kvPairs map[string]string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[key] = kvPairs

	return payload.GenerateBulkString([]byte(kvPairs["id"])), nil
}

func (s *Stream) Get(key string) (map[string]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	kvPair, exists := s.store[key]

	return kvPair, exists
}
