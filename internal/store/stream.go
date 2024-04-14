package store

import (
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/structures/stream"
)

type Stream struct {
	store map[string]*stream.NumericTrie
	mu    *sync.Mutex
}

func NewStream() *Stream {
	return &Stream{
		store: map[string]*stream.NumericTrie{},
		mu:    &sync.Mutex{},
	}
}

func (s *Stream) Exists(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.store[key]

	return exists
}

func (s *Stream) XAdd(key string, values map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.store[key]
	if !exists {
		s.store[key] = stream.NewNumericTrie(time.Now)
	}

	id, err := s.store[key].Insert(values["id"], values)
	if err != nil {
		return "", err
	}

	return id, nil
}
