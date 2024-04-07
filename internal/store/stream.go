package store

import (
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/structures/stream"
)

type Stream struct {
	store *stream.NumericTrie
	mu    *sync.Mutex
}

func NewStream() *Stream {
	return &Stream{
		store: &stream.NumericTrie{Root: &stream.Node{}},
		mu:    &sync.Mutex{},
	}
}

func (s *Stream) XAdd(key string, values map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.store.Insert(key, values)
	if err != nil {
		return "", err
	}

	return key, nil
}
