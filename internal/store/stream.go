package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/payload"
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

func (s *Stream) XRead(key, begin, end string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	trie, exists := s.store[key]
	if !exists {
		return "", fmt.Errorf("Key doesn't exist")
	}

	foundValues, err := trie.Range(begin, end)
	if err != nil {
		return "", fmt.Errorf("Failed to get range: %w", err)
	}

	values := make([]interface{}, 0)

	for _, foundValue := range foundValues {
		values = append(values, foundValue.ToInterface())
	}

	result, err := payload.GenerateNestedListToString(values)
	if err != nil {
		return "", fmt.Errorf("Failed to convert to Nested Redis List: %w", err)
	}

	return result, nil
}
