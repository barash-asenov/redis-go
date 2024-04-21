package store

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/payload"
	"github.com/codecrafters-io/redis-starter-go/internal/streamfn"
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

func (s *Stream) XAdd(key string, givenId string, values []string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.store[key]
	if !exists {
		s.store[key] = stream.NewNumericTrie(time.Now)
	}

	insertedId, err := s.store[key].Insert(givenId, values)
	if err != nil {
		return "", err
	}

	return insertedId, nil
}

func (s *Stream) XRange(key, begin, end string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	values, err := s.xRange(key, begin, end)
	if err != nil {
		return "", fmt.Errorf("Failed to search by range: %w", err)
	}

	result, err := payload.GenerateNestedListToString(values)
	if err != nil {
		return "", fmt.Errorf("Failed to convert to Nested Redis List: %w", err)
	}

	return result, nil
}

func (s *Stream) xRange(key, begin, end string) ([]interface{}, error) {
	trie, exists := s.store[key]
	if !exists {
		return nil, fmt.Errorf("Key doesn't exist")
	}

	if begin == "-" {
		begin = "0-1"
	}

	if end == "+" {
		end = strconv.Itoa(math.MaxInt64)
	}

	foundValues, err := trie.Range(begin, end)
	if err != nil {
		return nil, fmt.Errorf("Failed to get range: %w", err)
	}

	sort.Slice(foundValues, func(i, j int) bool {
		return foundValues[i].ID < foundValues[j].ID
	})

	values := make([]interface{}, 0)

	for _, foundValue := range foundValues {
		values = append(values, foundValue.ToInterface())
	}

	return values, nil
}

func (s *Stream) XRead(keys []string, ids []string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make([]interface{}, 0)

	for i, key := range keys {
		if len(ids) < i+1 {
			continue
		}

		id, err := streamfn.IncrementID(ids[i])
		if err != nil {
			return "", fmt.Errorf("Failed to increment ID: %w", err)
		}

		foundValues, err := s.xRange(key, id, "+")
		if err != nil {
			return "", fmt.Errorf("Failed to find values by range: %w", err)
		}

		res = append(res, []interface{}{key, foundValues})
	}

	result, err := payload.GenerateNestedListToString(res)
	if err != nil {
		return "", fmt.Errorf("Failed to convert to Nested Redis List: %w", err)
	}

	return result, nil
}
