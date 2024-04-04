package commands

import (
	"github.com/codecrafters-io/redis-starter-go/internal/payload"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

type TypeCommand struct {
	kvStore     *store.KVStore
	streamStore *store.Stream
}

func NewTypeCommand(kvStore *store.KVStore, streamStore *store.Stream) *TypeCommand {
	return &TypeCommand{
		kvStore:     kvStore,
		streamStore: streamStore,
	}
}

func (c *TypeCommand) GetType(key string) []byte {
	if _, exists := c.kvStore.Get(key); exists {
		return payload.GenerateBasicString([]byte("string"))
	}

	if _, exists := c.streamStore.Get(key); exists {
		return payload.GenerateBasicString([]byte("stream"))
	}

	return payload.GenerateBasicString([]byte("none"))
}
