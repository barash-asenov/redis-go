package commands

import (
	"github.com/codecrafters-io/redis-starter-go/internal/payload"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

type TypeCommand struct {
	kvStore *store.KVStore
}

func NewTypeCommand(kvStore *store.KVStore) *TypeCommand {
	return &TypeCommand{
		kvStore: kvStore,
	}
}

func (c *TypeCommand) GetType(key string) []byte {
	if _, exists := c.kvStore.Get(key); exists {
		return payload.GenerateBasicString([]byte("string"))
	}

	return payload.GenerateBasicString([]byte("none"))
}
