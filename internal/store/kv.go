package store

type KVStore map[string]string

func NewKVStore() KVStore {
	return map[string]string{}
}

func (s KVStore) Get(key string) (string, bool) {
	val, exists := s[key]

	return val, exists
}

func (s KVStore) Set(key, value string) {
	s[key] = value
}
