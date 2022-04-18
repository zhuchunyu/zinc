package storage

import (
	"fmt"
	"sync"
)

const (
	DBEngineBadger = "badger"
	DBEnginePebble = "pebble"
)

type Storager interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Gets(keys []string) (map[string][]byte, error)
	Delete(key string) error
	Bulk(update bool) StorageBulker
	Close()
}

type StorageBulker interface {
	Set(key string, value []byte) error
	Delete(key string) error
	Commit() error
}

type Storage struct {
	db   map[string]Storager
	lock sync.RWMutex
}

var Cli *Storage

func init() {
	Cli = new(Storage)
	Cli.db = make(map[string]Storager, 32)
}

func (t *Storage) GetIndex(indexName string, dbEngine string) (Storager, error) {
	t.lock.Lock()
	index, ok := t.db[indexName]
	t.lock.Unlock()
	if ok {
		return index, nil
	}

	var err error
	switch dbEngine {
	case DBEngineBadger:
		index, err = NewBadger(indexName)
	case DBEnginePebble:
		index, err = NewPebble(indexName)
	default:
		index, err = NewBadger(indexName)
	}
	if err != nil {
		return nil, fmt.Errorf("storage.GetIndex: create index err %s", err.Error())
	}

	t.lock.Lock()
	t.db[indexName] = index
	t.lock.Unlock()
	return index, nil
}

func (t *Storage) DeleteIndex(indexName string) {
	t.lock.Lock()
	index, ok := t.db[indexName]
	delete(t.db, indexName)
	t.lock.Unlock()
	if ok {
		index.Close()
	}
}

func (t *Storage) Close() {
	for _, index := range t.db {
		index.Close()
	}
}
