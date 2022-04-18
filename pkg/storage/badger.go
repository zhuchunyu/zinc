package storage

import (
	"fmt"
	"path"
	"runtime"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"

	"github.com/zinclabs/zinc/pkg/zutils"
)

type badgerStorage struct {
	db *badger.DB
}

type badgerStorageBulk struct {
	index *badgerStorage
	txn   *badger.Txn
}

func NewBadger(indexName string) (Storager, error) {
	db, err := openBadgerDB(indexName)
	if err != nil {
		return nil, fmt.Errorf("open badger db err %s", err.Error())
	}
	return &badgerStorage{db: db}, nil
}

func openBadgerDB(indexName string) (*badger.DB, error) {
	dataPath := zutils.GetEnv("ZINC_DATA_PATH", "./data")
	opt := badger.DefaultOptions(path.Join(dataPath, "_storage", indexName))
	opt.NumGoroutines = runtime.NumGoroutine() * 8
	opt.MemTableSize = 32 << 20
	opt.Compression = options.ZSTD
	opt.ZSTDCompressionLevel = 3
	opt.BlockSize = 1024 * 128
	opt.MetricsEnabled = false
	opt.Logger = Logger
	return badger.Open(opt)
}

func (t *badgerStorage) Set(key string, value []byte) error {
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
	if err != nil {
		return fmt.Errorf("storage.badger.Set: key[%s] err %s", key, err.Error())
	}
	return nil
}

func (t *badgerStorage) Get(key string) ([]byte, error) {
	var valCopy []byte
	err := t.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		valCopy, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("storage.badger.Get: key[%s] err %s", key, err.Error())
	}
	return valCopy, nil
}

func (t *badgerStorage) Gets(keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(keys))
	err := t.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result[key] = valCopy
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage.badger.Gets: err %s", err.Error())
	}
	return result, nil
}

func (t *badgerStorage) Delete(key string) error {
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	if err != nil {
		return fmt.Errorf("storage.badger.Delete: key[%s] err %s", key, err.Error())
	}
	return nil
}

func (t *badgerStorage) Bulk(update bool) StorageBulker {
	return &badgerStorageBulk{index: t, txn: t.db.NewTransaction(update)}
}

func (t *badgerStorage) Close() {
	t.db.Close()
}

func (t *badgerStorageBulk) Set(key string, value []byte) error {
	err := t.txn.Set([]byte(key), value)
	if err == nil {
		return nil
	}
	if err == badger.ErrTxnTooBig {
		if err = t.txn.Commit(); err != nil {
			return fmt.Errorf("storage.badger.bulk.Set: transaction.Commit err %s", err.Error())
		}
		t.txn = t.index.db.NewTransaction(true)
		if err := t.txn.Set([]byte(key), value); err != nil {
			return fmt.Errorf("storage.badger.bulk.Set: key[%s] err %s", key, err.Error())
		}
	}
	return fmt.Errorf("storage.badger.bulk.Set: key[%s] err %s", key, err.Error())
}

func (t *badgerStorageBulk) Delete(key string) error {
	if err := t.txn.Delete([]byte(key)); err != nil {
		return fmt.Errorf("storage.badger.bulk.Delete: key[%s] err %s", key, err.Error())
	}
	return nil
}

func (t *badgerStorageBulk) Commit() error {
	if err := t.txn.Commit(); err != nil {
		return fmt.Errorf("storage.badger.bulk.Commit: err %s", err.Error())
	}
	return nil
}
