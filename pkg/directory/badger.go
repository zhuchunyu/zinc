package directory

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"runtime"
	"strconv"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/rs/zerolog/log"
)

// GetBadgerConfig returns a bluge config that will store index data in badger
// path: the badger storage path
// indexName: the name of the index to use. It will be an badger prefix (folder)
func GetBadgerConfig(path string, indexName string) bluge.Config {
	return bluge.DefaultConfigWithDirectory(func() index.Directory {
		return NewBadgerDirectory(path, indexName)
	})
}

type BadgerDirectory struct {
	RootPath  string
	IndexName string
	Client    *badger.DB
}

func openBadgerDB(rootPath, prefix string, readOnly bool) (*badger.DB, error) {
	opt := badger.DefaultOptions(path.Join(rootPath, prefix))
	opt.NumGoroutines = runtime.NumGoroutine() * 8
	opt.MemTableSize = 32 << 20
	opt.Compression = options.ZSTD
	opt.ZSTDCompressionLevel = 3
	opt.BlockSize = 1024 * 128
	opt.MetricsEnabled = false
	// opt.Logger = nil
	opt.ReadOnly = readOnly
	return badger.Open(opt)
}

// NewBadgerDirectory creates a new BadgerDirectory instance which can be used to create badger backed indexes
func NewBadgerDirectory(rootPath, indexName string) index.Directory {
	return &BadgerDirectory{
		RootPath:  rootPath,
		IndexName: indexName,
	}
}

func (s *BadgerDirectory) Setup(readOnly bool) error {
	var err error
	s.Client, err = openBadgerDB(s.RootPath, s.IndexName, readOnly)
	if err != nil {
		return fmt.Errorf("BadgerDirectory.Setup: %v", err)
	}
	_badgerOpenedDirectory = append(_badgerOpenedDirectory, s.Client)
	return nil
}

func (s *BadgerDirectory) fileName(kind string, id uint64) string {
	return fmt.Sprintf("%s:%012x", kind, id)
}

// List the ids of all the items of the specified kind
// Items are returned in descending order by id
func (s *BadgerDirectory) List(kind string) ([]uint64, error) {
	var itemList []uint64
	err := s.Client.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(kind)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			parsedID, err := strconv.ParseUint(string(key[len(kind)+1:]), 16, 64)
			if err != nil {
				return fmt.Errorf("BadgerDirectory.List: failed to parse filename: %s, %s", key, err.Error())
			}
			itemList = append(itemList, parsedID)
		}
		return nil
	})
	return itemList, err
}

// Load the specified item
// Item data is accessible via the returned *segment.Data structure
// A io.Closer is returned, which must be called to release
// resources held by this open item.
// NOTE: care must be taken to handle a possible nil io.Closer
func (s *BadgerDirectory) Load(kind string, id uint64) (*segment.Data, io.Closer, error) {
	var valCopy []byte
	key := s.fileName(kind, id)
	err := s.Client.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		valCopy, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, nil, fmt.Errorf("BadgerDirectory.Load: failed to get filename: %s, %s", key, err.Error())

	}
	return segment.NewDataBytes(valCopy), nil, nil
}

// Persist a new item with data from the provided WriterTo
// Implementations should monitor the closeCh and return with error
// in the event it is closed before completion.
func (s *BadgerDirectory) Persist(kind string, id uint64, w index.WriterTo, closeCh chan struct{}) error {
	var buf bytes.Buffer
	_, err := w.WriteTo(&buf, closeCh)
	if err != nil {
		return fmt.Errorf("BadgerDirectory.Persist: failed to write object to buffer: %s", err.Error())
	}

	key := s.fileName(kind, id)
	err = s.Client.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), buf.Bytes())
	})
	if err != nil {
		return fmt.Errorf("BadgerDirectory.Persist: failed to set to storage: %s, %s", key, err.Error())
	}

	return nil
}

// Remove the specified item
func (s *BadgerDirectory) Remove(kind string, id uint64) error {
	key := s.fileName(kind, id)
	err := s.Client.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	if err != nil {
		return fmt.Errorf("BadgerDirectory.Remove: failed to delete from storage: %s, %s", key, err.Error())
	}
	return nil
}

// Stats returns total number of items and their cumulative size
func (s *BadgerDirectory) Stats() (numItems uint64, numBytes uint64) {
	objectCount := uint64(0)
	sizeOfObjects := uint64(0)

	err := s.Client.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			objectCount++
			_ = item.Value(func(v []byte) error {
				sizeOfObjects += uint64(len(v))
				return nil
			})
		}
		return nil
	})
	if err != nil {
		log.Error().Msgf("BadgerDirectory.Stats: failed to stats from storage: %s", err.Error())
	}

	return objectCount, sizeOfObjects
}

// Sync ensures directory metadata itself has been committed
func (s *BadgerDirectory) Sync() error {
	log.Info().Msgf("BadgerDirectory.Sync")
	return s.Client.Sync()
}

// Lock ensures this process has exclusive access to write in this directory
func (s *BadgerDirectory) Lock() error {
	return nil
}

// Unlock releases the lock held on this directory
func (s *BadgerDirectory) Unlock() error {
	return nil
}

// Close closes a DB. It's crucial to call it to ensure all the pending updates make their way to
// disk. Calling DB.Close() multiple times would still only close the DB once.
func (s *BadgerDirectory) Close() error {
	return s.Client.Close()
}

var _badgerOpenedDirectory = []*badger.DB{}

func BadgerDirectoryClose() {
	for _, db := range _badgerOpenedDirectory {
		if db != nil {
			_ = db.Close()
		}
	}
}
