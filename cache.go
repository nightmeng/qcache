package qcache

import (
	"errors"
	"github.com/dgraph-io/badger"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
)

type Cache interface {
	Put(key []byte, record *Record) error
	Get(prefix []byte) (key []byte, record *Record, err error)
	Del(key []byte, record *Record) error
	Close() error
}

type cache struct {
	db *badger.DB
}

func NewCache(file string) (Cache, error) {
	opts := badger.DefaultOptions
	opts.Dir = file
	opts.ValueDir = file
	opts.ValueLogFileSize = 1 << 20

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &cache{
		db: db,
	}, nil
}

func (c *cache) Put(key []byte, record *Record) error {
	for {
		txn := c.db.NewTransaction(true)

		txn.Set(makeKey([]byte("c"), key, record.Time), record.Data)

		if err := txn.Commit(nil); err != badger.ErrConflict {
			return err
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (c *cache) Get(prefix []byte) (key []byte, record *Record, err error) {
	txn := c.db.NewTransaction(false)

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	prefix = append([]byte("c"), prefix...)
	it.Seek(prefix)

	if it.ValidForPrefix(prefix) {
		key := it.Item().Key()
		data, err := it.Item().Value()
		if err != nil {
			return nil, nil, err
		}

		return extractKey(key), &Record{
			Data: data,
			Time: extractTime(key),
		}, nil
	}

	return nil, nil, ErrNotFound
}

func (c *cache) Del(key []byte, record *Record) error {
	for {
		txn := c.db.NewTransaction(true)

		txn.Delete(makeKey([]byte("c"), key, record.Time))

		if err := txn.Commit(nil); err != badger.ErrConflict {
			return err
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (c *cache) Close() error {
	return c.db.Close()
}
