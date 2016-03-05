package bucketstore

import (
	"fmt"
	"github.com/kohkimakimoto/bucketstore/v/bolt"
	"os"
)

var (
	// system bucket names.

	// bData is a data space bucket to store data.
	bData = []byte("d")
	// bIndex os a index space bucket to store index.
	bIndex = []byte("i")
	// bBucketsList is a bucket to store buckets list.
	bBucketsList = []byte("b")
)

type DB struct {
	// conn is the underlying handle to the Datastore.
	conn    *bolt.DB
	options *Options
}

func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	if options == nil {
		options = NewOptions()
	}

	if options.ReadOnly {
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("read only mode requires database file existence. :%v", err)
		}
	}

	handle, err := bolt.Open(path, mode, options.Options)
	if err != nil {
		return nil, err
	}

	return open(handle, options)
}

func open(handle *bolt.DB, options *Options) (*DB, error) {
	// Create the new store
	db := &DB{
		conn:    handle,
		options: options,
	}

	if !db.conn.IsReadOnly() {
		// create system buckets
		tx, err := db.conn.Begin(true)
		if err != nil {
			return nil, err
		}
		defer func() {
			if tx.DB() != nil {
				tx.Rollback()
			}
		}()

		if _, err := tx.CreateBucketIfNotExists(bData); err != nil {
			return nil, err
		}
		if _, err := tx.CreateBucketIfNotExists(bIndex); err != nil {
			return nil, err
		}
		if _, err := tx.CreateBucketIfNotExists(bBucketsList); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (db *DB) Path() string {
	return db.conn.Path()
}

// GoString returns the Go string representation of the database.
func (db *DB) GoString() string {
	return fmt.Sprintf("boltstore.DB{path:%q}", db.conn.Path())
}

// String returns the string representation of the database.
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.conn.Path())
}

func (db *DB) Conn() *bolt.DB {
	return db.conn
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) Begin(writable bool) (*Tx, error) {
	t, err := db.conn.Begin(writable)
	if err != nil {
		return nil, err
	}

	return newTx(db, t), nil
}

func (db *DB) View(fn func(*Tx) error) error {
	return db.conn.View(func(t *bolt.Tx) error {
		return fn(newTx(db, t))
	})
}

func (db *DB) Update(fn func(*Tx) error) error {
	return db.conn.Update(func(t *bolt.Tx) error {
		return fn(newTx(db, t))
	})
}

func (db *DB) Batch(fn func(*Tx) error) error {
	return db.conn.Batch(func(t *bolt.Tx) error {
		return fn(newTx(db, t))
	})
}

func (db *DB) Bucket(name string) *Bucket {
	return newBucket(name, db, nil)
}

func (db *DB) DeleteBucket(name string) error {
	return db.Update(func(tx *Tx) error {
		return tx.DeleteBucket(name)
	})
}
