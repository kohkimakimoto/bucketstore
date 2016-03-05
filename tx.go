package bucketstore

import (
	"fmt"
	"github.com/kohkimakimoto/bucketstore/v/bolt"
	"io"
	"os"
)

type Tx struct {
	db         *DB
	internalTx *bolt.Tx
}

func newTx(ds *DB, t *bolt.Tx) *Tx {
	return &Tx{
		db:         ds,
		internalTx: t,
	}
}

func (tx *Tx) InternalTx() *bolt.Tx {
	return tx.internalTx
}

func (tx *Tx) Rollback() error {
	return tx.internalTx.Rollback()
}

func (tx *Tx) Commit() error {
	return tx.internalTx.Commit()
}

func (tx *Tx) DeleteBucket(name string) error {
	if err := tx.bData().DeleteBucket([]byte(name)); err != nil {
		return err
	}

	if err := tx.bBucketsList().Delete([]byte(name)); err != nil {
		return err
	}

	if err := tx.bIndex().DeleteBucket([]byte(name)); err != nil {
		return err
	}

	return nil
}

func (tx *Tx) BucketNames(fn func(name string) error) error {
	return tx.bBucketsList().ForEach(func(k, v []byte) error {
		return fn(string(k))
	})
}

func (tx *Tx) Bucket(name string) (*Bucket, error) {
	baseBucket, err := tx.baseBucket([]byte(name))
	if err != nil {
		return nil, err
	}

	if baseBucket == nil {
		return nil, nil
	}

	return newBucket(name, tx.db, baseBucket), nil
}

func (tx *Tx) CreateBucket(name string) (*Bucket, error) {
	baseBucket, err := tx.createBaseBucket([]byte(name))
	if err != nil {
		return nil, err
	}

	return newBucket(name, tx.db, baseBucket), nil
}

func (tx *Tx) CreateBucketIfNotExists(name string) (*Bucket, error) {
	baseBucket, err := tx.createBaseBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}

	return newBucket(name, tx.db, baseBucket), nil
}

func (tx *Tx) WriteTo(w io.Writer) (n int64, err error) {
	return tx.internalTx.WriteTo(w)
}

func (tx *Tx) CopyFile(path string, mode os.FileMode) error {
	return tx.internalTx.CopyFile(path, mode)
}

func (tx *Tx) baseBucket(name []byte) (*BaseBucket, error) {
	data := tx.bData().Bucket(name)
	if data == nil {
		return nil, nil
	}

	index := tx.bIndex().Bucket(name)
	if index == nil {
		return nil, fmt.Errorf("the data structure was broken! not found the index bucket of %s.", string(name))
	}

	return newBaseBucket(name, tx, data, index), nil
}

func (tx *Tx) createBaseBucket(name []byte) (*BaseBucket, error) {
	data, err := tx.bData().CreateBucket(name)
	if err != nil {
		return nil, err
	}

	index, err := tx.bIndex().CreateBucket(name)
	if err != nil {
		return nil, err
	}

	err = tx.bBucketsList().Put(name, []byte("e"))
	if err != nil {
		return nil, err
	}

	return newBaseBucket(name, tx, data, index), nil
}

func (tx *Tx) createBaseBucketIfNotExists(name []byte) (*BaseBucket, error) {
	data, err := tx.bData().CreateBucketIfNotExists(name)
	if err != nil {
		return nil, err
	}

	index, err := tx.bIndex().CreateBucketIfNotExists(name)
	if err != nil {
		return nil, err
	}

	err = tx.bBucketsList().Put(name, []byte("e"))
	if err != nil {
		return nil, err
	}

	return newBaseBucket(name, tx, data, index), nil
}

func (tx *Tx) bData() *bolt.Bucket {
	return tx.internalTx.Bucket(bData)
}

func (tx *Tx) bBucketsList() *bolt.Bucket {
	return tx.internalTx.Bucket(bBucketsList)
}

func (tx *Tx) bIndex() *bolt.Bucket {
	return tx.internalTx.Bucket(bIndex)
}
